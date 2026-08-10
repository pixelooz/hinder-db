use std::{
    collections::{HashMap, HashSet, hash_map::Entry},
    fmt::Debug,
    sync::Arc,
};

use parking_lot::{Mutex, RwLock};

use crate::{
    error::Error,
    storage::{
        lru::LruReplacer,
        page::{BTreeNode, DiskManager, Page, PageId},
        wal::{WalManager, WalRecord},
    },
};

/// A thread-safe reference to a cached page. The `Arc` provides life cycle tracking
/// (pinning), and the `RwLock` provides per page "latching".
pub type Frame = Arc<RwLock<BTreeNode>>;

/// Tracks active transactions to prevent duplicate Undo logging and facilitate
/// Commit/Abort.
#[derive(Debug, Default)]
struct TxnState {
    /// Maps a modified PageId to its absolute byte offset in the Wal file.
    /// Used for O(1) retrieval of the Undo record during a runtime abort.
    undo_logged: HashMap<PageId, u64>,

    /// Pages modified by this transaction that need a Redo log on commit.
    dirty_pages: HashSet<PageId>,
}

/// Handles memory caching, page fetching, and eviction.
///
/// # Note to add in my Notes later
/// Since we want to access the BufferPool concurrently it needs to be `&self` rather
/// than `&mut self`, because of which previously while the borrow checker could stop
/// us from using the buffer pool simultaneously, now it won't, and two threads can
/// simultaneously perform mutations (which we are allowing, for background cleanup and
/// check-pointing and eventually mvcc after I learn how to integrate that).
///
/// To do that we are giving the replacer and page_table their own separate locks
/// because otherwise the entire buffer pool will have to be locked when the
/// background flusher wants to STEAL a page to write to disk - cause if at that
/// time any queries are made, the ENTIRE BufferPool will found locked and the
/// query thread will have to wait for the entire duration even though all it
/// needs to hit (assuming) is the page table and replacer if the page is cached.
///
/// If the page is not cached and have to be fetched, that means nothing else
/// (any other process) is accessing the page as well, meaning we don't need
/// any read/write locks for now, since our model is single node only, and only
/// the background flusher is the thread we need to handle correctly.
///
/// For ensuring pages don't get evicted mid flush, flush_page acquires its own Arc
/// clone separately to keep strong_count >= 2, as getting it from the page table
/// doesn't increment strong_count so it could get evicted mid background flush
/// (as nothing protected it before, since, they were behind `&mut`), and then, if a
/// foreground thread at that very second asked for the page again, its possible the
/// flusher may still be writing that page while we fetch it from disk - getting a torn
/// read. Also page_table is needed by flush_page only to get a frame.clone() so we
/// only hold the read lock on the page_table just for the lookup.
#[derive(Debug)]
pub struct BufferPool {
    replacer: Mutex<LruReplacer>,
    disk_manager: DiskManager,
    page_table: RwLock<HashMap<PageId, Frame>>,
    capacity: usize,
    wal_manager: Arc<Mutex<WalManager>>,

    /// Tracks active transactions for Undo/Redo generation.
    active_txns: Mutex<HashMap<u64, TxnState>>,
}

impl BufferPool {
    /// Creates a new BufferPool with a specified capacity.
    pub fn new(
        disk_manager: DiskManager,
        capacity: usize,
        wal_manager: Arc<Mutex<WalManager>>,
    ) -> Self {
        Self {
            disk_manager,
            replacer: Mutex::new(LruReplacer::new(capacity)),
            page_table: RwLock::new(HashMap::with_capacity(capacity)),
            capacity,
            wal_manager,
            active_txns: Mutex::new(HashMap::new()),
        }
    }

    /// Returns true if there are no active transactions, meaning it is safe
    /// to truncate the Wal.
    pub fn can_checkpoint(&self) -> bool {
        self.active_txns.lock().is_empty()
    }

    /// Exposes the WalManager to the background flusher for size checking and
    /// truncation
    pub fn get_wal_manager(&self) -> Arc<Mutex<WalManager>> {
        Arc::clone(&self.wal_manager)
    }

    /// Returns true if the underlying physical database is completely empty.
    pub fn is_empty(&self) -> bool {
        self.disk_manager.is_empty()
    }

    /// Fetches a page from the buffer pool. If it's a cache miss, it reads
    /// from disk, potentially evicting an old page.
    pub fn fetch_page(&self, page_id: PageId) -> Result<Frame, Error> {
        if let Some(frame) = self.page_table.read().get(&page_id) {
            self.replacer.lock().record_access(page_id);
            return Ok(frame.clone());
        }
        // cache miss: we'll have to load the page from disk.
        if self.page_table.read().len() >= self.capacity {
            self.evict_page()?;
        }
        // Read physical bytes from disk and decode it into a in-memory BTreeNode.
        let raw_page = self.disk_manager.read_page(&page_id)?;
        let node = BTreeNode::decode(&raw_page)?;
        let frame = Arc::new(RwLock::new(node));

        self.page_table
            .write()
            .insert(page_id, frame.clone());

        self.replacer.lock().record_access(page_id);
        Ok(frame)
    }

    /// Allocates a completely new page via the `DiskManager` and adds it to the pool.
    pub fn new_page(&self, is_leaf: bool) -> Result<(PageId, Frame), Error> {
        if self.page_table.read().len() >= self.capacity {
            self.evict_page()?;
        }
        let page_id = self.disk_manager.compute_new_page_id();

        let node = BTreeNode::new_empty(page_id, is_leaf);
        let frame = Arc::new(RwLock::new(node));

        self.page_table
            .write()
            .insert(page_id, frame.clone());

        self.replacer.lock().record_access(page_id);
        Ok((page_id, frame))
    }

    /// Prepares a page for modifications. Logs the 8KiB before-image (Undo) to the Wal
    /// exactly once per transaction, enabling safe STEAL eviction.
    pub fn begin_page_mutation(&self, page_id: PageId, txn_id: u64) -> Result<(), Error> {
        let frame = self.fetch_page(page_id)?;
        let mut txns = self.active_txns.lock();

        let state = txns.entry(txn_id).or_default();
        // Only log the Undo image if this is the first time this txn is touching
        // this page.
        if let Entry::Vacant(entry) = state.undo_logged.entry(page_id) {
            let mut page = Box::new(Page::new());

            frame.read().encode(&mut page)?; // Capture the before image.
            let record = WalRecord::Undo {
                page_id,
                page,
                txn_id,
            };
            let offset = self.wal_manager.lock().write_record(&record)?;

            match &mut *frame.write() {
                BTreeNode::Internal(node) => node.wal_offset = offset,
                BTreeNode::Leaf(node) => node.wal_offset = offset,
            }
            entry.insert(offset);
        }
        state.dirty_pages.insert(page_id);
        Ok(())
    }

    /// Commits a transaction. Generates 8KiB after-images (Redo) for all modified pages,
    /// batches them with a commit marker, and issues a single synchronous disk write.
    pub fn commit_transaction(&self, txn_id: u64) -> Result<(), Error> {
        let Some(state) = self.active_txns.lock().remove(&txn_id) else {
            return Ok(());
        };
        let page_table = self.page_table.read();

        let mut batch = Vec::with_capacity(state.dirty_pages.len() + 1);
        for page_id in state.dirty_pages {
            if let Some(frame) = page_table.get(&page_id) {
                let mut page = Box::new(Page::new());
                frame.read().encode(&mut page)?;
                batch.push(WalRecord::Redo {
                    page_id,
                    page,
                    txn_id,
                });
            }
        }
        batch.push(WalRecord::Commit { txn_id });

        let mut wal = self.wal_manager.lock();
        wal.write_batch(&batch)?;
        wal.sync()?;
        Ok(())
    }

    /// Aborts a transaction. Retrieves the original Undo records from the Wal via disk
    /// seeks using bytes offsets and overwrites the .db file to erase any stolen garbage
    /// data.
    ///
    /// Drops all modified pages from memory entirely.
    ///
    /// By invalidating the cache, the next read will fetch the uncorrupted data from disk,
    /// effectively rolling back the memory state.
    pub fn abort_transaction(&self, txn_id: u64) -> Result<(), Error> {
        if let Some(state) = self.active_txns.lock().remove(&txn_id) {
            let mut page_table = self.page_table.write();
            let mut wal = self.wal_manager.lock();
            let mut replacer = self.replacer.lock();

            for (page_id, offset) in state.undo_logged {
                let record = wal.read_record_at(offset)?;
                if let WalRecord::Undo { page, .. } = record {
                    // If this page was STEAL'd, this erases the garbage and if it was
                    // never stolen, this safely overwrites correct data with correct
                    // data.
                    self.disk_manager.write_page(page_id, &page)?;
                } else {
                    return Err(Error::CorruptPage(
                        "expected Undo record at cached Wal offset".into(),
                    ));
                }
                page_table.remove(&page_id);
                replacer.remove(page_id);
            }
        }
        Ok(())
    }

    /// Implements the 75/25 high/low watermark cleaning policy, if more that 75% of the
    /// pool is dirty, it flushes the oldest dirty pages until the dirty count drops to
    /// 25%, ensuring query threads rarely block on IO.
    pub fn clean_pages_watermark(&self) -> Result<(), Error> {
        let dirty_threshold = (self.capacity * 3) / 4;
        let dirty_allowed = self.capacity / 4;

        // Identify the dirty pages as the eviction candidates.
        let dirty_page_ids = {
            let page_table = self.page_table.read();
            let mut dirty_pages = Vec::new();

            // Get the oldest dirty pages by peeking the lru backwards.
            let candidates = self.replacer.lock().peek_rev(dirty_threshold);
            for page_id in candidates {
                if let Some(frame) = page_table.get(&page_id)
                    && frame.read().is_dirty()
                {
                    dirty_pages.push(page_id);
                }
            }
            dirty_pages
        };
        // If the amount of dirty pages is greater than threshold then flush till its not.
        if dirty_page_ids.len() >= dirty_threshold {
            let clean_limit = dirty_threshold - dirty_allowed;

            dirty_page_ids
                .iter()
                .take(clean_limit)
                .try_for_each(|&page_id| self.flush_page(page_id))?;
        }
        Ok(())
    }

    /// Flushes a specific page to disk if it is dirty.
    pub fn flush_page(&self, page_id: PageId) -> Result<(), Error> {
        let frame = match self.page_table.read().get(&page_id) {
            Some(frame) => frame.clone(),
            None => return Ok(()),
        };
        let mut node_guard = frame.upgradable_read();
        if !node_guard.is_dirty() {
            return Ok(());
        }
        let offset = match &*node_guard {
            BTreeNode::Internal(node) => node.wal_offset,
            BTreeNode::Leaf(node) => node.wal_offset,
        };
        let flushed = self.wal_manager.lock().flushed_offset();
        // Only make a fsync call if this page's Undo record hasn't been written to disk.
        if offset > flushed {
            self.wal_manager.lock().sync()?;
        }
        let mut raw_page = Page::new();
        node_guard.encode(&mut raw_page)?;
        self.disk_manager.write_page(page_id, &raw_page)?;
        node_guard.with_upgraded(|node| node.clear_dirty());
        Ok(())
    }

    /// Flushes all dirty pages to disk.
    pub fn flush_all_pages(&self) -> Result<(), Error> {
        let page_ids: Vec<PageId> = self.page_table.read().keys().copied().collect();
        for page_id in page_ids {
            self.flush_page(page_id)?;
        }
        self.disk_manager.save_header()?;
        Ok(())
    }

    /// Find a page that can be evicted, flush it if dirty, and remove it from
    /// memory.
    fn evict_page(&self) -> Result<(), Error> {
        let mut page_table = self.page_table.write();

        let evict_id = self
            .replacer
            .lock()
            .evict_if(|page_id| match page_table.get(page_id) {
                Some(frame) => Arc::strong_count(frame) == 1,
                None => {
                    panic!(
                        "LruReplacer contains PageId({:?}); should also be present in page_table",
                        page_id
                    );
                }
            })
            .ok_or(Error::LruEviction)?;

        if let Some(frame) = page_table.get(&evict_id) {
            let mut node_guard = frame.upgradable_read();
            if node_guard.is_dirty() {
                self.wal_manager.lock().sync()?;
                let mut raw_page = Page::new();
                node_guard.encode(&mut raw_page)?;
                self.disk_manager
                    .write_page(evict_id, &raw_page)?;
                node_guard.with_upgraded(|node| node.clear_dirty());
            }
        }
        page_table.remove(&evict_id);
        Ok(())
    }
}
