use crate::{
    error::Error,
    storage::{
        bptree::BpTree,
        buffer_pool::BufferPool,
        page::{BTreeNode, PageId},
    },
};

/// A forward-only cursor for scanning BpTree leaf pages. Uses lazy intialization
/// to defer disk IO until the first tuple is actually requested by the execution
/// pipeline.
#[derive(Debug)]
pub struct BpTreeIterator {
    /// The physical root id of the BpTree index being scanned.
    root_page_id: PageId,

    /// Tracks the curr leaf page being scanned.
    curr_page_id: Option<PageId>,

    /// True if the iterator has already traversed from the root to the leftmost
    /// leaf page.
    initialized: bool,

    /// Tracks the current physical slot index within the current leaf page.
    curr_slot_idx: usize,
}

impl BpTreeIterator {
    /// Initializes a new iterator starting at the given leftmost leaf page.
    pub fn new(root_page_id: PageId) -> Self {
        Self {
            root_page_id,
            curr_page_id: None,
            initialized: false,
            curr_slot_idx: 0,
        }
    }

    /// Advances the cursor to the next valid, non-deleted record. Fills the provided
    /// block buffer with raw byte references avoiding allocations for the eventual
    /// read operation.
    ///
    /// Returns `true` if a record was loaded, `false` if scan is exhausted.
    pub fn next(&mut self, pool: &BufferPool, block_buffer: &mut Vec<u8>) -> Result<bool, Error> {
        if !self.initialized {
            let leftmost_leaf = BpTree::get_leftmost_leaf(pool, self.root_page_id)?;
            self.curr_page_id = Some(leftmost_leaf);
            self.initialized = true;
        }
        while let Some(curr_page_id) = self.curr_page_id {
            let frame = pool.fetch_page(curr_page_id)?;
            let node_guard = frame.read();

            let BTreeNode::Leaf(leaf) = &*node_guard else {
                return Err(Error::CorruptPage(
                    "bptree iterator encountered a non-leaf page".into(),
                ));
            };
            'inner: while self.curr_slot_idx <= leaf.slot_array.len() {
                let rec_idx = leaf.slot_array[self.curr_slot_idx] as usize;
                let record = &leaf.records[rec_idx];

                self.curr_slot_idx += 1;
                if record.is_deleted {
                    // I know the label is not needed but its for clarity, I just confused
                    // my dumbass for 5 minutes thinking I'm skipping the outer loop and
                    // why would I do that?.
                    continue 'inner;
                }
                block_buffer.clear();
                block_buffer.extend_from_slice(&record.data);
                return Ok(true);
            }
            if leaf.has_next {
                self.curr_slot_idx = 0;
                self.curr_page_id = Some(leaf.next_page_id);
            } else {
                self.curr_page_id = None
            }
        }
        Ok(false)
    }
}
