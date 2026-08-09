use std::{
    cmp,
    fs::{File, OpenOptions},
    os::unix::fs::FileExt,
    path::Path,
    sync::atomic::{AtomicU64, Ordering},
};

use crate::error::Error;

/// Size of an element in the index/offset array (u16).
pub const SLOT_ELEM_SIZE: usize = 2;

/// Size of any page in shrimp-db, around 8KiB. We don't have true slotted page architecture,
/// where larger pages are written to another page which the slotted/normal leaf node just
/// holds a pointer to (I'll try that later).
pub const PAGE_SIZE: usize = 8192;

/// Identifier for a physical file offset.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PageId(pub u64);

impl From<PageId> for u64 {
    fn from(value: PageId) -> Self {
        value.0
    }
}

impl From<u64> for PageId {
    fn from(value: u64) -> Self {
        PageId(value)
    }
}

/// An owned, 4KB buffer representing a physical slotted page.
#[derive(Debug)]
pub struct Page {
    pub data: Box<[u8; PAGE_SIZE]>,
}

impl Page {
    /// Allocates a 4KB block onto the heap using `Box` and returns the pointer
    /// to it as `Page`.
    pub fn new() -> Self {
        Self {
            data: Box::new([0; PAGE_SIZE]),
        }
    }

    /// Access `Page` as immutable byte slice.
    pub fn as_bytes(&self) -> &[u8; PAGE_SIZE] {
        &self.data
    }

    /// Access `Page` as mutable byte slice.
    pub fn as_bytes_mut(&mut self) -> &mut [u8; PAGE_SIZE] {
        &mut self.data
    }
}

/// Fixed size of an internal cell (key + file_index).
pub const INDEX_ENTRY_SIZE: usize = 8 + 8;

/// Fixed size of a leaf cell metadata (key + deleted + value_size).
/// # Note
/// The actual bytes are appended after this.
pub const RECORD_META_SIZE: usize = 8 + 1 + 4;

/// Maximum allowed size of a value. Larger values than 1KiB will be rejected;
/// what is this? a real database (¬_¬)?
pub const MAX_VALUE_SIZE: usize = 1024;

/// Fixed size of the internal node header.
pub const INTERNAL_NODE_HEADER_SIZE: usize = 1 + // entry_type (u8)
8 + // page_id (u64)
8 + // rightmost_child_id (u64)
4 + // slot_entry (u32)
2; // free_size

/// Fixed size of the leaf node header.
pub const LEAF_NODE_HEADER_SIZE: usize = 1 + // entry_type (u8)
8 + // page_id (u64)
1 + // has_prev (u8 bool)
1 + // has_next (u8 bool)
8 + // prev_page_id (u64)
8 + // next_page_id (u64)
4 + // slot_count (u32)
2; // free_size

/// Maximum number of leaf cells a page can hold before requiring a split.
pub const MAX_LEAF_NODE_CELLS: usize =
    (PAGE_SIZE - LEAF_NODE_HEADER_SIZE) / (SLOT_ELEM_SIZE + RECORD_META_SIZE);

#[repr(u8)]
pub enum NodeType {
    Internal = 0,
    Leaf = 1,
}

/// Represents a routing boundary within a B+ Tree internal node.
/// The `child_page_id` acts as a left-pointer, routing to a child page that
/// contains values strictly less than the `key`.
#[derive(Debug, Default, Clone)]
pub struct IndexEntry {
    pub key: u64,
    pub child_page_id: PageId,
}

/// A physical database tuple stored within a B+ Tree leaf node.
/// The `row_id` uniquely identifies the record on disk, and `is_deleted` acts
/// as a logical tombstone to defer physical byte shifting during deletions.
#[derive(Debug, Default, Clone)]
pub struct Record {
    pub row_id: u64,
    pub data: Vec<u8>,
    pub is_deleted: bool,
}

/// An in-memory representation of a B+ Tree internal page frame.
/// Internal nodes never store user data; they act purely as a multi-level
/// traffic directory routing tree traversals down to the correct leaf page.
#[derive(Debug, Default, Clone)]
pub struct InternalNode {
    pub page_id: PageId,
    pub rightmost_child_id: PageId,
    pub slot_array: Vec<u16>,
    pub entries: Vec<IndexEntry>,
    pub free_size: u16,

    /// Tracks if the node was modified in memory. Excluded from encoding
    /// decoding.
    pub is_dirty: bool,
}

impl InternalNode {
    /// Returns true if the node has been modified in memory since being loaded
    /// from disk.
    pub fn is_dirty(&self) -> bool {
        self.is_dirty
    }

    /// Clears the dirty flag, signifying the node to be unmodified. Must be
    /// called immediately after the Buffer Pool successfully write the page
    /// to the disk.
    pub fn clear_dirty(&mut self) {
        self.is_dirty = false
    }

    /// Marks the node as dirty :) hehe, and updates its last (LSN).
    pub fn mark_dirty(&mut self) {
        self.is_dirty = true;
    }
}

impl InternalNode {
    /// Inserts the promoted key and its page id in the index entries.
    /// Swaps the child_page pointers to maintain consistency.
    pub fn insert_entry(&mut self, entry_key: u64, new_child_page: PageId) -> Result<(), Error> {
        let required_bytes = INDEX_ENTRY_SIZE + SLOT_ELEM_SIZE;

        let current_size = INTERNAL_NODE_HEADER_SIZE
            + (self.slot_array.len() * (INDEX_ENTRY_SIZE + SLOT_ELEM_SIZE));

        if current_size + required_bytes >= PAGE_SIZE {
            return Err(Error::PageFull);
        }
        let insert_idx = self.slot_array.partition_point(|&entry_idx| {
            let entry = &self.entries[entry_idx as usize];
            entry.key <= entry_key
        });
        let new_left_child: PageId;

        if insert_idx == self.slot_array.len() {
            // Appending to the far right; so, the rightmost child now
            // becomes the left child of the new key.
            new_left_child = self.rightmost_child_id;
            self.rightmost_child_id = new_child_page;
        } else {
            // Appending somewhere in the middle; so, the left child of
            // the new key is the child that was already present at the
            // insert idx.
            let entry_idx = self.slot_array[insert_idx] as usize;
            new_left_child = self.entries[entry_idx].child_page_id;
            // The pre-existing entry now points to the new child page.
            self.entries[entry_idx].child_page_id = new_child_page;
        }
        let new_entry_idx = self.entries.len() as u16;
        self.entries.push(IndexEntry {
            key: entry_key,
            child_page_id: new_left_child,
        });
        self.slot_array.insert(insert_idx, new_entry_idx);

        if (self.free_size as usize) >= required_bytes {
            self.free_size -= required_bytes as u16;
        } else {
            self.free_size = 0;
        }
        Ok(())
    }

    /// Splits an InternalNode into half, filling the given new_sibling with the
    /// other half of the data; and pushes the middle key upwards - removing it
    /// from this level.
    ///
    /// Returns the middle key that was removed from this node.
    pub fn split(&mut self, new_sibling: &mut InternalNode) -> Result<u64, Error> {
        let mid = self.slot_array.len() / 2;

        let entry_idx = self.slot_array[mid] as usize;
        let promoted_entry = self.entries[entry_idx].clone();

        for &entry_idx in &self.slot_array[mid + 1..] {
            let entry = self.entries[entry_idx as usize].clone();

            let new_idx = new_sibling.entries.len();
            new_sibling.entries.push(entry);
            new_sibling.slot_array.push(new_idx as u16);
        }
        new_sibling.rightmost_child_id = self.rightmost_child_id;

        // The middle entry's (the promoted one) page_id becomes the rightmost
        // child page id.
        self.rightmost_child_id = promoted_entry.child_page_id;

        // We remove everything till but not including the mid slot element.
        self.slot_array.truncate(mid);
        self.compact();
        new_sibling.compact(); // This line isn't necessary but I'm being safe.
        Ok(promoted_entry.key)
    }

    /// Re-writes the entries into a clean vector ordered by slot index and also
    /// adjusts `self.free_size` according to how much space is left.
    pub fn compact(&mut self) {
        let mut clean_entries = Vec::with_capacity(self.entries.len());
        let mut new_slots = Vec::with_capacity(self.slot_array.len());
        let mut bytes_used = 0;

        for (new_idx, &old_idx) in self.slot_array.iter().enumerate() {
            let entry = &self.entries[old_idx as usize];
            bytes_used += INDEX_ENTRY_SIZE + SLOT_ELEM_SIZE;

            clean_entries.push(entry.clone());
            new_slots.push(new_idx as u16);
        }
        self.entries = clean_entries;
        self.slot_array = new_slots;

        let net_size = INTERNAL_NODE_HEADER_SIZE + bytes_used;
        if net_size > PAGE_SIZE {
            self.free_size = 0;
        } else {
            self.free_size = (PAGE_SIZE - net_size) as u16;
        }
    }

    /// Evaluates internal routing brackets using O(log N) binary search to determine
    /// the PageId of the child node containing `target_key`.
    pub fn route_key(&self, target_key: u64) -> Result<PageId, Error> {
        let slot_idx = self.slot_array.partition_point(|&entry_idx| {
            // Find the first index where entry.key > target_key as that is
            // the internal node-point pointing to the leaf where the
            // key-val exists
            let entry_key = self.entries[entry_idx as usize].key;
            entry_key <= target_key
        });
        // If partition_idx == len(), target_key is >= all routing keys in
        // this node. Route to the rightmost child pointer
        if slot_idx == self.slot_array.len() {
            Ok(self.rightmost_child_id)
        } else {
            let entry_idx = self.slot_array[slot_idx] as usize;
            let page_id = self.entries[entry_idx].child_page_id;
            Ok(page_id)
        }
    }
}

/// An in-memory representation of a B+ Tree leaf page frame.
/// Leaf nodes store the actual user payload (`records`) and maintain horizontal
/// doubly-linked sibling pointers for efficient sequential table scans.
#[derive(Debug, Default, Clone)]
pub struct LeafNode {
    pub page_id: PageId,
    pub has_prev: bool,
    pub has_next: bool,
    pub prev_page_id: PageId,
    pub next_page_id: PageId,
    pub slot_array: Vec<u16>,
    pub records: Vec<Record>,
    pub free_size: u16,

    /// Tracks if the node was modified in memory. Excluded from encoding
    /// decoding.
    pub is_dirty: bool,
}

impl LeafNode {
    /// Returns true if the node has been modified in memory since being loaded
    /// from disk.
    pub fn is_dirty(&self) -> bool {
        self.is_dirty
    }

    /// Clears the dirty flag, signifying the node to be unmodified. Must be
    /// called immediately after the Buffer Pool successfully write the page
    /// to the disk.
    pub fn clear_dirty(&mut self) {
        self.is_dirty = false
    }

    /// Marks the node as dirty :) hehe, and updates its last (LSN).
    pub fn mark_dirty(&mut self) {
        self.is_dirty = true;
    }
}

impl LeafNode {
    /// Re-writes records into a clean vector ordered by slot index removing
    /// tombstones completely, reducing the size of our data files.
    /// Re-calculates the exact free_size available relative to `PAGE_SIZE`
    pub fn compact(&mut self) {
        let mut clean_records = Vec::with_capacity(self.slot_array.len());
        let mut new_slots = Vec::with_capacity(self.slot_array.len());
        let mut bytes_used = 0;

        for &old_slot_idx in self.slot_array.iter() {
            let rec = &self.records[old_slot_idx as usize];
            if !rec.is_deleted {
                bytes_used += RECORD_META_SIZE + rec.data.len();
                bytes_used += SLOT_ELEM_SIZE;

                clean_records.push(rec.clone());
                new_slots.push((clean_records.len() - 1) as u16);
            }
        }
        self.records = clean_records;
        self.slot_array = new_slots;

        let net_size = LEAF_NODE_HEADER_SIZE + bytes_used;
        match net_size.cmp(&PAGE_SIZE) {
            cmp::Ordering::Less => {
                self.free_size = (PAGE_SIZE - net_size) as u16;
            }
            cmp::Ordering::Equal => {
                self.free_size = 0;
            }
            cmp::Ordering::Greater => unreachable!(),
        }
    }

    /// Splits a full leaf node in half, transferring the upper 50% of sorted records
    /// into the given new_sibling parameter.
    pub fn split(&mut self, new_sibling: &mut LeafNode) -> Result<u64, Error> {
        let mid = self.slot_array.len() / 2;

        for &rec_idx in self.slot_array[mid..].iter() {
            let rec = &self.records[rec_idx as usize];
            let new_idx = new_sibling.records.len();
            new_sibling.records.push(rec.clone());
            new_sibling.slot_array.push(new_idx as u16);
        }
        self.slot_array.truncate(mid);

        self.compact();
        new_sibling.compact();

        if new_sibling.slot_array.is_empty() {
            return Err(Error::CorruptPage(
                "leaf split resulted in empty right sibling".into(),
            ));
        }
        // The promoted key is the very first key of the new right sibling.
        let promoted_key = new_sibling.records[new_sibling.slot_array[0] as usize].row_id;
        Ok(promoted_key)
    }

    /// Inserts the given key-value record into the page's `records` and updates
    /// the sorted slotted leaf array to reflect the newly inserted position.
    /// Enforces both record count and 4-KiB byte limits.
    pub fn insert_record(&mut self, row_id: u64, payload: Vec<u8>) -> Result<(), Error> {
        if payload.len() > MAX_VALUE_SIZE {
            return Err(Error::TupleTooLarge(payload.len()));
        }
        match self
            .slot_array
            .binary_search_by_key(&row_id, |&rec_idx| self.records[rec_idx as usize].row_id)
        {
            // key already exists, so we'll check if deleted or return duplicate err.
            Ok(slot_idx) => {
                let rec_idx = self.slot_array[slot_idx] as usize;
                let rec = &mut self.records[rec_idx];
                if !rec.is_deleted {
                    return Err(Error::DuplicateKey(row_id));
                }
                // overwrite tombstone data in place.
                rec.is_deleted = false;
                rec.data = payload;
                // reevaluate space after compaction.
                self.compact();
                Ok(())
            }
            // key does not exist, so insert_idx will the index where it should be.
            Err(insert_idx) => {
                let required_bytes = SLOT_ELEM_SIZE + RECORD_META_SIZE + payload.len();
                let header_len =
                    LEAF_NODE_HEADER_SIZE + ((self.slot_array.len() + 1) * SLOT_ELEM_SIZE);

                let footer_len = self
                    .slot_array
                    .iter()
                    .map(|&rec_idx| RECORD_META_SIZE + self.records[rec_idx as usize].data.len())
                    .sum::<usize>();

                if (header_len + footer_len + required_bytes) > PAGE_SIZE
                    || self.slot_array.len() > MAX_LEAF_NODE_CELLS
                {
                    return Err(Error::PageFull);
                }
                let new_rec_idx = self.records.len() as u16;
                self.records.push(Record {
                    row_id,
                    data: payload,
                    is_deleted: false,
                });
                self.slot_array.insert(insert_idx, new_rec_idx);

                if (self.free_size as usize) >= required_bytes {
                    self.free_size -= required_bytes as u16;
                } else {
                    self.free_size = 0;
                }
                Ok(())
            }
        }
    }

    /// Overwrites the raw byte payload of an existing record for the given key.
    pub fn update_record(&mut self, row_id: u64, payload: Vec<u8>) -> Result<(), Error> {
        if payload.len() >= MAX_VALUE_SIZE {
            return Err(Error::TupleTooLarge(payload.len()));
        }
        let update_idx = self
            .slot_array
            .binary_search_by_key(&row_id, |&rec_idx| self.records[rec_idx as usize].row_id)
            .map_err(|_| Error::KeyNotFound(row_id))?;

        let rec_idx = self.slot_array[update_idx] as usize;
        self.records[rec_idx].data = payload;
        Ok(())
    }

    /// Marks an existing cell as logically deleted without compacting the physical
    /// bytes immediately.
    pub fn delete_record(&mut self, row_id: u64) -> Result<(), Error> {
        let delete_idx = self
            .slot_array
            .binary_search_by_key(&row_id, |&rec_idx| self.records[rec_idx as usize].row_id)
            .map_err(|_| Error::KeyNotFound(row_id))?;

        let rec_idx = self.slot_array[delete_idx] as usize;
        self.records[rec_idx].is_deleted = true;
        Ok(())
    }

    /// Performs a binary search on the leaf over the slotted array to find record
    /// by key.
    ///
    /// Returns None if the key does not exist or has been logically deleted or
    /// actually deleted after I've added compaction (that's going to be so cool).
    pub fn get_record(&self, row_id: u64) -> Option<Vec<u8>> {
        let slot_idx = self
            .slot_array
            .binary_search_by_key(&row_id, |&rec_idx| self.records[rec_idx as usize].row_id)
            .ok()?;

        let rec_idx = self.slot_array[slot_idx] as usize;
        if let record = &self.records[rec_idx]
            && !record.is_deleted
        {
            Some(record.data.clone())
        } else {
            None
        }
    }
}

/* FIXME: maybe I can implement `Deref` or `AsRef` or `Borrow` on `BTreeNode`
for it to automatically convert to one of the types, whichever one is more
idiomatic for what I am trying to do. Then we won't have to match on node every
time we want to do something.*/

#[derive(Debug, Clone)]
pub enum BTreeNode {
    Internal(InternalNode),
    Leaf(LeafNode),
}

impl BTreeNode {
    pub fn new_empty(page_id: PageId, is_leaf: bool) -> Self {
        if is_leaf {
            let leaf_node = LeafNode {
                page_id,
                ..Default::default()
            };
            return Self::Leaf(leaf_node);
        }
        let internal_node = InternalNode {
            page_id,
            ..Default::default()
        };
        Self::Internal(internal_node)
    }

    /// Returns true if the node has been modified in memory since being loaded
    /// from disk.
    pub fn is_dirty(&self) -> bool {
        use BTreeNode::*;
        match self {
            Internal(node) => node.is_dirty,
            Leaf(node) => node.is_dirty,
        }
    }

    /// Clears the dirty flag, signifying the node to be unmodified. Must be
    /// called immediately after the Buffer Pool successfully write the page
    /// to the disk.
    pub fn clear_dirty(&mut self) {
        use BTreeNode::*;
        match self {
            Internal(node) => node.is_dirty = false,
            Leaf(node) => node.is_dirty = false,
        }
    }

    /// Marks the node as dirty :) hehe, and updates its last (LSN).
    pub fn mark_dirty(&mut self) {
        use BTreeNode::*;
        match self {
            Internal(node) => {
                node.is_dirty = true;
            }
            Leaf(node) => {
                node.is_dirty = true;
            }
        }
    }
}

/// A minimal helper to safely read/write bytes to our 4KB array sequentially.
struct ByteCursor<'a> {
    buffer: &'a mut [u8],
    pos: usize,
}

impl<'a> ByteCursor<'a> {
    fn new(buffer: &'a mut [u8]) -> Self {
        Self { buffer, pos: 0 }
    }

    fn write_u8(&mut self, val: u8) {
        self.buffer[self.pos] = val;
        self.pos += 1;
    }

    fn write_u16(&mut self, val: u16) {
        self.buffer[self.pos..self.pos + 2].copy_from_slice(&val.to_le_bytes());
        self.pos += 2;
    }

    fn write_u32(&mut self, val: u32) {
        self.buffer[self.pos..self.pos + 4].copy_from_slice(&val.to_le_bytes());
        self.pos += 4;
    }

    fn write_u64(&mut self, val: u64) {
        self.buffer[self.pos..self.pos + 8].copy_from_slice(&val.to_le_bytes());
        self.pos += 8;
    }

    fn write_bytes(&mut self, val: &[u8]) {
        self.buffer[self.pos..self.pos + val.len()].copy_from_slice(val);
        self.pos += val.len();
    }

    fn read_u8(&mut self) -> u8 {
        let val = self.buffer[self.pos];
        self.pos += 1;
        val
    }

    fn read_u16(&mut self) -> u16 {
        let val = u16::from_le_bytes(
            self.buffer[self.pos..self.pos + 2]
                .try_into()
                .unwrap(),
        );
        self.pos += 2;
        val
    }

    fn read_u32(&mut self) -> u32 {
        let val = u32::from_le_bytes(
            self.buffer[self.pos..self.pos + 4]
                .try_into()
                .unwrap(),
        );
        self.pos += 4;
        val
    }

    fn read_u64(&mut self) -> u64 {
        let val = u64::from_le_bytes(
            self.buffer[self.pos..self.pos + 8]
                .try_into()
                .unwrap(),
        );
        self.pos += 8;
        val
    }

    fn read_bytes(&mut self, len: usize) -> Vec<u8> {
        let val = self.buffer[self.pos..self.pos + len].to_vec();
        self.pos += len;
        val
    }
}

impl BTreeNode {
    /// Deserializes a raw 4KB page from disk into a structured B+ Tree node.
    /// It reads the leading byte (0 or 1) to determine the node type and
    /// extracts the slotted array pointers to recreate the memory state.
    pub fn decode(page: &Page) -> Result<Self, Error> {
        let mut buffer = *page.as_bytes();
        let mut cursor = ByteCursor::new(&mut buffer);

        match cursor.read_u8() {
            // LeafNode
            1 => {
                let file_index = cursor.read_u64();
                let has_lsib = cursor.read_u8() != 0;
                let has_rsib = cursor.read_u8() != 0;
                let lsib_index = cursor.read_u64();
                let rsib_index = cursor.read_u64();
                let cell_count = cursor.read_u32();

                let mut indices = Vec::with_capacity(cell_count as usize);
                for _ in 0..cell_count {
                    indices.push(cursor.read_u16());
                }
                let free_size = cursor.read_u16();
                cursor.pos += free_size as usize;

                let mut cells = vec![Record::default(); cell_count as usize];

                for &cell_idx in indices.iter().take(cell_count as usize) {
                    let key = cursor.read_u64();
                    let deleted = cursor.read_u8() != 0;
                    let size = cursor.read_u32() as usize;
                    let value = cursor.read_bytes(size);
                    cells[cell_idx as usize] = Record {
                        row_id: key,
                        is_deleted: deleted,
                        data: value,
                    };
                }
                Ok(BTreeNode::Leaf(LeafNode {
                    page_id: PageId(file_index),
                    has_prev: has_lsib,
                    has_next: has_rsib,
                    prev_page_id: lsib_index.into(),
                    next_page_id: rsib_index.into(),
                    slot_array: indices,
                    records: cells,
                    free_size,
                    is_dirty: false,
                }))
            }
            // InternalNode
            0 => {
                let file_index = cursor.read_u64();
                let right_index = cursor.read_u64();
                let entry_count = cursor.read_u32();

                let mut indices = Vec::with_capacity(entry_count as usize);
                for _ in 0..entry_count {
                    indices.push(cursor.read_u16());
                }
                let free_size = cursor.read_u16();
                cursor.pos += free_size as usize;

                let mut entries = vec![IndexEntry::default(); entry_count as usize];
                for &entry_idx in indices.iter().take(entry_count as usize) {
                    let key = cursor.read_u64();
                    let child_index = cursor.read_u64();

                    entries[entry_idx as usize] = IndexEntry {
                        key,
                        child_page_id: PageId(child_index),
                    };
                }
                Ok(BTreeNode::Internal(InternalNode {
                    page_id: PageId(file_index),
                    rightmost_child_id: PageId(right_index),
                    slot_array: indices,
                    entries,
                    free_size,
                    is_dirty: false,
                }))
            }
            val => Err(Error::CorruptPage(format!("Unknown node type: {}", val))),
        }
    }

    /// Serializes the in-memory B+ Tree node into a raw 4KB byte array for disk storage.
    /// Mathematically guarantees the combined size of the fixed header, slot array,
    /// and variable-length payload footprint never exceeds `PAGE_SIZE` (4096 bytes).
    pub fn encode(&self, page: &mut Page) -> Result<(), Error> {
        let mut cursor = ByteCursor::new(page.as_bytes_mut());
        match self {
            BTreeNode::Leaf(node) => {
                let header_len = LEAF_NODE_HEADER_SIZE + (node.slot_array.len() * SLOT_ELEM_SIZE);
                let footer_len: usize = node
                    .slot_array
                    .iter()
                    .map(|&cell_idx| {
                        let cell = &node.records[cell_idx as usize];
                        RECORD_META_SIZE + cell.data.len()
                    })
                    .sum();
                if header_len + footer_len > PAGE_SIZE {
                    return Err(Error::PageFull);
                }
                let free_size = (PAGE_SIZE - header_len - footer_len) as u16;

                cursor.write_u8(NodeType::Leaf as u8);
                cursor.write_u64(node.page_id.0);
                cursor.write_u8(node.has_prev as u8);
                cursor.write_u8(node.has_next as u8);
                cursor.write_u64(node.prev_page_id.into());
                cursor.write_u64(node.next_page_id.into());

                cursor.write_u32(node.slot_array.len() as u32);
                for &index in &node.slot_array {
                    cursor.write_u16(index);
                }
                cursor.write_u16(free_size);
                cursor.pos += free_size as usize;

                for &index in &node.slot_array {
                    let cell = &node.records[index as usize];
                    cursor.write_u64(cell.row_id);
                    cursor.write_u8(cell.is_deleted as u8);
                    cursor.write_u32(cell.data.len() as u32);
                    cursor.write_bytes(&cell.data);
                }
            }
            BTreeNode::Internal(node) => {
                let header_len =
                    INTERNAL_NODE_HEADER_SIZE + (node.slot_array.len() * SLOT_ELEM_SIZE);
                let footer_len = node.slot_array.len() * INDEX_ENTRY_SIZE;

                if header_len + footer_len > PAGE_SIZE {
                    return Err(Error::PageFull);
                }
                let free_size = (PAGE_SIZE - header_len - footer_len) as u16;

                cursor.write_u8(NodeType::Internal as u8);
                cursor.write_u64(node.page_id.0);
                cursor.write_u64(node.rightmost_child_id.into());

                cursor.write_u32(node.slot_array.len() as u32);
                for &offset in &node.slot_array {
                    cursor.write_u16(offset);
                }
                cursor.write_u16(free_size);
                cursor.pos += free_size as usize;

                for &offset in &node.slot_array {
                    let cell = &node.entries[offset as usize];
                    cursor.write_u64(cell.key);
                    cursor.write_u64(cell.child_page_id.into());
                }
            }
        }
        Ok(())
    }
}

/// Tracks the global physical state of the database file.
/// This 24-byte block is permanently persisted at the absolute beginning
/// (byte offset 0) of the OS file to survive database restarts.
#[derive(Debug)]
pub struct FileHeader {
    pub page_root_offset: u64,
    pub next_lsn: u64,
    pub next_free_offset: AtomicU64,
}

impl Default for FileHeader {
    fn default() -> Self {
        Self {
            page_root_offset: 0,
            next_lsn: 0,
            next_free_offset: AtomicU64::new(PAGE_SIZE as u64),
        }
    }
}

/// Manages low-level physical disk I/O, page allocation, and file state tracking.
/// Acts as the bridge between the logical `PageId` abstraction and physical OS
/// file boundaries.
#[derive(Debug)]
pub struct DiskManager {
    file: File,
    header: FileHeader,
}

impl DiskManager {
    /// Opens or creates the physical database file.
    /// If the file contains existing data, it decodes the `FileHeader` from byte
    /// offset 0 to restore the global database state (root node location and next free page).
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path)?;

        let mut header = FileHeader::default();
        let metadata = file.metadata()?;

        if metadata.len().cmp(&0) == cmp::Ordering::Greater {
            let mut buffer = [0u8; 28]; // 4 + 8 + 8 + 8 = 28
            file.read_exact_at(&mut buffer, 0)?;

            let mut cursor = ByteCursor::new(&mut buffer);
            header.page_root_offset = cursor.read_u64();
            header.next_lsn = cursor.read_u64();
            header.next_free_offset = AtomicU64::new(cursor.read_u64());
        }
        Ok(Self { file, header })
    }

    /// Returns true if database file contains no allocated pages (only the FileHeader).
    pub fn is_empty(&self) -> bool {
        // If the next free offset is exact the size of the 0th page, that means
        // no user or system page has been allocated.
        (PAGE_SIZE as u64).eq(&self
            .header
            .next_free_offset
            .load(Ordering::Acquire))
    }

    /// Reads exactly one 4KB block from the physical disk into a `Page` buffer,
    /// mapping the logical `PageId` directly to an absolute byte offset in the
    /// file.
    pub fn read_page(&self, page_id: &PageId) -> Result<Page, Error> {
        let mut page = Page::new();
        self.file
            .read_exact_at(page.as_bytes_mut(), page_id.0)?;
        Ok(page)
    }

    /// Flushes a 4KB `Page` buffer directly to disk at the specified `PageId` offset.
    pub fn write_page(&self, page_id: PageId, page: &Page) -> Result<(), Error> {
        self.file
            .write_all_at(page.as_bytes(), page_id.0)?;
        Ok(())
    }

    /// Reserves space for a new page by advancing the global `next_free_offset` tracker
    /// by exactly 4096 bytes. The returned `PageId` represents the start of the new block.
    pub fn compute_new_page_id(&self) -> PageId {
        let new_page_id = self
            .header
            .next_free_offset
            .fetch_add(PAGE_SIZE as u64, Ordering::Relaxed);
        PageId(new_page_id)
    }

    /// Synchronizes the global database state variables to byte offset 0 on disk.
    /// Executes a hard `sync_all` (fsync) to guarantee file structure consistency
    /// in the event of a crash.
    pub fn save_header(&self) -> Result<(), Error> {
        let mut buffer = [0u8; 28];
        let mut cursor = ByteCursor::new(&mut buffer);

        cursor.write_u64(self.header.page_root_offset);
        cursor.write_u64(self.header.next_lsn);
        cursor.write_u64(
            self.header
                .next_free_offset
                .load(Ordering::Relaxed),
        );

        self.file.write_all_at(&buffer, 0)?;
        self.file.sync_all()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::{self};
    use std::fs::remove_file;
    use std::time::SystemTime;

    /// Helper to generate a unique temporary file path for concurrent DiskManager
    /// tests.
    fn temp_db_path(test_name: &str) -> String {
        let timestamp = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        // FIXME: provide a way to pass custom paths for testing, db-files, etc.
        format!(
            "/Volumes/External T7/smol_db_test_{}_{}.db",
            test_name, timestamp
        )
    }

    /// Helper to construct a dummy Record with arbitrary key and data payload.
    fn make_record(key: u64, data: &[u8], is_deleted: bool) -> Record {
        Record {
            row_id: key,
            data: data.to_vec(),
            is_deleted,
        }
    }

    /// Verifies that ByteCursor correctly reads and writes mixed endian integers
    /// and slices sequentially.
    #[test]
    fn test_byte_cursor_primitives() {
        let mut buffer = [0u8; 64];
        let mut write_cursor = ByteCursor::new(&mut buffer);

        write_cursor.write_u8(42);
        write_cursor.write_u16(u16::MAX);
        write_cursor.write_u32(123456789);
        write_cursor.write_u64(u64::MAX);
        write_cursor.write_bytes(b"shrimp-db");

        let mut read_cursor = ByteCursor::new(&mut buffer);
        assert_eq!(read_cursor.read_u8(), 42);
        assert_eq!(read_cursor.read_u16(), u16::MAX);
        assert_eq!(read_cursor.read_u32(), 123456789);
        assert_eq!(read_cursor.read_u64(), u64::MAX);
        assert_eq!(read_cursor.read_bytes(9), b"shrimp-db");
    }

    /// Validates round-trip encoding and decoding of an empty LeafNode without
    /// siblings.
    #[test]
    fn test_empty_leaf_node_round_trip() -> Result<(), Box<dyn error::Error>> {
        let leaf = LeafNode {
            page_id: PageId(1),
            has_prev: false,
            has_next: false,
            prev_page_id: 0.into(),
            next_page_id: 0.into(),
            slot_array: vec![],
            records: vec![],
            free_size: 0, // Will be recalculated during encode
            is_dirty: false,
        };

        let mut page = Page::new();
        BTreeNode::Leaf(leaf.clone()).encode(&mut page)?;

        let decoded = BTreeNode::decode(&page)?;
        if let BTreeNode::Leaf(dec_leaf) = decoded {
            assert_eq!(dec_leaf.page_id, leaf.page_id);
            assert!(!dec_leaf.has_prev);
            assert!(!dec_leaf.has_next);
            assert!(dec_leaf.slot_array.is_empty());
            assert!(dec_leaf.records.is_empty());
            // Header is 33 bytes; free size should be PAGE_SIZE - 33
            assert_eq!(
                dec_leaf.free_size as usize,
                PAGE_SIZE - LEAF_NODE_HEADER_SIZE
            );
        } else {
            panic!("Expected BTreeNode::Leaf, got Internal");
        }

        Ok(())
    }

    /// Verifies round-trip encoding of a LeafNode containing multiple records,
    /// including deleted flags and sibling pointers.
    #[test]
    fn test_populated_leaf_node_round_trip() -> Result<(), Box<dyn error::Error>> {
        let records = vec![
            make_record(10, b"alice", false),
            make_record(20, b"bob-was-deleted", true),
            make_record(30, b"charlie", false),
        ];

        let leaf = LeafNode {
            page_id: PageId(42),
            has_prev: true,
            has_next: true,
            prev_page_id: 41.into(),
            next_page_id: 43.into(),
            // Slot array maps logical sorted order to physical record vector indices
            slot_array: vec![0, 1, 2],
            records: records.clone(),
            free_size: 0,
            is_dirty: false,
        };

        let mut page = Page::new();
        BTreeNode::Leaf(leaf).encode(&mut page)?;

        let decoded = BTreeNode::decode(&page)?;
        if let BTreeNode::Leaf(dec_leaf) = decoded {
            assert_eq!(dec_leaf.page_id, PageId(42));
            assert_eq!(dec_leaf.prev_page_id, 41.into());
            assert_eq!(dec_leaf.next_page_id, 43.into());
            assert_eq!(dec_leaf.slot_array, vec![0, 1, 2]);
            assert_eq!(dec_leaf.records.len(), 3);

            assert_eq!(dec_leaf.records[0].row_id, 10);
            assert_eq!(dec_leaf.records[0].data, b"alice");
            assert!(!dec_leaf.records[0].is_deleted);

            assert_eq!(dec_leaf.records[1].row_id, 20);
            assert_eq!(dec_leaf.records[1].data, b"bob-was-deleted");
            assert!(dec_leaf.records[1].is_deleted);
        } else {
            panic!("Expected BTreeNode::Leaf");
        }

        Ok(())
    }

    /// Ensures attempting to encode data exceeding the 4KB page boundary returns DbError::PageFull.
    #[test]
    fn test_leaf_node_overflow_returns_page_full() {
        // Create 11 records of 400 bytes each (11 * 400 = 4400 bytes > 4096 PAGE_SIZE)
        let mut records = Vec::new();
        let mut slot_array = Vec::new();

        for i in 0..11 {
            records.push(make_record(i, &[0u8; MAX_VALUE_SIZE], false));
            slot_array.push(i as u16);
        }
        let leaf = LeafNode {
            page_id: PageId(1),
            has_prev: false,
            has_next: false,
            prev_page_id: 0.into(),
            next_page_id: 0.into(),
            slot_array,
            records,
            free_size: 0,
            is_dirty: false,
        };
        let mut page = Page::new();

        let result = BTreeNode::Leaf(leaf).encode(&mut page);
        assert!(matches!(result, Err(Error::PageFull)));
    }

    /// Validates round-trip encoding and decoding of an InternalNode with routing
    /// keys and child pointers.
    #[test]
    fn test_internal_node_round_trip() -> Result<(), Box<dyn error::Error>> {
        let entries = vec![
            IndexEntry {
                key: 100,
                child_page_id: PageId(2),
            },
            IndexEntry {
                key: 200,
                child_page_id: PageId(3),
            },
            IndexEntry {
                key: 300,
                child_page_id: PageId(4),
            },
        ];
        let internal = InternalNode {
            page_id: PageId(1),
            rightmost_child_id: PageId(5),
            slot_array: vec![0, 1, 2],
            entries: entries.clone(),
            free_size: 0,
            is_dirty: false,
        };
        let mut page = Page::new();
        BTreeNode::Internal(internal).encode(&mut page)?;

        let decoded = BTreeNode::decode(&page)?;
        if let BTreeNode::Internal(dec_internal) = decoded {
            assert_eq!(dec_internal.page_id, PageId(1));
            assert_eq!(dec_internal.rightmost_child_id, PageId(5));
            assert_eq!(dec_internal.slot_array.len(), 3);
            assert_eq!(dec_internal.entries[1].key, 200);
            assert_eq!(dec_internal.entries[1].child_page_id, PageId(3));
        } else {
            panic!("Expected BTreeNode::Internal");
        }
        Ok(())
    }

    /// Ensures decoding a page with an invalid magic byte returns a CorruptPage
    /// error variant.
    #[test]
    fn test_decode_corrupt_node_type_fails() {
        let mut page = Page::new();
        page.as_bytes_mut()[0] = 99; // Invalid node type magic byte (valid is 0 or 1)

        let result = BTreeNode::decode(&page);
        assert!(matches!(result, Err(Error::CorruptPage(_))));
    }

    /// Verifies end-to-end persistence: allocating, writing, and reading pages
    /// via DiskManager.
    #[test]
    fn test_disk_manager_page_lifecycle() -> Result<(), Box<dyn error::Error>> {
        let path = temp_db_path("disk_manager_lifecycle");
        let dm = DiskManager::open(&path)?;

        // Allocate two physical pages
        let page_id_1 = dm.compute_new_page_id();
        let page_id_2 = dm.compute_new_page_id();
        assert_ne!(page_id_1, page_id_2);

        // Encode a leaf node onto page 1
        let mut page_1 = Page::new();
        let leaf = LeafNode {
            page_id: page_id_1,
            has_prev: false,
            has_next: false,
            prev_page_id: 0.into(),
            next_page_id: 0.into(),
            slot_array: vec![0],
            records: vec![make_record(1, b"disk-test", false)],
            free_size: 0,
            is_dirty: false,
        };
        BTreeNode::Leaf(leaf).encode(&mut page_1)?;
        dm.write_page(page_id_1, &page_1)?;

        // Read page 1 back from disk and decode
        let read_page = dm.read_page(&page_id_1)?;
        let decoded = BTreeNode::decode(&read_page)?;

        if let BTreeNode::Leaf(dec_leaf) = decoded {
            assert_eq!(dec_leaf.records[0].data, b"disk-test");
        } else {
            panic!("Failed to decode written leaf page from DiskManager");
        }
        remove_file(path)?;
        Ok(())
    }

    /// Validates that DiskManager correctly persists and restores the 28-byte
    /// FileHeader across file reopens.
    #[test]
    fn test_disk_manager_header_persistence() -> Result<(), Box<dyn error::Error>> {
        let path = temp_db_path("header_persistence");
        {
            let mut dm = DiskManager::open(&path)?;
            dm.header.page_root_offset = 8192;
            dm.header.next_lsn = 1000;
            dm.header
                .next_free_offset
                .store(16384, Ordering::Release);
            dm.save_header()?;
        }
        let dm_reopened = DiskManager::open(&path)?;
        assert_eq!(dm_reopened.header.page_root_offset, 8192);
        assert_eq!(dm_reopened.header.next_lsn, 1000);
        assert_eq!(
            dm_reopened
                .header
                .next_free_offset
                .load(Ordering::Acquire),
            16384
        );
        remove_file(path)?;
        Ok(())
    }
}
