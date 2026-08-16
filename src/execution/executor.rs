use parking_lot::RwLock;

use crate::{
    catalog::manager::CatalogManager, error::Error, relation::tuple::Tuple,
    storage::buffer_pool::BufferPool,
};

/// The runtime context that will be passed down the Volcano execution Tree.
/// For a running query, it will provide access to storage, metadata, and a
/// reusable memory block.
pub struct ExecutionContext<'a> {
    /// A reference to the buffer pool for fetching page.
    pub buffer_pool: &'a BufferPool,

    /// A reference to the catalog for O(1) metadata lookups.
    pub catalog: &'a RwLock<CatalogManager>,

    /// The unique transaction id grouping all operations in this query.
    ///
    /// We don't support transactions (like the multiple table kind) tho
    /// so this isn't that. I likely will add that in the future, I do
    /// have the structure for it, but need to learn more to get it right.
    pub txn_id: u64,

    /// A reusable buffer to avoid heap allocation when fetching raw bytes
    /// from pages, defaults as 2KiB.
    pub block_buffer: Vec<u8>,
}

impl<'a> ExecutionContext<'a> {
    /// Initializes a new `ExecutionContext`.
    pub fn new(pool: &'a BufferPool, catalog: &'a RwLock<CatalogManager>, txn_id: u64) -> Self {
        Self {
            buffer_pool: pool,
            catalog,
            block_buffer: Vec::with_capacity(2048),
            txn_id,
        }
    }
}

/// The Volcano execution model interface.
pub trait Executor {
    /// Fetches the next [Tuple] from the tree in the `ExecutionContext`'s buffer
    /// and then decodes them returning the tuple.
    ///
    /// Returns Ok(None) if the scan was exhausted. An Err is only returned if
    /// the method encounters any I/O error.
    fn next(&mut self, ctx: &mut ExecutionContext) -> Result<Option<Tuple>, Error>;
}
