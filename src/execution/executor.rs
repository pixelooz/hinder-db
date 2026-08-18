use std::{
    collections::hash_map,
    hash::{Hash, Hasher},
};

use parking_lot::RwLock;

use crate::{
    catalog::manager::CatalogManager,
    error::Error,
    relation::{tuple::Tuple, types::Value},
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

/// Computes 64 bit BTree routing key from any supported value type. Uses xor based
/// order-preserving conversion for numbers hashes for strings.
pub fn encode_secondary_key(value: &Value) -> u64 {
    match value {
        // Flip the highest bit to preserve sorting order when converting from
        // signed to unsigned.
        Value::BigInt(val) => (*val as u64) ^ (1 << 63),
        Value::Int(val) => ((*val as i64) as u64) ^ (1 << 63),
        Value::Null => 0,
        Value::Boolean(val) => {
            if *val {
                1
            } else {
                0
            }
        }
        Value::Varchar(val) => {
            let mut hasher = hash_map::DefaultHasher::new();
            val.hash(&mut hasher);
            hasher.finish()
        }
    }
}
