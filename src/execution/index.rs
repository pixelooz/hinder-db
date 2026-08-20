use std::{io::Cursor, vec::IntoIter};

use crate::{
    error::Error,
    execution::{ExecutionContext, Executor, iterator::BpTreeIterator},
    relation::{schema::Schema, tuple::Tuple},
    storage::{bptree::BpTree, page::PageId},
};

/// Represents the type of index scan we are performing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexType {
    /// Scanning the primary index organized table where the payload is the actual tuple.
    Primary,
    /// Scanning a secondary index where the payload is a list of primary keys.
    Secondary { primary_root_id: PageId },
}

/// An indexed access executor that fetches tuples using a O(log N) BTree lookup.
pub struct IndexScanExecutor {
    iterator: BpTreeIterator,
    scan_type: IndexType,
    search_key: u64,
    schema: Schema,
    /// A queue of primary keys, extracted from a secondary index's tuple.
    primary_row_ids: IntoIter<u64>,
}

impl IndexScanExecutor {
    /// Constructor.
    pub fn new(
        iterator: BpTreeIterator,
        scan_type: IndexType,
        search_key: u64,
        schema: Schema,
    ) -> Self {
        Self {
            iterator,
            scan_type,
            search_key,
            schema,
            primary_row_ids: Vec::new().into_iter(),
        }
    }
}

impl Executor for IndexScanExecutor {
    fn next(&mut self, ctx: &mut ExecutionContext) -> Result<Option<Tuple>, Error> {
        loop {
            // Check if there are any pending primary keys, if there are drain them.
            if let Some(primary_key) = self.primary_row_ids.next() {
                let IndexType::Secondary { primary_root_id } = self.scan_type else {
                    unreachable!("Primary index type does not use pending queue");
                };
                let primary_tree = BpTree::new(ctx.buffer_pool, primary_root_id);

                if let Some(payload) = primary_tree.find_record(primary_key)? {
                    let mut cursor = Cursor::new(payload);
                    let mut tuple = Tuple::decode(&self.schema, &mut cursor)?;
                    tuple.row_id = Some(primary_key);
                    return Ok(Some(tuple));
                }
                // We need this continue because the index may have some phantom
                // data of deleted keys in which case it'll queue the primary keys
                // but `find_record` will return None skipping the above if block.
                continue;
            }
            // Pull the next key payload from the indexed payload.
            let Some(index_key) = self
                .iterator
                .next(ctx.buffer_pool, &mut ctx.block_buffer)?
            else {
                return Ok(None);
            };
            // There are no more records for the search_key as the key returned has
            // changed.
            if index_key != self.search_key {
                return Ok(None);
            }
            match &self.scan_type {
                IndexType::Primary => {
                    let mut cursor = Cursor::new(&ctx.block_buffer);
                    let mut tuple = Tuple::decode(&self.schema, &mut cursor)?;
                    tuple.row_id = Some(index_key);
                    return Ok(Some(tuple));
                }
                IndexType::Secondary { .. } => {
                    let mut primary_row_ids = Vec::new();
                    for chunk in ctx.block_buffer.chunks_exact(8) {
                        let enc_chunk: [u8; 8] = chunk.try_into().map_err(|_| {
                            Error::CorruptPage("invalid primary-key list chunk-length".into())
                        })?;
                        let stored_id = u64::from_le_bytes(enc_chunk);
                        primary_row_ids.push(stored_id);
                    }
                    self.primary_row_ids = primary_row_ids.into_iter();
                }
            }
        }
    }
}
