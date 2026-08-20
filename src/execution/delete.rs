use crate::{
    error::Error,
    execution::{ExecutionContext, Executor},
    relation::{schema::Schema, tuple::Tuple},
    storage::bptree::BpTree,
};

/// The `Executor` for delete operations.
pub struct DeleteExecutor {
    child: Box<dyn Executor>,
    table_name: String,
    schema: Schema,
}

impl DeleteExecutor {
    /// Constructor.
    pub fn new(child: Box<dyn Executor>, table_name: String, schema: Schema) -> Self {
        Self {
            child,
            table_name,
            schema,
        }
    }
}

impl Executor for DeleteExecutor {
    fn next(&mut self, ctx: &mut ExecutionContext) -> Result<Option<Tuple>, Error> {
        let Some(tuple) = self.child.next(ctx)? else {
            return Ok(None);
        };
        let row_id = tuple.row_id.ok_or_else(|| {
            Error::CorruptPage("Tuple passed to DeleteExecutor is missing row_id".into())
        })?;
        let primary_root_id = ctx
            .catalog
            .read()
            .get_table_root(&self.table_name)?;

        let primary_tree = BpTree::new(ctx.buffer_pool, primary_root_id);
        primary_tree.delete_record(row_id, ctx.txn_id)?;

        let Some(indexes) = ctx
            .catalog
            .read()
            .get_table_indexes(&self.table_name)
            .cloned()
        else {
            return Ok(Some(tuple));
        };
        for (_, index_meta) in indexes {
            let col_idx = self.schema.get_col_idx(&index_meta.column_name)?;
            let sec_key = tuple.values[col_idx].to_index_key();

            let sec_tree = BpTree::new(ctx.buffer_pool, index_meta.root_page_id);

            if let Some(existing_row_ids) = sec_tree.find_record(sec_key)? {
                let mut new_row_ids = Vec::with_capacity(existing_row_ids.len());

                // Filter out the target row_id from the id list.
                for chunk in existing_row_ids.chunks_exact(8) {
                    let enc_chunk: [u8; 8] = chunk.try_into().map_err(|_| {
                        Error::CorruptPage("invalid primary-key list chunk-length".into())
                    })?;
                    let stored_id = u64::from_le_bytes(enc_chunk);
                    if stored_id != row_id {
                        new_row_ids.extend_from_slice(chunk);
                    }
                }
                if new_row_ids.is_empty() {
                    // If there are no row ids for this encoded index key, then mark
                    // it deleted to prevent false reads.
                    sec_tree.delete_record(sec_key, ctx.txn_id)?;
                } else if new_row_ids.len() != existing_row_ids.len() {
                    // The list shrank so we need to update the disk data now.
                    sec_tree.update(sec_key, new_row_ids, ctx.txn_id)?;
                }
            }
        }
        Ok(Some(tuple))
    }
}
