use std::io::Cursor;

use crate::{
    error::Error,
    execution::executor::{ExecutionContext, Executor, encode_secondary_key},
    relation::{schema::Schema, tuple::Tuple},
    storage::bptree::BpTree,
};

/// A physical mutation executor that inserts tuples into the database. It pulls tuples
/// from its child, generates a monotonic primary key, writes the encoded tuples to the
/// primary BTree, and updates all associated secondary indexes.
pub struct InsertExecutor {
    /// The child operator yielding the tuples to be inserted.
    child: Box<dyn Executor>,
    /// The name of the target table for insertion.
    table_name: String,
    /// The schema of the target table.
    schema: Schema,
}

impl InsertExecutor {
    /// Constructor.
    pub fn new(child: Box<dyn Executor>, table_name: String, schema: Schema) -> Self {
        Self {
            child,
            table_name,
            schema,
        }
    }
}

impl Executor for InsertExecutor {
    fn next(&mut self, ctx: &mut ExecutionContext) -> Result<Option<Tuple>, Error> {
        let Some(tuple) = self.child.next(ctx)? else {
            return Ok(None);
        };
        let next_row_id = ctx
            .catalog
            .read()
            .generate_next_row_id(&self.table_name)?;

        let primary_root_id = ctx
            .catalog
            .read()
            .get_table_root(&self.table_name)?;

        let primary_tree = BpTree::new(ctx.buffer_pool, primary_root_id);
        ctx.block_buffer.clear();

        let mut cursor = Cursor::new(&mut ctx.block_buffer);
        tuple.encode(&self.schema, &mut cursor)?;

        let payload = ctx.block_buffer.clone();
        primary_tree.insert(next_row_id, payload, ctx.txn_id)?;

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
            let col_val = &tuple.values[col_idx];

            let sec_key = encode_secondary_key(col_val);
            let sec_tree = BpTree::new(ctx.buffer_pool, index_meta.root_page_id);

            // The payload for a secondary index is an array or primary row ids.
            let initial_payload = next_row_id.to_le_bytes().to_vec();
            match sec_tree.insert(sec_key, initial_payload, ctx.txn_id) {
                Err(Error::DuplicateKey(_)) => {
                    if index_meta.is_unique {
                        return Err(Error::ConstraintViolation(format!(
                            "duplicate key value violates unique constraint on index: '{}'",
                            index_meta.index_name,
                        )));
                    }
                    // For non-unique indexes we append to the array.
                    let mut existing_payload = sec_tree.find_record(sec_key)?.ok_or_else(|| {
                        Error::CorruptPage("duplicate key hit but payload missing".into())
                    })?;
                    existing_payload.extend_from_slice(&next_row_id.to_le_bytes());
                    sec_tree.update(sec_key, existing_payload, ctx.txn_id)?;
                }
                Err(other_err) => return Err(other_err),
                Ok(_) => {}
            }
        }
        Ok(Some(tuple))
    }
}
