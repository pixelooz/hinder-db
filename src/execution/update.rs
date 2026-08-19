use std::io::Cursor;

use crate::{
    error::Error,
    execution::{
        evaluator::Evaluator,
        {ExecutionContext, Executor, encode_secondary_key},
    },
    planner::bound_expr::BoundExpr,
    relation::{schema::Schema, tuple::Tuple},
    storage::bptree::BpTree,
};

/// Maps a column index to the new expression that should overwrite it.
pub struct ExecAssignment {
    pub col_idx: usize,
    pub expr: BoundExpr,
}

/// A mutation `Executor` that modifies tuples in the database.
pub struct UpdateExecutor {
    child: Box<dyn Executor>,
    table_name: String,
    schema: Schema,
    exec_assigns: Vec<ExecAssignment>,
}

impl UpdateExecutor {
    /// Constructor.
    #[rustfmt::skip]
    pub fn new(
        child: Box<dyn Executor>,
        table_name: String,
        schema: Schema,
        exec_assigns: Vec<ExecAssignment>,
    ) -> Self {
        Self {child, table_name, schema, exec_assigns}
    }
}

impl Executor for UpdateExecutor {
    fn next(&mut self, ctx: &mut ExecutionContext) -> Result<Option<Tuple>, Error> {
        let Some(old_tuple) = self.child.next(ctx)? else {
            return Ok(None);
        };
        let row_id = old_tuple.row_id.ok_or_else(|| {
            Error::CorruptPage("Tuple passed to UpdateExecutor is missing row id".into())
        })?;
        let mut new_tuple = old_tuple.clone();

        // Constructs the new logical tuple by applying the assignments.
        for exec_assign in &self.exec_assigns {
            let new_value = Evaluator::evaluate(&exec_assign.expr, &old_tuple)?;
            new_tuple.values[exec_assign.col_idx] = new_value;
        }
        let primary_root_id = ctx
            .catalog
            .read()
            .get_table_root(&self.table_name)?;

        let primary_tree = BpTree::new(ctx.buffer_pool, primary_root_id);
        ctx.block_buffer.clear();

        let mut cursor = Cursor::new(&mut ctx.block_buffer);
        new_tuple.encode(&self.schema, &mut cursor)?;

        let payload = ctx.block_buffer.clone();
        primary_tree.update(row_id, payload, ctx.txn_id)?;

        let Some(indexes) = ctx
            .catalog
            .read()
            .get_table_indexes(&self.table_name)
            .cloned()
        else {
            return Ok(Some(new_tuple));
        };
        for (_, index_meta) in indexes {
            let col_idx = self.schema.get_col_idx(&index_meta.column_name)?;

            let old_val = &old_tuple.values[col_idx];
            let new_val = &new_tuple.values[col_idx];

            if old_val != new_val {
                continue;
            }
            let sec_tree = BpTree::new(ctx.buffer_pool, index_meta.root_page_id);
            let old_sec_key = encode_secondary_key(old_val);

            // remove this row_id from the old key's row list.
            if let Some(existing_row_ids) = sec_tree.find_record(old_sec_key)? {
                let mut new_row_ids = Vec::with_capacity(existing_row_ids.len());

                for chunk in existing_row_ids.chunks_exact(8) {
                    let stored_id = u64::from_le_bytes(chunk.try_into().unwrap());
                    if row_id != stored_id {
                        new_row_ids.extend_from_slice(chunk);
                    }
                }
                if new_row_ids.is_empty() {
                    sec_tree.delete_record(old_sec_key, ctx.txn_id)?;
                } else if new_row_ids.len() != existing_row_ids.len() {
                    sec_tree.update(old_sec_key, new_row_ids, ctx.txn_id)?;
                }
            }
            let new_sec_key = encode_secondary_key(new_val);
            let enc_vec_row = row_id.to_le_bytes();

            // Append row_id to the new key's row list.
            match sec_tree.insert(new_sec_key, enc_vec_row.to_vec(), ctx.txn_id) {
                Ok(_) => {}
                Err(Error::DuplicateKey(_)) => {
                    if index_meta.is_unique {
                        return Err(Error::ConstraintViolation(format!(
                            "update violates unique constraint on {}",
                            row_id
                        )));
                    }
                    let mut existing_row_ids = sec_tree
                        .find_record(new_sec_key)?
                        .expect("should not be 'None' since insert returned duplicate_key");

                    existing_row_ids.extend_from_slice(&enc_vec_row);
                    sec_tree.update(new_sec_key, existing_row_ids, ctx.txn_id)?;
                }
                Err(other_err) => return Err(other_err),
            }
        }
        Ok(Some(new_tuple))
    }
}
