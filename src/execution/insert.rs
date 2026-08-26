use std::io::Cursor;

use crate::{
    error::Error,
    execution::{ExecutionContext, Executor},
    relation::{
        schema::Schema,
        tuple::Tuple,
        types::{DataType, Value},
    },
    storage::bptree::BpTree,
};

/// A record mutation executor that inserts tuples into the database. It pulls tuples
/// from its child, generates a monotonic primary key if not given, writes the encoded
/// tuples to the primary BTree, and updates all associated secondary indexes.
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
        let Some(mut tuple) = self.child.next(ctx)? else {
            return Ok(None);
        };
        let mut explicit_row_id = None;

        if let Some(pk_idx) = self.schema.primary_key_idx {
            match &tuple.values[pk_idx] {
                Value::Null => {
                    // User omitted the primary key, so we auto generate it and insert.
                    let row_id = ctx.catalog.read().generate_next_row_id(&self.table_name)?;
                    let col_type = self.schema.columns[pk_idx].data_type;
                    tuple.values[pk_idx] = match col_type {
                        DataType::BigInt => Value::BigInt(row_id as i64),
                        DataType::Int => Value::Int(row_id as i32),
                        _ => unreachable!(
                            "wrong pk_type should've been caught at schema declaration"
                        ),
                    };
                    explicit_row_id = Some(row_id);
                }
                Value::Int(val) => {
                    let row_id = *val as u64;
                    explicit_row_id = Some(row_id);
                    ctx.catalog
                        .read()
                        .update_high_watermark(&self.table_name, row_id)?;
                }
                Value::BigInt(val) => {
                    let row_id = *val as u64;
                    explicit_row_id = Some(row_id);
                    ctx.catalog
                        .read()
                        .update_high_watermark(&self.table_name, row_id)?;
                }
                _ => {
                    return Err(Error::SyntaxErr(format!(
                        "PRIMARY KEY must be INT or BIGINT. found {:?}",
                        tuple.values[pk_idx],
                    )));
                }
            }
        }
        let next_row_id = match explicit_row_id {
            Some(row_id) => row_id,
            None => ctx.catalog.read().generate_next_row_id(&self.table_name)?,
        };
        let primary_root_id = ctx.catalog.read().get_table_root(&self.table_name)?;

        let primary_tree = BpTree::new(ctx.buffer_pool, primary_root_id);
        ctx.block_buffer.clear();

        let mut cursor = Cursor::new(&mut ctx.block_buffer);
        tuple.encode(&self.schema, &mut cursor)?;

        let payload = ctx.block_buffer.clone();
        primary_tree.insert(next_row_id, payload, ctx.txn_id)?;

        let Some(indexes) = ctx.catalog.read().table_indexes(&self.table_name).cloned() else {
            return Ok(Some(tuple));
        };
        for (_, index_meta) in indexes {
            let col_idx = self.schema.get_col_idx(&index_meta.column_name)?;
            let sec_key = tuple.values[col_idx].to_index_key();

            let sec_tree = BpTree::new(ctx.buffer_pool, index_meta.root_page_id);

            // The payload for a secondary index is an array or primary row ids.
            let initial_payload = next_row_id.to_le_bytes().to_vec();
            match sec_tree.insert(sec_key, initial_payload, ctx.txn_id) {
                Err(Error::DuplicateKey(_)) => {
                    if index_meta.is_unique {
                        return Err(Error::ConstraintViolation(format!(
                            "duplicate key value violates unique constraint on secondary_index: '{}'",
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

    fn reset(&mut self) -> Result<(), Error> {
        self.child.reset()
    }
}
