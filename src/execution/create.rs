use crate::{
    error::Error,
    execution::executor::{ExecutionContext, Executor},
    relation::{schema::Schema, tuple::Tuple},
};

/*  TODO: After the database starts working and can execute all the queries and supports
multiple instances with table-wide locks; optimize the code and reducing cloning. */

/// Represents a create operation and holds the required data.
pub enum CreateOperation {
    Table {
        table_name: String,
        schema: Schema,
    },
    Index {
        table_name: String,
        index_name: String,
        is_unique: bool,
        column_name: String,
    },
}

/// The `Executor` for create operation.
pub struct CreateExecutor {
    operation: CreateOperation,
    has_executed: bool,
}

impl CreateExecutor {
    /// Initializes the CreateExecutor.
    pub fn new(operation: CreateOperation) -> Self {
        Self {
            operation,
            has_executed: false,
        }
    }
}

impl Executor for CreateExecutor {
    fn next(&mut self, ctx: &mut ExecutionContext) -> Result<Option<Tuple>, Error> {
        if self.has_executed {
            return Ok(None);
        }
        let mut catalog_guard = ctx.catalog.write();

        let lsn = ctx.txn_id; // Temporary before we wire the wal up.
        match &self.operation {
            CreateOperation::Table { table_name, schema } => {
                catalog_guard.create_table(
                    ctx.buffer_pool,
                    table_name.clone(),
                    schema.clone(),
                    lsn,
                )?;
            }
            CreateOperation::Index {
                table_name,
                index_name,
                is_unique,
                column_name,
            } => {
                catalog_guard.create_index(
                    ctx.buffer_pool,
                    index_name.clone(),
                    table_name.clone(),
                    *is_unique,
                    column_name.clone(),
                    lsn,
                )?;
            }
        }
        self.has_executed = true;
        Ok(None)
    }
}
