use crate::{
    error::Error,
    execution::{ExecutionContext, Executor},
    relation::tuple::Tuple,
};

/// An executor responsible for logically dropping the table by marking it as deleted
/// in the sys_pages. Later when we add VACCUM command we'll claim that space for
/// reuse as well.
pub struct DropTableExecutor {
    table_name: String,
    has_executed: bool,
}

impl DropTableExecutor {
    /// Constructor.
    pub fn new(table_name: String) -> Self {
        Self {
            table_name,
            has_executed: false,
        }
    }
}

impl Executor for DropTableExecutor {
    fn next(&mut self, ctx: &mut ExecutionContext) -> Result<Option<Tuple>, Error> {
        if self.has_executed {
            return Ok(None);
        }
        ctx.catalog
            .write()
            .drop_table(ctx.buffer_pool, &self.table_name, ctx.txn_id)?;
        self.has_executed = false;
        Ok(None)
    }

    fn reset(&mut self) -> Result<(), Error> {
        self.has_executed = false;
        Ok(())
    }
}
