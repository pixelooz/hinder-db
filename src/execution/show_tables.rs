use std::vec::IntoIter;

use crate::{
    error::Error,
    execution::{ExecutionContext, Executor},
    relation::{tuple::Tuple, types::Value},
};

/// Executor to create a iterable of table names as Tuples and serves them into
/// the volcano pipeline.
pub struct ShowTablesExecutor {
    output_iter: Option<IntoIter<Tuple>>,
}

impl ShowTablesExecutor {
    /// Constructor.
    pub fn new() -> Self {
        Self { output_iter: None }
    }
}

impl Executor for ShowTablesExecutor {
    fn next(&mut self, ctx: &mut ExecutionContext) -> Result<Option<Tuple>, Error> {
        if let Some(iterator) = &mut self.output_iter {
            return Ok(iterator.next());
        }
        let catalog = ctx.catalog.read();
        let mut tuples = Vec::new();

        for table_name in catalog.table_schemas().keys() {
            tuples.push(Tuple::new(vec![Value::Varchar(table_name.clone())]));
        }
        let iterator = self.output_iter.insert(tuples.into_iter());
        Ok(iterator.next())
    }

    fn reset(&mut self) -> Result<(), Error> {
        self.output_iter = None;
        Ok(())
    }
}
