use std::vec::IntoIter;

use crate::{
    error::Error,
    execution::{ExecutionContext, Executor},
    relation::{tuple::Tuple, types::Value},
};

/// Executor to create a iterable of index data as Tuples and serves them into
/// the volcano pipeline.
pub struct ShowIndexesExecutor {
    output_iter: Option<IntoIter<Tuple>>,
}

impl ShowIndexesExecutor {
    /// Constructor.
    pub fn new() -> Self {
        Self { output_iter: None }
    }
}

impl Executor for ShowIndexesExecutor {
    fn next(&mut self, ctx: &mut ExecutionContext) -> Result<Option<Tuple>, Error> {
        if let Some(iterator) = &mut self.output_iter {
            return Ok(iterator.next());
        }
        let catalog = ctx.catalog.read();
        let mut tuples = Vec::new();

        for (table_name, indexes) in catalog.index_roots() {
            for (index_name, meta) in indexes {
                tuples.push(Tuple::new(vec![
                    Value::Varchar(table_name.clone()),
                    Value::Varchar(index_name.clone()),
                    Value::Varchar(meta.column_name.clone()),
                    Value::Boolean(meta.is_unique),
                ]));
            }
        }
        let iterator = self.output_iter.insert(tuples.into_iter());
        Ok(iterator.next())
    }

    fn reset(&mut self) -> Result<(), Error> {
        self.output_iter = None;
        Ok(())
    }
}
