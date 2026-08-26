use std::vec::IntoIter;

use crate::{
    error::Error,
    execution::{ExecutionContext, Executor},
    relation::{tuple::Tuple, types::Value},
};

/// Executor to create a iterable of table names as Tuples and serves them into
/// the volcano pipeline.
pub struct ShowTablesExecutor {
    tables_iter: Option<IntoIter<String>>,
}

impl ShowTablesExecutor {
    /// Constructor.
    pub fn new() -> Self {
        Self { tables_iter: None }
    }
}

impl Executor for ShowTablesExecutor {
    fn next(&mut self, ctx: &mut ExecutionContext) -> Result<Option<Tuple>, Error> {
        if self.tables_iter.is_none() {
            // Fetch all the table names from the catalog cache.
            let table_names: Vec<String> =
                ctx.catalog.read().table_schemas().keys().cloned().collect();
            self.tables_iter = Some(table_names.into_iter());
        }
        if let Some(iterator) = &mut self.tables_iter
            && let Some(table_name) = iterator.next()
        {
            Ok(Some(Tuple::new(vec![Value::Varchar(table_name)])))
        } else {
            Ok(None)
        }
    }

    fn reset(&mut self) -> Result<(), Error> {
        self.tables_iter = None;
        Ok(())
    }
}
