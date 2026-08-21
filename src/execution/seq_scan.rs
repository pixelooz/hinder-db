use std::io::Cursor;

use crate::{
    error::Error,
    execution::{
        iterator::BpTreeIterator,
        {ExecutionContext, Executor},
    },
    relation::{schema::Schema, tuple::Tuple},
};

/// The logical iterator that performs a full table scans.
#[derive(Debug)]
pub struct SeqScanExecutor {
    iterator: BpTreeIterator,
    schema: Schema,
}

impl SeqScanExecutor {
    /// Initializes a new sequential scan executor.
    pub fn new(iterator: BpTreeIterator, schema: Schema) -> Self {
        Self { iterator, schema }
    }
}

impl Executor for SeqScanExecutor {
    fn next(&mut self, ctx: &mut ExecutionContext) -> Result<Option<Tuple>, Error> {
        let row_id = match self
            .iterator
            .next(ctx.buffer_pool, &mut ctx.block_buffer)?
        {
            Some(row_id) => row_id,
            None => return Ok(None),
        };
        let mut cursor = Cursor::new(&ctx.block_buffer);
        let mut tuple = Tuple::decode(&self.schema, &mut cursor)?;
        tuple.row_id = Some(row_id);
        Ok(Some(tuple))
    }

    fn reset(&mut self) -> Result<(), Error> {
        self.iterator.reset();
        Ok(())
    }
}
