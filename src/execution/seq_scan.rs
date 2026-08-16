use std::io::Cursor;

use crate::{
    error::Error,
    execution::{
        executor::{ExecutionContext, Executor},
        iterator::BpTreeIterator,
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
        if !self
            .iterator
            .next(ctx.buffer_pool, &mut ctx.block_buffer)?
        {
            return Ok(None);
        }
        let mut cursor = Cursor::new(&ctx.block_buffer);
        let tuple = Tuple::decode(&self.schema, &mut cursor)?;
        Ok(Some(tuple))
    }
}
