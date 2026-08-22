use crate::{
    error::Error,
    execution::{ExecutionContext, Executor},
    relation::tuple::Tuple,
};

/// An early terminal executor that can skip `offset` tuples and yield up to `limit`
/// tuples. Once the limit is reached, it short-circuits the Volcano pipeline.
pub struct LimitOffsetExecutor {
    child: Box<dyn Executor>,
    limit: Option<usize>,
    offset: Option<usize>,
    skipped: usize,
    yielded: usize,
}

impl LimitOffsetExecutor {
    /// Constructor.
    #[rustfmt::skip]
    pub fn new(child: Box<dyn Executor>, limit: Option<usize>, offset: Option<usize>) -> Self {
        Self { child, limit, offset, skipped: 0, yielded: 0 }
    }
}

impl Executor for LimitOffsetExecutor {
    fn next(&mut self, ctx: &mut ExecutionContext) -> Result<Option<Tuple>, Error> {
        // Return if we have already reached the limit.
        if let Some(limit) = self.limit
            && self.yielded >= limit
        {
            return Ok(None);
        }
        // Skip rows until offset is met.
        if let Some(offset) = self.offset {
            while self.skipped < offset {
                if self.child.next(ctx)?.is_none() {
                    return Ok(None); // Stream exhausted before offset was met.
                }
                self.skipped += 1;
            }
        }
        // Yield the next row and track it.
        if let Some(tuple) = self.child.next(ctx)? {
            self.yielded += 1;
            return Ok(Some(tuple));
        }
        Ok(None)
    }

    fn reset(&mut self) -> Result<(), Error> {
        self.child.reset()?;
        self.yielded = 0;
        self.skipped = 0;
        Ok(())
    }
}
