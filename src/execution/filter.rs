use crate::{
    error::Error,
    execution::{
        evaluator::Evaluator,
        {ExecutionContext, Executor},
    },
    planner::bound_expr::BoundExpr,
    relation::{tuple::Tuple, types::Value},
};

/// A logical executor that filters tuples based on a boolean predicate. It fetches
/// tuples from its child executor, evaluates the provided AST expression against
/// each tuple, and yeilds only those evaluating to `true`.
pub struct FilterExecutor {
    /// The logical predicate to evaluate; the WHERE clause.
    predicate: BoundExpr,

    /// The child operator in the Volcano pipeline, like a SeqScan or Join.
    child: Box<dyn Executor>,
}

impl FilterExecutor {
    /// Initializes a new FilterExecutor.
    pub fn new(child: Box<dyn Executor>, predicate: BoundExpr) -> Self {
        Self { child, predicate }
    }
}

impl Executor for FilterExecutor {
    fn next(&mut self, ctx: &mut ExecutionContext) -> Result<Option<Tuple>, Error> {
        loop {
            let Some(tuple) = self.child.next(ctx)? else {
                return Ok(None); // pipeline exhausted
            };
            let eval_res = Evaluator::evaluate(&self.predicate, &tuple)?;
            match eval_res {
                // Tuple does not satisfy predicate or is Null, loop to the next one.
                Value::Boolean(false) | Value::Null => continue,
                Value::Boolean(true) => return Ok(Some(tuple)),
                _ => {
                    // The expression evaluated to a non-boolean type. Ex: "WHERE 'hello'";
                    return Err(Error::SyntaxErr(
                        "WHERE clause predicate must evaluate to a boolean".into(),
                    ));
                }
            }
        }
    }

    fn reset(&mut self) -> Result<(), Error> {
        self.child.reset()
    }
}
