use crate::{
    error::Error,
    execution::{
        evaluator::Evaluator,
        {ExecutionContext, Executor},
    },
    planner::bound_expr::BoundExpr,
    relation::tuple::Tuple,
};

/// A logical executor that reshapes the tuples to match the requested SELECT list.
///
/// It pulls a physical tuple from its child, evaluates the list of expressions
/// against them, and yields a new logical tuple. This allows queries to return a
/// subset of columns, computed columns, or constants.
pub struct EmitExecutor {
    child: Box<dyn Executor>,
    exprs: Vec<BoundExpr>,
}

impl EmitExecutor {
    /// Constructor.
    pub fn new(child: Box<dyn Executor>, exprs: Vec<BoundExpr>) -> Self {
        Self { child, exprs }
    }
}

impl Executor for EmitExecutor {
    fn next(&mut self, ctx: &mut ExecutionContext) -> Result<Option<Tuple>, Error> {
        let Some(tuple) = self.child.next(ctx)? else {
            return Ok(None);
        };
        let mut values = Vec::with_capacity(self.exprs.len());

        for expr in &self.exprs {
            let value = Evaluator::evaluate(expr, &tuple)?;
            values.push(value);
        }
        Ok(Some(Tuple::new(values)))
    }
}
