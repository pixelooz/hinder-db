use crate::{
    error::Error,
    execution::{
        evaluator::Evaluator,
        executor::{ExecutionContext, Executor},
    },
    planner::bound_expr::BoundExpr,
    relation::tuple::Tuple,
};

/// A source executor that yields literal rows of data. It takes a matrix of pre-bound
/// expressions, evaluates them into concrete `Value`s, and yields them as `Tuple`s.
#[derive(Debug)]
pub struct ValuesExecutor {
    /// The matrix of bound expressions representing the rows to be inserted.
    values: Vec<Vec<BoundExpr>>,
    /// The current row index being processed.
    cursor: usize,
}

impl ValuesExecutor {
    /// Constructor
    pub fn new(values: Vec<Vec<BoundExpr>>) -> Self {
        Self { values, cursor: 0 }
    }
}

impl Executor for ValuesExecutor {
    fn next(&mut self, _ctx: &mut ExecutionContext) -> Result<Option<Tuple>, Error> {
        if self.cursor >= self.values.len() {
            return Ok(None);
        }
        let row_exprs = &self.values[self.cursor];
        self.cursor += 1;

        let mut evaluated_values = Vec::with_capacity(row_exprs.len());
        let empty_tuple = Tuple::new(vec![]);

        for expr in row_exprs {
            let eval = Evaluator::evaluate(expr, &empty_tuple)?;
            evaluated_values.push(eval);
        }
        Ok(Some(Tuple::new(evaluated_values)))
    }
}
