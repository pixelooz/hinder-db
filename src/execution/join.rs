use std::{collections::VecDeque, iter};

use crate::{
    error::Error,
    execution::{ExecutionContext, Executor, evaluator::Evaluator},
    planner::bound_expr::BoundExpr,
    relation::{tuple::Tuple, types::Value},
    sql::ast::JoinType,
};

/// Buffer a block of tuples from the outer tables to minimize the number of times the
/// inner table needs to be rewound and scanned. Uses internal `VecDequeue` to flatten
/// the iterator state machine and produce resultant tuples.
///
/// Interesting detail: So After I was finished with the implementation of the exec,
/// I thought about merging the `left_block` and `left_match` fields from 2 vectors
/// to 1, thinking instead of two allocations and risking out of sync issues, we can
/// just have one tuple of (Tuple, bool), however, turns out it might be a "bad"
/// decision because of CPU cache locality. Tuple is a heavy struct and adding
/// bool into that will introduce 7 bytes of padding since bool only takes 1 byte
/// each. So previously where the cpu could just breeze through the bool vectors,
/// if we were to change it, it would have to perform look ups which could be
/// worse performance wise. Now, I don't care how fast this database ends up being
/// but this was a nice detail that I learnt; I don't know if it applies exactly
/// in this case but yeah.
pub struct BlockNestedLoopJoinExecutor {
    left_child: Box<dyn Executor>,
    right_child: Box<dyn Executor>,
    join_type: JoinType,
    condition: BoundExpr,

    /// The buffer for the outer table's current block.
    left_block: Vec<Tuple>,
    /// Tracks which tuples found a match.
    left_matches: Vec<bool>,

    /// A decoupled queue to yield output tuples sequentially.
    output_queue: VecDeque<Tuple>,
    block_size: usize,

    /// The exact width of the right table, used for NULL padding in LEFT JOINs.
    right_schema_len: usize,
}

impl BlockNestedLoopJoinExecutor {
    /// Constructor.
    pub fn new(
        left_child: Box<dyn Executor>,
        right_child: Box<dyn Executor>,
        join_type: JoinType,
        condition: BoundExpr,
        right_schema_len: usize,
    ) -> Self {
        Self {
            left_child,
            right_child,
            join_type,
            condition,
            left_block: Vec::with_capacity(1000),
            left_matches: Vec::with_capacity(1000),
            output_queue: VecDeque::new(),
            block_size: 1000,
            right_schema_len,
        }
    }
}

impl Executor for BlockNestedLoopJoinExecutor {
    fn next(&mut self, ctx: &mut ExecutionContext) -> Result<Option<Tuple>, Error> {
        loop {
            if let Some(tuple) = self.output_queue.pop_front() {
                return Ok(Some(tuple));
            }
            if self.left_block.is_empty() {
                for _ in 0..self.block_size {
                    match self.left_child.next(ctx)? {
                        Some(left_tuple) => self.left_block.push(left_tuple),
                        None => break, // Left child exhausted.
                    }
                }
                // If we couldn't fetch any outer tuples, the join is complete.
                if self.left_block.is_empty() {
                    return Ok(None);
                }
                // Prepare for a fresh inner table scan against this new block..
                self.right_child.reset()?;
                self.left_matches.clear();
                self.left_matches
                    .resize(self.left_block.len(), false);
            }
            if let Some(right_tuple) = self.right_child.next(ctx)? {
                for (i, left_tuple) in self.left_block.iter().enumerate() {
                    #[rustfmt::skip]
                    let joined_values = left_tuple.values.iter()
                        .chain(right_tuple.values.iter())
                        .cloned()
                        .collect();

                    let joined_tuple = Tuple::new(joined_values);
                    let eval_res = Evaluator::evaluate(&self.condition, &joined_tuple)?;
                    if Value::Boolean(true) == eval_res {
                        self.left_matches[i] = true;
                        self.output_queue.push_back(joined_tuple);
                    }
                }
            } else {
                // Inner table exhausted for this block. Handle LEFT JOIN padding.
                if self.join_type == JoinType::Left {
                    for (left_tuple, &matched) in self
                        .left_block
                        .iter()
                        .zip(self.left_matches.iter())
                    {
                        if !matched {
                            let repeat_iter = iter::repeat_n(Value::Null, self.right_schema_len);
                            #[rustfmt::skip]
                            let null_padded = left_tuple.values.iter()
                                .cloned()
                                .chain(repeat_iter)
                                .collect();

                            let padded_tuples = Tuple::new(null_padded);
                            self.output_queue.push_back(padded_tuples);
                        };
                    }
                }
                self.left_block.clear();
            }
        }
    }

    fn reset(&mut self) -> Result<(), Error> {
        self.right_child.reset()?;
        self.left_child.reset()?;
        self.left_block.clear();
        self.left_matches.clear();
        self.output_queue.clear();
        Ok(())
    }
}
