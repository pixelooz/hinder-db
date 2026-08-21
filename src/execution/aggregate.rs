use std::{collections::HashMap, vec::IntoIter};

use crate::{
    error::Error,
    execution::{ExecutionContext, Executor},
    relation::{tuple::Tuple, types::Value},
};

/// Defines the supported aggregate functions accepting the distinct column index
/// for current operation.
pub enum AggregateFunc {
    CountStar,
    Count(usize),
    Average(usize),
}

/// Tracks the running state of an aggregate function for a specific group.
/// Mostly its here to support the sql standard.
#[derive(Debug, Clone)]
pub enum AggregateState {
    Count(i64),
    Average { sum: i64, count: i64 },
}

impl AggregateFunc {
    /// Returns the initial state - everything set to 0.
    fn initial_state(&self) -> AggregateState {
        match self {
            AggregateFunc::CountStar | AggregateFunc::Count(_) => AggregateState::Count(0),
            AggregateFunc::Average(_) => AggregateState::Average { sum: 0, count: 0 },
        }
    }

    /// Works somewhat like the fold in rust's std library, or like a reduce function;
    /// in our case for increments the count or "calculates" the average.
    fn accumulate(&self, state: &mut AggregateState, tuple: &Tuple) -> Result<(), Error> {
        match (self, state) {
            (AggregateFunc::CountStar, AggregateState::Count(count)) => {
                *count += 1;
            }
            (AggregateFunc::Count(idx), AggregateState::Count(count)) => {
                if !matches!(tuple.values[*idx], Value::Null) {
                    *count += 1;
                }
            }
            (AggregateFunc::Average(idx), AggregateState::Average { sum, count }) => {
                match tuple.values[*idx] {
                    Value::BigInt(val) => {
                        *sum += val;
                        *count += 1;
                    }
                    Value::Int(val) => {
                        *sum += val as i64;
                        *count += 1;
                    }
                    Value::Null => {} // AVGs ignore nulls,
                    _ => {
                        return Err(Error::SyntaxErr(
                            "AVG can only be applied to numeric columns".into(),
                        ));
                    }
                }
            }
            _ => unreachable!("how did we even get here?"),
        }
        Ok(())
    }
}

impl AggregateState {
    fn finalize(self) -> Value {
        match self {
            AggregateState::Count(count) => Value::BigInt(count),
            AggregateState::Average { sum, count } => {
                if count == 0 {
                    return Value::Null; // Avg of zero rows is NULL
                }
                Value::BigInt(sum / count)
            }
        }
    }
}

/// A pipeline breaker that group tuples and computes running aggregates.
pub struct HashAggregateExecutor {
    child: Box<dyn Executor>,
    gb_indices: Vec<usize>,
    aggr_funcs: Vec<AggregateFunc>,
    output_iter: Option<IntoIter<Tuple>>,
}

impl HashAggregateExecutor {
    /// Constructor.
    #[rustfmt::skip]
    pub fn new(child: Box<dyn Executor>, gb_indices: Vec<usize>, aggr_funcs: Vec<AggregateFunc>) -> Self {
        Self {
            child,
            gb_indices,
            aggr_funcs,
            output_iter: None,
        }
    }
}

impl Executor for HashAggregateExecutor {
    fn next(&mut self, ctx: &mut ExecutionContext) -> Result<Option<Tuple>, Error> {
        match &mut self.output_iter {
            None => {
                let mut groups: HashMap<Vec<Value>, Vec<AggregateState>> = HashMap::new();
                let mut had_input = false;

                while let Some(tuple) = self.child.next(ctx)? {
                    // Extract the grouping key/s
                    let group_key: Vec<Value> = self
                        .gb_indices
                        .iter()
                        .map(|&i| tuple.values[i].clone())
                        .collect();

                    // Initialize the aggregate state.
                    let states = groups.entry(group_key).or_insert_with(|| {
                        self.aggr_funcs
                            .iter()
                            .map(|f| f.initial_state())
                            .collect()
                    });
                    had_input = true;
                    // Update the running total.
                    for (i, aggr_func) in self.aggr_funcs.iter().enumerate() {
                        aggr_func.accumulate(&mut states[i], &tuple)?;
                    }
                }
                // Edge case: global aggregate on an empty table yields 1 row with
                // default states.
                if !had_input && self.gb_indices.is_empty() {
                    let default_states: Vec<AggregateState> = self
                        .aggr_funcs
                        .iter()
                        .map(|f| f.initial_state())
                        .collect();
                    groups.insert(vec![], default_states);
                }
                let mut results = Vec::with_capacity(groups.len());

                for (mut key, states) in groups {
                    for state in states {
                        key.push(state.finalize());
                    }
                    results.push(Tuple::new(key));
                }
                let iterator = self.output_iter.insert(results.into_iter());
                Ok(iterator.next())
            }
            Some(iterator) => Ok(iterator.next()),
        }
    }
}
