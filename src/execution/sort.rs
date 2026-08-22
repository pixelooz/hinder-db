use std::{cmp, vec::IntoIter};

use crate::{
    error::Error,
    execution::{ExecutionContext, Executor},
    relation::tuple::Tuple,
};

/// A Sort Executor that materializes the entire child stream into memory, sorts it
/// based on the specified keys, and yields the sorted results.
pub struct SortExecutor {
    child: Box<dyn Executor>,

    // The Join principle about cache locality doesn't apply here since an order by
    // is usually incredibly small like 1-3 fields.
    /// A list of (column_index, is_desc) tuples.
    sort_keys: Vec<(usize, bool)>,

    /// The iterator over the sorted dataset, populated on the first call to `next`;
    output_iter: Option<IntoIter<Tuple>>,
}

impl SortExecutor {
    /// Constructor.
    #[rustfmt::skip]
    pub fn new(child: Box<dyn Executor>, sort_keys: Vec<(usize,bool)>) -> Self {
        Self { child, sort_keys, output_iter: None }
    }
}

impl Executor for SortExecutor {
    fn next(&mut self, ctx: &mut ExecutionContext) -> Result<Option<Tuple>, Error> {
        if let Some(iterator) = &mut self.output_iter {
            Ok(iterator.next())
        } else {
            let mut tuples = Vec::new();
            // Store the entire child stream in memory and pray to god the user won't
            // make a query so large we encounter OOM error.
            while let Some(tuple) = self.child.next(ctx)? {
                tuples.push(tuple);
            }
            // We iterate through the sort keys in order of precedence.
            tuples.sort_by(|t1, t2| {
                for &(idx, is_desc) in &self.sort_keys {
                    let mut cmp = t1.values[idx].cmp(&t2.values[idx]);
                    if is_desc {
                        cmp = cmp.reverse();
                    }
                    // If the current column differentiates the tuples, return the
                    // ordering. Otherwise continue to next sort-key (tie-breaker)
                    if cmp != cmp::Ordering::Equal {
                        return cmp;
                    }
                }
                cmp::Ordering::Equal
            });
            let iterator = self.output_iter.insert(tuples.into_iter());
            Ok(iterator.next())
        }
    }

    fn reset(&mut self) -> Result<(), Error> {
        self.child.reset()?;
        self.output_iter = None;
        Ok(())
    }
}
