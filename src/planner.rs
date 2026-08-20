use std::collections::HashMap;

use crate::{
    catalog::manager::{CatalogManager, IndexMeta},
    error::Error,
    execution::{
        Executor,
        create::{CreateExecutor, CreateOperation},
        delete::DeleteExecutor,
        emit::EmitExecutor,
        filter::FilterExecutor,
        index::{IndexScanExecutor, IndexType},
        insert::InsertExecutor,
        iterator::BpTreeIterator,
        seq_scan::SeqScanExecutor,
        update::{ExecAssignment, UpdateExecutor},
        value::ValuesExecutor,
    },
    planner::bound_expr::BoundExpr,
    relation::{
        schema::{Column, Schema},
        types::{DataType, Value},
    },
    sql::{
        ast::{
            BinaryOperator, CreateIndex, CreateTable, Delete, Expr, Insert, Select, Statement,
            TableReference, Update,
        },
        parser::AstLiteral,
    },
};

pub(crate) mod bound_expr;

/// Represents a completely planned execution tree and its metadata.
pub struct QueryPlan {
    /// The root node of the volcano execution pipeline.
    pub executor: Box<dyn Executor>,

    /// The schema of this pipeline/query.
    pub schema: Schema,

    /// Whether this query is DQL (SELECT) or not (INSERT/UPDATE/DELETE/CREATE).
    pub is_query: bool,
}

/// The Query Transpiler. It is responsible for Semantic Analysis of the given Query
/// ensuring its correctness and binding it to intermediate data.
/// It validates Ast nodes against the Catalog, performs type checking, and constructs
/// a ready to run Volcano execution pipeline.
pub struct Planner<'a> {
    catalog: &'a CatalogManager,
}

impl<'a> Planner<'a> {
    /// Initializes a new `Planner` with a reference to the global catalog.
    pub fn new(catalog: &'a CatalogManager) -> Self {
        Self { catalog }
    }

    /// Converts a logical statement into a physical execution tree.
    pub fn plan_statement(&self, stmt: Statement) -> Result<QueryPlan, Error> {
        use Statement::*;
        match stmt {
            CreateTable(stmt) => self.plan_create_table(stmt),
            CreateIndex(stmt) => self.plan_create_index(stmt),
            Select(stmt) => self.plan_select(stmt),
            Insert(stmt) => self.plan_insert(stmt),
            Delete(stmt) => self.plan_delete(stmt),
            Update(stmt) => self.plan_update(stmt),

            _ => Err(Error::NotImplementedYet(
                "only select statements are currently supported by the planner".into(),
            )),
        }
    }

    /// Translates the Ast `CreateTable` node into a logical `Schema` and routes it to
    /// the `CreateExecutor`
    fn plan_create_table(&self, stmt: CreateTable) -> Result<QueryPlan, Error> {
        let mut columns = Vec::with_capacity(stmt.columns.len());
        for col_def in stmt.columns {
            columns.push(Column::new(col_def.name, col_def.data_type, col_def.length));
        }
        let schema = Schema::new(columns);

        let operation = CreateOperation::Table {
            table_name: stmt.table_name,
            schema,
        };
        let query_plan = QueryPlan {
            executor: Box::new(CreateExecutor::new(operation)),
            schema: Schema::new(vec![]),
            is_query: false,
        };
        Ok(query_plan)
    }

    /// Converts the `CreateIndex` Ast and passes it to the `CreateExecutor`.
    fn plan_create_index(&self, stmt: CreateIndex) -> Result<QueryPlan, Error> {
        let operation = CreateOperation::Index {
            table_name: stmt.table_name,
            index_name: stmt.index_name,
            is_unique: stmt.unique,
            column_name: stmt.column_name,
        };
        let query_plan = QueryPlan {
            executor: Box::new(CreateExecutor::new(operation)),
            schema: Schema::new(vec![]),
            is_query: false,
        };
        Ok(query_plan)
    }

    /// Analyses a bound predicate to find an exact-match condition on an indexed column.
    fn find_indexable_condition<'b>(
        &self,
        bound_expr: &BoundExpr,
        schema: &Schema,
        indexes: &'b HashMap<String, IndexMeta>,
    ) -> Option<(&'b IndexMeta, Value)> {
        use BinaryOperator::*;
        use BoundExpr::*;

        match bound_expr {
            BinaryOp { left, op: Eq, right } => {
                if let ColumnRef { col_idx, .. } = **left
                    && let Constant(val) = &**right
                {
                    let col_name = &schema.columns[col_idx].name;
                    if let Some(meta) = indexes.get(col_name) {
                        return Some((meta, val.clone()));
                    }
                }
                if let ColumnRef { col_idx, .. } = **right
                    && let Constant(val) = &**left
                {
                    let col_name = &schema.columns[col_idx].name;
                    if let Some(meta) = indexes.get(col_name) {
                        return Some((meta, val.clone()));
                    }
                }
                None
            }
            BinaryOp { left, op: And, right } => self
                .find_indexable_condition(left, schema, indexes)
                .or_else(|| self.find_indexable_condition(right, schema, indexes)),
            _ => None,
        }
    }

    /// Determines the optimal scan type for the query type. If a where predicate is
    /// present it attempts to build an indexed pipeline if an index is present on
    /// any of the `ColumnReference`s if given. Otherwise falls back to sequential
    /// scan executor.
    fn build_index_or_seq_pipeline(
        &self,
        table_name: &str,
        schema: &Schema,
        bound_predicate: Option<&BoundExpr>,
    ) -> Result<Box<dyn Executor>, Error> {
        let primary_root_id = self.catalog.get_table_root(table_name)?;

        // Attempt to use an Index scan if where clause is present.
        // Look for an exact match predicate on an indexed column.
        if let Some(predicate) = bound_predicate
            && let Some(indexes) = self.catalog.get_table_indexes(table_name)
            && let Some((index_meta, value)) =
                self.find_indexable_condition(predicate, schema, indexes)
        {
            let search_key = value.to_index_key();
            let scan_type = IndexType::Secondary { primary_root_id };
            let iterator = BpTreeIterator::new_at_key(index_meta.root_page_id, search_key);
            let index_executor = Box::new(IndexScanExecutor::new(
                iterator,
                scan_type,
                search_key,
                schema.clone(),
            ));
            return Ok(index_executor);
        }
        let iterator = BpTreeIterator::new(primary_root_id);
        Ok(Box::new(SeqScanExecutor::new(iterator, schema.clone())))
    }

    /// Plans a SELECT query, building a pipeline of SeqScan -> Filter.
    fn plan_select(&self, stmt: Select) -> Result<QueryPlan, Error> {
        let table_name = match stmt.from {
            Some(TableReference::BaseTable { name, .. }) => name,
            Some(TableReference::Join(_)) => {
                return Err(Error::NotImplementedYet(
                    "Joins are not yet supported".into(),
                ));
            }
            None => {
                return Err(Error::NotImplementedYet(
                    "SELECT without FROM is not supported".into(),
                ));
            }
        };
        let base_schema = self.catalog.get_table_schema(&table_name)?;

        // Bind the where clause early so we can use it for building the optimal pipeline.
        let bound_predicate = match stmt.where_clause {
            Some(where_expr) => {
                Some(self.bind_expr(&where_expr, base_schema, Some(DataType::Boolean), None)?)
            }
            None => None,
        };
        let mut pipeline =
            self.build_index_or_seq_pipeline(&table_name, base_schema, bound_predicate.as_ref())?;

        if let Some(predicate) = bound_predicate {
            pipeline = Box::new(FilterExecutor::new(pipeline, predicate));
        }
        let is_star = matches!(
            &stmt.select_list[0].expr,
            Expr::Column(col_ref) if col_ref.column_name == "*"
        );
        if stmt.select_list.len() == 1 && is_star {
            let query_plan = QueryPlan {
                executor: pipeline,
                schema: base_schema.clone(),
                is_query: true,
            };
            return Ok(query_plan);
        }
        let mut emit_exprs = Vec::with_capacity(stmt.select_list.len());
        let mut output_cols = Vec::with_capacity(stmt.select_list.len());

        for (idx, derived_cols) in stmt.select_list.into_iter().enumerate() {
            let inferred_type = self.infer_expr_type(&derived_cols.expr, base_schema)?;
            let bound_expr =
                self.bind_expr(&derived_cols.expr, base_schema, inferred_type, None)?;

            let col_name = derived_cols
                .alias
                .unwrap_or_else(|| match derived_cols.expr {
                    Expr::Column(col_ref) => col_ref.column_name,
                    _ => format!("col_{}", idx),
                });
            let data_type = inferred_type.unwrap_or(DataType::BigInt);

            // This lets us use user provided aliases and we infer the datatype from the schema.
            // For numbers, its not needed we differentiate between INT and BIGINT in the output
            // schema if it can't be inferred from the schema so we just use BIGINT.
            output_cols.push(Column::new(col_name, data_type, None));
            emit_exprs.push(bound_expr);
        }
        // Build the project layer and the potentially aliased output schema.
        pipeline = Box::new(EmitExecutor::new(pipeline, emit_exprs));
        let output_schema = Schema::new(output_cols);

        let query_plan = QueryPlan {
            executor: pipeline,
            schema: output_schema,
            is_query: true,
        };
        Ok(query_plan)
    }

    /// Converts the logical Insert Ast node into a physical ValuesExec -> InsertExec
    /// pipeline. Confirms the validity of the statement and handles implicit NULL
    /// padding for intentionally omitted columns.
    fn plan_insert(&self, stmt: Insert) -> Result<QueryPlan, Error> {
        let schema = self.catalog.get_table_schema(&stmt.table_name)?;

        // If the query omitted the column list (Ex: INSERT INTO users VALUE ...;).
        // We automatically target all the columns in their schema order.
        let target_columns = if stmt.columns.is_empty() {
            (0..schema.columns.len()).collect()
        } else {
            let mut indices = Vec::with_capacity(stmt.columns.len());
            for col_name in stmt.columns {
                indices.push(schema.get_col_idx(&col_name)?);
            }
            indices
        };
        let mut bound_values = Vec::with_capacity(target_columns.len());

        for row_exprs in stmt.values {
            if row_exprs.len() != target_columns.len() {
                return Err(Error::SyntaxErr(format!(
                    "INSERT has more/less expressions than target columns. expected {}, got {}",
                    target_columns.len(),
                    row_exprs.len(),
                )));
            }
            // A physical row template filled with nulls so that the values being
            // omitted intentionally compared to original schema will be filled
            // with nulls and won't error any of the encoders because of missing
            // data.
            let mut physical_row = vec![BoundExpr::Constant(Value::Null); schema.columns.len()];
            for (&physical_idx, expr) in target_columns.iter().zip(row_exprs.iter()) {
                let column = &schema.columns[physical_idx];
                let bound_expr =
                    self.bind_expr(expr, schema, Some(column.data_type), column.length)?;
                physical_row[physical_idx] = bound_expr;
            }
            bound_values.push(physical_row);
        }
        let values_executor = Box::new(ValuesExecutor::new(bound_values));
        let insert_executor = Box::new(InsertExecutor::new(
            values_executor,
            stmt.table_name,
            schema.clone(),
        ));
        let query_plan = QueryPlan {
            executor: insert_executor,
            schema: Schema::new(vec![]),
            is_query: false,
        };
        Ok(query_plan)
    }

    /// Translates the Ast Delete node into a physical `SeqScan -> Filter -> Delete` pipeline.
    /// Binds the optional where predicate if present.
    fn plan_delete(&self, stmt: Delete) -> Result<QueryPlan, Error> {
        let schema = self.catalog.get_table_schema(&stmt.table_name)?;

        // Bind the where clause early so we can use it for building the optimal pipeline.
        let bound_predicate = match stmt.where_clause {
            Some(where_expr) => {
                Some(self.bind_expr(&where_expr, schema, Some(DataType::Boolean), None)?)
            }
            None => None,
        };
        let mut pipeline =
            self.build_index_or_seq_pipeline(&stmt.table_name, schema, bound_predicate.as_ref())?;

        if let Some(predicate) = bound_predicate {
            pipeline = Box::new(FilterExecutor::new(pipeline, predicate));
        }
        let delete_executor = Box::new(DeleteExecutor::new(
            pipeline,
            stmt.table_name,
            schema.clone(),
        ));
        let query_plan = QueryPlan {
            executor: delete_executor,
            schema: Schema::new(vec![]),
            is_query: false,
        };
        Ok(query_plan)
    }

    /// Translates the Ast Update node into a physical `SeqScan -> Filter -> Update` pipeline.
    /// Performs type checking on all the SET assignments against the schema.
    fn plan_update(&self, stmt: Update) -> Result<QueryPlan, Error> {
        let schema = self.catalog.get_table_schema(&stmt.table_name)?;

        // Bind the where clause early so we can use it for building the optimal pipeline.
        let bound_predicate = match stmt.where_clause {
            Some(where_expr) => {
                Some(self.bind_expr(&where_expr, schema, Some(DataType::Boolean), None)?)
            }
            None => None,
        };
        let mut pipeline =
            self.build_index_or_seq_pipeline(&stmt.table_name, schema, bound_predicate.as_ref())?;

        if let Some(predicate) = bound_predicate {
            pipeline = Box::new(FilterExecutor::new(pipeline, predicate));
        }
        let mut exec_assignments = Vec::with_capacity(stmt.assignments.len());

        // Binds and type-checks the SET assignments.
        for ast_assign in stmt.assignments {
            let col_idx = schema.get_col_idx(&ast_assign.column_name)?;
            let column = &schema.columns[col_idx];

            // Binds the assigned value to the datatype of the column and errs
            // immediately if the data_types are different than expected.
            // `SET age = 'string'` will return a SyntaxError.
            let bound_expr = self.bind_expr(
                &ast_assign.value,
                schema,
                Some(column.data_type),
                column.length,
            )?;
            exec_assignments.push(ExecAssignment {
                col_idx,
                expr: bound_expr,
            });
        }
        let update_executor = Box::new(UpdateExecutor::new(
            pipeline,
            stmt.table_name,
            schema.clone(),
            exec_assignments,
        ));
        let query_plan = QueryPlan {
            executor: update_executor,
            schema: Schema::new(vec![]),
            is_query: false,
        };
        Ok(query_plan)
    }
}

impl<'a> Planner<'a> {
    /// Recursively walks an Ast Expression validating it against the Schema, enforces
    /// type coercion, and converts it into a `BoundExpr` type.
    fn bind_expr(
        &self,
        expr: &Expr,
        schema: &Schema,
        expected_type: Option<DataType>,
        max_length: Option<u32>,
    ) -> Result<BoundExpr, Error> {
        match expr {
            Expr::Column(col_ref) => {
                let col_idx = schema.get_col_idx(&col_ref.column_name)?;
                let data_type = schema.columns[col_idx].data_type;
                Ok(BoundExpr::ColumnRef { col_idx, data_type })
            }
            Expr::Literal(ast_lit) => {
                let val = self.bind_literal(ast_lit, expected_type, max_length)?;
                Ok(BoundExpr::Constant(val))
            }
            Expr::BinaryOp { left, op, right } => {
                let right_type = self.infer_expr_type(right, schema)?;
                let left_type = self.infer_expr_type(left, schema)?;

                // If one side has a definitively known type, propagate that type
                // to the other side to enforce coercion.
                let type_for_left = right_type.or(expected_type);
                let type_for_right = left_type.or(expected_type);

                let bound_left = self.bind_expr(left, schema, type_for_left, max_length)?;
                let bound_right = self.bind_expr(right, schema, type_for_right, max_length)?;

                Ok(BoundExpr::BinaryOp {
                    left: Box::new(bound_left),
                    op: *op,
                    right: Box::new(bound_right),
                })
            }
            Expr::Average(_) | Expr::Count(_) => Err(Error::SyntaxErr(
                "aggregate functions cannot be used in a linear context".into(),
            )),
        }
    }

    /// If possible, peeks at an Ast Expression to determine its intrinsic data type.
    fn infer_expr_type(&self, expr: &Expr, schema: &Schema) -> Result<Option<DataType>, Error> {
        match expr {
            Expr::Column(col_ref) => {
                let col_idx = schema.get_col_idx(&col_ref.column_name)?;
                Ok(Some(schema.columns[col_idx].data_type))
            }
            /* Literals have flexible types (Ex: AstLiteral::Int can be Int or BigInt).
            Returning None allows the other side to dictate the coercion. */
            Expr::Literal(_) => Ok(None),
            Expr::BinaryOp { op, .. } => {
                use BinaryOperator::*;
                match op {
                    And | Or | Neq | Eq | Gt | Lt | Gte | Lte => Ok(Some(DataType::Boolean)),
                }
            }
            _ => Ok(None),
        }
    }

    /// Coerces a raw Ast literal into a runtime `Value` ensuring Schema compatibility.
    fn bind_literal(
        &self,
        ast_lit: &AstLiteral,
        expected_type: Option<DataType>,
        max_length: Option<u32>,
    ) -> Result<Value, Error> {
        match ast_lit {
            AstLiteral::String(strs) => match expected_type {
                Some(DataType::Varchar) | None => {
                    if let Some(limit) = max_length
                        && strs.chars().count() > limit as usize
                    {
                        return Err(Error::ConstraintViolation(format!(
                            "VARCHAR limit exceeded: expected <= {}, got {}",
                            limit,
                            strs.chars().count()
                        )));
                    }
                    Ok(Value::Varchar(strs.clone()))
                }
                Some(dt) => Err(Error::SyntaxErr(format!(
                    "type mismatch; cannot coerce string literal to {:?}",
                    dt
                ))),
            },
            AstLiteral::Int(val) => match expected_type {
                Some(DataType::BigInt) | None => Ok(Value::BigInt(*val)),
                Some(DataType::Int) => {
                    let val = i32::try_from(*val).map_err(|_| {
                        Error::SyntaxErr(format!(
                            // ? idk if this should be a syntax error.
                            "integer literal {} overflows INT type constraints",
                            val
                        ))
                    })?;
                    Ok(Value::Int(val))
                }
                Some(dt) => Err(Error::SyntaxErr(format!(
                    "type mismatch; cannot coerce integer literal to {:?}",
                    dt
                ))),
            },
            AstLiteral::Null => Ok(Value::Null),
            AstLiteral::Boolean(val) => match expected_type {
                Some(DataType::Boolean) | None => Ok(Value::Boolean(*val)),
                Some(dt) => Err(Error::SyntaxErr(format!(
                    "type mismatch; cannot coerce boolean literal to {:?}",
                    dt
                ))),
            },
        }
    }
}
