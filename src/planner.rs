use crate::{
    error::Error,
    execution::{
        Executor,
        aggregate::{AggregateFunc, HashAggregateExecutor},
        create::{CreateExecutor, CreateOperation},
        delete::DeleteExecutor,
        drop::DropTableExecutor,
        emit::EmitExecutor,
        filter::FilterExecutor,
        index::{IndexScanExecutor, IndexType},
        insert::InsertExecutor,
        iterator::BpTreeIterator,
        join::BlockNestedLoopJoinExecutor,
        limit::LimitOffsetExecutor,
        seq_scan::SeqScanExecutor,
        show_indexes::ShowIndexesExecutor,
        show_tables::ShowTablesExecutor,
        sort::SortExecutor,
        update::{ExecAssignment, UpdateExecutor},
        value::ValuesExecutor,
    },
    manager::CatalogManager,
    planner::bound_expr::BoundExpr,
    relation::{
        schema::{Column, Schema},
        types::{DataType, Value},
    },
    sql::{
        ast::{
            BinaryOperator, ColumnReference, CreateIndex, CreateTable, Delete, Expr, Insert,
            Select, Statement, TableReference, Update,
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
            DropTable(table_name) => Ok(QueryPlan {
                executor: Box::new(DropTableExecutor::new(table_name)),
                schema: Schema::new(vec![]),
                is_query: false,
            }),
            ShowTables => {
                let column = Column::new(None, "table_name", DataType::Varchar, None, false);
                let schema = Schema::new(vec![column]);
                Ok(QueryPlan {
                    executor: Box::new(ShowTablesExecutor::new()),
                    schema,
                    is_query: true,
                })
            }
            ShowIndexes => {
                let schema = Schema::new(vec![
                    Column::new(None, "table_name", DataType::Varchar, None, false),
                    Column::new(None, "index_name", DataType::Varchar, None, false),
                    Column::new(None, "column_name", DataType::Varchar, None, false),
                    Column::new(None, "is_unique", DataType::Boolean, None, false),
                ]);
                Ok(QueryPlan {
                    executor: Box::new(ShowIndexesExecutor::new()),
                    schema,
                    is_query: true,
                })
            }
            _ => Err(Error::NotImplementedYet(
                "only select statements are currently supported by the planner".into(),
            )),
        }
    }

    /// Translates the Ast `CreateTable` node into a logical `Schema` and routes it to
    /// the `CreateExecutor`
    fn plan_create_table(&self, stmt: CreateTable) -> Result<QueryPlan, Error> {
        let mut columns = Vec::with_capacity(stmt.columns.len());

        for col_def in &stmt.columns {
            if col_def.is_primary_key
                && !matches!(col_def.data_type, DataType::BigInt | DataType::Int)
            {
                return Err(Error::SyntaxErr(format!(
                    "PRIMARY KEY must be INT or BIGINT. found: {:?}",
                    col_def.data_type
                )));
            }
        }
        for col_def in stmt.columns {
            columns.push(Column::new(
                None,
                col_def.name,
                col_def.data_type,
                col_def.length,
                col_def.is_primary_key,
            ));
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

    /// Analyzes the given `bound_expr` to extract an exact-match or range condition.
    /// Returns the physical column index, the operator, and the comparison value.
    fn extract_predicate(&self, bound_expr: &BoundExpr) -> Option<(usize, BinaryOperator, Value)> {
        match bound_expr {
            BoundExpr::BinaryOp { left, op, right } => {
                if matches!(
                    op,
                    BinaryOperator::Eq
                        | BinaryOperator::Gt
                        | BinaryOperator::Gte
                        | BinaryOperator::Lt
                        | BinaryOperator::Lte
                ) {
                    // Case 1: [Column Op Constant] (WHERE age >= 20)
                    if let BoundExpr::ColumnRef { col_idx, .. } = **left
                        && let BoundExpr::Constant(val) = &**right
                    {
                        return Some((col_idx, *op, val.clone()));
                    }
                    // Case 2: [Constant Op Column] (WHERE 20 <= age)
                    if let BoundExpr::ColumnRef { col_idx, .. } = **right
                        && let BoundExpr::Constant(val) = &**left
                    {
                        // Flip the operator. (20 <= age) -> (age >= 20)
                        let opp_op = match op {
                            BinaryOperator::Eq => BinaryOperator::Eq,
                            BinaryOperator::Neq => BinaryOperator::Neq,
                            BinaryOperator::Gt => BinaryOperator::Lt,
                            BinaryOperator::Gte => BinaryOperator::Lte,
                            BinaryOperator::Lt => BinaryOperator::Gt,
                            BinaryOperator::Lte => BinaryOperator::Gte,
                            _ => unreachable!(),
                        };
                        return Some((col_idx, opp_op, val.clone()));
                    }
                }
                // If it wasn't a valid condition, checking if its an And condition
                if *op == BinaryOperator::And {
                    return self
                        .extract_predicate(left)
                        .or_else(|| self.extract_predicate(right));
                }
                None
            }
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

        // Attempt to use an Index scan if where clause is present. Look for an exact
        // match predicate on an indexed column.
        if let Some(predicate) = bound_predicate
            && let Some((col_idx, op, value)) = self.extract_predicate(predicate)
        {
            let search_key = match value {
                Value::BigInt(val) => val as u64,
                Value::Int(val) => val as u64,
                _ => {
                    return Err(Error::SyntaxErr(format!(
                        "PRIMARY KEY must be INT or BIGINT, found: {:?}",
                        value
                    )));
                }
            };
            let start_key = match op {
                BinaryOperator::Eq | BinaryOperator::Gt | BinaryOperator::Gte => Some(search_key),
                BinaryOperator::Lt | BinaryOperator::Lte => None, // Start at the leftmost leaf.
                _ => unreachable!("OR and AND should not have been encountered."),
            };
            // Check if the predicate (where clause) targets the primary_key.
            if Some(col_idx) == schema.primary_key_idx {
                let scan_type = IndexType::Primary;
                let iterator = BpTreeIterator::new_at_key(primary_root_id, start_key);
                let index_executor = Box::new(IndexScanExecutor::new(
                    iterator,
                    scan_type,
                    search_key,
                    op,
                    schema.clone(),
                ));
                return Ok(index_executor);
            }
            let col_name = &schema.columns[col_idx].name;
            let search_key = value.to_index_key();
            if let Some(indexes) = self.catalog.table_indexes(table_name)
                && let Some(meta) = indexes.values().find(|im| im.column_name == *col_name)
            {
                let scan_type = IndexType::Secondary { primary_root_id };
                let iterator = BpTreeIterator::new_at_key(meta.root_page_id, start_key);
                let index_executor = Box::new(IndexScanExecutor::new(
                    iterator,
                    scan_type,
                    search_key,
                    op,
                    schema.clone(),
                ));
                return Ok(index_executor);
            }
        }
        let iterator = BpTreeIterator::new(primary_root_id);
        Ok(Box::new(SeqScanExecutor::new(iterator, schema.clone())))
    }

    /// Recursively computes the logical schema for a typical join operation from a
    /// typical `TableReference` tree.
    fn compute_table_reference_schema(&self, table_ref: &TableReference) -> Result<Schema, Error> {
        match table_ref {
            TableReference::BaseTable { name, alias } => {
                let mut schema = self.catalog.table_schema(name)?.clone();
                let effective_name = alias.as_ref().unwrap_or(name).clone();
                for col in &mut schema.columns {
                    col.table_name = Some(effective_name.clone());
                }
                Ok(schema)
            }
            TableReference::Join(join) => {
                let left_schema = self.compute_table_reference_schema(&join.left)?;
                let right_schema = self.compute_table_reference_schema(&join.right)?;

                let joined_columns = left_schema
                    .columns
                    .into_iter()
                    .chain(right_schema.columns)
                    .collect();
                Ok(Schema::new(joined_columns))
            }
        }
    }

    /// Recursively builds a JOIN execution pipeline from a `TableReference` tree.
    fn plan_join_execution(&self, table_ref: &TableReference) -> Result<Box<dyn Executor>, Error> {
        match table_ref {
            TableReference::BaseTable { name, .. } => {
                let schema = self.compute_table_reference_schema(table_ref)?;
                // We have to pass NONE here because we don't have cost based optimization
                // meaning our join will, for each table, scan all the records before only
                // keeping the ones requested, by passing them through the filter executor.
                // Building the CBO will take a lot of time, I just don't have plus it
                // complicates the entire build as its an optimization only real databases
                // need and not applicable in our case. The algorithms for joins still stand.
                self.build_index_or_seq_pipeline(name, &schema, None)
            }
            TableReference::Join(join) => {
                let left_exec = self.plan_join_execution(&join.left)?;
                let right_exec = self.plan_join_execution(&join.right)?;

                let left_schema = self.compute_table_reference_schema(&join.left)?;
                let right_schema = self.compute_table_reference_schema(&join.right)?;

                #[rustfmt::skip]
                let joined_columns = left_schema.columns.iter()
                    .chain(right_schema.columns.iter())
                    .cloned()
                    .collect();

                let joined_schema = Schema::new(joined_columns);
                let bound_condition = self.bind_expr(
                    &join.condition,
                    &joined_schema,
                    Some(DataType::Boolean),
                    None,
                )?;
                Ok(Box::new(BlockNestedLoopJoinExecutor::new(
                    left_exec,
                    right_exec,
                    join.join_type,
                    bound_condition,
                    right_schema.columns.len(),
                )))
            }
        }
    }

    /// Plans a SELECT query, building a pipeline of SeqScan -> Filter.
    fn plan_select(&self, stmt: Select) -> Result<QueryPlan, Error> {
        let schema = match &stmt.from {
            Some(table_ref) => self.compute_table_reference_schema(table_ref)?,
            None => {
                return Err(Error::NotImplementedYet(
                    "SELECT without FROM is not supported".into(),
                ));
            }
        };
        // Bind the where clause early so we can use it for building the optimal pipeline.
        let bound_predicate = match stmt.where_clause {
            Some(where_expr) => {
                Some(self.bind_expr(&where_expr, &schema, Some(DataType::Boolean), None)?)
            }
            None => None,
        };
        let mut pipeline = match stmt.from.as_ref().unwrap() {
            TableReference::BaseTable { name, .. } => {
                self.build_index_or_seq_pipeline(name, &schema, bound_predicate.as_ref())?
            }
            TableReference::Join(_) => self.plan_join_execution(stmt.from.as_ref().unwrap())?,
        };
        if let Some(predicate) = bound_predicate {
            pipeline = Box::new(FilterExecutor::new(pipeline, predicate));
        }
        let is_star = matches!(
            &stmt.select_list[0].expr,
            Expr::Column(col_ref) if col_ref.column_name == "*"
        );
        let mut aggregates = Vec::new();

        for item in &stmt.select_list {
            extract_aggregates(&item.expr, &mut aggregates);
        }
        let has_aggregates = !stmt.group_by.is_empty() || !aggregates.is_empty();

        if is_star && has_aggregates {
            return Err(Error::SyntaxErr(
                "cannot use 'SELECT *' with GROUP BY or aggregate functions".into(),
            ));
        }
        if has_aggregates {
            let mut aggr_funcs = Vec::with_capacity(aggregates.len());
            for aggregate in &aggregates {
                match aggregate {
                    Expr::Count(Some(col_ref)) => {
                        let idx = schema.get_col_idx_with_qualifier(
                            col_ref.qualifier.as_deref(),
                            &col_ref.column_name,
                        )?;
                        aggr_funcs.push(AggregateFunc::Count(idx));
                    }
                    Expr::Count(None) => aggr_funcs.push(AggregateFunc::CountStar),
                    Expr::Average(col_ref) => {
                        let idx = schema.get_col_idx_with_qualifier(
                            col_ref.qualifier.as_deref(),
                            &col_ref.column_name,
                        )?;
                        let data_type = schema.columns[idx].data_type;

                        // AVG requires numeric columns.
                        if !matches!(data_type, DataType::Int | DataType::BigInt) {
                            return Err(Error::SyntaxErr(format!(
                                "function AVG() cannot be applied to column '{}' of type {:?}",
                                col_ref.column_name, data_type
                            )));
                        }
                        aggr_funcs.push(AggregateFunc::Average(idx));
                    }
                    _ => unreachable!("extract_aggregates only yields COUNT or AVERAGE"),
                }
            }
            let mut gb_indices = Vec::with_capacity(stmt.group_by.len());
            for gb in &stmt.group_by {
                gb_indices.push(
                    schema.get_col_idx_with_qualifier(gb.qualifier.as_deref(), &gb.column_name)?,
                );
            }
            pipeline = Box::new(HashAggregateExecutor::new(pipeline, gb_indices, aggr_funcs))
        }
        if stmt.select_list.len() == 1 && is_star {
            let query_plan = QueryPlan {
                executor: pipeline,
                schema: schema.clone(),
                is_query: true,
            };
            return Ok(query_plan);
        }
        let aggregate_ctx = if has_aggregates {
            Some(AggregateContext {
                group_bys: &stmt.group_by,
                aggregates: &aggregates,
            })
        } else {
            None
        };
        let mut emit_exprs = Vec::with_capacity(stmt.select_list.len());
        let mut output_cols = Vec::with_capacity(stmt.select_list.len());

        for (idx, derived_cols) in stmt.select_list.into_iter().enumerate() {
            let inferred_type = self.infer_expr_type(&derived_cols.expr, &schema)?;

            // Using bind_emit_expr to handle aggregates.
            let bound_expr = self.bind_emit_expr(
                &derived_cols.expr,
                &schema,
                inferred_type,
                aggregate_ctx.as_ref(),
            )?;
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
            output_cols.push(Column::new(None, col_name, data_type, None, false));
            emit_exprs.push(bound_expr);
        }
        // Build the projection layer and the potentially aliased output schema.
        let output_schema = Schema::new(output_cols);
        pipeline = Box::new(EmitExecutor::new(pipeline, emit_exprs));

        if !stmt.order_by.is_empty() {
            let mut sort_keys = Vec::with_capacity(stmt.order_by.len());
            for sort_spec in stmt.order_by {
                // Resolving against the output schema so that aliases work as expected.
                let idx = output_schema.get_col_idx_with_qualifier(
                    sort_spec.column.qualifier.as_deref(),
                    &sort_spec.column.column_name,
                )?;
                sort_keys.push((idx, sort_spec.descending));
            }
            pipeline = Box::new(SortExecutor::new(pipeline, sort_keys));
        }
        if stmt.limit.is_some() || stmt.offset.is_some() {
            pipeline = Box::new(LimitOffsetExecutor::new(pipeline, stmt.limit, stmt.offset));
        }
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
        let schema = self.catalog.table_schema(&stmt.table_name)?;

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
        let schema = self.catalog.table_schema(&stmt.table_name)?;

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
        let schema = self.catalog.table_schema(&stmt.table_name)?;

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
                let col_idx = schema.get_col_idx_with_qualifier(
                    col_ref.qualifier.as_deref(),
                    &col_ref.column_name,
                )?;
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
                let col_idx = schema.get_col_idx_with_qualifier(
                    col_ref.qualifier.as_deref(),
                    &col_ref.column_name,
                )?;
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

    /// A specialized binder for the Projection layer that maps AST expressions
    /// directly to the output of the HashAggregator.
    fn bind_emit_expr(
        &self,
        expr: &Expr,
        schema: &Schema,
        expected_type: Option<DataType>,
        aggregate_ctx: Option<&AggregateContext>,
    ) -> Result<BoundExpr, Error> {
        if let Some(ctx) = aggregate_ctx {
            // Is it an aggregate function? Map it to the end of the tuple.
            if let Some(pos) = ctx.aggregates.iter().position(|e| e == expr) {
                return Ok(BoundExpr::ColumnRef {
                    col_idx: ctx.group_bys.len() + pos,
                    data_type: DataType::BigInt, // COUNT and AVG always return BigInt.
                });
            }
            // Is it a column? Must be part of the GROUP BY clause.
            if let Expr::Column(col_ref) = expr {
                if let Some(pos) = ctx.group_bys.iter().position(|gb| gb == col_ref) {
                    let idx = schema.get_col_idx_with_qualifier(
                        col_ref.qualifier.as_deref(),
                        &col_ref.column_name,
                    )?;
                    let data_type = schema.columns[idx].data_type;
                    return Ok(BoundExpr::ColumnRef {
                        col_idx: pos,
                        data_type,
                    });
                } else {
                    return Err(Error::SyntaxErr(format!(
                        "column '{}' must appear in the GROUP BY clause or be used in an aggregate function",
                        col_ref.column_name,
                    )));
                }
            }
            // If it's a binary operation (COUNT(id) + 1), recursively bind the children.
            if let Expr::BinaryOp { left, op, right } = expr {
                let right_type = self.infer_expr_type(right, schema)?;
                let left_type = self.infer_expr_type(left, schema)?;

                // If one side has a definitively known type, propagate that type
                // to the other side to enforce coercion.
                let type_for_left = right_type.or(expected_type);
                let type_for_right = left_type.or(expected_type);

                let bound_left = self.bind_emit_expr(left, schema, type_for_left, aggregate_ctx)?;
                let bound_right =
                    self.bind_emit_expr(right, schema, type_for_right, aggregate_ctx)?;

                return Ok(BoundExpr::BinaryOp {
                    left: Box::new(bound_left),
                    op: *op,
                    right: Box::new(bound_right),
                });
            }
        }
        // Fallback to standard binding for non-aggregate queries or raw-literals.
        self.bind_expr(expr, schema, expected_type, None)
    }
}

/// This struct acts as a "Virtual Schema", allowing the Planner to map AST expressions
/// to the new physical indices output by the aggregator.
///
/// It holds borrowed references to the AST's `GROUP BY` clauses and the deduplicated
/// aggregate expressions (`COUNT`, `AVG`).
struct AggregateContext<'a> {
    group_bys: &'a [ColumnReference],
    aggregates: &'a [Expr],
}

/// Recursively scan an AST expression to extract aggregate functions.
///
/// If a user queries `SELECT COUNT(id), COUNT(id) / 2`, it won't record the second COUNT(id).
/// This ensures the executor only computes `COUNT(id)` exactly once saving on execution cost.
/// The `ProjectExecutor` will simply read that single computed value twice.
///
/// It appends unique `Expr::Count` and `Expr::Average` AST nodes  into the mutable `aggregates`
/// vector.
fn extract_aggregates(expr: &Expr, aggregates: &mut Vec<Expr>) {
    match expr {
        Expr::Count(_) | Expr::Average(_) => {
            if !aggregates.contains(expr) {
                aggregates.push(expr.clone());
            }
        }
        Expr::BinaryOp { left, right, .. } => {
            extract_aggregates(left, aggregates);
            extract_aggregates(right, aggregates);
        }
        _ => {}
    }
}
