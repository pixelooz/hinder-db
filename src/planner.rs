use crate::{
    catalog::manager::CatalogManager,
    error::Error,
    execution::{
        create::{CreateExecutor, CreateOperation},
        delete::DeleteExecutor,
        executor::Executor,
        filter::FilterExecutor,
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
    pub fn plan_statement(&self, stmt: Statement) -> Result<Box<dyn Executor>, Error> {
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
    fn plan_create_table(&self, stmt: CreateTable) -> Result<Box<dyn Executor>, Error> {
        let mut columns = Vec::with_capacity(stmt.columns.len());
        for col_def in stmt.columns {
            columns.push(Column::new(col_def.name, col_def.data_type, col_def.length));
        }
        let schema = Schema::new(columns);

        let operation = CreateOperation::Table {
            table_name: stmt.table_name,
            schema,
        };
        Ok(Box::new(CreateExecutor::new(operation)))
    }

    /// Converts the `CreateIndex` Ast and passes it to the `CreateExecutor`.
    fn plan_create_index(&self, stmt: CreateIndex) -> Result<Box<dyn Executor>, Error> {
        let operation = CreateOperation::Index {
            table_name: stmt.table_name,
            index_name: stmt.index_name,
            is_unique: stmt.unique,
            column_name: stmt.column_name,
        };
        Ok(Box::new(CreateExecutor::new(operation)))
    }

    /// Plans a SELECT query, building a pipeline of SeqScan -> Filter.
    fn plan_select(&self, stmt: Select) -> Result<Box<dyn Executor>, Error> {
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
        let root_page_id = self.catalog.get_table_root(&table_name)?;

        let schema = self.catalog.get_table_schema(&table_name)?;
        let iterator = BpTreeIterator::new(root_page_id);

        let mut pipeline: Box<dyn Executor> =
            Box::new(SeqScanExecutor::new(iterator, schema.clone()));

        if let Some(where_expr) = stmt.where_clause {
            let bound_predicate = self.bind_expr(&where_expr, schema, Some(DataType::Boolean))?;
            pipeline = Box::new(FilterExecutor::new(pipeline, bound_predicate));
        }
        Ok(pipeline)
    }

    /// Converts the logical Insert Ast node into a physical ValuesExec -> InsertExec
    /// pipeline. Confirms the validity of the statement and handles implicit NULL
    /// padding for intentionally omitted columns.
    fn plan_insert(&self, stmt: Insert) -> Result<Box<dyn Executor>, Error> {
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
            for (idx, expr) in row_exprs.iter().enumerate() {
                let physical_idx = target_columns[idx];
                let expected_type = schema.columns[physical_idx].data_type;
                let bound_expr = self.bind_expr(expr, schema, Some(expected_type))?;
                physical_row[physical_idx] = bound_expr;
            }
            bound_values.push(physical_row);
        }
        let values_executor = Box::new(ValuesExecutor::new(bound_values));
        let insert_executor = Box::new(InsertExecutor::new(
            values_executor,
            stmt.table_name,
            /*  TODO: The executors only need to borrow the schema as far as I can see till now.
            Remodel the ownership after the db is complete. */
            schema.clone(),
        ));
        Ok(insert_executor)
    }

    /// Translates the Ast Delete node into a physical `SeqScan -> Filter -> Delete` pipeline.
    /// Binds the optional where predicate if present.
    fn plan_delete(&self, stmt: Delete) -> Result<Box<dyn Executor>, Error> {
        let root_page_id = self.catalog.get_table_root(&stmt.table_name)?;
        let schema = self.catalog.get_table_schema(&stmt.table_name)?;

        let iterator = BpTreeIterator::new(root_page_id);
        let mut pipeline: Box<dyn Executor> =
            Box::new(SeqScanExecutor::new(iterator, schema.clone()));

        if let Some(where_expr) = stmt.where_clause {
            let bound_predicate = self.bind_expr(&where_expr, schema, Some(DataType::Boolean))?;
            pipeline = Box::new(FilterExecutor::new(pipeline, bound_predicate));
        }
        Ok(Box::new(DeleteExecutor::new(
            pipeline,
            stmt.table_name,
            schema.clone(),
        )))
    }

    /// Translates the Ast Update node into a physical `SeqScan -> Filter -> Update` pipeline.
    /// Performs type checking on all the SET assignments against the schema.
    fn plan_update(&self, stmt: Update) -> Result<Box<dyn Executor>, Error> {
        let root_page_id = self.catalog.get_table_root(&stmt.table_name)?;
        let schema = self.catalog.get_table_schema(&stmt.table_name)?;

        let iterator = BpTreeIterator::new(root_page_id);
        let mut pipeline: Box<dyn Executor> =
            Box::new(SeqScanExecutor::new(iterator, schema.clone()));

        if let Some(where_expr) = stmt.where_clause {
            let bound_expr = self.bind_expr(&where_expr, schema, Some(DataType::Boolean))?;
            pipeline = Box::new(FilterExecutor::new(pipeline, bound_expr));
        }
        let mut exec_assignments = Vec::with_capacity(stmt.assignments.len());

        // Binds and type-checks the SET assignments.
        for ast_assign in stmt.assignments {
            let col_idx = schema.get_col_idx(&ast_assign.column_name)?;
            let expected_type = Some(schema.columns[col_idx].data_type);

            // Binds the assigned value to the datatype of the column and errs
            // immediately if the data_types are different than expected.
            // `SET age = 'string'` will return a SyntaxError.
            let bound_expr = self.bind_expr(&ast_assign.value, schema, expected_type)?;
            exec_assignments.push(ExecAssignment {
                col_idx,
                expr: bound_expr,
            });
        }
        Ok(Box::new(UpdateExecutor::new(
            pipeline,
            stmt.table_name,
            schema.clone(),
            exec_assignments,
        )))
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
    ) -> Result<BoundExpr, Error> {
        match expr {
            Expr::Column(col_ref) => {
                let col_idx = schema.get_col_idx(&col_ref.column_name)?;
                let data_type = schema.columns[col_idx].data_type;
                Ok(BoundExpr::ColumnRef { col_idx, data_type })
            }
            Expr::Literal(ast_lit) => {
                let val = self.bind_literal(ast_lit, expected_type)?;
                Ok(BoundExpr::Constant(val))
            }
            Expr::BinaryOp { left, op, right } => {
                let right_type = self.infer_expr_type(right, schema)?;
                let left_type = self.infer_expr_type(left, schema)?;

                // If one side has a definitively known type, propagate that type
                // to the other side to enforce coercion.
                let type_for_left = right_type.or(expected_type);
                let type_for_right = left_type.or(expected_type);

                let bound_left = self.bind_expr(left, schema, type_for_left)?;
                let bound_right = self.bind_expr(right, schema, type_for_right)?;

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
    ) -> Result<Value, Error> {
        match ast_lit {
            AstLiteral::String(val) => match expected_type {
                Some(DataType::Varchar) | None => Ok(Value::Varchar(val.clone())),
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
