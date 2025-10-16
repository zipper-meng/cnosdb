use datafusion::arrow::datatypes::SchemaBuilder;
use datafusion::common::tree_node::{TreeNode, TreeNodeRewriter};
use datafusion::error::Result as DfResult;
use datafusion::logical_expr::expr::Placeholder;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::Expr;
use models::arrow::{DataType, Field, Schema};

pub fn rewrite_placeholders(plan: LogicalPlan) -> DfResult<(LogicalPlan, Schema)> {
    let mut r = LogicalPlanRewriter::new();
    let new_plan = plan.rewrite_down(&mut r)?;
    Ok((new_plan, r.schema_builder.finish()))
}

struct ExprRewriter<'a> {
    placeholder_id: &'a mut usize,
    placeholder_schema: Option<&'a mut SchemaBuilder>,
}

impl<'a> ExprRewriter<'a> {
    pub fn new(
        placeholder_id: &'a mut usize,
        placeholder_schema: Option<&'a mut SchemaBuilder>,
    ) -> Self {
        Self {
            placeholder_id,
            placeholder_schema,
        }
    }
}

impl TreeNodeRewriter for ExprRewriter<'_> {
    type N = Expr;

    fn mutate(&mut self, node: Self::N) -> DfResult<Self::N> {
        match node {
            Expr::Placeholder(placeholder) => {
                if placeholder.id == "?" {
                    let data_type = placeholder.data_type.unwrap_or(DataType::Null);
                    let new_id = format!("${}", self.placeholder_id);
                    *self.placeholder_id += 1;
                    println!("## transforming Placeholder::id: ? -> {new_id}({data_type})");
                    if let Some(schema) = self.placeholder_schema.as_mut() {
                        schema.push(Field::new(&new_id, data_type.clone(), true));
                    }

                    Ok(Expr::Placeholder(Placeholder::new(new_id, Some(data_type))))
                } else {
                    if let Some(schema) = self.placeholder_schema.as_mut() {
                        let data_type = placeholder.data_type.clone().unwrap_or(DataType::Null);
                        schema.push(Field::new(&placeholder.id, data_type, true));
                    }
                    Ok(Expr::Placeholder(placeholder))
                }
            }
            other => Ok(other),
        }
    }
}

struct LogicalPlanRewriter {
    placeholder_id: usize,
    schema_builder: SchemaBuilder,
}

impl LogicalPlanRewriter {
    pub fn new() -> Self {
        Self {
            placeholder_id: 1,
            schema_builder: SchemaBuilder::new(),
        }
    }
}

impl TreeNodeRewriter for LogicalPlanRewriter {
    type N = LogicalPlan;

    fn mutate(&mut self, node: Self::N) -> DfResult<Self::N> {
        match node {
            // WHERE c = ?
            LogicalPlan::Filter(mut filter) => {
                let mut r =
                    ExprRewriter::new(&mut self.placeholder_id, Some(&mut self.schema_builder));
                filter.predicate = filter.predicate.rewrite(&mut r).unwrap();
                Ok(LogicalPlan::Filter(filter))
            }
            // Filters pushed to table-scan.
            LogicalPlan::TableScan(mut table_scan) => {
                let mut r =
                    ExprRewriter::new(&mut self.placeholder_id, Some(&mut self.schema_builder));
                let mut new_filters = Vec::with_capacity(table_scan.filters.len());
                for expr in table_scan.filters.into_iter() {
                    new_filters.push(expr.rewrite(&mut r).unwrap());
                }
                table_scan.filters = new_filters;
                Ok(LogicalPlan::TableScan(table_scan))
            }
            // INSERT ..... VALUES (?, ?, ?)
            LogicalPlan::Values(mut values) => {
                let mut r =
                    ExprRewriter::new(&mut self.placeholder_id, Some(&mut self.schema_builder));
                let mut new_values = Vec::with_capacity(values.values.len());
                for expr_group in values.values.into_iter() {
                    let mut new_expr_group = Vec::with_capacity(expr_group.len());
                    for expr in expr_group.into_iter() {
                        new_expr_group.push(expr.rewrite(&mut r).unwrap());
                    }
                    new_values.push(new_expr_group);
                }
                values.values = new_values;
                Ok(LogicalPlan::Values(values))
            }
            other => Ok(other),
        }
    }
}
