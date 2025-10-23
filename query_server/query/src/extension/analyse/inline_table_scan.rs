use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::Column;
use datafusion::config::ConfigOptions;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::expr::{Exists, InSubquery};
use datafusion::logical_expr::{Filter, LogicalPlan, LogicalPlanBuilder, TableScan};
use datafusion::optimizer::AnalyzerRule;
use datafusion::prelude::Expr;

#[derive(Default, Debug)]
pub struct InlineTableScan;

impl InlineTableScan {
    pub fn new() -> Self {
        Self {}
    }
}

impl AnalyzerRule for InlineTableScan {
    fn analyze(&self, plan: LogicalPlan, _: &ConfigOptions) -> DFResult<LogicalPlan> {
        Ok(plan.transform_up(analyze_internal)?.data)
    }

    fn name(&self) -> &str {
        "inline_table_scan"
    }
}

fn analyze_internal(plan: LogicalPlan) -> DFResult<Transformed<LogicalPlan>> {
    println!("## input plan:\n{}", plan.display_indent());
    Ok(match plan {
        LogicalPlan::TableScan(table_scan) if table_scan.filters.is_empty() => {
            println!("## table_scan:\n{table_scan:?}");
            println!(
                "## table_scan_source_plan:\n{:?}",
                table_scan.source.get_logical_plan()
            );
            let proj_exprs = generate_projection_expr(&table_scan.projection, &table_scan);
            let plan_builder =
                LogicalPlanBuilder::new(LogicalPlan::TableScan(table_scan)).project(proj_exprs)?;
            let new_plan = plan_builder.build()?;
            println!("## output plan:\n{}", new_plan.display_indent());
            println!("==========");
            Transformed::yes(new_plan)

            // Transformed::no(LogicalPlan::TableScan(table_scan))
        }
        LogicalPlan::Filter(filter) => {
            println!("## filter:\n{filter:?}");
            println!("==========");
            let new_expr = filter.predicate.transform(&rewrite_subquery)?.data;
            Transformed::yes(LogicalPlan::Filter(Filter::try_new(
                new_expr,
                filter.input,
            )?))
        }
        _ => Transformed::no(plan),
    })
}

fn rewrite_subquery(expr: Expr) -> DFResult<Transformed<Expr>> {
    match expr {
        Expr::Exists(Exists { subquery, negated }) => {
            let plan = subquery.subquery.as_ref().clone();
            let new_plan = plan.transform_up(&analyze_internal)?;
            let subquery = subquery.with_plan(Arc::new(new_plan.data));
            Ok(Transformed::yes(Expr::Exists(Exists { subquery, negated })))
        }
        Expr::InSubquery(InSubquery {
            expr,
            subquery,
            negated,
        }) => {
            let plan = subquery.subquery.as_ref().clone();
            let new_plan = plan.transform_up(&analyze_internal)?;
            let subquery = subquery.with_plan(Arc::new(new_plan.data));
            Ok(Transformed::yes(Expr::InSubquery(InSubquery::new(
                expr, subquery, negated,
            ))))
        }
        Expr::ScalarSubquery(subquery) => {
            let plan = subquery.subquery.as_ref().clone();
            let new_plan = plan.transform_up(&analyze_internal)?;
            let subquery = subquery.with_plan(Arc::new(new_plan.data));
            Ok(Transformed::yes(Expr::ScalarSubquery(subquery)))
        }
        _ => Ok(Transformed::no(expr)),
    }
}

fn generate_projection_expr(projection: &Option<Vec<usize>>, table_scan: &TableScan) -> Vec<Expr> {
    let mut exprs = vec![];
    let schema = &table_scan.projected_schema;
    if let Some(projection) = projection {
        // Projection for selections.
        for i in projection {
            let (t, f) = schema.qualified_field(*i);
            let c = Column::new(t.cloned(), f.name());
            exprs.push(Expr::Column(c));
        }
    } else {
        // Projection for wildcard.
        for f in schema.fields() {
            let c = Column::new(Some(table_scan.table_name.clone()), f.name());
            exprs.push(Expr::Column(c));
        }
    }
    exprs
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;
    use std::sync::Arc;
    use std::vec;

    use datafusion::common::Result;
    use datafusion::config::ConfigOptions;
    use datafusion::logical_expr::{
        LogicalPlan, LogicalPlanBuilder, TableProviderFilterPushDown, TableSource,
    };
    use datafusion::optimizer::{Analyzer, AnalyzerRule};
    use datafusion::prelude::{col, lit, Expr};
    use models::arrow::{DataType, Field, Schema, SchemaRef};

    use crate::extension::analyse::inline_table_scan::InlineTableScan;

    pub struct RawTableSource {}

    impl TableSource for RawTableSource {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn schema(&self) -> SchemaRef {
            Arc::new(Schema::new(vec![
                Field::new("a", DataType::Int64, false),
                Field::new("b", DataType::Int64, false),
            ]))
        }

        fn supports_filters_pushdown(
            &self,
            filters: &[&Expr],
        ) -> Result<Vec<TableProviderFilterPushDown>> {
            Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
        }
    }

    pub struct CustomSource {
        plan: LogicalPlan,
    }

    impl CustomSource {
        fn new() -> Self {
            Self {
                plan: LogicalPlanBuilder::scan("y", Arc::new(RawTableSource {}), None)
                    .unwrap()
                    .build()
                    .unwrap(),
            }
        }
    }

    impl TableSource for CustomSource {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn supports_filters_pushdown(
            &self,
            filters: &[&Expr],
        ) -> Result<Vec<TableProviderFilterPushDown>> {
            Ok(vec![TableProviderFilterPushDown::Exact; filters.len()])
        }

        fn schema(&self) -> SchemaRef {
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]))
        }

        fn get_logical_plan(&self) -> Option<Cow<LogicalPlan>> {
            Some(Cow::Borrowed(&self.plan))
        }
    }

    pub fn assert_analyzed_plan_eq(
        rule: Arc<dyn AnalyzerRule + Send + Sync>,
        plan: &LogicalPlan,
        expected: &str,
    ) -> Result<()> {
        let options = ConfigOptions::default();
        let analyzed_plan = Analyzer::with_rules(vec![rule]).execute_and_check(
            plan.clone(),
            &options,
            |_, _| {},
        )?;
        let formatted_plan = analyzed_plan.display_indent().to_string();
        assert_eq!(formatted_plan, expected);

        Ok(())
    }

    #[test]
    fn inline_table_scan() -> Result<()> {
        let scan = LogicalPlanBuilder::scan("x".to_string(), Arc::new(CustomSource::new()), None)?;
        let plan = scan.filter(col("x.a").eq(lit(1)))?.build()?;
        let expected = "Filter: x.a = Int32(1)\
        \n  SubqueryAlias: x\
        \n    Projection: y.a, y.b\
        \n      TableScan: y";

        assert_analyzed_plan_eq(Arc::new(InlineTableScan::new()), &plan, expected)
    }

    #[test]
    fn inline_table_scan_with_projection() -> Result<()> {
        let scan = LogicalPlanBuilder::scan(
            "x".to_string(),
            Arc::new(CustomSource::new()),
            Some(vec![0]),
        )?;

        let plan = scan.build()?;
        let expected = "SubqueryAlias: x\
        \n  Projection: y.a\
        \n    TableScan: y";

        assert_analyzed_plan_eq(Arc::new(InlineTableScan::new()), &plan, expected)
    }
}
