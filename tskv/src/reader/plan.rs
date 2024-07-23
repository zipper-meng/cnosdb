use datafusion::common::DFSchemaRef;
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion::prelude::Expr;

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct P {
    inputs: Vec<LogicalPlan>,
    schema: DFSchemaRef,
}

impl UserDefinedLogicalNodeCore for P {
    fn name(&self) -> &str {
        "P"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        self.inputs.iter().collect()
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "P")
    }

    fn from_template(&self, exprs: &[Expr], inputs: &[LogicalPlan]) -> Self {
        for e in exprs {
            println!("{e}");
        }
        Self {
            inputs: inputs.to_vec(),
            schema: self.schema.clone(),
        }
    }
}

/// - of [`SchemaAlignmenter`](crate::reader::schema_alignmenter::SchemaAlignmenter)
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct SchemaAlignmenterPlanNode {}

/// - of [`ParallelMergeAdapter`](crate::reader::paralle_merge::ParallelMergeAdapter)
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct ParallelMergeAdapterPlanNode {}

/// - of [`SeriesReader`](crate::reader::series::SeriesReader)
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct SeriesReaderPlanNode {}

/// - of [`DataMerger`](crate::reader::merge::DataMerger)
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct DataMergerPlanNode {}

/// - of [`DataFilter`](crate::reader::filter::DataFilter)
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct DataFilterPlanNode {}

/// - of [`TraceCollectorBatcherReaderProxy`](crate::reader::trace::TraceCollectorBatcherReaderProxy)
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct TraceCollectorBatcherReaderProxyPlanNode {}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use datafusion::common::DFSchema;

    use super::*;

    #[test]
    fn test() {
        let p = P {
            inputs: vec![],
            schema: Arc::new(DFSchema::empty()),
        };
        let exprs = vec![];
        let inputs = vec![];
        let _ = p.from_template(&exprs, &inputs);
    }
}
