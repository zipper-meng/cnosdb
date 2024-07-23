use std::hash::Hash;
use std::sync::Arc;

use datafusion::common::{DFSchema, DFSchemaRef};
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion::prelude::Expr;

/// User-defined logical node only for display.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct DisplayPlanNode {
    /// Name to display.
    name: String,
    /// Arguments to display.
    arguments: Vec<(String, String)>,

    inputs: Vec<LogicalPlan>,
    schema: DFSchemaRef,
}

impl DisplayPlanNode {
    /// Create a new `DisplayPlanNode`.
    pub fn new(name: String, arguments: Vec<(String, String)>) -> Self {
        Self {
            name,
            arguments,
            inputs: vec![],
            schema: Arc::new(DFSchema::empty()),
        }
    }
}

impl UserDefinedLogicalNodeCore for DisplayPlanNode {
    fn name(&self) -> &str {
        self.name.as_str()
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
        write!(f, "{}", self.name)?;
        if self.arguments.is_empty() {
            return Ok(());
        }
        write!(f, ": ")?;
        for (k, v) in self.arguments.iter() {
            write!(f, "{}={}", k, v)?;
        }
        Ok(())
    }

    fn from_template(&self, _exprs: &[Expr], inputs: &[LogicalPlan]) -> Self {
        Self {
            name: self.name.clone(),
            arguments: self.arguments.clone(),
            inputs: inputs.to_vec(),
            schema: self.schema.clone(),
        }
    }
}
