use crate::planner::operator::LogicalOperator;

pub type OptExprNodeId = usize;

#[derive(Debug)]
pub enum OptExprNode {
    OperatorRef(LogicalOperator),
    OptExpr(OptExprNodeId),
}

#[derive(Debug)]
pub struct OptExpr {
    pub root: OptExprNode,
    pub children: Vec<OptExpr>,
}
