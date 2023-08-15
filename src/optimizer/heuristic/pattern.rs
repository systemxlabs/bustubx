use crate::planner::operator::LogicalOperator;

pub struct Pattern {
    // the root node predicate, not contains the children
    pub predicate: fn(&LogicalOperator) -> bool,
    // the children's predicate of current node
    pub children: PatternChildrenPredicate,
}

pub enum PatternChildrenPredicate {
    MatchedRecursive,
    Predicate(Vec<Pattern>),
    None,
}