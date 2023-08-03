use super::BoundExpression;

#[derive(Debug)]
pub enum BinaryOperator {
    Plus,
    Minus,
    Multiply,
    Divide,
    Gt,
    Lt,
    GtEq,
    LtEq,
    Eq,
    NotEq,
    And,
    Or,
}
impl BinaryOperator {
    pub fn from_sqlparser_operator(op: &sqlparser::ast::BinaryOperator) -> Self {
        match op {
            sqlparser::ast::BinaryOperator::Plus => BinaryOperator::Plus,
            sqlparser::ast::BinaryOperator::Minus => BinaryOperator::Minus,
            sqlparser::ast::BinaryOperator::Multiply => BinaryOperator::Multiply,
            sqlparser::ast::BinaryOperator::Divide => BinaryOperator::Divide,
            sqlparser::ast::BinaryOperator::Gt => BinaryOperator::Gt,
            sqlparser::ast::BinaryOperator::Lt => BinaryOperator::Lt,
            sqlparser::ast::BinaryOperator::GtEq => BinaryOperator::GtEq,
            sqlparser::ast::BinaryOperator::LtEq => BinaryOperator::LtEq,
            sqlparser::ast::BinaryOperator::Eq => BinaryOperator::Eq,
            sqlparser::ast::BinaryOperator::NotEq => BinaryOperator::NotEq,
            sqlparser::ast::BinaryOperator::And => BinaryOperator::And,
            sqlparser::ast::BinaryOperator::Or => BinaryOperator::Or,
            _ => unimplemented!(),
        }
    }
}

/// A bound binary operator, e.g., `a+b`.
#[derive(Debug)]
pub struct BoundBinaryOp {
    pub larg: Box<BoundExpression>,
    pub op: BinaryOperator,
    pub rarg: Box<BoundExpression>,
}
