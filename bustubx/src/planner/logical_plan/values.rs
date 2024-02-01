use crate::catalog::SchemaRef;
use crate::expression::Expr;

#[derive(derive_new::new, Debug, Clone)]
pub struct Values {
    pub schema: SchemaRef,
    pub values: Vec<Vec<Expr>>,
}
