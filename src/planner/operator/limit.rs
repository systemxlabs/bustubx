#[derive(derive_new::new, Debug, Clone)]
pub struct LogicalLimitOperator {
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}
