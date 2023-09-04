use crate::binder::order_by::BoundOrderBy;

#[derive(derive_new::new, Debug, Clone)]
pub struct LogicalSortOperator {
    pub order_bys: Vec<BoundOrderBy>,
}
