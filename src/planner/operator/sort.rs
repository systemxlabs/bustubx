use crate::binder::order_by::BoundOrderBy;

#[derive(Debug, Clone)]
pub struct LogicalSortOperator {
    pub order_bys: Vec<BoundOrderBy>,
}
impl LogicalSortOperator {
    pub fn new(order_bys: Vec<BoundOrderBy>) -> Self {
        Self { order_bys }
    }
}
