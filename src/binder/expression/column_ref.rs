/// A bound column reference, e.g., `y.x` in the SELECT list.
#[derive(Debug, Clone)]
pub struct BoundColumnRef {
    pub col_names: Vec<String>,
}
