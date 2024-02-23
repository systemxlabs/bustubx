use crate::common::ScalarValue;
use crate::function::Accumulator;
use crate::BustubxResult;

#[derive(Debug, Clone)]
pub struct CountAccumulator {
    count: i64,
}

impl CountAccumulator {
    pub fn new() -> Self {
        Self { count: 0 }
    }
}

impl Accumulator for CountAccumulator {
    fn update_value(&mut self, value: &ScalarValue) -> BustubxResult<()> {
        if !value.is_null() {
            self.count += 1;
        }
        Ok(())
    }

    fn evaluate(&self) -> BustubxResult<ScalarValue> {
        Ok(self.count.into())
    }
}
