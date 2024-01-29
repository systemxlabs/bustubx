use bustubx::Database;
use bustubx::{BustubxError, Tuple};
use sqllogictest::{DBOutput, DefaultColumnType};

pub struct BustubxDB {
    db: Database,
}

impl BustubxDB {
    pub fn new() -> Self {
        let db = Database::new_temp();
        Self { db }
    }
}

fn tuples_to_sqllogictest_string(tuples: Vec<Tuple>) -> Vec<Vec<String>> {
    todo!()
}

impl sqllogictest::DB for BustubxDB {
    type Error = BustubxError;
    type ColumnType = DefaultColumnType;

    fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>, Self::Error> {
        let is_query_sql = {
            let lower_sql = sql.trim_start().to_ascii_lowercase();
            lower_sql.starts_with("select")
        };
        let tuples = self.db.run(sql)?;
        if tuples.is_empty() {
            if is_query_sql {
                return Ok(DBOutput::Rows {
                    types: vec![],
                    rows: vec![],
                });
            } else {
                return Ok(DBOutput::StatementComplete(0));
            }
        }
        // TODO fix type count
        let types = vec![DefaultColumnType::Any; 3];
        let rows = tuples_to_sqllogictest_string(tuples);
        Ok(DBOutput::Rows { types, rows })
    }
}
