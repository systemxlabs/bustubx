use bustubx::database::Database;
use sqllogictest::DBOutput;
use tempfile::TempDir;

pub struct Bustubx {
    db: Database,
    tmp_dir: TempDir,
}

impl Bustubx {
    pub fn new() -> Self {
        let tmp_dir = TempDir::new().unwrap();
        let db = Database::new_on_disk(tmp_dir.path().to_str().unwrap());
        Self { db, tmp_dir }
    }
}

#[async_trait::async_trait]
impl sqllogictest::AsyncDB for &Bustubx {
    type Error = ();
    type ColumnType = ();

    async fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>, Self::Error> {
        todo!()
    }
}
