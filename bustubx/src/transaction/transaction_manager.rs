use crate::transaction::Transaction;

pub enum IsolationLevel {
    ReadUncommitted,
    SnapshotIsolation,
    Serializable,
}

pub struct TransactionManager {}

impl TransactionManager {
    pub fn begin(&self, isolation_level: IsolationLevel) -> Transaction {
        todo!()
    }

    pub fn commit(&self, txn: Transaction) -> bool {
        todo!()
    }

    pub fn abort(&self, txn: Transaction) {
        todo!()
    }
}
