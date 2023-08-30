# bustubx - a relational database for educational purpose (CMU 15-445)
- DDL
  - [ ] Create
    - [x] Create Table
    - [ ] Create Index
  - [ ] Drop
  - [ ] Alter
  - [ ] Truncate
- DQL
  - [x] Select
  - [x] Where
  - [ ] Distinct
  - [ ] Aggregation: Count / Sum / Avg / Min / Max
  - [ ] Subquery
  - [ ] Join: Left Outer / Right Outer / Full Outer / Inner / Cross
  - [ ] Group By
  - [ ] Having
  - [x] Order By
  - [x] Limit
- DML
  - [x] Insert
  - [ ] Update
  - [ ] Delete
- Data Types
  - [x] Bool
  - [x] TinyInt
  - [x] SmallInt
  - [x] Int
  - [ ] BigInt
  - [ ] Float
  - [ ] Varchar
  - [ ] Null
- Optimizer rules
  - [x] Limit Project Transpose
  - [x] Eliminate Limits
  - [x] Push Limit Through Join
  - [ ] Push Limit Into Scan
  - [ ] Combine Filters
  - [ ] Column Pruning
  - [ ] Collapse Project
- Executors
  - [x] Create Table
  - [x] Table Scan
  - [ ] Index Scan
  - [x] Filter
  - [x] Project
  - [x] Limit
  - [x] Nested Loop Join
  - [ ] Hash Join
  - [x] Sort
  - [x] Insert
  - [x] Values
  - [ ] Update
  - [ ] Delete
- Transaction
  - [ ] Begin
  - [ ] Commit
  - [ ] Rollback
  - [ ] Isolation Level
    - [ ] Read Uncommitted
    - [ ] Read Committed
    - [ ] Repeatable Read
    - [ ] Serializable
- Recovery
  - [ ] Redo
  - [ ] Undo
  - [ ] Checkpoint

## Architecture
![architecture](./docs/bustubx-architecture.png)


## Get started
Install rust toolchain first.
```
cargo run
```
test command
```mysql
create table t1(a int, b int);

insert into t1 values(1,1),(2,3),(5,4);

select * from t1;

select * from t1 where a <= b;

select a from t1 where a <= b;
```

![demo](./docs/bustubx-demo.png)

## Reference
- [CMU 15-445/645 Database Systems](https://15445.courses.cs.cmu.edu/fall2022/)
- [cmu-db/bustub](https://github.com/cmu-db/bustub)
- [Fedomn/sqlrs](https://github.com/Fedomn/sqlrs) and [blogs](https://frankma.me/categories/sqlrs/)
- [KipData/KipSQL](https://github.com/KipData/KipSQL)
- [talent-plan/tinysql](https://github.com/talent-plan/tinysql)
- [CMU 15-445课程笔记-zhenghe](https://zhenghe.gitbook.io/open-courses/cmu-15-445-645-database-systems/relational-data-model)
- [CMU15-445 22Fall通关记录 - 知乎](https://www.zhihu.com/column/c_1605901992903004160)
- [B+ Tree Visualization](https://www.cs.usfca.edu/~galles/visualization/BPlusTree.html)

## Questions
**1.What if same keys in B+ tree node?**

**2.What if key size exceeds b+ tree index page capacity?**