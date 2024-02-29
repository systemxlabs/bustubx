# BustubX - a relational database for educational purpose (CMU 15-445)
![License](https://img.shields.io/badge/license-MIT-blue.svg)
[![Crates.io](https://img.shields.io/crates/v/bustubx.svg)](https://crates.io/crates/bustubx)

- [x] Planner
- [x] Expression
- [x] Functions
- [x] Rule-based Optimizer
- [x] Volcano Executor
- [x] Disk Management
- [x] Buffer Pool
- [x] Table Heap
- [x] System Metadata (information_schema)
- [x] B+ Tree Index
- [ ] Parallel Execution
- [ ] Two Phase Locking
- [ ] Multi-Version Concurrency Control
- [ ] Crash Recovery
- [ ] WASM

P.S. See [here](tests/sqllogictest/slt) to know which sql statements are supported already.

## Architecture
![architecture](./docs/bustubx-architecture.png)


## Get started
Install rust toolchain first.
```
RUST_LOG=info,bustubx=debug cargo run --bin bustubx-cli
```

![demo](./docs/bustubx-demo.png)

## Reference
- [CMU 15-445/645 Database Systems](https://15445.courses.cs.cmu.edu/fall2022/)
- [cmu-db/bustub](https://github.com/cmu-db/bustub)
- [Fedomn/sqlrs](https://github.com/Fedomn/sqlrs) and [blogs](https://frankma.me/categories/sqlrs/)
- [KipData/KipSQL](https://github.com/KipData/KipSQL)
- [talent-plan/tinysql](https://github.com/talent-plan/tinysql)
- [arrow-datafusion](https://github.com/apache/arrow-datafusion)
- [CMU 15-445课程笔记-zhenghe](https://zhenghe.gitbook.io/open-courses/cmu-15-445-645-database-systems/relational-data-model)
- [CMU15-445 22Fall通关记录 - 知乎](https://www.zhihu.com/column/c_1605901992903004160)
- [B+ Tree Visualization](https://www.cs.usfca.edu/~galles/visualization/BPlusTree.html)