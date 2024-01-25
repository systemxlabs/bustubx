use crate::planner::logical_plan_v2::create_table::CreateTable;

mod create_table;

pub enum LogicalPlanV2 {
    CreateTable(CreateTable),
}
