
//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// insert_plan.cpp
//
// Identification: /peloton/src/planner/insert_plan.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
 
#include "planner/insert_plan.h"
#include "planner/project_info.h"
#include "storage/data_table.h"
#include "storage/tuple.h"
#include "parser/peloton/insert_parse.h"

namespace peloton{
namespace planner{
InsertPlan::InsertPlan(storage::DataTable *table, oid_t bulk_insert_count)
    : target_table_(table), bulk_insert_count(bulk_insert_count) {}

// This constructor takes in a project info
InsertPlan::InsertPlan(
    storage::DataTable *table,
    std::unique_ptr<const planner::ProjectInfo> &&project_info,
    oid_t bulk_insert_count)
    : target_table_(table),
      project_info_(std::move(project_info)),
      bulk_insert_count(bulk_insert_count) {}

// This constructor takes in a tuple
InsertPlan::InsertPlan(storage::DataTable *table,
                    std::unique_ptr<storage::Tuple> &&tuple,
                    oid_t bulk_insert_count)
    : target_table_(table),
      tuple_(std::move(tuple)),
      bulk_insert_count(bulk_insert_count) {}

InsertPlan::InsertPlan(parser::InsertParse *parse_tree){
  // Here try to get the columns vector (if size if over zero)
  // and the values vector and genertate a tuple. Table name can also
  // be taken from this parse_tree , HAPPY EID!!
  LOG_INFO("Table Name is %s" , parse_tree->GetTableName().c_str());
  
}

}
}


