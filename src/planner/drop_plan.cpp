//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// drop_plan.cpp
//
// Identification: src/planner/drop_plan.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include "planner/drop_plan.h"

#include "storage/data_table.h"
#include "parser/peloton/drop_parse.h"

namespace peloton {
namespace planner {

DropPlan::DropPlan(storage::DataTable *table) :
        target_table_(table) {
  missing = false;
}

DropPlan::DropPlan(std::string name) :
        table_name(name) {
  missing = false;
}

DropPlan::DropPlan(parser::DropParse *parse_tree) :
        table_name(parse_tree->GetEntityName()) {
  missing = parse_tree->IsMissing();
}

}  // namespace planner
}  // namespace peloton
