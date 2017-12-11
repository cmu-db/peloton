//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// create_function_plan.cpp
//
// Identification: src/planner/create_function_plan.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "planner/create_function_plan.h"
#include "storage/data_table.h"

namespace peloton {
namespace planner {

CreateFunctionPlan::CreateFunctionPlan(UNUSED_ATTRIBUTE std::string func) {}

CreateFunctionPlan::CreateFunctionPlan(
    parser::CreateFunctionStatement *parse_tree) {
  language = parse_tree->language;
  function_body = parse_tree->function_body;
  is_replace = parse_tree->replace;
  function_name = parse_tree->function_name;

  for (auto col : *(parse_tree->func_parameters)) {
    // Adds the function names
    function_param_names.push_back(col->name);
    param_count++;
    // Adds the function types
    function_param_types.push_back(col->GetValueType(col->type));
  }

  auto ret_type_obj = *(parse_tree->return_type);
  return_type = ret_type_obj.GetValueType(ret_type_obj.type);
}

}  // namespace planner
}  // namespace peloton
