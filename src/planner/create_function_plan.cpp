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

#include "parser/create_function_statement.h"
#include "storage/data_table.h"

namespace peloton {
namespace planner {

CreateFunctionPlan::CreateFunctionPlan(parser::CreateFunctionStatement *parse_tree) {

  language = parse_tree->language;
  std::string function_body(parse_tree->function_body[0]);
  is_replace = parse_tree->replace;
  std::string function_name(parse_tree->function_name);

  for (auto col : *(parse_tree->func_parameters)) {
    //Adds the function names
    function_param_names.push_back(col->name);

    //Adds the function types
    function_param_types.push_back(col->GetValueType(col->type));
  }

  auto ret_type_obj = *(parse_tree->return_type);
  return_type = ret_type_obj.GetValueType(ret_type_obj.type);

}

}  // namespace planner
}  // namespace peloton
