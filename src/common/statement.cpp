//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// statement.cpp
//
// Identification: src/common/statement.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/statement.h"
#include "common/logger.h"
#include "planner/abstract_plan.h"

namespace peloton {

Statement::Statement(const std::string& statement_name,
                     const std::string& query_string)
: statement_name(statement_name),
  query_string(query_string) {

  LOG_INFO("Statement created : %s", statement_name.c_str());

}

Statement::~Statement() {

  LOG_INFO("Statement destroyed : %s", statement_name.c_str());

}

std::vector<FieldInfoType> Statement::GetTupleDescriptor() const {
  return tuple_descriptor;
}

void Statement::SetStatementName(const std::string& statement_name_){
  statement_name = statement_name_;
}

std::string Statement::GetStatementName() const {
  return statement_name;
}

void Statement::SetQueryString(const std::string& query_string_){
  query_string = query_string_;
}

std::string Statement::GetQueryString() const {
  return query_string;
}

void Statement::SetQueryType(const std::string& query_type_){
  query_type = query_type_;
}

std::string Statement::GetQueryType() const {
  return query_type;
}

void Statement::SetParamTypes(const std::vector<int32_t>& param_types_){
  param_types = param_types_;
}

std::vector<int32_t> Statement::GetParamTypes() const {
  return param_types;
}

void Statement::SetTupleDescriptor(const std::vector<FieldInfoType>& tuple_descriptor_){
  tuple_descriptor = tuple_descriptor_;
}

void Statement::SetPlanTree(std::shared_ptr<planner::AbstractPlan> plan_tree_){
  plan_tree = std::move(plan_tree_);
}

const std::shared_ptr<planner::AbstractPlan>& Statement::GetPlanTree() const {
  return plan_tree;
}

}  // namespace peloton
