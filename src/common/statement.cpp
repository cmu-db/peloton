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
#include <cstdio>
#include "common/logger.h"
#include "planner/abstract_plan.h"

namespace peloton {

Statement::Statement(const std::string& statement_name,
                     const std::string& query_string)
    : statement_name_(statement_name), query_string_(query_string) {
  ParseQueryType(query_string_, query_type_);
}

Statement::~Statement() {}

void Statement::ParseQueryType(const std::string& query_string,
                               std::string& query_type) {
  std::stringstream stream(query_string);
  stream >> query_type;
	if (query_type.back() == ';') {
		query_type = query_type.substr(0, query_type.length() - 1);
	}
	boost::to_upper(query_type);
}

std::vector<FieldInfo> Statement::GetTupleDescriptor() const {
  return tuple_descriptor_;
}

void Statement::SetStatementName(const std::string& statement_name) {
  statement_name_ = statement_name;
}

std::string Statement::GetStatementName() const { return statement_name_; }

void Statement::SetQueryString(const std::string& query_string) {
  query_string_ = query_string;
}

std::string Statement::GetQueryString() const { return query_string_; }

std::string Statement::GetQueryType() const { return query_type_; }

void Statement::SetParamTypes(const std::vector<int32_t>& param_types) {
  param_types_ = param_types;
}

std::vector<int32_t> Statement::GetParamTypes() const { return param_types_; }

void Statement::SetTupleDescriptor(
    const std::vector<FieldInfo>& tuple_descriptor) {
  tuple_descriptor_ = tuple_descriptor;
}

void Statement::SetPlanTree(std::shared_ptr<planner::AbstractPlan> plan_tree) {
  plan_tree_ = std::move(plan_tree);
}

void Statement::SetReferencedTables(const std::set<oid_t> table_ids) {
  table_ids_.insert(table_ids.begin(), table_ids.end());
}

const std::set<oid_t> Statement::GetReferencedTables() const {
  return (table_ids_);
}

const std::shared_ptr<planner::AbstractPlan>& Statement::GetPlanTree() const {
  return plan_tree_;
}

const std::string Statement::GetInfo() const {
  std::ostringstream os;
  os << "Statement[";
  if (statement_name_.empty()) {
    os << "**UNNAMED**";
  } else {
    os << statement_name_;
  }
  os << "] -> " << query_string_ << " (";

  // Tables Oids Referenced
  os << "TablesRef={";
  bool first = true;
  for (auto t_id : table_ids_) {
    if (first == false) os << ",";
    os << t_id;
    first = false;
  }
  os << "}";

  // Replan Flag
  os << ", ReplanNeeded=" << needs_replan_;

  // Query Type
  os << ", QueryType=" << query_type_;

  os << ")";

  return os.str();
}

}  // namespace peloton
