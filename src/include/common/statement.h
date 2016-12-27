//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// statement.h
//
// Identification: src/include/common/statement.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <vector>
#include <memory>

#include "type/types.h"

namespace peloton {
namespace planner {
class AbstractPlan;
}

typedef std::pair<std::vector<unsigned char>, std::vector<unsigned char>>
    ResultType;

// FIELD INFO TYPE : field name, oid (data type), size
typedef std::tuple<std::string, oid_t, size_t> FieldInfoType;

class Statement {

 public:
  Statement() = delete;
  Statement(const Statement&) = delete;
  Statement& operator=(const Statement&) = delete;
  Statement(Statement&&) = delete;
  Statement& operator=(Statement&&) = delete;

  Statement(const std::string& statement_name, const std::string& query_string);

  ~Statement();

  std::vector<FieldInfoType> GetTupleDescriptor() const;

  void SetStatementName(const std::string& statement_name);

  std::string GetStatementName() const;

  void SetQueryString(const std::string& query_string);

  std::string GetQueryString() const;

  std::string GetQueryType() const;

  void SetParamTypes(const std::vector<int32_t>& param_types);

  std::vector<int32_t> GetParamTypes() const;

  void SetTupleDescriptor(const std::vector<FieldInfoType>& tuple_descriptor);

  void SetPlanTree(std::shared_ptr<planner::AbstractPlan> plan_tree);

  const std::shared_ptr<planner::AbstractPlan>& GetPlanTree() const;

 private:
  // logical name of statement
  std::string statement_name_;

  // query string
  std::string query_string_;

  // first token in query
  std::string query_type_;

  // format codes of the parameters
  std::vector<int32_t> param_types_;

  // schema of result tuple
  std::vector<FieldInfoType> tuple_descriptor_;

  // cached plan tree
  std::shared_ptr<planner::AbstractPlan> plan_tree_;
};

}  // namespace peloton
