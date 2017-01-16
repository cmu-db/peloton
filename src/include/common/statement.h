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

#include <memory>
#include <set>
#include <string>
#include <vector>

#include "common/printable.h"
#include "type/types.h"

namespace peloton {
namespace planner {
class AbstractPlan;
}

// TODO: Somebody needs to define what the hell this is???
typedef std::pair<std::vector<unsigned char>, std::vector<unsigned char>>
    StatementResult;

// FIELD INFO TYPE : field name, oid (data type), size
typedef std::tuple<std::string, oid_t, size_t> FieldInfo;

class Statement : public Printable {
 public:
  Statement() = delete;
  Statement(const Statement&) = delete;
  Statement& operator=(const Statement&) = delete;
  Statement(Statement&&) = delete;
  Statement& operator=(Statement&&) = delete;

  Statement(const std::string& statement_name, const std::string& query_string);

  ~Statement();

  static void ParseQueryType(const std::string& query_string,
                             std::string& query_type);

  std::vector<FieldInfo> GetTupleDescriptor() const;

  void SetStatementName(const std::string& statement_name);

  std::string GetStatementName() const;

  void SetQueryString(const std::string& query_string);

  std::string GetQueryString() const;

  std::string GetQueryType() const;

  void SetParamTypes(const std::vector<int32_t>& param_types);

  std::vector<int32_t> GetParamTypes() const;

  void SetTupleDescriptor(const std::vector<FieldInfo>& tuple_descriptor);

  void SetReferencedTables(const std::set<oid_t> table_ids);

  const std::set<oid_t> GetReferencedTables() const;

  void SetPlanTree(std::shared_ptr<planner::AbstractPlan> plan_tree);

  const std::shared_ptr<planner::AbstractPlan>& GetPlanTree() const;

  inline bool GetNeedsPlan() const { return (needs_replan_); }

  inline void SetNeedsPlan(bool replan) { needs_replan_ = replan; }

  // Get a string representation for debugging
  const std::string GetInfo() const;

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
  std::vector<FieldInfo> tuple_descriptor_;

  // cached plan tree
  std::shared_ptr<planner::AbstractPlan> plan_tree_;

  // the oids of the tables referenced by this statement
  // this may be empty
  std::set<oid_t> table_ids_;

  // If this flag is true, then somebody wants us to replan this query
  bool needs_replan_ = false;
};

}  // namespace peloton
