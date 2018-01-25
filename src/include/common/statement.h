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
#include "internal_types.h"
#include "parser/sql_statement.h"

namespace peloton {
namespace planner {
class AbstractPlan;
}

// Contains the value of a column in a tuple of the result set.
// std::string since the result is sent to the client over the network.
// Previously it used to be StatementResult of type
// std::pair<std::vector<unsigned char>, std::vector<unsigned char>>.
using ResultValue = std::string;

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
  Statement(const std::string& statement_name, QueryType query_type,
            std::string query_string, std::unique_ptr<parser::SQLStatementList> sql_stmt_list);

  ~Statement();

  std::vector<FieldInfo> GetTupleDescriptor() const;

  void SetStatementName(const std::string& statement_name);

  std::string GetStatementName() const;

  void SetQueryString(const std::string& query_string);

  std::string GetQueryString() const;

  std::string GetQueryTypeString() const;

  QueryType GetQueryType() const;

  void SetParamTypes(const std::vector<int32_t>& param_types);

  std::vector<int32_t> GetParamTypes() const;

  void SetTupleDescriptor(const std::vector<FieldInfo>& tuple_descriptor);

  void SetReferencedTables(const std::set<oid_t> table_ids);

  const std::set<oid_t> GetReferencedTables() const;

  void SetPlanTree(std::shared_ptr<planner::AbstractPlan> plan_tree);

  const std::shared_ptr<planner::AbstractPlan>& GetPlanTree() const;

  std::unique_ptr<parser::SQLStatementList>const& GetStmtParseTreeList() {return sql_stmt_list_;}

  std::unique_ptr<parser::SQLStatementList> PassStmtParseTreeList() {return std::move(sql_stmt_list_);}

  inline bool GetNeedsReplan() const { return (needs_replan_); }

  inline void SetNeedsReplan(bool replan) { needs_replan_ = replan; }

  // Get a string representation for debugging
  const std::string GetInfo() const;

 private:
  // logical name of statement
  std::string statement_name_;

  // enum value of query_type
  QueryType query_type_;

  // query string
  std::string query_string_;

  // query parse tree
  std::unique_ptr<parser::SQLStatementList> sql_stmt_list_;

  // first token in query
  // Keep the string token of the query_type because it is returned 
  // as responses after executing commands.
  std::string query_type_string_;

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
