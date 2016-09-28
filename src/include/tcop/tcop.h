//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tcop.h
//
// Identification: src/include/tcop/tcop.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

#include <stdlib.h>
#include <stdio.h>
#include <mutex>
#include <vector>

#include "common/portal.h"
#include "common/statement.h"
#include "common/types.h"
#include "common/type.h"

#include "executor/plan_executor.h"

#include <boost/algorithm/string.hpp>
#include <boost/thread/future.hpp>

namespace peloton {
namespace tcop {

//===--------------------------------------------------------------------===//
// TRAFFIC COP
//===--------------------------------------------------------------------===//

class TrafficCop {

  TrafficCop(TrafficCop const &) = delete;

 public:

  // global singleton
  static TrafficCop &GetInstance(void);

  TrafficCop();
  ~TrafficCop();

  // PortalExec - Execute query string
  Result ExecuteStatement(const std::string& query,
                          std::vector<ResultType> &result,
                          std::vector<FieldInfoType> &tuple_descriptor,
                          int &rows_changed,
                          std::string &error_message);

  // ExecPrepStmt - Execute a statement from a prepared and bound statement
  Result ExecuteStatement(const std::shared_ptr<Statement>& statement,
                          const bool unnamed,
                          std::vector<ResultType> &result,
                          int &rows_change,
                          std::string &error_message);

  /*
 * @brief Based on the Volcano 'Exchange' intra-query parallelism model.
 * Currently supported only for sequential scans. This operator spawns
 * multiple execution trees (tasks), each deployed on a different executor
 * thread. For now, the operator spawns as many tasks as there are cores
 * in the system. The tiles to be processed are divided into modulo
 * partitions such that each thread gets roughly the same amount of work.
 * The intermediate results from each thread are coalesced to give the
 * final output.
 */
  static bridge::peloton_status ExchangeOperator(
      const planner::AbstractPlan *plan,
      const std::vector<common::Value *> &params,
      std::vector<ResultType>& result);

  // InitBindPrepStmt - Prepare and bind a query from a query string
  std::shared_ptr<Statement> PrepareStatement(const std::string& statement_name,
                                              const std::string& query_string,
                                              std::string &error_message);

  std::vector<FieldInfoType> GenerateTupleDescriptor(std::string query);

  FieldInfoType GetColumnFieldForValueType(std::string column_name , common::Type::TypeId column_type);
  
  FieldInfoType GetColumnFieldForAggregates(std::string name , ExpressionType expr_type);  

  int BindParameters(std::vector<std::pair<int, std::string>> &parameters,
                     Statement **stmt,
                     std::string &error_message);

};

}  // End tcop namespace
}  // End peloton namespace
