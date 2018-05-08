
//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// explain_plan.h
//
// Identification: src/include/planner/explain_plan.h
//
// Copyright (c) 2015-18, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/sql_statement.h"
#include "planner/abstract_plan.h"

#include <memory>

namespace peloton {
namespace concurrency {
class TransactionContext;
}

namespace planner {
class ExplainPlan : public AbstractPlan {
 public:
  ExplainPlan(const ExplainPlan &) = delete;
  ExplainPlan &operator=(const ExplainPlan &) = delete;
  ExplainPlan(ExplainPlan &&) = delete;
  ExplainPlan &operator=(ExplainPlan &&) = delete;

  explicit ExplainPlan(std::unique_ptr<parser::SQLStatement> sql_stmt,
                       std::string default_database_name)
      : sql_stmt_(sql_stmt.release()),
        default_database_name_(default_database_name){};

  explicit ExplainPlan(std::shared_ptr<parser::SQLStatement> sql_stmt,
                       std::string default_database_name)
      : sql_stmt_(sql_stmt), default_database_name_(default_database_name){};

  inline PlanNodeType GetPlanNodeType() const { return PlanNodeType::EXPLAIN; }

  const std::string GetInfo() const { return std::string("Explain") + sql_stmt_->GetInfo(); }

  inline std::unique_ptr<AbstractPlan> Copy() const {
    // FIXME: support deep copy for sql statement, then use a unique_ptr here
    return std::unique_ptr<AbstractPlan>(
        new ExplainPlan(sql_stmt_, default_database_name_));
  }

  parser::SQLStatement *GetSQLStatement() const { return sql_stmt_.get(); }

  std::string GetDatabaseName() const { return default_database_name_; }

 private:
  /**
   * @brief The SQL statement to explain (owned by the AST)
   */
  std::shared_ptr<parser::SQLStatement> sql_stmt_;
  /**
   * @brief The database name to be used in the binder
   */
  std::string default_database_name_;
};

}  // namespace planner
}  // namespace peloton
