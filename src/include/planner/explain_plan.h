
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

#include "planner/abstract_plan.h"

#include <memory>

namespace peloton {
namespace storage {
class DataTable;
}
namespace parser {
class AnalyzeStatement;
}
namespace catalog {
class Schema;
}
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

  explicit ExplainPlan(parser::SQLStatement *sql_stmt) : sql_stmt(sql_stmt){};

  inline PlanNodeType GetPlanNodeType() const { return PlanNodeType::EXPLAIN; }

  const std::string GetInfo() const { return "Explain table"; }

  inline std::unique_ptr<AbstractPlan> Copy() const {
    return std::unique_ptr<AbstractPlan>(new ExplainPlan(sql_stmt));
  }

 private:
  /**
   * @brief The SQL statement to explain (owned by the AST)
   */
  parser::SQLStatement *sql_stmt;
};

}  // namespace planner
}  // namespace peloton
