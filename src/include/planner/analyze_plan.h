//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// analyze_plan.h
//
// Identification: src/include/planner/analyze_plan.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/abstract_plan.h"

#include <vector>

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
class AnalyzePlan : public AbstractPlan {
 public:
  AnalyzePlan(const AnalyzePlan &) = delete;
  AnalyzePlan &operator=(const AnalyzePlan &) = delete;
  AnalyzePlan(AnalyzePlan &&) = delete;
  AnalyzePlan &operator=(AnalyzePlan &&) = delete;

  explicit AnalyzePlan(storage::DataTable *table);

  explicit AnalyzePlan(std::string table_name,
                       std::string database_name,
                       concurrency::TransactionContext *txn);

  explicit AnalyzePlan(std::string table_name, 
                       std::string database_name, 
                       std::vector<char *> column_names,
                       concurrency::TransactionContext *txn);

  explicit AnalyzePlan(parser::AnalyzeStatement *parse_tree,
                       concurrency::TransactionContext *txn);

  inline PlanNodeType GetPlanNodeType() const { return PlanNodeType::ANALYZE; }

  inline storage::DataTable *GetTable() const { return target_table_; }

  inline std::string GetTableName() const { return table_name_; }

  inline std::vector<char *> GetColumnNames() const { return column_names_; }

  const std::string GetInfo() const { return "Analyze table"; }

  inline std::unique_ptr<AbstractPlan> Copy() const {
    return std::unique_ptr<AbstractPlan>(new AnalyzePlan(target_table_));
  }

 private:
  storage::DataTable* target_table_ = nullptr;
  std::string table_name_;
  std::vector<char*> column_names_;
};

}  // namespace planner
}  // namespace peloton
