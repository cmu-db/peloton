//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// analyze_plan.cpp
//
// Identification: src/planner/Analyze_plan.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "planner/analyze_plan.h"

#include "parser/analyze_statement.h"
#include "storage/data_table.h"
#include "catalog/catalog.h"

namespace peloton {
namespace planner {

AnalyzePlan::AnalyzePlan(storage::DataTable *table) : target_table_(table) {}

AnalyzePlan::AnalyzePlan(std::string table_name,
                         std::string database_name,
                         concurrency::TransactionContext *txn)
    : table_name_(table_name) {
  target_table_ = catalog::Catalog::GetInstance()->GetTableWithName(
    database_name, table_name, txn);
}

AnalyzePlan::AnalyzePlan(std::string table_name,
                         std::string database_name,
                         std::vector<char *> column_names,
                         concurrency::TransactionContext *txn)
    : table_name_(table_name), column_names_(column_names) {
  target_table_ = catalog::Catalog::GetInstance()->GetTableWithName(
    database_name, table_name, txn);
}

AnalyzePlan::AnalyzePlan(parser::AnalyzeStatement *analyze_stmt,
                         concurrency::TransactionContext *txn) {
  table_name_ = analyze_stmt->GetTableName();
  column_names_ = std::vector<char*>();
  for (auto& name : analyze_stmt->GetColumnNames())
    column_names_.push_back((char*)name.c_str());
  if (!table_name_.empty()) {
    target_table_ = catalog::Catalog::GetInstance()->GetTableWithName(
        analyze_stmt->GetDatabaseName(), table_name_, txn);
  }
}

}  // namespace planner
}  // namespace peloton
