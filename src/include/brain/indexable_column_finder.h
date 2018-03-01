//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// index_selection.h
//
// Identification: src/include/brain/indexable_column_finder.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//



#include <vector>
#include <map>
#include <string>
#include <memory>
#include <unordered_map>
#include <unordered_set>

#include "index/index.h"
#include "parser/sql_statement.h"
#include "parser/statements.h"
#include "catalog/column.h"
#include "common/sql_node_visitor.h"
#include "brain/index_selection.h"

namespace peloton {
  namespace brain {

    class IndexableColumnFinder : public SqlNodeVisitor {
    public:
      IndexableColumnFinder(concurrency::Transaction *txn) : indexable_columns(), txn(txn) {
        indexable_columns = std::make_unique<columns>();
      }
      std::unique_ptr<columns> &getIndexableColumns() {
        return indexable_columns;
      }

      void Visit(const parser::SelectStatement *) override;

      // Some sub query nodes inside SelectStatement
      void Visit(const parser::JoinDefinition *) override;
      void Visit(const parser::TableRef *) override;
      void Visit(const parser::GroupByDescription *) override;
      void Visit(const parser::OrderDescription *) override;
      void Visit(const parser::LimitDescription *) override;

      void Visit(const parser::CreateStatement *) override;
      void Visit(const parser::InsertStatement *) override;
      void Visit(const parser::DeleteStatement *) override;
      void Visit(const parser::DropStatement *) override;
      void Visit(const parser::PrepareStatement *) override;
      void Visit(const parser::ExecuteStatement *) override;
      void Visit(const parser::TransactionStatement *) override;
      void Visit(const parser::UpdateStatement *) override;
      void Visit(const parser::CopyStatement *) override;
      void Visit(const parser::AnalyzeStatement *) override;

      void Visit(expression::CaseExpression *expr) override;
      void Visit(expression::TupleValueExpression *expr) override;

    private:
      std::unique_ptr<columns> indexable_columns;
      concurrency::Transaction *txn;
    };

  }  // namespace brain
}  // namespace peloton
