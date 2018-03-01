//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// index_selection.h
//
// Identification: src/include/brain/clusterer.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

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

namespace peloton {
  namespace brain {
    typedef std::unordered_set<catalog::Column> columns;

    class IndexSelectionConfiguration {
      friend class IndexSelection;
      IndexSelectionConfiguration(size_t candidate_index_limit, size_t brute_force_limit, size_t result_limit) :
          candidate_index_limit(candidate_index_limit),
          brute_force_limit(brute_force_limit),
          result_limit(result_limit) {}
    private:
      const size_t candidate_index_limit, brute_force_limit, result_limit;
    };

    class IndexSelection {
      IndexSelection() = delete;

    public:
      static std::unique_ptr<columns> GenerateCandidateIndex(std::vector<std::string> workload,
                                            IndexSelectionConfiguration config);

    private:

      class CostCalculator {
        friend class IndexSelection;

      private:
        // TODO: figure how to derive cost from Atomic Configurations
        CostCalculator(std::unordered_map<std::string, std::unique_ptr<columns>> &indexable_columns);
        double DeriveCost(const std::unique_ptr<columns> configuration, const std::vector<std::string> &workload);
      };

      static std::unique_ptr<columns> FindIndexableColumnsInQuery(std::unique_ptr<parser::SQLStatementList> queries);
      static bool compareIndexes(const catalog::Column &i, const catalog::Column &j);
      static std::unique_ptr<columns> EnumerateCandidateIndexes(size_t limit, std::vector<std::string> &workload);
      static std::unique_ptr<columns> BruteForceSearch(size_t limit,
                                      CostCalculator &cost_calculator,
                                      std::unique_ptr<columns> &candidates,
                                      std::vector<std::string> &workload);
    };

    class IndexableColumnExtractor : public SqlNodeVisitor {
    public:
      IndexableColumnExtractor(concurrency::Transaction *txn) : indexable_columns(), txn(txn) {
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

