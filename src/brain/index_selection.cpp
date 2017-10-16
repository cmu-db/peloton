//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// index_selection.cpp
//
// Identification: src/brain/index_selection.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <algorithm>


#include "parser/postgresparser.h"
#include "brain/index_selection.h"
#include "binder/bind_node_visitor.h"

namespace peloton {
  namespace brain {

    std::unique_ptr<columns> IndexSelection::GenerateCandidateIndex(std::vector<std::string> workload,
                                                   IndexSelectionConfiguration config) {
      auto &peloton_parer = parser::PostgresParser::GetInstance();
      std::unordered_map<std::string, std::unique_ptr<columns>> indexable_columns;
      for (std::string query_string : workload) {
        auto indexable_for_query = FindIndexableColumnsInQuery(peloton_parer.BuildParseTree(query_string));
        indexable_columns.insert({query_string, indexable_for_query});
      }

      CostCalculator cost_calculator = CostCalculator(indexable_columns);
      std::unique_ptr<columns> candidate = EnumerateCandidateIndexes(config.candidate_index_limit, workload);

      std::unique_ptr<columns> result = BruteForceSearch(config.brute_force_limit, cost_calculator, candidate, workload);
      // Erase all std::unique_ptr<columns
      //> in the result from the candidate std::unique_ptr<columns
      //> to avoid duplicates
      candidate->erase(result->begin(), result->end());

      for (size_t index_size = config.brute_force_limit; index_size <= config.result_limit; index_size++) {
        auto best_index = std::max_element(candidate->begin(), candidate->end(), compareIndexes);
        candidate->erase(best_index);
        result->insert(*best_index);
      }
      return result;
    }

    std::unique_ptr<columns> IndexSelection::FindIndexableColumnsInQuery(std::unique_ptr<parser::SQLStatementList> queries) {
      std::unique_ptr<columns> result = std::make_unique<std::unordered_set<catalog::Column>>();
      if (!queries->is_valid) {
        return result;
      }
      for (size_t i = 0; i < queries->GetNumStatements(); i++) {
        parser::SQLStatement *query = queries->GetStatement(i);
        // TODO implement
      }
      return result;
    }

    bool IndexSelection::compareIndexes(const catalog::Column &i, const catalog::Column &j) {
      // TODO implement
      return false;
    }

    std::unique_ptr<columns> IndexSelection::EnumerateCandidateIndexes(size_t limit, std::vector<std::string> &workload) {
      // TODO implement
      return nullptr;
    }

    std::unique_ptr<columns> IndexSelection::BruteForceSearch(size_t limit, CostCalculator &cost_calculator, std::unique_ptr<columns> &candidates,
                                             std::vector<std::string> &workload) {
      // TODO implement
      return nullptr;
    }

    IndexSelection::CostCalculator::CostCalculator(std::unordered_map<std::string, std::unique_ptr<columns>> &indexable_columns) {
      // TODO implement
    }

    double IndexSelection::CostCalculator::DeriveCost(const std::unique_ptr<columns> configuration,
                                                      const std::vector<std::string> &workload) {
      // TODO implement
      return 0;
    }
  }
}