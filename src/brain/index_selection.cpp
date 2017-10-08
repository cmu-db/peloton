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

namespace peloton {
  namespace brain {



    indexes IndexSelection::GenerateCandidateIndex(std::vector<std::string> workload,
                                                   IndexSelectionConfiguration config) {
      auto &peloton_parer = parser::PostgresParser::GetInstance();
      std::unordered_map<std::string, indexes> indexable_columns;
      for (std::string query_string : workload) {
        auto indexable_for_query = FindIndexableColumnsInQuery(peloton_parer.BuildParseTree(query_string));
        indexable_columns.insert({query_string, indexable_for_query});
      }

      CostCalculator cost_calculator = CostCalculator(indexable_columns);
      indexes candidate_indexes = EnumerateCandidateIndexes(config.candidate_index_limit, workload);

      indexes result = BruteForceSearch(config.brute_force_limit, cost_calculator, candidate_indexes, workload);
      // Erase all indexes in the result from the candidate indexes to avoid duplicates
      candidate_indexes->erase(result->begin(), result->end());

      for (size_t index_size = config.brute_force_limit; index_size <= config.result_limit; index_size++) {
        auto best_index = std::max_element(candidate_indexes->begin(), candidate_indexes->end(), compareIndexes);
        candidate_indexes->erase(best_index);
        result->insert(*best_index);
      }
      return result;
    }

    indexes IndexSelection::FindIndexableColumnsInQuery(std::unique_ptr<parser::SQLStatementList> query) {
      // TODO implement
      return nullptr;
    }

    bool IndexSelection::compareIndexes(const catalog::Column &i, const catalog::Column &j) {
      // TODO implement
      return false;
    }

    indexes IndexSelection::EnumerateCandidateIndexes(size_t limit, std::vector<std::string> &workload) {
      // TODO implement
      return nullptr;
    }

    indexes IndexSelection::BruteForceSearch(size_t limit, CostCalculator &cost_calculator, indexes &candidates,
                                             std::vector<std::string> &workload) {
      // TODO implement
      return nullptr;
    }

    IndexSelection::CostCalculator::CostCalculator(std::unordered_map<std::string, indexes> &indexable_columns) {
      // TODO implement
    }

    double IndexSelection::CostCalculator::DeriveCost(const indexes configuration,
                                                      const std::vector<std::string> &workload) {
      // TODO implement
      return 0;
    }
  }
}