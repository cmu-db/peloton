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
#include "catalog/column.h"

namespace peloton {
  namespace brain {
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
      typedef std::unique_ptr<std::unordered_set<catalog::Column>> indexes;

      IndexSelection() = delete;

    public:
      static indexes GenerateCandidateIndex(std::vector<std::string> workload,
                                            IndexSelectionConfiguration config);

    private:

      class CostCalculator{
        friend class IndexSelection;

      private:
        // TODO: figure how to derive cost from Atomic Configurations
        CostCalculator(std::unordered_map<std::string, indexes> &indexable_columns);
        double DeriveCost(const indexes configuration, const std::vector<std::string> &workload);
      };

      static indexes FindIndexableColumnsInQuery(std::unique_ptr<parser::SQLStatementList> query);
      static bool compareIndexes(const catalog::Column &i, const catalog::Column &j);
      static indexes EnumerateCandidateIndexes(size_t limit, std::vector<std::string> &workload);
      static indexes BruteForceSearch(size_t limit,
                                      CostCalculator &cost_calculator,
                                      indexes &candidates,
                                      std::vector<std::string> &workload);
    };

  }  // namespace brain
}  // namespace peloton

