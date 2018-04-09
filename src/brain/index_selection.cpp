//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// index_selection.cpp
//
// Identification: src/brain/index_selection.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "brain/index_selection.h"
#include "common/logger.h"

namespace peloton {
namespace brain {

IndexSelection::IndexSelection(std::shared_ptr<Workload> query_set) {
  query_set_ = query_set;
}

std::unique_ptr<Configuration> IndexSelection::GetBestIndexes() {
  std::unique_ptr<Configuration> C(new Configuration());
  // Figure 4 of the "Index Selection Tool" paper.
  // Split the workload 'W' into small workloads 'Wi', with each
  // containing one query, and find out the candidate indexes
  // for these 'Wi'
  // Finally, combine all the candidate indexes 'Ci' into a larger
  // set to form a candidate set 'C' for the provided workload 'W'.
  auto queries = query_set_->GetQueries();
  for (auto query: queries) {
    // Get admissible indexes 'Ai'
    Configuration Ai;
    GetAdmissableIndexes(query, Ai);

    Workload Wi;
    Wi.AddQuery(query);

    // Get candidate indexes 'Ci' for the workload.
    Configuration Ci;
    Enumerate(Ai, Ci, Wi);

    // Add the 'Ci' to the union configuration set 'C'
    C->Add(Ci);
  }
  return C;
}

// TODO: [Siva]
// Given a set of given indexes, this function
// finds out the set of cheapest indexes for the workload.
void IndexSelection::Enumerate(Configuration &indexes,
                               Configuration &chosen_indexes,
                               Workload &workload) {
  (void) indexes;
  (void) chosen_indexes;
  (void) workload;
  return;
}

// TODO: [Vamshi]
void IndexSelection::GetAdmissableIndexes(SQLStatement *query,
                                          Configuration &indexes) {
  (void) query;
  (void) indexes;
  return;
}

}  // namespace brain
}  // namespace peloton
