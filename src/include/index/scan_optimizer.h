//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// index.h
//
// Identification: src/include/index/scan_optimizer.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>

#include "index/index.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/pool.h"
#include "catalog/schema.h"
#include "catalog/manager.h"
#include "storage/tuple.h"

#include "index/index_util.h"

namespace catalog {
class Schema;
}

namespace storage {
class Tuple;
}

namespace peloton {
namespace index {

/*
 * class ConjunctionScanPredicate - Represents a series of AND-ed predicates
 *
 * This class does not store the predicate itself (they should be stored as
 * vectors elsewhere)
 */
class ConjunctionScanPredicate {
 private:
   
  // This is the list holding indices of the upper bound and lower bound
  // for the scan key. We only keep index in the corresponding Value[]
  // since some values might not be able to bind when constructing the
  // IndexScanPlan
  //
  // The length of this vector should equal the number of columns in
  // index key
  std::vector<std::pair<oid_t, oid_t>> value_index_list;
  
  // This vector holds indices for those index key columns that have
  // a free variable
  // We use this to speed up value binding since we could just skip
  // columns that do not have a free variabke
  //
  // Also if there is no value to bind, we just check its size and
  // skip the binding stage entirely to make query even faster
  std::vector<oid_t> missing_value_index_list;
  
  // Whether the query is a point query.
  //
  // If is point query then the ubound and lbound index must be equal
  bool is_point_query;
  
  // These two represents low key and high key of the predicate scan
  // We fill in these two values using the information available as much as
  // we can, and if there are still values missing (that needs to be bound at
  // run time, then just leave them there and bind at run time) they are left
  // blank, and we use these two keys as template to construct search key
  // at run time
  storage::Tuple *low_key_p;
  storage::Tuple *high_key_p;
  
 public:
   
  /*
   * ConstructScanInterval() - Find value indices for scan start key and end key
   *
   * NOTE: Currently only AND operation is supported inside IndexScanPlan, in a
   * sense that we buffer the binding between key columns and actual values in
   * the IndexScanPlan object, assuming that for each column there is only one
   * interval to scan, such that the scan could be classified by its high key
   * and low key. This is true for AND, but not true for OR
   */
  void ConstructScanInterval(const IndexMetadata *metadata_p,
                             const std::vector<oid_t> &tuple_column_id_list,
                             const std::vector<ExpressionType> &expr_list) {

    // This must hold for all cases
    PL_ASSERT(tuple_column_id_list.size() == expr_list.size());

    // This function will modify value_index_list, but value_index_list
    // should have capacity 0 to avoid further problems
    is_point_query = IsPointQuery(metadata_p,
                                  tuple_column_id_list,
                                  expr_list,
                                  value_index_list);
    
    LOG_TRACE("Constructing scan interval. Point query = %d", is_point_query);
    
    // First reserve some space to avoid calling malloc() for multiple times
    missing_value_index_list.reserve(metadata_p->GetColumnCount());
    
    // For each column in the index key, if there is not a bound
    // representable as Value object then we use min and max of the
    // corresponding type
    for(const auto &index_pair : value_index_list) {
      if(index_pair.first == INVALID_OID) {
        
      }
    }
  }
};
  
}  // End index namespace
}  // End peloton namespace
