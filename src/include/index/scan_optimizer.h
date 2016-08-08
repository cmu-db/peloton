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
#include "common/printable.h"

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
  // The element is a pair of oid_t. The first oid_t is the index
  // in index key that needs a binding, and the second oid_t is the
  // index inside the new Value array during late binding
  std::vector<std::pair<oid_t, oid_t>> low_key_bind_list;
  std::vector<std::pair<oid_t, oid_t>> high_key_bind_list;
  
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
   * Construction - Initializes boundary keys
   */
  ConjunctionScanPredicate(Index *index_p,
                           const std::vector<Value> &value_list,
                           const std::vector<oid_t> &tuple_column_id_list,
                           const std::vector<ExpressionType> &expr_list) {
    
    // It contains a pointer to the index key schema
    IndexMetadata *metadata_p = index_p->GetMetadata();
    
    // Then allocate storage for key template
    
    // Give it a schema and true flag to let the constructor allocate
    // memory for holding fields inside the tuple
    //
    // The schema will not be freed by tuple, but its internal data array will
    low_key_p = new storage::Tuple(metadata_p->GetKeySchema(), true);
    high_key_p = new storage::Tuple(metadata_p->GetKeySchema(), true);
    
    // This further initializes is_point_query flag, and then
    // fill the high key and low key with boundary values
    ConstructScanInterval(index_p,
                          value_list,
                          tuple_column_id_list,
                          expr_list);
    
    return;
  }
  
  /*
   * Destructor - Deletes low key and high key template tuples
   *
   * NOTE: If there is memory leak then it is likely to be caused
   * by pointer ownership problem brought about by Tuple and Value
   */
  ~ConjunctionScanPredicate() {
    delete low_key_p;
    delete high_key_p;
    
    return;
  }
  
  /*
   * BindValueToIndexKey() - Bind the value to a column of a given tuple
   *
   * This function binds the given value to the given column of a given key,
   * if the value is not a placeholder for late binding. If it is, then
   * it does not bind actual value, but instead return the index of the
   * value object for future binding.
   *
   * If this function is called for late binding then caller is responsible
   * for checking return value not being INVALID_OID, since during late binding
   * stage all values must be valid
   */
  oid_t BindValueToIndexKey(Index *index_p,
                            const Value &value,
                            storage::Tuple *index_key_p,
                            oid_t index) {
    ValueType bind_type = value.GetValueType();
    
    if(bind_type == VALUE_TYPE_FOR_BINDING_ONLY_INTEGER) {
      return static_cast<oid_t>(ValuePeeker::PeekBindingOnlyInteger(value));
    }
    
    // This is the type of the actual column
    ValueType column_type = index_key_p->GetType(index);
    
    // If the given value's type equals expected type for the column then
    // set value directly
    // Otherwise we need to cast the value first
    if(column_type == bind_type) {
      index_key_p->SetValue(index, value, index_p->GetPool());
    } else {
      index_key_p->SetValue(index, value.CastAs(column_type), index_p->GetPool());
    }
    
    return INVALID_OID;
  }
   
  /*
   * ConstructScanInterval() - Find value indices for scan start key and end key
   *
   * NOTE: Currently only AND operation is supported inside IndexScanPlan, in a
   * sense that we buffer the binding between key columns and actual values in
   * the IndexScanPlan object, assuming that for each column there is only one
   * interval to scan, such that the scan could be classified by its high key
   * and low key. This is true for AND, but not true for OR
   */
  void ConstructScanInterval(Index *index_p,
                             const std::vector<Value> &value_list,
                             const std::vector<oid_t> &tuple_column_id_list,
                             const std::vector<ExpressionType> &expr_list) {

    // This must hold for all cases
    PL_ASSERT(tuple_column_id_list.size() == expr_list.size());

    // We need to check index key schema
    const IndexMetadata *metadata_p = index_p->GetMetadata();

    // This function will modify value_index_list, but value_index_list
    // should have capacity 0 to avoid further problems
    is_point_query = IsPointQuery(metadata_p,
                                  tuple_column_id_list,
                                  expr_list,
                                  value_index_list);
    
    // value_index_list should be of the same length as the index key
    // schema, since it maps index key column to indices inside value_list
    PL_ASSERT(metadata_p->GetColumnCount() == value_index_list.size());
    
    LOG_INFO("Constructing scan interval. Point query = %d", is_point_query);
    
    // For each column in the index key, if there is not a bound
    // representable as Value object then we use min and max of the
    // corresponding type
    for(oid_t i = 0;i < value_index_list.size();i++) {
      const std::pair<oid_t, oid_t> &index_pair = value_index_list[i];
      
      // We use the type of the current index key column to get the
      // +Inf, -Inf and/or casted type for Value object
      ValueType index_key_column_type = metadata_p->GetKeySchema()->GetType(i);
      
      // If the lower bound of this column is not specified by the predicate
      // then we fill it with the minimum
      //
      // Also do the same for upper bound
      if(index_pair.first == INVALID_OID) {
        
        // We set the value using index's varlen pool, if any VARCHAR is
        // involved (this is OK since the routine only runs for once)
        low_key_p->SetValue(i,
                            Value::GetMinValue(index_key_column_type),
                            index_p->GetPool());
      } else {
        oid_t bind_ret = BindValueToIndexKey(index_p,
                                             value_list[index_pair.first],
                                             low_key_p,
                                             i);
                                             
        if(bind_ret != INVALID_OID) {
          LOG_INFO("Low key for column %u needs late binding!", i);
          
          // The first element is index, and the second element
          // is the return value, which is the future index in the
          // value object array
          low_key_bind_list.push_back(std::make_pair(i, bind_ret));
        }
      }
      
      if(index_pair.second == INVALID_OID) {
        
        // We set the value using index's varlen pool, if any VARCHAR is
        // involved (this is OK since the routine only runs for once)
        high_key_p->SetValue(i,
                             Value::GetMaxValue(index_key_column_type),
                             index_p->GetPool());
      } else {
        oid_t bind_ret = BindValueToIndexKey(index_p,
                                             value_list[index_pair.second],
                                             high_key_p,
                                             i);

        if(bind_ret != INVALID_OID) {
          LOG_INFO("High key for column %u needs late binding!", i);

          // The first element is index, and the second element
          // is the return value, which is the future index in the
          // value object array
          high_key_bind_list.push_back(std::make_pair(i, bind_ret));
        }
      }
      
      // At the end of loop
      // All control paths must pass through this
      i++;
    } // for index_pair in the list
    
    return;
  }
};
  
}  // End index namespace
}  // End peloton namespace
