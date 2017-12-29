//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// index_util.cpp
//
// Identification: src/index/index_util.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "index/index.h"

#include <algorithm>
#include <sstream>

#include "index/index_util.h"
#include "type/value_factory.h"

namespace peloton {
namespace index {

/*
 * HasNonOptimizablePredicate() - Check whether there are expressions that
 *                                cause the entire predicate non-optmizable
 *
 * This function simply checks whether there is any expression of the
 * following types that renders the scan predicate non-optmizable:
 *
 * NOTEQUAL
 * IN
 * LIKE
 * NOTLIKE
 *
 * If any of these appears as part of the scan predicate then we could
 * stop optimizing the predicate at the beginning. So this check serves
 * as a fast path
 *
 * Please refer to src/include/type/types.h for a complete list of comparison
 * operators
 */
bool IndexUtil::HasNonOptimizablePredicate(const std::vector<ExpressionType> &expr_types) {
  for(auto t : expr_types) {
    if (t == ExpressionType::COMPARE_NOTEQUAL ||
        t == ExpressionType::COMPARE_IN       ||
        t == ExpressionType::COMPARE_LIKE     ||
        t == ExpressionType::COMPARE_NOTLIKE) {
      return true;
    }
  }
  
  return false;
}

/*
 * FindValueIndex() - Check whether a predicate scan is point query
 *                    and construct value_index_list
 *
 * Point query may be implemented by the index as a more efficient
 * operation than scanning with a key, which is the case for BwTree.
 * Therefore, it is desirable for the index system to be albe to
 * identify point queries and optimize for them.
 *
 * NOTE: This function checks whether all expression types are equality
 * and also it checks WHETHER THE COLUMN ID LIST COVERS ALL COLUMNS IN
 * THE INDEX KEY. Since it is possible that some columns are left not bounded
 * by the predicate in which case even if all predicates are equal on
 * the remaining columns, it is not a point query
 *
 * The tuple column id list complies with the following protocol between
 * the optimizer and index wrapper:
 *   1. tuple_column_id_list contains indices into tuple key schema rather
 *      than the key schema
 *   2. tuple_column_id_list contains indices only for columns that are indexed
 *      For those columns not indexed by the current metadata they shall not
 *      appear
 *   3. tuple_column_id_list is not sorted by any means
 *   4. tuple_column_id_list may have duplicated entries, because
 *     4.1 there might be more than one predicate on the same column, e.g.
 *         a > 5 AND a < 3
 *     4.2 there might be duplicated (malformed) input from the user, e.g.
 *         a == 5 AND a== 3; a == 5 AND a > 10. It is assumed that such
 *         malformed expressions are not removed by the optimizer, but returned
 *         tuples should be filtered out by the executor (which keeps a separate
 *         predicate)
 *
 * NOTE 2: This function takes an extra argument value_index_list, which is a
 * list of indices into the value array for constructing the actual lower bound
 * and upper bound search key during runtime. The vector element has two
 * components, the first being index into value list for lower bound, the
 * second being the index into value list for upper bound. If the query is a
 * point query then these two are equal.
 *
 * NOTE 3: This function does not guarantee it is ma11oc()-free, since it calls
 * reserve() on value_index_list.
 */
bool IndexUtil::FindValueIndex(const IndexMetadata *metadata_p,
                    const std::vector<oid_t> &tuple_column_id_list,
                    const std::vector<ExpressionType> &expr_list,
                    std::vector<std::pair<oid_t, oid_t>> &value_index_list) {

  // Make sure these two are consistent at least on legnth
  PL_ASSERT(tuple_column_id_list.size() == expr_list.size());

  // Number of columns in index key
  size_t index_column_count = metadata_p->GetColumnCount();

  // This will call reserve() and also changes size accordingly
  // Since we want random accessibility on this vector
  //
  // Also resize the vector to pairs of INVALID_OID, which means the
  // column has not seen any constraint yet
  value_index_list.resize(index_column_count,
                          std::make_pair(INVALID_OID, INVALID_OID));
  
  // This is used to count how many index columns have an "==" predicate
  size_t counter = 0;

  // i in the common index for tuple column id, expression and value
  for(oid_t i = 0;i < tuple_column_id_list.size();i++) {
    
    // This is only used to retrieve index column id
    oid_t tuple_column_id = tuple_column_id_list[i];

    // Map tuple column to index column
    oid_t index_column_id = \
      metadata_p->GetTupleToIndexMapping()[tuple_column_id];
      
    // Make sure the mapping exists (i.e. the tuple column is in
    // index columns)
    PL_ASSERT(index_column_id != INVALID_OID);
    PL_ASSERT(index_column_id < index_column_count);
    
    ExpressionType e_type = expr_list[i];
    
    // Fill in lower bound and upper bound separately
    // Node that for "==" expression it both defines a lower bound
    // and an upper bound, so we should run if statements twice
    // rather than if ... else ...

    if(DefinesLowerBound(e_type) == true) {
      if(value_index_list[index_column_id].first == INVALID_OID) {
        // The lower bound is on value i
        value_index_list[index_column_id].first = i;
      }
    }
    
    if(DefinesUpperBound(e_type) == true) {
      if(value_index_list[index_column_id].second == INVALID_OID) {
        // The upper bound is on value i
        value_index_list[index_column_id].second = i;
        
        // If all constraints are equal, then everytime we set
        // an upperbound it's time to check
        
        // Since we already know second should not be INVALID_OID
        // just checking whether these two equals suffices
        if(value_index_list[index_column_id].second == \
           value_index_list[index_column_id].first) {
          PL_ASSERT(value_index_list[index_column_id].second != INVALID_OID);
          
          // We have seen an equality relation
          counter++;
          
          // We could return since we have seen all columns being
          // filled with equality relation
          //
          // Also if this happens then all future expression does not
          // have any effect - since they
          if(counter == index_column_count) {
            return true;
          }
        } // if current index key column is an equality
      } // if the upperbound has not yet been filled
    } // if current expression defines an upper bound
  } // for all tuple column ids in the list
    
  return false;
}


/*
 * ValuePairComparator() - Compares std::pair<Value, int>
 *
 * Values are compared first and if not equal return result
 * If values are equal then compare the second element which is an int
 * and return the result
 */
bool IndexUtil::ValuePairComparator(const std::pair<type::Value, int> &i,
                                    const std::pair<type::Value, int> &j) {

  // If first elements are equal then compare the second element
  if (i.first.CompareEquals(j.first) == CmpBool::TRUE) {
    return i.second < j.second;
  }
  
  // Otherwise compare the first element for "<" or ">"
  return i.first.CompareLessThan(j.first) == CmpBool::TRUE;
}

void IndexUtil::ConstructIntervals(oid_t leading_column_id,
                        const std::vector<type::Value> &values,
                        const std::vector<oid_t> &key_column_ids,
                        const std::vector<ExpressionType> &expr_types,
          std::vector<std::pair<type::Value, type::Value>> &intervals) {
  // Find all contrains of leading column.
  // Equal --> > < num
  // > >= --->  > num
  // < <= ----> < num
  std::vector<std::pair<type::Value, int>> nums;
  for (size_t i = 0; i < key_column_ids.size(); i++) {
    if (key_column_ids[i] != leading_column_id) {
      continue;
    }

    // If leading column
    if (DefinesLowerBound(expr_types[i])) {
      nums.push_back(std::pair<type::Value, int>(values[i], -1));
    } else if (DefinesUpperBound(expr_types[i])) {
      nums.push_back(std::pair<type::Value, int>(values[i], 1));
    } else {
      // Currently if it is not >  < <= then it must be ==
      // *** I could not find BETWEEN expression in types.h so did not add it
      // into the list
      PL_ASSERT(expr_types[i] == ExpressionType::COMPARE_EQUAL);
      
      nums.push_back(std::pair<type::Value, int>(values[i], -1));
      nums.push_back(std::pair<type::Value, int>(values[i], 1));
    }
  }

  // Have merged all constraints in a single line, sort this line.
  std::sort(nums.begin(), nums.end(), ValuePairComparator);
  
  // This enforces that there must be at least one constraint on the
  // leading column, if the search is eligible for optimization
  PL_ASSERT(nums.size() > 0);

  // Build intervals.
  // get some dummy value
  type::Value cur;
  size_t i = 0;
  if (nums[0].second < 0) {
    cur = nums[0].first;
    i++;
  } else {
    cur = type::Type::GetMinValue(nums[0].first.GetTypeId());
  }

  while (i < nums.size()) {
    if (nums[i].second > 0) {
      if (i + 1 < nums.size() && nums[i + 1].second < 0) {
        // right value
        intervals.push_back(std::pair<type::Value, type::Value>(
          cur, nums[i].first));
        cur = nums[i + 1].first;
      } else if (i + 1 == nums.size()) {
        // Last value while right value
        intervals.push_back(std::pair<type::Value, type::Value>(
          cur, nums[i].first));
        cur = type::ValueFactory::GetNullValueByType(nums[0].first.GetTypeId());
      }
    }
    i++;
  }

  if (cur.IsNull() == false) {
    intervals.push_back(std::pair<type::Value, type::Value>(
        cur, type::Type::GetMaxValue(nums[0].first.GetTypeId())));
  }

  // Finish invtervals building.
};

void IndexUtil::FindMaxMinInColumns(oid_t leading_column_id,
                         const std::vector<type::Value> &values,
                         const std::vector<oid_t> &key_column_ids,
                         const std::vector<ExpressionType> &expr_types,
                         std::map<oid_t, std::pair<type::Value,
                             type::Value>> &non_leading_columns) {
  // find extreme nums on each column.
  LOG_TRACE("FindMinMax leading column %d\n", leading_column_id);
  for (size_t i = 0; i < key_column_ids.size(); i++) {
    oid_t column_id = key_column_ids[i];
    if (column_id == leading_column_id) {
      continue;
    }

    if (non_leading_columns.find(column_id) == non_leading_columns.end()) {
      auto type = values[i].GetTypeId();
      // std::pair<Value, Value> *range = new std::pair<Value,
      // Value>(Value::GetMaxValue(type),
      //                                            Value::GetMinValue(type));
      // std::pair<oid_t, std::pair<Value, Value>> key_value(column_id, range);
      non_leading_columns.insert(std::pair<oid_t,
        std::pair<type::Value, type::Value>>(
          column_id, std::pair<type::Value, type::Value>(
            type::ValueFactory::GetNullValueByType(type),
            type::ValueFactory::GetNullValueByType(type))));
      //  non_leading_columns[column_id] = *range;
      // delete range;
      LOG_TRACE("Insert a init bounds, left size: %lu, right description: %s\n",
                non_leading_columns[column_id].first.GetInfo().size(),
                non_leading_columns[column_id].second.GetInfo().c_str());
    }

    if (DefinesLowerBound(expr_types[i]) ||
        expr_types[i] == ExpressionType::COMPARE_EQUAL) {
      LOG_TRACE("min cur %lu compare with %s\n",
                non_leading_columns[column_id].first.GetInfo().size(),
                values[i].GetInfo().c_str());
      if (non_leading_columns[column_id].first.IsNull() ||
          non_leading_columns[column_id].first
            .CompareGreaterThan(values[i]) == CmpBool::TRUE) {
        LOG_TRACE("Update min\n");
        non_leading_columns[column_id].first = values[i].Copy();
      }
    }

    if (DefinesUpperBound(expr_types[i]) ||
        expr_types[i] == ExpressionType::COMPARE_EQUAL) {
      LOG_TRACE("max cur %s compare with %s\n",
                non_leading_columns[column_id].second.GetInfo().c_str(),
                values[i].GetInfo().c_str());
      if (non_leading_columns[column_id].first.IsNull() ||
          non_leading_columns[column_id].second.
            CompareLessThan(values[i]) == CmpBool::TRUE) {
        LOG_TRACE("Update max\n");
        non_leading_columns[column_id].second = values[i].Copy();
      }
    }
  }

  // check if min value is right bound or max value is left bound, if so, update
  for (const auto &k_v : non_leading_columns) {
    if (k_v.second.first.IsNull()) {
      non_leading_columns[k_v.first].first =
          type::Type::GetMinValue(k_v.second.first.GetTypeId());
    }
    if (k_v.second.second.IsNull()) {
      non_leading_columns[k_v.first].second =
          type::Type::GetMinValue(k_v.second.second.GetTypeId());
    }
  }
}

std::string IndexUtil::Debug(Index *index) {
  std::vector<ItemPointer *> location_ptrs;
  index->ScanAllKeys(location_ptrs);

  std::ostringstream os;
  int i = 0;
  for (auto ptr : location_ptrs) {
    os << StringUtil::Format("%03d: {%d, %d}\n", i, ptr->block, ptr->offset);
    i += 1;
  }
  return (os.str());
}

}  // namespace index
}  // namespace peloton
