//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// scan_optimizer.h
//
// Identification: src/include/index/scan_optimizer.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>

#include "catalog/schema.h"
#include "common/logger.h"
#include "index/index.h"
#include "storage/tuple.h"
#include "type/value_peeker.h"

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
 *
 * There are two corner cases with this class: full index scan and point query
 *
 * For full index scan, no key allocation is done, and both low key and high key
 * should not be touched on all cases, including construction of bounds and
 * value late binding. For point query, only the low key should be touched,
 * and it is exactly the key for construction of lower bound and for value
 * late binding
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
  std::vector<std::pair<oid_t, oid_t>> value_index_list_;

  // This vector holds indices for those index key columns that have
  // a free variable
  // We use this to speed up value binding since we could just skip
  // columns that do not have a free variable
  //
  // The element is a pair of oid_t. The first oid_t is the index
  // in index key that needs a binding, and the second oid_t is the
  // index inside the new Value array during late binding
  std::vector<std::pair<oid_t, oid_t>> low_key_bind_list_;
  std::vector<std::pair<oid_t, oid_t>> high_key_bind_list_;

  // Whether the query is a point query.
  //
  // If is point query then the ubound and lbound index must be equal
  bool is_point_query_;

  // Only queries with > >= == < <= get a chance to be optimized
  // i.e. those with IN NOT IN LIKE NOT LIKE NOT EQUAL could not be optimized
  // and they will result in an index scan
  //
  // There is a trick to make the situation less miserable: if there are
  // many conjunctive predicates, and if one of them require a full index
  // scan, then we do not conduct index scan multiple times, and instead
  // only a full index scan is done and we let the index scan plan to
  // filter tuples using its full predicate
  bool full_index_scan_;

  // These two represents low key and high key of the predicate scan
  // We fill in these two values using the information available as much as
  // we can, and if there are still values missing (that needs to be bound at
  // run time, then just leave them there and bind at run time) they are left
  // blank, and we use these two keys as template to construct search key
  // at run time
  storage::Tuple *low_key_p_;
  storage::Tuple *high_key_p_;

 private:
  /*
   * SetTupleColumnValueForKey() - Similar to SetTupleColumnValue() but only
   *                               sets a given key
   *
   * This function is designed to work for range query. This function accepts
   * a key pointer, which must be either the low key or high key
   */
  void SetTupleColumnValueForKey(Index *index_p,
                                 const std::vector<oid_t> &column_id_list,
                                 const std::vector<type::Value> &new_value_list,
                                 storage::Tuple *key_p) {
    // Since we used low key and high key, this cannot be a point query
    PELOTON_ASSERT(is_point_query_ == false);
    PELOTON_ASSERT(new_value_list.size() == column_id_list.size());

    // We do not accept using other keys except
    // low key and high key of this instance
    PELOTON_ASSERT(key_p == low_key_p_ || key_p == high_key_p_);

    int i = 0;
    for (oid_t tuple_column : column_id_list) {
      oid_t index_column = index_p->TupleColumnToKeyColumn(tuple_column);

      UNUSED_ATTRIBUTE oid_t bind_ret =
          BindValueToIndexKey(index_p, new_value_list[i], key_p, index_column);

      PELOTON_ASSERT(bind_ret == INVALID_OID);
      i++;
    }

    return;
  }

 public:
  /*
   * Construction - Initializes boundary keys
   */
  ConjunctionScanPredicate(Index *index_p,
                           const std::vector<type::Value> &value_list,
                           const std::vector<oid_t> &tuple_column_id_list,
                           const std::vector<ExpressionType> &expr_list) {
    // It contains a pointer to the index key schema
    IndexMetadata *metadata_p = index_p->GetMetadata();

    // If there are expressions that result in full index scan
    // then this flag is set, and we do not construct lower bound
    //
    // Actually if we do not do this optimization, ConstructScanInterval()
    // could still correctly construct all bounds as +/-Inf, but the index
    // scanner does not know that we will do a table scan so in the case
    // of multiple disjunctive predicates there might be multiple scan
    // which is wasteful since a single index full scan should cover all cases
    full_index_scan_ = IndexUtil::HasNonOptimizablePredicate(expr_list);

    // Only construct the two keys when full table scan is unnecessary
    if (full_index_scan_ == false) {
      // Then allocate storage for key template

      // Give it a schema and true flag to let the constructor allocate
      // memory for holding fields inside the tuple
      //
      // The schema will not be freed by tuple, but its internal data array will
      low_key_p_ = new storage::Tuple(metadata_p->GetKeySchema(), true);
      high_key_p_ = new storage::Tuple(metadata_p->GetKeySchema(), true);

      // This further initializes is_point_query flag, and then
      // fill the high key and low key with boundary values
      ConstructScanInterval(index_p, value_list, tuple_column_id_list,
                            expr_list);
    } else {
      // This will not be touched in the destructor
      low_key_p_ = nullptr;
      high_key_p_ = nullptr;

      // This must hold since full table scan could not be point query
      is_point_query_ = false;
    }

    return;
  }

  /*
   * Copy Constructor - Deleted
   *
   * This was deleted to avoid copying the two member pointers without
   * a clear pointer ownership
   */
  ConjunctionScanPredicate(const ConjunctionScanPredicate &other) {
    value_index_list_ = other.value_index_list_;
    low_key_bind_list_ = other.low_key_bind_list_;
    high_key_bind_list_ = other.high_key_bind_list_;
    is_point_query_ = other.is_point_query_;
    full_index_scan_ = other.full_index_scan_;

    low_key_p_ = new storage::Tuple(*(other.low_key_p_));
    high_key_p_ = new storage::Tuple(*(other.high_key_p_));
  }

  /*
   * operator= - Deleted
   *
   * Same reason as deleting the copy constructor
   */
  ConjunctionScanPredicate &operator=(const ConjunctionScanPredicate &other) {
    value_index_list_ = other.value_index_list_;
    low_key_bind_list_ = other.low_key_bind_list_;
    high_key_bind_list_ = other.high_key_bind_list_;
    is_point_query_ = other.is_point_query_;
    full_index_scan_ = other.full_index_scan_;

    low_key_p_ = new storage::Tuple(*(other.low_key_p_));
    high_key_p_ = new storage::Tuple(*(other.high_key_p_));

    return *this;
  }

  /*
   * Move assignment - Deleted
   *
   * We do not need move it.
   */
  ConjunctionScanPredicate &operator=(ConjunctionScanPredicate &&) = delete;

  /*
   * Destructor - Deletes low key and high key template tuples
   *
   * NOTE: If there is memory leak then it is likely to be caused
   * by pointer ownership problem brought about by Tuple and Value
   *
   * NOTE 2: We must check whether the pointer is nullptr, since for a
   * move constructor it is possible that the keys are cleared
   */
  ~ConjunctionScanPredicate() {
    if (low_key_p_ != nullptr) {
      delete low_key_p_;

      PELOTON_ASSERT(high_key_p_ != nullptr);
    }

    delete high_key_p_;
  
    return;
  }

  /*
   * SetTupleColumnValue() - Sets a value for a particular column inside
   *                         both low key and high key
   *
   * This function behaves in a way like this:
   *   1. For non-point query the same key field in both low key and high key
   *      is set
   *   2. For point query the low key will be updated
   *   3. This function does not deal with range queries - it knows no semantics
   *      of range query and the expression type for each index key column
   *      so use this at your own risk
   *   4. column_id is the column ID inside table rather than inside index key
   *   5. The index must be the same as the index passed into the constructor
   *
   * Note that for this special case we do not call
   * SetTupleColumnValueForLowKey() because this function requires two traverses
   * of the two lists in the argument which could be avoided
   */
  void SetTupleColumnValue(Index *index_p,
                           const std::vector<oid_t> &column_id_list,
                           const std::vector<type::Value> &new_value_list) {
    // They are one to one
    PELOTON_ASSERT(new_value_list.size() == column_id_list.size());

    // This is used to address new_value_list
    int i = 0;
    for (oid_t tuple_column : column_id_list) {
      // Translate tuple column to index column
      // This function guarantees the return value must be valid one
      oid_t index_column = index_p->TupleColumnToKeyColumn(tuple_column);

      UNUSED_ATTRIBUTE oid_t bind_ret;

      // Always bind the value to low key
      // It should return INVALID_OID since we do not have any
      // late binding here
      bind_ret = BindValueToIndexKey(index_p, new_value_list[i], low_key_p_,
                                     index_column);
      PELOTON_ASSERT(bind_ret == INVALID_OID);

      // is_point_query_ is only determined by expression type
      // Also if not point query then also bind to high key
      if (is_point_query_ == true) {
        bind_ret = BindValueToIndexKey(index_p, new_value_list[i], high_key_p_,
                                       index_column);
        PELOTON_ASSERT(bind_ret == INVALID_OID);
      }

      i++;
    }

    return;
  }

  /*
   * SetTupleColumnValueForLowKey() - Sets columns for low key
   */
  inline void SetTupleColumnValueForLowKey(
      Index *index_p, const std::vector<oid_t> &column_id_list,
      const std::vector<type::Value> &new_value_list) {
    SetTupleColumnValueForKey(index_p, column_id_list, new_value_list,
                              low_key_p_);

    return;
  }

  /*
   * SetTupleColumnValueForHighKey() - Sets columns for high key
   */
  inline void SetTupleColumnValueForHighKey(
      Index *index_p, const std::vector<oid_t> &column_id_list,
      const std::vector<type::Value> &new_value_list) {

    SetTupleColumnValueForKey(index_p, column_id_list, new_value_list,
                              high_key_p_);

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
   * for checking return value not being INVALID_OID, since during late
   *binding
   * stage all values must be valid
   *
   * NOTE: This function is made static to reflact the fact that it does not
   * require any data member
   */
  static oid_t BindValueToIndexKey(Index *index_p, const type::Value &value,
                                   storage::Tuple *index_key_p, oid_t index) {
    type::TypeId bind_type = value.GetTypeId();

    if (bind_type == type::TypeId::PARAMETER_OFFSET) {
      return static_cast<oid_t>(type::ValuePeeker::PeekParameterOffset(value));
    }

    // This is the type of the actual column
    type::TypeId column_type = index_key_p->GetType(index);

    // If the given value's type equals expected type for the column then
    // set value directly
    // Otherwise we need to cast the value first
    if (column_type == bind_type) {
      index_key_p->SetValue(index, value, index_p->GetPool());
    } else {
      type::Value casted_val = value.CastAs(column_type);
      index_key_p->SetValue(index, casted_val, index_p->GetPool());
    }

    return INVALID_OID;
  }

  /*
   * ConstructScanInterval() - Find value indices for scan start key and end
   *key
   *
   * NOTE: Currently only AND operation is supported inside IndexScanPlan, in
   *a
   * sense that we buffer the binding between key columns and actual values in
   * the IndexScanPlan object, assuming that for each column there is only one
   * interval to scan, such that the scan could be classified by its high key
   * and low key. This is true for AND, but not true for OR
   *
   * NOTE 2: This function should not be called if full_index_scan is true
   */
  void ConstructScanInterval(Index *index_p,
                             const std::vector<type::Value> &value_list,
                             const std::vector<oid_t> &tuple_column_id_list,
                             const std::vector<ExpressionType> &expr_list) {
    // This must hold for all cases
    PELOTON_ASSERT(tuple_column_id_list.size() == expr_list.size());

    // We need to check index key schema
    const IndexMetadata *metadata_p = index_p->GetMetadata();

    // This function will modify value_index_list, but value_index_list
    // should have capacity 0 to avoid further problems
    is_point_query_ = IndexUtil::FindValueIndex(metadata_p, tuple_column_id_list,
                                     expr_list, value_index_list_);

    // value_index_list should be of the same length as the index key
    // schema, since it maps index key column to indices inside value_list
    PELOTON_ASSERT(metadata_p->GetColumnCount() == value_index_list_.size());

    LOG_TRACE("Constructing scan interval. Point query = %d", is_point_query_);

    // For each column in the index key, if there is not a bound
    // representable as Value object then we use min and max of the
    // corresponding type
    for (oid_t i = 0; i < value_index_list_.size(); i++) {
      const std::pair<oid_t, oid_t> &index_pair = value_index_list_[i];

      // We use the type of the current index key column to get the
      // +Inf, -Inf and/or casted type for Value object
      type::TypeId index_key_column_type =
          metadata_p->GetKeySchema()->GetType(i);

      // If the lower bound of this column is not specified by the predicate
      // then we fill it with the minimum
      //
      // Also do the same for upper bound
      if (index_pair.first == INVALID_OID) {
        PELOTON_ASSERT(is_point_query_ == false);

        // We set the value using index's varlen pool, if any VARCHAR is
        // involved (this is OK since the routine only runs for once)
        type::Value val = (type::Type::GetMinValue(index_key_column_type));
        low_key_p_->SetValue(i, val, index_p->GetPool());
      } else {
        oid_t bind_ret = BindValueToIndexKey(
            index_p, value_list[index_pair.first], low_key_p_, i);

        if (bind_ret != INVALID_OID) {
          LOG_TRACE("Low key for column %u needs late binding!", i);

          // The first element is index, and the second element
          // is the return value, which is the future index in the
          // value object array
          low_key_bind_list_.push_back(std::make_pair(i, bind_ret));
        }
      }

      // Only bind the second half if point query is false
      if (is_point_query_ == false) {
        if (index_pair.second == INVALID_OID) {
          // We set the value using index's varlen pool, if any VARCHAR is
          // involved (this is OK since the routine only runs for once)
          type::Value val(type::Type::GetMaxValue(index_key_column_type));
          high_key_p_->SetValue(i, val, index_p->GetPool());
        } else {
          oid_t bind_ret = BindValueToIndexKey(
              index_p, value_list[index_pair.second], high_key_p_, i);

          if (bind_ret != INVALID_OID) {
            LOG_TRACE("High key for column %u needs late binding!", i);

            // The first element is index, and the second element
            // is the return value, which is the future index in the
            // value object array
            high_key_bind_list_.push_back(std::make_pair(i, bind_ret));
          }
        }
      }  // if is point query == false
    }    // for index_pair in the list

    return;
  }

  /*
   * LateBind() - Bind values to all columns of a given index key according
   *              to the previous planning result
   *
   * This function assumes that all values must be valid and should not
   * be placeholders
   *
   * NOTE: This function could not be called if full_index_scan is true
   */
  void LateBind(Index *index_p, const std::vector<type::Value> &value_list,
                const std::vector<std::pair<oid_t, oid_t>> key_bind_list,
                storage::Tuple *index_key_p) {
    PELOTON_ASSERT(full_index_scan_ == false);

    // Need to check there is not out of bound access
    LOG_TRACE("value list length = %lu", value_list.size());

    // For each item <key column index, value list index> do the binding job
    for (auto &bind_item : key_bind_list) {

      LOG_TRACE("bind first: %d; second: %d", bind_item.first,
                bind_item.second);
      LOG_TRACE("bind value: %s",
                value_list[bind_item.second].GetInfo().c_str());

      oid_t bind_ret = BindValueToIndexKey(
          index_p, value_list[bind_item.second], index_key_p, bind_item.first);

      LOG_TRACE("bind OK");

      // This could not be other values since all values must be
      // valid during the binding stage
      PELOTON_ASSERT(bind_ret == INVALID_OID);
      (void)bind_ret;
    }

    return;
  }

  /*
   * LateBindValues() - Late bind values given in the argument to the high key
   *                    and low key inside this object
   *
   * NOTE: This function is not thread-safe since if multiple threads are
   * calling this function, then the outcome is undefined. But anyway this
   * should be called together with IndexScanPlan's SetParameterValues(), so
   * if that function is not multithreaded then it is OK for this to be not
   * multithreaded
   *
   * NOTE 2: If the current query is point query then we only bind the low key
   * since for point query there is one query key
   *
   * NOTE 3: This function could not be called if full_index_scan if true
   */
  void LateBindValues(Index *index_p,
                      const std::vector<type::Value> &value_list) {
    PELOTON_ASSERT(full_index_scan_ == false);

    // Bind values to low key and high key respectively
    LateBind(index_p, value_list, low_key_bind_list_, low_key_p_);

    if (is_point_query_ == false) {
      LateBind(index_p, value_list, high_key_bind_list_, high_key_p_);
    }

    return;
  }

  /*
   * IsFullIndexScan() - Return whether this conjunction predicate is a full
   *                     index scan
   */
  inline bool IsFullIndexScan() const { return full_index_scan_; }

  /*
   * IsPointQuery() - Returns whether this conjunction is a point query
   */
  inline bool IsPointQuery() const { return is_point_query_; }

  /*
   * GetLowKey() - Returns the scan low key if this is not a point query
   *
   * For point queries assertion fails, since it is mandatory to call another
   * version of the same function to get query key for point query
   */
  inline const storage::Tuple *GetLowKey() const {
    PELOTON_ASSERT(is_point_query_ == false);
    PELOTON_ASSERT(full_index_scan_ == false);

    return low_key_p_;
  }

  /*
   * GetHighKey() - Returns the scan high key
   *
   * For point queries assertion fails, since there is no high key
   * for a point query
   */
  inline const storage::Tuple *GetHighKey() const {
    PELOTON_ASSERT(is_point_query_ == false);
    PELOTON_ASSERT(full_index_scan_ == false);

    return high_key_p_;
  }

  /*
   * GetPointQueryKey() - Returns the key for point query
   *
   * This function could only be called if the current query is a point query
   */
  inline const storage::Tuple *GetPointQueryKey() const {
    PELOTON_ASSERT(is_point_query_ == true);
    PELOTON_ASSERT(full_index_scan_ == false);

    return low_key_p_;
  }

  /*
   * GetBindingCount() - Returns the number of bindings that must be done
   *
   * This function is majorly for debugging purposes. Non-debugging
   * routines may call this function but the return value is not quite
   * enlightening
   */
  inline size_t GetBindingCount() const {
    return low_key_bind_list_.size() + high_key_bind_list_.size();
  }
};

/////////////////////////////////////////////////////////////////////
// End of class definition
/////////////////////////////////////////////////////////////////////

/////////////////////////////////////////////////////////////////////
// Start of another class definition
/////////////////////////////////////////////////////////////////////

/*
 * class IndexScanPredicate - The predicate for index scan
 *
 * This class maintains planner information about index scan, especially
 * columns involved in the index scan and their corresponding values.
 *
 * Please note that this class represents the most general form of scan
 * predicate that does not have any other constraints
 */
class IndexScanPredicate {
 private:
  // This holds all conjunctions which are assumed to be connected
  // together by disjunctions
  std::vector<ConjunctionScanPredicate> conjunction_list_;

  // This is the OR of all full_index_scan field in existing conjunction lists
  // If this is true then we could simply do an index scan and return all
  // entries inside the index rather than doing scans on each conjunction
  // predicate first and then do a union using hash table
  bool full_index_scan_;

 public:
  /*
   * Constructor - Initialize nothing
   *
   * Please note that we do not push any conjunction scan predicate into the
   * list here because they are usually done in another stage after the
   * containing IndexScanPlan has been initialized
   */
  IndexScanPredicate() : conjunction_list_{}, full_index_scan_{false} {}

  /*
   * AddConjunctionScanPredicate() - Adds a conjunction scan predicate
   *                                 i.e. (attr op value) AND (attr2 op
   *value)..
   *
   * This is the basic unit that we scan the index. If one of the conjunction
   * predicates are full index scan due to an expression that is not
   * optimizable, then we just set the full_index_scan flag inside this
   * object and always does full index scan
   *
   * Also note that for full index scan we do not need to bind the actual
   *value
   * because anyway a full scan will be conducted and there is no point
   * updating the low key and high key
   */
  void AddConjunctionScanPredicate(
      Index *index_p, const std::vector<type::Value> &value_list,
      const std::vector<oid_t> &tuple_column_id_list,
      const std::vector<ExpressionType> &expr_list) {
    // Construct a conjunction predicate in-place and initialize all
    // low key and high key and binding slots
    conjunction_list_.emplace_back(index_p, value_list, tuple_column_id_list,
                                   expr_list);

    // If any of the newly added predicate results in a full index scan
    // then the entire predicate is a scan
    full_index_scan_ =
        full_index_scan_ || conjunction_list_.back().IsFullIndexScan();

    return;
  }

  /*
   * LateBindValues() - Bind values to all conjunction predicates of the
   *                    index scan predicate
   *
   * This function only operates on all predicates present in the array, and
   * are not responsible for future addition of predicates if any
   *
   * If the current predicate has already degraded into a full index scan,
   *then
   * this function simply return, since there is no point updating the low
   * key and high key with full index scan
   */
  void LateBindValues(Index *index_p,
                      const std::vector<type::Value> &value_list) {
    if (full_index_scan_ == true) {
      LOG_TRACE("Fast path: For full index scan do not bind");

      return;
    }

    // For every conjunction predicate, bind value on them one by one
    for (ConjunctionScanPredicate &conjunction_item : conjunction_list_) {
      // This should be true since this object's index scan flag is an "OR"
      // of all elements in the array
      PELOTON_ASSERT(conjunction_item.IsFullIndexScan() == false);

      conjunction_item.LateBindValues(index_p, value_list);
    }

    return;
  }

  /*
   * GetConjunctionList() - Returns the conjunction list to caller
   *
   * The returned value is a const reference which means it is read-only
   * and that all modifications should be done through member function call
   */
  inline const std::vector<ConjunctionScanPredicate> &GetConjunctionList()
      const {
    return conjunction_list_;
  }

  /*
   * GetConjunctionListToSetup() - Returns the conjunction list
   *
   * The returned value is *not* const, since it will be used to set new value
   */
  inline std::vector<ConjunctionScanPredicate> &GetConjunctionListToSetup() {
    return conjunction_list_;
  }

  /*
   * IsFullIndexScan() - Returns whether the entire predicate should be
   *                     done by one and only one full index scan
   */
  inline bool IsFullIndexScan() const { return full_index_scan_; }
};

}  // namespace index
}  // namespace peloton
