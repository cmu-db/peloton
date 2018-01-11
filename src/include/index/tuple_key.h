
//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tuple_key.h
//
// Identification: src/include/index/tuple_key.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

namespace peloton {
namespace index {

/*
 * class TupleKey - General purpose key that represents a combination of columns
 *                  inside a table
 *
 * This class is used to represent index keys when the key could not be 
 * coded as either IntsKey or GenricKey, and thus has the most loose restriction
 * on the composition of the key as well as operations allowed. 
 *
 * TupleKey consists of three pointer, one pointing to the underlying tuple 
 * in a persistent table (i.e. the lifetime of the index should at least be
 * within the lifetime of the table to ensure safe memory access); another
 * pointing to the schema of the table that defines methods on the key including
 * comparison and equality check. The last pointer points to a mapping relation
 * (an array of ints) that maps index column to table columns to assist 
 * extrating necessary columns from a table tuple. 
 *
 * Among the three data members, tuple pointer and schema pointer is 
 * indispensable for a functioning TupleKey instance, while mapping relation
 * may be nullptr for an emphermal key.
 *
 * The usage of TupleKey requires another level of indirection and thus is less
 * efficient than the more specialized counterparts. Also, key comparison and
 * equality checking requires an interpreted comparison of all its columns
 * which further lowers the performance of TupleKey. Unless necessary users
 * should always consider IntsKey or GenericKey whenever possible
 */
class TupleKey {
 public:
  // TableIndex owns this array - NULL if an ephemeral key
  const int *column_indices;

  // Pointer a persistent tuple in non-ephemeral case.
  char *key_tuple;
  const catalog::Schema *key_tuple_schema;
  
 public:
  
  /*
   * Default Constructor
   *
   * Normally this constructor should not be used since it constructs a key
   * that could not be directly used. However, it is truly useful if we just
   * need a placeholder that could be assigned values later
   */
  TupleKey() :
    column_indices{nullptr},
    key_tuple{nullptr},
    key_tuple_schema{nullptr} 
  {}

  /*
   * SetFromKey() - Moves a tuple's data and schema into this key
   *
   * This function is called before index operation in order to derive an
   * TupleKey instance that has the data of a given tuple and also its schema
   * 
   * Note that this function does not involve any column mapping since the 
   * tuple given here is the key without any unused column
   */
  inline void SetFromKey(const storage::Tuple *tuple) {
    PL_ASSERT(tuple != nullptr);

    // Do not use the column mapping - we use all columns in the tuple
    // as index key
    column_indices = nullptr;

    // The index key shares the same data and schema with the tuple
    key_tuple = tuple->GetData();
    key_tuple_schema = tuple->GetSchema();
    
    return;
  }

  // Set a key from a table-schema tuple.
  inline void SetFromTuple(const storage::Tuple *tuple, const int *indices,
                           UNUSED_ATTRIBUTE const catalog::Schema *key_schema) {
    PL_ASSERT(tuple);
    PL_ASSERT(indices);
    column_indices = indices;
    key_tuple = tuple->GetData();
    key_tuple_schema = tuple->GetSchema();
  }

  // Return true if the TupleKey references an ephemeral index key.
  bool IsKeySchema() const { return column_indices == nullptr; }

  // Return a table tuple that is valid for comparison
  const storage::Tuple GetTupleForComparison(
      const catalog::Schema *key_tuple_schema) const {
    return storage::Tuple(key_tuple_schema, key_tuple);
  }

  // Return the indexColumn'th key-schema column.
  int ColumnForIndexColumn(int indexColumn) const {
    if (IsKeySchema())
      return indexColumn;
    else
      return column_indices[indexColumn];
  }
};

/*
 * class TupleKeyHasher - Hash function for tuple keys
 *
 * This function is defined to fulfill requirements from the BwTree index
 * because it uses bloom filter inside and needs hash value 
 */
struct TupleKeyHasher {

  /** Generate a 64-bit number for the key value */
  inline size_t operator()(const TupleKey &p) const {
    storage::Tuple pTuple = p.GetTupleForComparison(p.key_tuple_schema);
    return pTuple.HashCode();
  }

  /*
   * Copy Constructor - To make compiler happy
   */
  TupleKeyHasher(const TupleKeyHasher &) {}
  TupleKeyHasher() {};
};

/*
 * class TupleKeyComparator - Compares tuple keys for less than relation
 *
 * This function is needed in all kinds of indices based on partial ordering
 * of keys. This invokation of the class instance returns true if one key
 * is less than another 
 */
class TupleKeyComparator {
 public:
  
  /*
   * operator()() - Function invocation
   *
   * This function compares two keys
   */
  inline bool operator()(const TupleKey &lhs, const TupleKey &rhs) const {
    // We assume two keys have the same schema (executor should guarantee this)
    storage::Tuple lhTuple = lhs.GetTupleForComparison(lhs.key_tuple_schema);
    storage::Tuple rhTuple = rhs.GetTupleForComparison(rhs.key_tuple_schema);
    
    // The length of two schemas must be different from each other
    auto lhs_schema = lhs.key_tuple_schema;
    auto rhs_schema = rhs.key_tuple_schema;
    assert(lhs_schema->GetColumnCount() == rhs_schema->GetColumnCount());
    (void)rhs_schema;

    unsigned int columt_count = lhs_schema->GetColumnCount();

    // Do a filed by field comparison
    // This will return true for the first column in LHS that is smaller
    // than the same column in RHS
    for (unsigned int col_itr = 0; 
         col_itr < columt_count;
         ++col_itr) {
      type::Value lhValue = \
        lhTuple.GetValue(lhs.ColumnForIndexColumn(col_itr));
      type::Value rhValue = \
        rhTuple.GetValue(rhs.ColumnForIndexColumn(col_itr));

      // If there is a field in LHS < RHS then return true;
      if (lhValue.CompareLessThan(rhValue) == CmpBool::TRUE) {
        return true;
      }
      
      // If there is a field in LHS > RHS then return false
      if (lhValue.CompareGreaterThan(rhValue) == CmpBool::TRUE) {
        return false;
      }
    }
    
    // If we get here then two keys are equal. Still return false
    return false;
  }

  /*
   * Copy constructor
   */
  TupleKeyComparator(const TupleKeyComparator &) {}
  TupleKeyComparator() {};
};

/*
 * class TupleComparatorRaw - Different kind of comparator that not only 
 *                            determines partial ordering but also equality
 *
 * This class returns -1 (VALUE_COMPARE_LESSTHAN) for less than relation
 *                     0 (VALUE_COMPARE_EQUAL) for equality relation
 *                     1 (VALUE_COMPARE_GREATERTHAN) for greater than relation
 */
class TupleKeyComparatorRaw {
 public:
  
  /*
   * operator()() - Returns partial ordering as well as equality relation
   *
   * Please refer to the class comment for more information about return value
   */
  inline int operator()(const TupleKey &lhs, const TupleKey &rhs) const {
    storage::Tuple lhTuple = lhs.GetTupleForComparison(lhs.key_tuple_schema);
    storage::Tuple rhTuple = rhs.GetTupleForComparison(rhs.key_tuple_schema);
    
    // The length of two schemas must be different from each other
    auto lhs_schema = lhs.key_tuple_schema;
    auto rhs_schema = rhs.key_tuple_schema;
    assert(lhs_schema->GetColumnCount() == rhs_schema->GetColumnCount());
    (void)rhs_schema;
    
    unsigned int columt_count = lhs_schema->GetColumnCount();

    for (unsigned int col_itr = 0; 
         col_itr < columt_count;
         ++col_itr) {
      type::Value lhValue = \
          lhTuple.GetValue(lhs.ColumnForIndexColumn(col_itr));
      type::Value rhValue = \
          rhTuple.GetValue(rhs.ColumnForIndexColumn(col_itr));

      if (lhValue.CompareLessThan(rhValue) == CmpBool::TRUE) {
        return VALUE_COMPARE_LESSTHAN;
      }

      if (lhValue.CompareGreaterThan(rhValue) == CmpBool::TRUE) {
        return VALUE_COMPARE_GREATERTHAN;
      }
    }

    // If all columns are equal then two keys are equal
    return VALUE_COMPARE_EQUAL;
  }

  /*
   * Copy constructor
   */
  TupleKeyComparatorRaw(const TupleKeyComparatorRaw &) {}
  TupleKeyComparatorRaw() {};
};

/*
 * class TupleKeyEqualityChecker - Checks equality relation of tuple key
 *
 * This class has almost the same logic with class TupleKeyComparatorRaw
 * however we could not directly use TupleKeyComparatorRaw because otherwise
 * we will do two comparison instead of one to determine equality
 */
class TupleKeyEqualityChecker {
 public:
  
  /*
   * operator()() - Determines whether two keys are equal
   */
  inline bool operator()(const TupleKey &lhs, const TupleKey &rhs) const {
    storage::Tuple lhTuple = lhs.GetTupleForComparison(lhs.key_tuple_schema);
    storage::Tuple rhTuple = rhs.GetTupleForComparison(rhs.key_tuple_schema);
    
    // The length of two schemas must be different from each other
    auto lhs_schema = lhs.key_tuple_schema;
    auto rhs_schema = rhs.key_tuple_schema;
    assert(lhs_schema->GetColumnCount() == rhs_schema->GetColumnCount());
    (void)rhs_schema;

    unsigned int columt_count = lhs_schema->GetColumnCount();

    // Do a filed by field comparison
    // This will return true for the first column in LHS that is smaller
    // than the same column in RHS
    for (unsigned int col_itr = 0; 
         col_itr < columt_count;
         ++col_itr) {
      type::Value lhValue = (
          lhTuple.GetValue(lhs.ColumnForIndexColumn(col_itr)));
      type::Value rhValue = (
          rhTuple.GetValue(rhs.ColumnForIndexColumn(col_itr)));

      // If any of these two columns differ then just return false
      // because we know they could not be equal 
      if (lhValue.CompareNotEquals(rhValue) == CmpBool::TRUE) {
        return false;
      }
    }
    
    return true; 
  }

  /*
   * Copy Constructor
   */
  TupleKeyEqualityChecker(const TupleKeyEqualityChecker &) {}
  TupleKeyEqualityChecker() {}
};

}  // namespace index
}  // namespace peloton
