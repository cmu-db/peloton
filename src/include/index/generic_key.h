//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// generic_key.h
//
// Identification: src/include/index/generic_key.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "type/type_util.h"

namespace peloton {
namespace index {

/*
 * class GenericKey - Key used for indexing with opaque data
 *
 * This key type uses an fixed length array to hold data for indexing
 * purposes, the actual size of which is specified and instanciated
 * with a template argument.
 */
template <std::size_t KeySize>
class GenericKey {
 public:
  inline void SetFromKey(const storage::Tuple *tuple) {
    PL_ASSERT(tuple);
    PL_MEMCPY(data, tuple->GetData(), tuple->GetLength());
    schema = tuple->GetSchema();
  }

  const storage::Tuple GetTupleForComparison(
      const catalog::Schema *key_schema) {
    return storage::Tuple(key_schema, data);
  }

  inline type::Value ToValue(const catalog::Schema *schema,
                             int column_id) const {
    const type::TypeId column_type = schema->GetType(column_id);
    const char *data_ptr = &data[schema->GetOffset(column_id)];
    const bool is_inlined = schema->IsInlined(column_id);
    return type::Value::DeserializeFrom(data_ptr, column_type, is_inlined);
  }

  inline const char *GetRawData(const catalog::Schema *schema,
                                int column_id) const {
    const char *data_ptr = &data[schema->GetOffset(column_id)];
    return (data_ptr);
  }

  // actual location of data, extends past the end.
  char data[KeySize];

  const catalog::Schema *schema;
};

/**
 * Function object returns true if lhs < rhs, used for trees
 */
template <std::size_t KeySize>
class GenericComparator {
 public:
  inline bool operator()(const GenericKey<KeySize> &lhs,
                         const GenericKey<KeySize> &rhs) const {
    auto schema = lhs.schema;

    type::Value lhs_value;
    type::Value rhs_value;

    for (oid_t col_itr = 0; col_itr < schema->GetColumnCount(); col_itr++) {
      const type::Value lhs_value = (lhs.ToValue(schema, col_itr));
      const type::Value rhs_value = (rhs.ToValue(schema, col_itr));

      if (lhs_value.CompareLessThan(rhs_value) == CmpBool::TRUE) return true;

      if (lhs_value.CompareGreaterThan(rhs_value) == CmpBool::TRUE)
        return false;
    }

    return false;
  }

  GenericComparator(const GenericComparator &) {}
  GenericComparator() {}
};

/**
 * Function object returns true if lhs < rhs, used for trees
 */
template <std::size_t KeySize>
class FastGenericComparator {
 public:
  inline bool operator()(const GenericKey<KeySize> &lhs,
                         const GenericKey<KeySize> &rhs) const {
    auto schema = lhs.schema;

    type::Value lhs_value;
    type::Value rhs_value;

    for (oid_t col_itr = 0; col_itr < schema->GetColumnCount(); col_itr++) {
      const char *lhs_data = lhs.GetRawData(schema, col_itr);
      const char *rhs_data = rhs.GetRawData(schema, col_itr);
      type::Type type = schema->GetType(col_itr);
      bool inlined = schema->IsInlined(col_itr);

      if (type::TypeUtil::CompareLessThanRaw(type, lhs_data, rhs_data,
                                             inlined) == CmpBool::TRUE)
        return true;
      else if (type::TypeUtil::CompareGreaterThanRaw(type, lhs_data, rhs_data,
                                                     inlined) == CmpBool::TRUE)
        return false;
    }

    return false;
  }

  FastGenericComparator(const FastGenericComparator &) {}
  FastGenericComparator() {}
};

/**
 * Function object returns true if lhs < rhs, used for trees
 */
template <std::size_t KeySize>
class GenericComparatorRaw {
 public:
  inline int operator()(const GenericKey<KeySize> &lhs,
                        const GenericKey<KeySize> &rhs) const {
    auto schema = lhs.schema;

    for (oid_t column_itr = 0; column_itr < schema->GetColumnCount();
         column_itr++) {
      const type::Value lhs_value = (lhs.ToValue(schema, column_itr));
      const type::Value rhs_value = (rhs.ToValue(schema, column_itr));

      if (lhs_value.CompareLessThan(rhs_value) == CmpBool::TRUE)
        return VALUE_COMPARE_LESSTHAN;

      if (lhs_value.CompareGreaterThan(rhs_value) == CmpBool::TRUE)
        return VALUE_COMPARE_GREATERTHAN;
    }

    /* equal */
    return VALUE_COMPARE_EQUAL;
  }

  GenericComparatorRaw(const GenericComparatorRaw &) {}
  GenericComparatorRaw() {}
};

/**
 * Equality-checking function object
 */
template <std::size_t KeySize>
class GenericEqualityChecker {
 public:
  inline bool operator()(const GenericKey<KeySize> &lhs,
                         const GenericKey<KeySize> &rhs) const {
    auto schema = lhs.schema;

    storage::Tuple lhTuple(schema);
    lhTuple.MoveToTuple(reinterpret_cast<const void *>(&lhs));
    storage::Tuple rhTuple(schema);
    rhTuple.MoveToTuple(reinterpret_cast<const void *>(&rhs));
    return lhTuple.EqualsNoSchemaCheck(rhTuple);
  }

  GenericEqualityChecker(const GenericEqualityChecker &) {}
  GenericEqualityChecker() {}
};

/**
 * Hash function object for an array of SlimValues
 */
template <std::size_t KeySize>
struct GenericHasher : std::unary_function<GenericKey<KeySize>, std::size_t> {
  /** Generate a 64-bit number for the key value */
  inline size_t operator()(GenericKey<KeySize> const &p) const {
    auto schema = p.schema;

    storage::Tuple pTuple(schema);
    pTuple.MoveToTuple(reinterpret_cast<const void *>(&p));
    return pTuple.HashCode();
  }

  GenericHasher(const GenericHasher &) {}
  GenericHasher(){};
};

}  // namespace index
}  // namespace peloton
