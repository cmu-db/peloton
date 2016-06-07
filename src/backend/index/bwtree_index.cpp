//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// bwtree_index.cpp
//
// Identification: src/backend/index/bwtree_index.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/common/logger.h"
#include "backend/index/bwtree_index.h"
#include "backend/index/index_key.h"
#include "backend/storage/tuple.h"

namespace peloton {
namespace index {

BWTREE_TEMPLATE_ARGUMENTS
BWTREE_INDEX_TYPE::BWTreeIndex(IndexMetadata *metadata) :
      // Base class
      Index{metadata},
      // Key "less than" relation comparator
      comparator{metadata},
      // Key equality checker
      equals{metadata},
      // NOTE: These two arguments need to be constructed in advance
      // and do not have trivial constructor
      container{comparator,
                equals} {
  return;
}

BWTREE_TEMPLATE_ARGUMENTS
BWTREE_INDEX_TYPE::~BWTreeIndex() {
  return;
}

/*
 * InsertEntry() - insert a key-value pair into the map
 *
 * If the key value pair already exists in the map, just return false
 */
BWTREE_TEMPLATE_ARGUMENTS
bool
BWTREE_INDEX_TYPE::InsertEntry(UNUSED_ATTRIBUTE const storage::Tuple *key,
                               UNUSED_ATTRIBUTE const ItemPointer &location) {
  KeyType index_key;
  index_key.SetFromKey(key);
  
  ItemPointer *item_p = new ItemPointer{location};
  
  bool ret = container.Insert(index_key, item_p);
  // If insertion fails we just delete the new value and return false
  // to notify the caller
  if(ret == false) {
    delete item_p;
  }

  return ret;
}

/*
 * DeleteEntry() - Removes a key-value pair
 *
 * If the key-value pair does not exists yet in the map return false
 */
BWTREE_TEMPLATE_ARGUMENTS
bool
BWTREE_INDEX_TYPE::DeleteEntry(UNUSED_ATTRIBUTE const storage::Tuple *key,
                               UNUSED_ATTRIBUTE const ItemPointer &location) {
  KeyType index_key;
  index_key.SetFromKey(key);
  
  // In Delete() since we just use the value for comparison (i.e. read-only)
  // it is unnecessary for us to allocate memory
  bool ret = container.DeleteItemPointer(index_key, location);

  return ret;
}

BWTREE_TEMPLATE_ARGUMENTS
bool
BWTREE_INDEX_TYPE::CondInsertEntry(const storage::Tuple *key,
                                   const ItemPointer &location,
                                   std::function<bool(const ItemPointer &)> predicate,
                                   ItemPointer **itemptr_ptr) {
  KeyType index_key;
  index_key.SetFromKey(key);
  
  ItemPointer *item_p = new ItemPointer{location};
  bool predicate_satisfied = false;

  // This function will complete them in one step
  // predicate will be set to nullptr if the predicate
  // returns true for some value
  bool ret = container.ConditionalInsert(index_key,
                                         item_p,
                                         predicate,
                                         &predicate_satisfied);

  // If predicate is not satisfied then we know insertion successes
  if(predicate_satisfied == false) {
    *itemptr_ptr = item_p;
  } else {
    // Otherwise insertion fails. and we need to delete memory
    *itemptr_ptr = nullptr;
    delete item_p;
  }

  return ret;
}

BWTREE_TEMPLATE_ARGUMENTS
void
BWTREE_INDEX_TYPE::Scan(const std::vector<Value> &values,
                        const std::vector<oid_t> &key_column_ids,
                        const std::vector<ExpressionType> &expr_types,
                        const ScanDirectionType &scan_direction,
                        std::vector<ItemPointer> &result) {
  KeyType index_key;

  // Checkif we have leading (leftmost) column equality
  // refer : http://www.postgresql.org/docs/8.2/static/indexes-multicolumn.html
  oid_t leading_column_id = 0;
  auto key_column_ids_itr = std::find(key_column_ids.begin(),
                                      key_column_ids.end(), leading_column_id);

  // SPECIAL CASE : leading column id is one of the key column ids
  // and is involved in a equality constraint
  bool special_case = false;
  if (key_column_ids_itr != key_column_ids.end()) {
    auto offset = std::distance(key_column_ids.begin(), key_column_ids_itr);
    if (expr_types[offset] == EXPRESSION_TYPE_COMPARE_EQUAL) {
      special_case = true;
    }
  }

  LOG_TRACE("Special case : %d ", special_case);

  auto scan_begin_itr = container.Begin();
  std::unique_ptr<storage::Tuple> start_key;
  bool all_constraints_are_equal = false;

  // If it is a special case, we can figure out the range to scan in the index
  if (special_case == true) {
    start_key.reset(new storage::Tuple(metadata->GetKeySchema(), true));

    // Construct the lower bound key tuple
    all_constraints_are_equal = ConstructLowerBoundTuple(
        start_key.get(), values, key_column_ids, expr_types);
    LOG_TRACE("All constraints are equal : %d ", all_constraints_are_equal);
    index_key.SetFromKey(start_key.get());

    // This returns an iterator pointing to index_key's values
    scan_begin_itr = container.Begin(index_key);
  }

  switch (scan_direction) {
    case SCAN_DIRECTION_TYPE_FORWARD:
    case SCAN_DIRECTION_TYPE_BACKWARD: {
      // Scan the index entries in forward direction
      for (auto scan_itr = scan_begin_itr;
           scan_itr.IsEnd() == false;
           scan_itr++) {
        auto &scan_current_key = *scan_itr.GetCurrentKey();
        auto tuple =
            scan_current_key.GetTupleForComparison(metadata->GetKeySchema());

        // Compare the current key in the scan with "values" based on
        // "expression types"
        // For instance, "5" EXPR_GREATER_THAN "2" is true
        if (Compare(tuple, key_column_ids, expr_types, values) == true) {
          result.push_back(**scan_itr);
        } else {
          // We can stop scanning if we know that all constraints are equal
          if (all_constraints_are_equal == true) {
            break;
          }
        }
      }

    } break;

    case SCAN_DIRECTION_TYPE_INVALID:
    default:
      throw Exception("Invalid scan direction \n");
      break;
  }

  return;
}

BWTREE_TEMPLATE_ARGUMENTS
void
BWTREE_INDEX_TYPE::ScanAllKeys(std::vector<ItemPointer> &result) {
  auto it = container.Begin();

  // scan all entries
  while (it.IsEnd() == false) {
    result.push_back(**it);
    it++;
  }

  return;
}

BWTREE_TEMPLATE_ARGUMENTS
void
BWTREE_INDEX_TYPE::ScanKey(const storage::Tuple *key,
                           std::vector<ItemPointer> &result) {
  KeyType index_key;
  index_key.SetFromKey(key);

  // This function in BwTree fills a given vector
  auto value_set = container.GetValue(index_key);
  for(auto item_pointer : value_set) {
    result.push_back(*item_pointer);
  }

  return;
}

BWTREE_TEMPLATE_ARGUMENTS
void
BWTREE_INDEX_TYPE::Scan(const std::vector<Value> &values,
                        const std::vector<oid_t> &key_column_ids,
                        const std::vector<ExpressionType> &expr_types,
                        const ScanDirectionType &scan_direction,
                        std::vector<ItemPointer *> &result) {
  KeyType index_key;

  // Checkif we have leading (leftmost) column equality
  // refer : http://www.postgresql.org/docs/8.2/static/indexes-multicolumn.html
  oid_t leading_column_id = 0;
  auto key_column_ids_itr = std::find(key_column_ids.begin(),
                                      key_column_ids.end(), leading_column_id);

  // SPECIAL CASE : leading column id is one of the key column ids
  // and is involved in a equality constraint
  bool special_case = false;
  if (key_column_ids_itr != key_column_ids.end()) {
    auto offset = std::distance(key_column_ids.begin(), key_column_ids_itr);
    if (expr_types[offset] == EXPRESSION_TYPE_COMPARE_EQUAL) {
      special_case = true;
    }
  }

  LOG_TRACE("Special case : %d ", special_case);

  auto scan_begin_itr = container.Begin();
  std::unique_ptr<storage::Tuple> start_key;
  bool all_constraints_are_equal = false;

  // If it is a special case, we can figure out the range to scan in the index
  if (special_case == true) {
    start_key.reset(new storage::Tuple(metadata->GetKeySchema(), true));

    // Construct the lower bound key tuple
    all_constraints_are_equal = ConstructLowerBoundTuple(
        start_key.get(), values, key_column_ids, expr_types);
    LOG_TRACE("All constraints are equal : %d ", all_constraints_are_equal);
    index_key.SetFromKey(start_key.get());

    // This returns an iterator pointing to index_key's values
    scan_begin_itr = container.Begin(index_key);
  }

  switch (scan_direction) {
    case SCAN_DIRECTION_TYPE_FORWARD:
    case SCAN_DIRECTION_TYPE_BACKWARD: {
      // Scan the index entries in forward direction
      for (auto scan_itr = scan_begin_itr;
           scan_itr.IsEnd() == false;
           scan_itr++) {
        auto &scan_current_key = *scan_itr.GetCurrentKey();
        auto tuple =
            scan_current_key.GetTupleForComparison(metadata->GetKeySchema());

        // Compare the current key in the scan with "values" based on
        // "expression types"
        // For instance, "5" EXPR_GREATER_THAN "2" is true
        if (Compare(tuple, key_column_ids, expr_types, values) == true) {
          result.push_back(*scan_itr);
        } else {
          // We can stop scanning if we know that all constraints are equal
          if (all_constraints_are_equal == true) {
            break;
          }
        }
      }

    } break;

    case SCAN_DIRECTION_TYPE_INVALID:
    default:
      throw Exception("Invalid scan direction \n");
      break;
  }

  return;
}

BWTREE_TEMPLATE_ARGUMENTS
void
BWTREE_INDEX_TYPE::ScanAllKeys(std::vector<ItemPointer *> &result) {
  auto it = container.Begin();

    // scan all entries
    while (it.IsEnd() == false) {
      result.push_back(*it);
      it++;
    }

    return;
}

BWTREE_TEMPLATE_ARGUMENTS
void
BWTREE_INDEX_TYPE::ScanKey(const storage::Tuple *key,
                           std::vector<ItemPointer *> &result) {
  KeyType index_key;
  index_key.SetFromKey(key);

  // This function in BwTree fills a given vector
  container.GetValue(index_key, result);

  return;
}

BWTREE_TEMPLATE_ARGUMENTS
std::string
BWTREE_INDEX_TYPE::GetTypeName() const {
  return "BWTree";
}

// Explicit template instantiation
// Integer key
template class BWTreeIndex<IntsKey<1>,
                           ItemPointer *,
                           IntsComparator<1>,
                           IntsEqualityChecker<1>>;
template class BWTreeIndex<IntsKey<2>,
                           ItemPointer *,
                           IntsComparator<2>,
                           IntsEqualityChecker<2>>;
template class BWTreeIndex<IntsKey<3>,
                           ItemPointer *,
                           IntsComparator<3>,
                           IntsEqualityChecker<3>>;
template class BWTreeIndex<IntsKey<4>,
                           ItemPointer *,
                           IntsComparator<4>,
                           IntsEqualityChecker<4>>;

// Generic key
template class BWTreeIndex<GenericKey<4>,
                           ItemPointer *,
                           GenericComparator<4>,
                           GenericEqualityChecker<4>>;
template class BWTreeIndex<GenericKey<8>,
                           ItemPointer *,
                           GenericComparator<8>,
                           GenericEqualityChecker<8>>;
template class BWTreeIndex<GenericKey<12>,
                           ItemPointer *,
                           GenericComparator<12>,
                           GenericEqualityChecker<12>>;
template class BWTreeIndex<GenericKey<16>,
                           ItemPointer *,
                           GenericComparator<16>,
                           GenericEqualityChecker<16>>;
template class BWTreeIndex<GenericKey<24>,
                           ItemPointer *,
                           GenericComparator<24>,
                           GenericEqualityChecker<24>>;
template class BWTreeIndex<GenericKey<32>,
                           ItemPointer *,
                           GenericComparator<32>,
                           GenericEqualityChecker<32>>;
template class BWTreeIndex<GenericKey<48>,
                           ItemPointer *,
                           GenericComparator<48>,
                           GenericEqualityChecker<48>>;
template class BWTreeIndex<GenericKey<64>,
                           ItemPointer *,
                           GenericComparator<64>,
                           GenericEqualityChecker<64>>;
template class BWTreeIndex<GenericKey<96>,
                           ItemPointer *,
                           GenericComparator<96>,
                           GenericEqualityChecker<96>>;
template class BWTreeIndex<GenericKey<128>,
                           ItemPointer *,
                           GenericComparator<128>,
                           GenericEqualityChecker<128>>;
template class BWTreeIndex<GenericKey<256>,
                           ItemPointer *,
                           GenericComparator<256>,
                           GenericEqualityChecker<256>>;
template class BWTreeIndex<GenericKey<512>,
                           ItemPointer *,
                           GenericComparator<512>,
                           GenericEqualityChecker<512>>;

// Tuple key
template class BWTreeIndex<TupleKey,
                           ItemPointer *,
                           TupleKeyComparator,
                           TupleKeyEqualityChecker>;

}  // End index namespace
}  // End peloton namespace
