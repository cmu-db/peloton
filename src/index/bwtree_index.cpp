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

#include "common/logger.h"
#include "index/bwtree_index.h"
#include "index/index_key.h"
#include "storage/tuple.h"

namespace peloton {
namespace index {

BWTREE_TEMPLATE_ARGUMENTS
BWTREE_INDEX_TYPE::BWTreeIndex(IndexMetadata *metadata) :
      // Base class
      Index{metadata},
      // Key "less than" relation comparator
      comparator{},
      // Key equality checker
      equals{},
      // Key hash function
      hash_func{},
      // NOTE: These two arguments need to be constructed in advance
      // and do not have trivial constructor
      container{comparator,
                equals,
                hash_func} {
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
BWTREE_INDEX_TYPE::InsertEntry(const storage::Tuple *key,
                               const ItemPointer &location) {
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
BWTREE_INDEX_TYPE::DeleteEntry(const storage::Tuple *key,
                               const ItemPointer &location) {
  KeyType index_key;
  index_key.SetFromKey(key);
  
  // Must allocate new memory here
  ItemPointer *ip_p = new ItemPointer{location};
  
  // In Delete() since we just use the value for comparison (i.e. read-only)
  // it is unnecessary for us to allocate memory
  bool ret = container.DeleteExchange(index_key, &ip_p);
  
  // IF delete succeeds then DeleteExchange() will exchange the deleted
  // value into this variable
  if(ret == true) {
    //delete ip_p;
  } else {
    // This will delete the unused memory
    delete ip_p;
  }

  return ret;
}

BWTREE_TEMPLATE_ARGUMENTS
bool
BWTREE_INDEX_TYPE::CondInsertEntry(const storage::Tuple *key,
                                   const ItemPointer &location,
                                   std::function<bool(const ItemPointer &)> predicate) {
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
    // So it should always succeed?
    assert(ret == true);
  } else {
    assert(ret == false);
    
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

  // This is only a placeholder that cannot be moved but can be assigned to
  auto scan_begin_itr = container.NullIterator();
  
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
    
    // If it is point query then we just do a GetValue() since GetValue()
    // is way more faster than scanning using iterator
    if(all_constraints_are_equal == true) {
      std::vector<ItemPointer *> item_p_list{};
      
      // This retrieves a list of ItemPointer *
      container.GetValue(index_key, item_p_list);

      // To reduce allocation
      result.reserve(item_p_list.size());
      
      // Dereference pointers one by one
      for(auto p : item_p_list) {
        result.push_back(*p);
      }
      
      return;
    }

    // This returns an iterator pointing to index_key's values
    scan_begin_itr = container.Begin(index_key);
  } else {
    scan_begin_itr = container.Begin();
  }
  
  //printf("Start scanning\n");

  switch (scan_direction) {
    case SCAN_DIRECTION_TYPE_FORWARD:
    case SCAN_DIRECTION_TYPE_BACKWARD: {
      // Scan the index entries in forward direction
      for (auto scan_itr = scan_begin_itr;
           scan_itr.IsEnd() == false;
           scan_itr++) {
        KeyType &scan_current_key = const_cast<KeyType &>(scan_itr->first);
        
        auto tuple =
            scan_current_key.GetTupleForComparison(metadata->GetKeySchema());

        // Compare the current key in the scan with "values" based on
        // "expression types"
        // For instance, "5" EXPR_GREATER_THAN "2" is true
        if (Compare(tuple, key_column_ids, expr_types, values) == true) {
          result.push_back(*(scan_itr->second));
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
    result.push_back(*(it->second));
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
  
  std::vector<ItemPointer *> temp_list{};

  // This function in BwTree fills a given vector
  container.GetValue(index_key, temp_list);
  for(auto item_pointer_p : temp_list) {
    result.push_back(*item_pointer_p);
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

  // This is only a placeholder that cannot be moved but can be assigned to
  auto scan_begin_itr = container.NullIterator();

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
/*
    // If it is point query then we just do a GetValue() since GetValue()
    // is way more faster than scanning using iterator
    if(all_constraints_are_equal == true) {
      //printf("All constraints are equal\n");
      
      // This retrieves a list of ItemPointer *
      container.GetValue(index_key, result);

      return;
    }
*/
    //printf("Non special case\n");

    // This returns an iterator pointing to index_key's values
    scan_begin_itr = container.Begin(index_key);
  } else {
    scan_begin_itr = container.Begin();
  }

  switch (scan_direction) {
    case SCAN_DIRECTION_TYPE_FORWARD:
    case SCAN_DIRECTION_TYPE_BACKWARD: {
      // Scan the index entries in forward direction
      for (auto scan_itr = scan_begin_itr;
           scan_itr.IsEnd() == false;
           scan_itr++) {
        KeyType &scan_current_key = const_cast<KeyType &>(scan_itr->first);
          
        auto tuple =
            scan_current_key.GetTupleForComparison(metadata->GetKeySchema());

        // Compare the current key in the scan with "values" based on
        // "expression types"
        // For instance, "5" EXPR_GREATER_THAN "2" is true
        if (Compare(tuple, key_column_ids, expr_types, values) == true) {
          result.push_back(scan_itr->second);
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
    result.push_back(it->second);
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

// NOTE: If ints key is used as an optimization just uncommend
// the following in order to instanciation the template before it is
// linked in linking stage

/*
template class BWTreeIndex<IntsKey<1>,
                           ItemPointer *,
                           IntsComparator<1>,
                           IntsEqualityChecker<1>,
                           IntsHasher<1>,
                           ItemPointerComparator,
                           ItemPointerHashFunc>;
template class BWTreeIndex<IntsKey<2>,
                           ItemPointer *,
                           IntsComparator<2>,
                           IntsEqualityChecker<2>,
                           IntsHasher<2>,
                           ItemPointerComparator,
                           ItemPointerHashFunc>;
template class BWTreeIndex<IntsKey<3>,
                           ItemPointer *,
                           IntsComparator<3>,
                           IntsEqualityChecker<3>,
                           IntsHasher<3>,
                           ItemPointerComparator,
                           ItemPointerHashFunc>;
template class BWTreeIndex<IntsKey<4>,
                           ItemPointer *,
                           IntsComparator<4>,
                           IntsEqualityChecker<4>,
                           IntsHasher<4>,
                           ItemPointerComparator,
                           ItemPointerHashFunc>;
*/

// Generic key
template class BWTreeIndex<GenericKey<4>,
                           ItemPointer *,
                           GenericComparator<4>,
                           GenericEqualityChecker<4>,
                           GenericHasher<4>,
                           ItemPointerComparator,
                           ItemPointerHashFunc>;
template class BWTreeIndex<GenericKey<8>,
                           ItemPointer *,
                           GenericComparator<8>,
                           GenericEqualityChecker<8>,
                           GenericHasher<8>,
                           ItemPointerComparator,
                           ItemPointerHashFunc>;
template class BWTreeIndex<GenericKey<16>,
                           ItemPointer *,
                           GenericComparator<16>,
                           GenericEqualityChecker<16>,
                           GenericHasher<16>,
                           ItemPointerComparator,
                           ItemPointerHashFunc>;
template class BWTreeIndex<GenericKey<64>,
                           ItemPointer *,
                           GenericComparator<64>,
                           GenericEqualityChecker<64>,
                           GenericHasher<64>,
                           ItemPointerComparator,
                           ItemPointerHashFunc>;
template class BWTreeIndex<GenericKey<256>,
                           ItemPointer *,
                           GenericComparator<256>,
                           GenericEqualityChecker<256>,
                           GenericHasher<256>,
                           ItemPointerComparator,
                           ItemPointerHashFunc>;

// Tuple key
template class BWTreeIndex<TupleKey,
                           ItemPointer *,
                           TupleKeyComparator,
                           TupleKeyEqualityChecker,
                           TupleKeyHasher,
                           ItemPointerComparator,
                           ItemPointerHashFunc>;

}  // End index namespace
}  // End peloton namespace
