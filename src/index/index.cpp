//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// index.cpp
//
// Identification: src/index/index.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "index/index.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/varlen_pool.h"
#include "catalog/schema.h"
#include "catalog/manager.h"
#include "storage/tuple.h"

#include "index/scan_optimizer.h"

#include <iostream>
#include <algorithm>

namespace peloton {
namespace index {

/*
 * GetColumnCount() - Returns the number of indexed columns
 *
 * Please note that this returns the column count of columns in the base
 * table that are indexed, i.e. not the column count of the base table
 */
oid_t IndexMetadata::GetColumnCount() const {
  return GetKeySchema()->GetColumnCount();
}

/*
 * Constructor - Initializes tuple key to index mapping
 *
 * NOTE: This metadata object owns key_schema since it is specially
 * constructed for the index
 *
 * However, tuple schema belongs to the table, such that this metadata should
 * not destroy the tuple schema object on destruction
 */
IndexMetadata::IndexMetadata(
    std::string index_name, oid_t index_oid, oid_t table_oid,
    oid_t database_oid, IndexType method_type, IndexConstraintType index_type,
    const catalog::Schema *tuple_schema, const catalog::Schema *key_schema,
    const std::vector<oid_t> &key_attrs, bool unique_keys)
    : index_name(index_name),
      index_oid(index_oid),
      table_oid(table_oid),
      database_oid(database_oid),
      method_type(method_type),
      index_type(index_type),
      tuple_schema(tuple_schema),
      key_schema(key_schema),
      key_attrs(key_attrs),
      tuple_attrs(),
      unique_keys(unique_keys) {

  // Push the reverse mapping relation into tuple_attrs which maps
  // tuple key's column into index key's column
  // resize() automatially does allocation, extending and insertion
  tuple_attrs.resize(tuple_schema->GetColumnCount(), INVALID_OID);

  // For those column IDs not mapped, they are set to INVALID_OID
  for (oid_t i = 0; i < key_attrs.size(); i++) {
    // That is the tuple column ID that index key column i is mapped to
    oid_t tuple_column_id = key_attrs[i];

    // The tuple column must be included into key_attrs, otherwise
    // the construction argument is malformed
    PL_ASSERT(tuple_column_id < tuple_attrs.size());

    tuple_attrs[tuple_column_id] = i;
  }

  return;
}

IndexMetadata::~IndexMetadata() {
  // clean up key schema
  delete key_schema;

  // no need to clean the tuple schema
  return;
}

const std::string IndexMetadata::GetInfo() const {
  std::stringstream os;

  os << "\tINDEX METADATA: [";

  for (auto key_attr : key_attrs) {
    os << key_attr << " ";
  }

  os << " ] :: ";

  os << utility_ratio;

  return os.str();
}

/////////////////////////////////////////////////////////////////////
// Member function definition for class Index
/////////////////////////////////////////////////////////////////////

/*
 * Constructor
 *
 * NOTE: Though Index object receives the index metadata pointer
 * from the caller, the Index object owns that metadata and is responsible
 * for destructing the metadata object on its own destruction
 */
Index::Index(IndexMetadata *metadata)
    : metadata(metadata), indexed_tile_group_offset(0) {

  // This is redundant
  index_oid = metadata->GetOid();

  // initialize counters
  lookup_counter = insert_counter = delete_counter = update_counter = 0;

  // initialize pool
  pool = new common::VarlenPool(BACKEND_TYPE_MM);
  
  return;
}

/*
 * Destructor
 */
Index::~Index() {

  // Free metadata which frees the key schema but not tuple schema
  // This is passed in as construction argument but Index object is
  // responsible for its destruction
  delete metadata;

  // Free the varlen pool - it is allocted during construction
  delete pool;

  return;
}

void Index::ScanTest(const std::vector<common::Value> &value_list,
                     const std::vector<oid_t> &tuple_column_id_list,
                     const std::vector<ExpressionType> &expr_list,
                     const ScanDirectionType &scan_direction,
                     std::vector<ItemPointer *> &result) {
  IndexScanPredicate isp{};

  isp.AddConjunctionScanPredicate(this, value_list, tuple_column_id_list,
                                  expr_list);

  Scan(value_list, tuple_column_id_list, expr_list, scan_direction, result,
       &isp.GetConjunctionList()[0]);

  return;
}

/*
 * Compare() - Check whether a given index key satisfies a predicate
 *
 * The predicate has the same specification as those in Scan()
 */
bool Index::Compare(const AbstractTuple &index_key,
                    const std::vector<oid_t> &tuple_column_id_list,
                    const std::vector<ExpressionType> &expr_list,
                    const std::vector<common::Value > &value_list) {
  //int diff;
  
  // The size of these three arrays must be the same
  PL_ASSERT(tuple_column_id_list.size() == expr_list.size());
  PL_ASSERT(expr_list.size() == value_list.size());

  // Need the mapping
  const IndexMetadata *metadata_p = GetMetadata();

  // This is the end of loop
  oid_t cond_num = tuple_column_id_list.size();

  // This maps the tuple column ID to index key column ID
  // Since tuple_column_id_list is a list returned from the optimizer,
  // we should first map them to columns on index and then retrieve
  // comparion operand
  const std::vector<oid_t> &tuple_to_index_map =
      metadata_p->GetTupleToIndexMapping();

  // Go over each attribute in the list of comparison columns
  // The key_columns_ids, as the name shows, saves the key column ids that
  // have values and expression needs to be compared.

  for (oid_t i = 0; i < cond_num; i++) {
    // First retrieve the tuple column ID from the map, and then map
    // it to the column ID of index key
    oid_t tuple_key_column_id = tuple_column_id_list[i];
    oid_t index_key_column_id = tuple_to_index_map[tuple_key_column_id];

    // This the comparison right hand side operand
    const common::Value &rhs = value_list[i];
    
    // Also retrieve left hand side operand using index key column ID
    common::Value val = (index_key.GetValue(index_key_column_id));
    const common::Value &lhs = val;
    
    // Expression type. We use this to interpret comparison result
    //
    // Possible results of comparison are: EQ, >, <
    const ExpressionType expr_type = expr_list[i];

    // If the operation is IN, then use the boolean values comparator
    // that determines whether a value is in a list
    //
    // To make the procedure more uniform, we interpret IN as EQUAL
    // and NOT IN as NOT EQUAL, and react based on expression type below
    // accordingly
    /*if (expr_type == EXPRESSION_TYPE_COMPARE_IN) {
      bool bret = lhs.InList(rhs);

      if (bret == true) {
        diff = VALUE_COMPARE_EQUAL;
      } else {
        diff = VALUE_COMPARE_NO_EQUAL;
      }
    } else {
      diff = lhs.Compare(rhs);
    }

    LOG_TRACE("Difference : %d ", diff);*/
    common::Value cmp_eq = (lhs.CompareEquals(rhs));
    if (cmp_eq.IsTrue()) {
      switch (expr_type) {
        case EXPRESSION_TYPE_COMPARE_EQUAL:
        case EXPRESSION_TYPE_COMPARE_LESSTHANOREQUALTO:
        case EXPRESSION_TYPE_COMPARE_GREATERTHANOREQUALTO:
        case EXPRESSION_TYPE_COMPARE_IN:
          continue;

        case EXPRESSION_TYPE_COMPARE_NOTEQUAL:
        case EXPRESSION_TYPE_COMPARE_LESSTHAN:
        case EXPRESSION_TYPE_COMPARE_GREATERTHAN:
          return false;

        default:
          throw IndexException("Unsupported expression type : " +
                               std::to_string(expr_type));
      }
    }
    else {
      common::Value cmp_lt = (lhs.CompareLessThan(rhs));
      if (cmp_lt.IsTrue()) {
        switch (expr_type) {
          case EXPRESSION_TYPE_COMPARE_NOTEQUAL:
          case EXPRESSION_TYPE_COMPARE_LESSTHAN:
          case EXPRESSION_TYPE_COMPARE_LESSTHANOREQUALTO:
            continue;

          case EXPRESSION_TYPE_COMPARE_EQUAL:
          case EXPRESSION_TYPE_COMPARE_GREATERTHAN:
          case EXPRESSION_TYPE_COMPARE_GREATERTHANOREQUALTO:
          case EXPRESSION_TYPE_COMPARE_IN:
            return false;

          default:
            throw IndexException("Unsupported expression type : " +
                                 std::to_string(expr_type));
        }
      }
      else {
        common::Value cmp_gt(lhs.CompareGreaterThan(rhs));
        if (cmp_gt.IsTrue()) {
          switch (expr_type) {
            case EXPRESSION_TYPE_COMPARE_NOTEQUAL:
            case EXPRESSION_TYPE_COMPARE_GREATERTHAN:
            case EXPRESSION_TYPE_COMPARE_GREATERTHANOREQUALTO:
              continue;

            case EXPRESSION_TYPE_COMPARE_EQUAL:
            case EXPRESSION_TYPE_COMPARE_LESSTHAN:
            case EXPRESSION_TYPE_COMPARE_LESSTHANOREQUALTO:
            case EXPRESSION_TYPE_COMPARE_IN:
              return false;

            default:
              throw IndexException("Unsupported expression type : " +
                                   std::to_string(expr_type));
          }
        }else {
          // Since it is an AND predicate, we could directly return false
          return false;
        }
      }
    }
  }

  return true;
}

/*
 * ConstructLowerBoundTuple() - Constructs a lower bound of index key that
 *                              satisfies a given tuple
 *
 * The predicate has the same specification as those in Scan()
 * This function works even if there are multiple predicates on a single
 * column, e.g. both "<" and ">" could be applied to the same column. Even
 * in this case this function correctly identifies the lower bound, though not
 * necessarily be a tight lower bound.
 *
 * Note that this function logically is more proper to be in index_util than
 * in here. But it must call the varlen pool which makes moving out to
 * index_util impossible.
 */
bool Index::ConstructLowerBoundTuple(
    storage::Tuple *index_key,
    const std::vector<common::Value > &values,
    const std::vector<oid_t> &key_column_ids,
    const std::vector<ExpressionType> &expr_types) {

  auto schema = index_key->GetSchema();
  auto col_count = schema->GetColumnCount();
  bool all_constraints_equal = true;

  // Go over each column in the key tuple
  // Setting either the placeholder or the min value
  for (oid_t column_itr = 0; column_itr < col_count; column_itr++) {

    // If the current column of the key has a predicate item
    // specified in the key column list
    auto key_column_itr =
        std::find(key_column_ids.begin(), key_column_ids.end(), column_itr);

    bool placeholder = false;
    common::Value value = common::ValueFactory::GetBooleanValue(0);

    // This column is part of the key column ids
    if (key_column_itr != key_column_ids.end()) {

      // This is the index into value list and expression type list
      auto offset = std::distance(key_column_ids.begin(), key_column_itr);

      // If there is an "==" for the current column then we could fix the value
      // for index key
      // otherwise we know not all predicate items are "==", i.e. this is not
      // a point query and potentially requires an index scan
      if (expr_types[offset] == EXPRESSION_TYPE_COMPARE_EQUAL) {
        placeholder = true;

        // This is the value object that will be filled into the index key
        value = values[offset];
      } else {
        all_constraints_equal = false;
      }
    }

    LOG_TRACE("Column itr : %u  Placeholder : %d ", column_itr, placeholder);

    // If the value is available then just fill in the value for the
    // current "==" relation
    // Otherwise if there is not a value then we could only fill the
    // min possible value of the current column's type
    if (placeholder == true) {
      index_key->SetValue(column_itr, value, GetPool());
    } else {
      auto value_type = schema->GetType(column_itr);
      
      index_key->SetValue(column_itr,
                          common::Type::GetMinValue(value_type),
                          GetPool());
    }
  }  // for all columns in index key

  LOG_TRACE("Lower Bound Tuple :: %s", index_key->GetInfo().c_str());

  // Corner case: If not all columns have a "==" relation then still
  // this is not a point query though all existing predicate items
  // are "=="
  if (col_count > values.size()) all_constraints_equal = false;

  return all_constraints_equal;
}

const std::string Index::GetInfo() const {
  std::stringstream os;

  os << "\t-----------------------------------------------------------\n";

  os << "\tINDEX\n";

  os << GetTypeName() << "\t(" << GetName() << ")";
  os << (HasUniqueKeys() ? " UNIQUE " : " NON-UNIQUE") << "\n";

  os << "\tValue schema : " << *(GetKeySchema());

  os << "\t-----------------------------------------------------------\n";

  return os.str();
}

/**
 * @brief Increase the number of tuples in this table
 * @param amount amount to increase
 */
void Index::IncreaseNumberOfTuplesBy(const size_t amount) {
  number_of_tuples += amount;
  dirty = true;
}

/**
 * @brief Decrease the number of tuples in this table
 * @param amount amount to decrease
 */
void Index::DecreaseNumberOfTuplesBy(const size_t amount) {
  number_of_tuples -= amount;
  dirty = true;
}

/**
 * @brief Set the number of tuples in this table
 * @param num_tuples number of tuples
 */
void Index::SetNumberOfTuples(const size_t num_tuples) {
  number_of_tuples = num_tuples;
  dirty = true;
}

/**
 * @brief Get the number of tuples in this table
 * @return number of tuples
 */
size_t Index::GetNumberOfTuples() const { return number_of_tuples; }

/**
 * @brief return dirty flag
 * @return dirty flag
 */
bool Index::IsDirty() const { return dirty; }

/**
 * @brief Reset dirty flag
 */
void Index::ResetDirty() { dirty = false; }

}  // End index namespace
}  // End peloton namespace
