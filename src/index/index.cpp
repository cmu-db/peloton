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
#include "common/pool.h"
#include "catalog/schema.h"
#include "catalog/manager.h"
#include "storage/tuple.h"

#include <iostream>

namespace peloton {
namespace index {

Index::~Index() {
  // clean up metadata
  delete metadata;

  // clean up pool
  delete pool;
}

IndexMetadata::~IndexMetadata() {
  // clean up key schema
  delete key_schema;
  // no need to clean the tuple schema
}

oid_t IndexMetadata::GetColumnCount() const {
  return GetKeySchema()->GetColumnCount();
}

/*
 * Compare() - Check whether a given index key satisfies a predicate
 *
 * The predicate has the same specification as those in Scan()
 */
bool Index::Compare(const AbstractTuple &index_key,
                    const std::vector<oid_t> &key_column_ids,
                    const std::vector<ExpressionType> &expr_types,
                    const std::vector<Value> &values) {
  int diff;

  oid_t key_column_itr = -1;
  // Go over each attribute in the list of comparison columns
  // The key_columns_ids, as the name shows, saves the key column ids that
  // have values and expression needs to be compared.

  // Example:
  // 1.
  //    key_column_ids { 0 }
  //    expr_types { == }
  //    values    { 5 }
  // basically it's saying get the tuple whose 0 column, which is the key
  // column,
  //  equals to 5
  //
  // 2.
  //   key_column_ids {0, 1}
  //   expr_types { > , >= }
  //  values  {5, 10}
  // it's saysing col[0] > 5 && col[1] >= 10, where 0 and 1 are key columns.

  for (auto column_itr : key_column_ids) {
    key_column_itr++;

    const Value &rhs = values[key_column_itr];
    const Value &lhs = index_key.GetValue(column_itr);
    const ExpressionType expr_type = expr_types[key_column_itr];

    if (expr_type == EXPRESSION_TYPE_COMPARE_IN) {
      bool bret = lhs.InList(rhs);
      if (bret == true) {
        diff = VALUE_COMPARE_EQUAL;
      } else {
        diff = VALUE_COMPARE_NO_EQUAL;
      }
    } else {
      diff = lhs.Compare(rhs);
    }

    LOG_TRACE("Difference : %d ", diff);

    if (diff == VALUE_COMPARE_EQUAL) {
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
    } else if (diff == VALUE_COMPARE_LESSTHAN) {
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
    } else if (diff == VALUE_COMPARE_GREATERTHAN) {
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
    } else if (diff == VALUE_COMPARE_NO_EQUAL) {
      // problems here when there are multiple
      // conditions with OR in the query
      return false;
    }
  }

  return true;
}

/*
 * IfForwardExpression() - Returns true if the expression is > or >=
 */
bool Index::IfForwardExpression(ExpressionType e) {
  // To reduce branch misprediction penalty
  return e == EXPRESSION_TYPE_COMPARE_GREATERTHAN ||
         e == EXPRESSION_TYPE_COMPARE_GREATERTHANOREQUALTO;
}

/*
 * IfBackWardExpression() - Returns true if the expression is < or <=
 */
bool Index::IfBackwardExpression(ExpressionType e) {
  return e == EXPRESSION_TYPE_COMPARE_LESSTHAN ||
         e == EXPRESSION_TYPE_COMPARE_LESSTHANOREQUALTO;
}

bool Index::ValuePairComparator(const std::pair<peloton::Value, int> &i,
                                const std::pair<peloton::Value, int> &j) {
  if (i.first.Compare(j.first) == VALUE_COMPARE_EQUAL) {
    return i.second < j.second;
  }
  return i.first.Compare(j.first) == VALUE_COMPARE_LESSTHAN;
}

/*
 * ConstructLowerBoundTuple() - Constructs a lower bound of index key that
 *                              satisfies a given tuple
 *
 * The predicate has the same specification as those in Scan()
 */
bool Index::ConstructLowerBoundTuple(
    storage::Tuple *index_key,
    const std::vector<peloton::Value> &values,
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
    Value value;

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
                          Value::GetMinValue(value_type),
                          GetPool());
    }
  } // for all columns in index key

  LOG_TRACE("Lower Bound Tuple :: %s", index_key->GetInfo().c_str());
  
  // Corner case: If not all columns have a "==" relation then still
  // this is not a point query though all existing predicate items
  // are "=="
  if (col_count > values.size()) all_constraints_equal = false;

  return all_constraints_equal;
}

Index::Index(IndexMetadata *metadata) : metadata(metadata) {
  index_oid = metadata->GetOid();
  // initialize counters
  lookup_counter = insert_counter = delete_counter = update_counter = 0;

  // initialize pool
  pool = new VarlenPool(BACKEND_TYPE_MM);
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
void Index::IncreaseNumberOfTuplesBy(const float amount) {
  number_of_tuples += amount;
  dirty = true;
}

/**
 * @brief Decrease the number of tuples in this table
 * @param amount amount to decrease
 */
void Index::DecreaseNumberOfTuplesBy(const float amount) {
  number_of_tuples -= amount;
  dirty = true;
}

/**
 * @brief Set the number of tuples in this table
 * @param num_tuples number of tuples
 */
void Index::SetNumberOfTuples(const float num_tuples) {
  number_of_tuples = num_tuples;
  dirty = true;
}

/**
 * @brief Get the number of tuples in this table
 * @return number of tuples
 */
float Index::GetNumberOfTuples() const { return number_of_tuples; }

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
