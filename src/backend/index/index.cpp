//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// index.cpp
//
// Identification: src/backend/index/index.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/index/index.h"
#include "backend/common/exception.h"
#include "backend/common/logger.h"
#include "backend/common/pool.h"
#include "backend/catalog/schema.h"
#include "backend/catalog/manager.h"
#include "backend/storage/tuple.h"

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


bool Index::Compare(const AbstractTuple &index_key,
                    const std::vector<oid_t> &key_column_ids,
                    const std::vector<ExpressionType> &expr_types,
                    const std::vector<Value> &values) {
  int diff;

  oid_t key_column_itr = -1;
  // Go over each attribute in the list of comparison columns
  for (auto column_itr : key_column_ids) {
    key_column_itr++;

    const Value &lhs = index_key.GetValue(column_itr);
    const Value &rhs = values[key_column_itr];
    const ExpressionType expr_type = expr_types[key_column_itr];

    diff = lhs.Compare(rhs);

    if (diff == VALUE_COMPARE_EQUAL) {
      switch (expr_type) {
        case EXPRESSION_TYPE_COMPARE_EQUAL:
        case EXPRESSION_TYPE_COMPARE_LESSTHANOREQUALTO:
        case EXPRESSION_TYPE_COMPARE_GREATERTHANOREQUALTO:
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
          return false;

        default:
          throw IndexException("Unsupported expression type : " +
                               std::to_string(expr_type));
      }
    }
  }

  return true;
}

bool Index::SetLowerBoundTuple(storage::Tuple *index_key,
                               const std::vector<peloton::Value> &values,
                               const std::vector<oid_t> &key_column_ids,
                               const std::vector<ExpressionType> &expr_types) {
  auto schema = index_key->GetSchema();
  auto col_count = schema->GetColumnCount();
  bool all_equal = true;

  // Go over each column in the key tuple
  // Setting either the placeholder or the min value
  for (oid_t column_itr = 0; column_itr < col_count; column_itr++) {
    auto key_column_itr =
        std::find(key_column_ids.begin(), key_column_ids.end(), column_itr);
    bool placeholder = false;
    Value value;

    if (key_column_itr != key_column_ids.end()) {
      auto offset = std::distance(key_column_ids.begin(), key_column_itr);
      if (expr_types[offset] == EXPRESSION_TYPE_COMPARE_EQUAL) {
        placeholder = true;
        value = values[offset];
      } else {
        // not all expressions are equal
        all_equal = false;
      }
    }

    // fill in the placeholders
    if (placeholder == true) {
      index_key->SetValue(column_itr, value, GetPool());
    }
    // get the min value
    else {
      auto value_type = schema->GetType(column_itr);
      index_key->SetValue(column_itr, Value::GetMinValue(value_type), GetPool());
    }
  }

  LOG_TRACE("Lower Bound Tuple :: %s", index_key->GetInfo().c_str());
  if (col_count > values.size()) all_equal = false;

  return all_equal;
}

Index::Index(IndexMetadata *metadata) : metadata(metadata) {
  index_oid = metadata->GetOid();
  // initialize counters
  lookup_counter = insert_counter = delete_counter = update_counter = 0;

  // initialize pool
  pool = new VarlenPool();
}

std::ostream &operator<<(std::ostream &os, const Index &index) {
  os << "\t-----------------------------------------------------------\n";

  os << "\tINDEX\n";

  os << index.GetTypeName() << "\t(" << index.GetName() << ")";
  os << (index.HasUniqueKeys() ? " UNIQUE " : " NON-UNIQUE") << "\n";

  os << "\tValue schema : " << *(index.GetKeySchema());

  os << "\t-----------------------------------------------------------\n";

  return os;
}

void Index::GetInfo() const {
  std::stringstream os;

  os << this->GetName() << ",";
  os << GetTypeName() << ",";
  os << lookup_counter << ",";
  os << insert_counter << ",";
  os << delete_counter << ",";
  os << update_counter << std::endl;

  LOG_INFO("Info :: %s", os.str().c_str());
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
