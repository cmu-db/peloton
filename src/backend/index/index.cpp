/*-------------------------------------------------------------------------
 *
 * index.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/index/index.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "backend/index/index.h"
#include "backend/common/exception.h"
#include "backend/common/logger.h"
#include "backend/catalog/manager.h"

#include <iostream>

namespace peloton {
namespace index {

bool Index::IndexKeyComparator(const storage::Tuple& index_key,
                               const std::vector<Value>& values,
                               const std::vector<oid_t>& index_key_columns,
                               const std::vector<ExpressionType>& exprs) {
  int diff;

  // Go over each attribute in the list of comparison columns
  for(auto column_itr : index_key_columns) {
    const Value& lhs = index_key.GetValue(column_itr);
    const Value& rhs = values[column_itr];
    const ExpressionType expr_type = exprs[column_itr];

    diff = lhs.Compare(rhs);

    if(diff == VALUE_COMPARE_EQUAL) {
      switch(expr_type) {
        case EXPRESSION_TYPE_COMPARE_EQ:
        case EXPRESSION_TYPE_COMPARE_LTE:
        case EXPRESSION_TYPE_COMPARE_GTE:
          continue;

        case EXPRESSION_TYPE_COMPARE_NE:
        case EXPRESSION_TYPE_COMPARE_LT:
        case EXPRESSION_TYPE_COMPARE_GT:
          return false;

        default:
          throw IndexException("Unsupported expression type : " + std::to_string(expr_type));
      }
    }
    else if(diff == VALUE_COMPARE_LESSTHAN) {
      switch(expr_type) {
        case EXPRESSION_TYPE_COMPARE_NE:
        case EXPRESSION_TYPE_COMPARE_LT:
        case EXPRESSION_TYPE_COMPARE_LTE:
          continue;

        case EXPRESSION_TYPE_COMPARE_EQ:
        case EXPRESSION_TYPE_COMPARE_GT:
        case EXPRESSION_TYPE_COMPARE_GTE:
          return false;

        default:
          throw IndexException("Unsupported expression type : " + std::to_string(expr_type));
      }
    }
    else if(diff == VALUE_COMPARE_GREATERTHAN) {
      switch(expr_type) {
        case EXPRESSION_TYPE_COMPARE_NE:
        case EXPRESSION_TYPE_COMPARE_GT:
        case EXPRESSION_TYPE_COMPARE_GTE:
          continue;

        case EXPRESSION_TYPE_COMPARE_EQ:
        case EXPRESSION_TYPE_COMPARE_LT:
        case EXPRESSION_TYPE_COMPARE_LTE:
          return false;

        default:
          throw IndexException("Unsupported expression type : " + std::to_string(expr_type));
      }
    }

  }

  return true;
}


Index::Index(IndexMetadata* metadata) : metadata(metadata) {
  index_oid = metadata->GetOid();
  // initialize counters
  lookup_counter = insert_counter = delete_counter = update_counter = 0;
}

std::ostream& operator<<(std::ostream& os, const Index& index) {
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
bool Index::IsDirty() const{
  return dirty;
}

/**
 * @brief Reset dirty flag
 */
void Index::ResetDirty() {
  dirty = false;
}

}  // End index namespace
}  // End peloton namespace
