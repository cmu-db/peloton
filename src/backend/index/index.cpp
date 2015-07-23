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
#include "backend/catalog/manager.h"

#include <iostream>

namespace peloton {
namespace index {

Index::Index(IndexMetadata *metadata)
: metadata(metadata) {

  index_oid = metadata->GetOid();
  // initialize counters
  lookup_counter = insert_counter = delete_counter = update_counter = 0;

}

std::ostream& operator<<(std::ostream& os, const Index& index) {

  os << "\t-----------------------------------------------------------\n";

  os << "\tINDEX\n";

  os << index.GetTypeName() << "\t(" << index.GetName() << ")";
  os << (index.HasUniqueKeys() ? " UNIQUE " : " NON-UNIQUE") << "\n";

  os << "\tValue schema : " << *(index.GetKeySchema() );

  os << "\t-----------------------------------------------------------\n";

  return os;
}

void Index::GetInfo() const {

  std::cout << this->GetName() << ",";
  std::cout << GetTypeName() << ",";
  std::cout << lookup_counter << ",";
  std::cout << insert_counter << ",";
  std::cout << delete_counter << ",";
  std::cout << update_counter << std::endl;

}

/**
 * @brief Increase the number of tuples in this table
 * @param amount amount to increase
 */
void Index::IncreaseNumberOfTuplesBy(const float amount){
  number_of_tuples += amount;
}

/**
 * @brief Decrease the number of tuples in this table
 * @param amount amount to decrease
 */
void Index::DecreaseNumberOfTuplesBy(const float amount){
  number_of_tuples -= amount;
}

/**
 * @brief Set the number of tuples in this table
 * @param num_tuples number of tuples
 */
void Index::SetNumberOfTuples(const float num_tuples){
  number_of_tuples = num_tuples;
}

/**
 * @brief Get the number of tuples in this table
 * @return number of tuples
 */
float Index::GetNumberOfTuples() const{
  return number_of_tuples;
}

} // End index namespace
} // End peloton namespace



