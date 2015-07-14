/*-------------------------------------------------------------------------
 *
 * column.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/catalog/column.cpp
 *
 *-------------------------------------------------------------------------
 */

#include <sstream>

#include "backend/catalog/column.h"
#include "backend/common/types.h"

namespace peloton {
namespace catalog {

void Column::SetLength(oid_t column_length) {

  // Set the column length based on whether it is inlined
  if(is_inlined){
    fixed_length = column_length;
    variable_length = 0;
  }
  else{
    fixed_length = sizeof(uintptr_t);
    variable_length = column_length;
  }
}

std::ostream& operator<< (std::ostream& os, const Column& column){

  os << " name = " << column.GetName() << "," <<
      " type = " << GetTypeName(column.GetType()) << "," <<
      " offset = " << column.column_offset << "," <<
      " fixed length = " << column.fixed_length << "," <<
      " variable length = " << column.variable_length << "," <<
      " inlined = " << column.is_inlined << std::endl;

  return os;
}

} // End catalog namespace
} // End peloton namespace


