//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// date_functions.h
//
// Identification: src/include/expression/date_functions.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <vector>

#include "expression/abstract_expression.h"
#include "type/types.h"

namespace peloton {
namespace expression {

class DateFunctions{
public:
  // Extract
  // Arguments:
  // the arguements are contained in the args vector
  // the first argument is the part of the date to extract (see type/types.h DatePart)
  // the second argument is the timestamp to extract the part from
  //
  // Return value: the Value returned should be of type Double and
  // should be constructed using type::ValueFactory
  static type::Value Extract(const std::vector<type::Value>& args){
    UNUSED_ATTRIBUTE DatePart date_part = args[0].GetAs<DatePart>();
    UNUSED_ATTRIBUTE uint64_t timestamp = args[1].GetAs<uint64_t>();

    // TODO: define what parts should be implemented for the assignment

    return type::ValueFactory::GetNullValueByType(type::Type::DECIMAL);
  }


};
}  // namespace expression
}  // namespace peloton
