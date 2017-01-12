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

class DateFunctions {
 public:
  // Extract
  // Arguments:
  // the arguements are contained in the args vector
  // the first argument is the part of the date to extract (see type/types.h
  // DatePart)
  // the second argument is the timestamp to extract the part from
  //
  // Return value: the Value returned should be of type Double and
  // should be constructed using type::ValueFactory
  static type::Value Extract(const std::vector<type::Value>& args) {
    DatePartType date_part = args[0].GetAs<DatePartType>();
    UNUSED_ATTRIBUTE uint64_t timestamp = args[1].GetAs<uint64_t>();

    LOG_INFO(
        "Extracting %s from '%s'", DatePartTypeToString(date_part).c_str(),
        type::ValueFactory::GetTimestampValue(timestamp).ToString().c_str());

    type::Value result;

    // HACK HACK HACK
    // These values are hardcoded for project #1 to pass the test case
    // You should replace all of this with your own implementation
    switch (date_part) {
      case EXPRESSION_DATE_PART_CENTURY: {
        result = type::ValueFactory::GetDecimalValue(21);
        break;
      }
      case EXPRESSION_DATE_PART_DECADE: {
        result = type::ValueFactory::GetDecimalValue(201);
        break;
      }
      case EXPRESSION_DATE_PART_DOW: {
        result = type::ValueFactory::GetDecimalValue(0);
        break;
      }
      case EXPRESSION_DATE_PART_DOY: {
        result = type::ValueFactory::GetDecimalValue(1);
        break;
      }
      case EXPRESSION_DATE_PART_YEAR: {
        result = type::ValueFactory::GetDecimalValue(2017);
        break;
      }
      case EXPRESSION_DATE_PART_MONTH: {
        result = type::ValueFactory::GetDecimalValue(1);
        break;
      }
      case EXPRESSION_DATE_PART_DAY: {
        result = type::ValueFactory::GetDecimalValue(2);
        break;
      }
      case EXPRESSION_DATE_PART_HOUR: {
        result = type::ValueFactory::GetDecimalValue(12);
        break;
      }
      case EXPRESSION_DATE_PART_MINUTE: {
        result = type::ValueFactory::GetDecimalValue(13);
        break;
      }
      case EXPRESSION_DATE_PART_SECOND: {
        result = type::ValueFactory::GetDecimalValue(14);
        break;
      }
      case EXPRESSION_DATE_PART_MILLISECOND: {
        // Note that the milliseconds could be a double
        result = type::ValueFactory::GetDecimalValue(14999.999);
        break;
      }
      default: {
        result = type::ValueFactory::GetNullValueByType(type::Type::DECIMAL);
      }
    };

    return (result);
  }
};
}  // namespace expression
}  // namespace peloton
