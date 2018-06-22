//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// sequence_functions.h
//
// Identification: src/include/function/sequence_functions.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstdint>
#include "type/value.h"

namespace peloton {

namespace executor {
class ExecutorContext;
}  // namespace executor

namespace function {

class SequenceFunctions {
 public:

  /**
   * @brief   The actual implementation to get the incremented value
   *          for the specified sequence.
   * @param  executor context
   * @param  sequence name
   * @return  the next value for the sequence
   * @exception the sequence does not exist
   */
  static uint32_t Nextval(executor::ExecutorContext &ctx, const char *sequence_name);

  /**
   * @brief   The actual implementation to get the current value for the
   *          specified sequence.
   * @param  sequence name
   * @param  executor context
   * @return  the current value of a sequence
   * @exception either the sequence does not exist, or 'call nextval before currval'
   */
  static uint32_t Currval(executor::ExecutorContext &ctx, const char *sequence_name);

  //===--------------------------------------------------------------------===//
  // Wrapper function used for AddBuiltin Functions
  //===--------------------------------------------------------------------===//

  /**
   * @brief   The wrapper function to get the incremented value for the
   *          specified sequence.
   * @param  args {executor context, sequence name}
   * @return  the result of executing NextVal
   */
  static type::Value _Nextval(const std::vector<type::Value> &args);

  /**
   * @brief   The wrapper function to get the current value for the specified sequence
   * @param args {executor context, sequence name}
   * @return the result of executing CurrVal
   */
  static type::Value _Currval(const std::vector<type::Value> &args);
};

}  // namespace function
}  // namespace peloton
