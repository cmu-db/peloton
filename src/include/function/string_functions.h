//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// string_functions.h
//
// Identification: src/include/function/string_functions.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstdint>

namespace peloton {

namespace codegen {
namespace type {
class Type;
}  // namespace type
}  // namespace codegen

namespace executor {
class ExecutorContext;
}  // namespace executor

namespace type {
class AbstractPool;
}  // namespace type;

namespace function {

class StringFunctions {
 public:
  struct StrWithLen {
    // Constructor
    StrWithLen(const char *str, uint32_t length) : str(str), length(length) {}

    // Member variables
    const char *str;
    uint32_t length;
  };

  // ASCII code of the first character of the argument.
  static uint32_t Ascii(executor::ExecutorContext &ctx, const char *str,
                        uint32_t length);

  // Like
  static bool Like(executor::ExecutorContext &ctx, const char *t, uint32_t tlen,
                   const char *p, uint32_t plen);

  // Substring
  static StrWithLen Substr(executor::ExecutorContext &ctx, const char *str,
                           uint32_t str_length, int32_t from, int32_t len);

  // Repeat the given string a given number of times
  static StrWithLen Repeat(executor::ExecutorContext &ctx, const char *str,
                           uint32_t length, uint32_t num_repeat);

  // Remove the longest string containing only characters from characters
  // from the start of string
  static StrWithLen LTrim(executor::ExecutorContext &ctx, const char *str,
                          uint32_t str_len, const char *from,
                          uint32_t from_len);

  // Remove the longest string containing only characters from characters
  // from the end of string
  static StrWithLen RTrim(executor::ExecutorContext &ctx, const char *str,
                          uint32_t str_len, const char *from,
                          uint32_t from_len);

  // Trim
  static StrWithLen Trim(executor::ExecutorContext &ctx, const char *str,
                         uint32_t str_len);

  // Remove the longest string consisting only of characters in characters
  // from the start and end of string
  static StrWithLen BTrim(executor::ExecutorContext &ctx, const char *str,
                          uint32_t str_len, const char *from,
                          uint32_t from_len);

  // Length will return the number of characters in the given string
  static uint32_t Length(executor::ExecutorContext &ctx, const char *str,
                         uint32_t length);

  /**
   * Compare two (potentially empty) strings returning an integer value
   * indicating their sort order.
   *
   * @param str1 A pointer to the first string
   * @param len1 The length of the first string
   * @param str2 A pointer to the second string
   * @param len2 The length of the second string
   * @return -1 if the first string is strictly less than the second; 0 if the
   * two strings are equal; 1 if the second string is strictly greater than the
   * second.
   */
  static int32_t CompareStrings(const char *str1, uint32_t len1,
                                const char *str2, uint32_t len2);

  /**
   * Write the provided variable length object into the target buffer.
   *
   * @param data The bytes we wish to serialize
   * @param len The length of the byte array
   * @param buf The target position we wish to write to
   * @param pool A memory pool to source memory from
   */
  static void WriteString(const char *data, uint32_t len, char *buf,
                          peloton::type::AbstractPool &pool);

  /**
   *
   * @param type
   * @param data
   * @param len
   * @return
   */
  static StrWithLen InputString(const codegen::type::Type &type,
                                const char *data, uint32_t len);
};

}  // namespace function
}  // namespace peloton
