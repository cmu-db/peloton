//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// value_peeker_proxy.h
//
// Identification: src/include/codegen/value_peeker_proxy.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/codegen.h"
#include "type/value_factory.h"
#include "type/value.h"

namespace peloton {
namespace codegen {

class ValueFactoryProxy {
 public:
  static void GetTinyIntValue(
          type::Value *values, uint32_t offset, int8_t value) ;

  static void GetSmallIntValue(
          type::Value *values, uint32_t offset, int16_t value) ;

  static void GetIntegerValue(
          type::Value *values, uint32_t offset, int32_t value) ;

  static void GetBigIntValue(
          type::Value *values, uint32_t offset, int64_t value) ;

  static void GetDateValue(
          type::Value *values, uint32_t offset, uint32_t value) ;

  static void GetTimestampValue(
          type::Value *values, uint32_t offset, int64_t value) ;

  static void GetDecimalValue(
          type::Value *values, uint32_t offset, double value) ;

  static void GetBooleanValue(
          type::Value *values, uint32_t offset, bool value) ;

  static void GetVarcharValue(
          type::Value *values, uint32_t offset, char *c_str, int len) ;

  static void GetVarbinaryValue(
          type::Value *values, uint32_t offset, char *c_str, int len) ;

  // The proxy around ValuePeeker::PeekTinyInt()
  struct _GetTinyIntValue {
    static const std::string &GetFunctionName();
    static llvm::Function *GetFunction(CodeGen &codegen);
  };

  // The proxy around ValuePeeker::PeekSmallInt()
  struct _GetSmallIntValue {
    static const std::string &GetFunctionName();
    static llvm::Function *GetFunction(CodeGen &codegen);
  };

  // The proxy around ValuePeeker::PeekInteger()
  struct _GetIntegerValue {
    static const std::string &GetFunctionName();
    static llvm::Function *GetFunction(CodeGen &codegen);
  };

  // The proxy around ValuePeeker::PeekBigInt()
  struct _GetBigIntValue {
    static const std::string &GetFunctionName();
    static llvm::Function *GetFunction(CodeGen &codegen);
  };

  // The proxy around ValuePeeker::PeekDate()
  struct _GetDateValue {
    static const std::string &GetFunctionName();
    static llvm::Function *GetFunction(CodeGen &codegen);
  };

  // The proxy around ValuePeeker::PeekTimestamp()
  struct _GetTimestampValue {
    static const std::string &GetFunctionName();
    static llvm::Function *GetFunction(CodeGen &codegen);
  };

  struct _GetDecimalValue{
    static const std::string &GetFunctionName();
    static llvm::Function *GetFunction(CodeGen &codegen);
  };

  struct _GetBooleanValue{
    static const std::string &GetFunctionName();
    static llvm::Function *GetFunction(CodeGen &codegen);
  };

  struct _GetVarcharValue{
    static const std::string &GetFunctionName();
    static llvm::Function *GetFunction(CodeGen &codegen);
  };

  struct _GetVarbinaryValue{
    static const std::string &GetFunctionName();
    static llvm::Function *GetFunction(CodeGen &codegen);
  };
};

}  // namespace codegen
}  // namespace peloton
