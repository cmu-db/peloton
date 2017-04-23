//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// primitive_value_proxy.h
//
// Identification: src/include/codegen/primitive_value_proxy.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/codegen.h"
#include "type/value_peeker.h"
#include "type/value.h"

namespace peloton {
namespace codegen {

class PrimitiveValueProxy {
 public:
  static int8_t GetTinyInt(int8_t *values, uint32_t offset);

  static int16_t GetSmallInt(int16_t *values, uint32_t offset);

  static int32_t GetInteger(int32_t *values, uint32_t offset);

  static int64_t GetBigInt(int64_t *values, uint32_t offset);

  static double GetDouble(double *values, uint32_t offset);

  static int32_t GetDate(int32_t *values, uint32_t offset);

  static uint64_t GetTimestamp(int8_t *values, uint32_t offset);

  static char *GetVarcharVal(char **values, uint32_t offset);

  static size_t GetVarcharLen(int32_t *values, uint32_t offset);

  // The proxy around ValuePeeker::PeekTinyInt()
  struct _GetTinyInt {
    static const std::string &GetFunctionName();
    static llvm::Function *GetFunction(CodeGen &codegen);
  };

  // The proxy around ValuePeeker::PeekSmallInt()
  struct _GetSmallInt {
    static const std::string &GetFunctionName();
    static llvm::Function *GetFunction(CodeGen &codegen);
  };

  // The proxy around ValuePeeker::PeekInteger()
  struct _GetInteger {
    static const std::string &GetFunctionName();
    static llvm::Function *GetFunction(CodeGen &codegen);
  };

  // The proxy around ValuePeeker::PeekBigInt()
  struct _GetBigInt {
    static const std::string &GetFunctionName();
    static llvm::Function *GetFunction(CodeGen &codegen);
  };

  // The proxy around ValuePeeker::PeekDouble()
  struct _GetDouble {
    static const std::string &GetFunctionName();
    static llvm::Function *GetFunction(CodeGen &codegen);
  };

  // The proxy around ValuePeeker::PeekDate()
  struct _GetDate {
    static const std::string &GetFunctionName();
    static llvm::Function *GetFunction(CodeGen &codegen);
  };

  // The proxy around ValuePeeker::PeekTimestamp()
  struct _GetTimestamp {
    static const std::string &GetFunctionName();
    static llvm::Function *GetFunction(CodeGen &codegen);
  };

  // The proxy around ValuePeeker::PeekVarchar()
  struct _GetVarcharVal {
    static const std::string &GetFunctionName();
    static llvm::Function *GetFunction(CodeGen &codegen);
  };

  struct _GetVarcharLen {
    static const std::string &GetFunctionName();
    static llvm::Function *GetFunction(CodeGen &codegen);
  };
};

}  // namespace codegen
}  // namespace peloton
