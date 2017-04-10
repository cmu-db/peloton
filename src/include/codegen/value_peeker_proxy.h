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
#include "type/value_peeker.h"
#include "type/value.h"

namespace peloton {
namespace codegen {

class ValuePeekerProxy {
 public:
  static int8_t PeekTinyInt(type::Value *value);

  static int16_t PeekSmallInt(type::Value *value);

  static int32_t PeekInteger(type::Value *value);

  static int64_t PeekBigInt(type::Value *value);

  static double PeekDouble(type::Value *value);

  static int32_t PeekDate(type::Value *value);

  static uint64_t PeekTimestamp(type::Value *value);

  static char *PeekVarcharVal(type::Value *value);

  static size_t PeekVarcharLen(type::Value *value);

  // The proxy around ValuePeeker::PeekTinyInt()
  struct _PeekTinyInt {
    static const std::string &GetFunctionName();
    static llvm::Function *GetFunction(CodeGen &codegen);
  };

  // The proxy around ValuePeeker::PeekSmallInt()
  struct _PeekSmallInt {
    static const std::string &GetFunctionName();
    static llvm::Function *GetFunction(CodeGen &codegen);
  };

  // The proxy around ValuePeeker::PeekInteger()
  struct _PeekInteger {
    static const std::string &GetFunctionName();
    static llvm::Function *GetFunction(CodeGen &codegen);
  };

  // The proxy around ValuePeeker::PeekBigInt()
  struct _PeekBigInt {
    static const std::string &GetFunctionName();
    static llvm::Function *GetFunction(CodeGen &codegen);
  };

  // The proxy around ValuePeeker::PeekDouble()
  struct _PeekDouble {
    static const std::string &GetFunctionName();
    static llvm::Function *GetFunction(CodeGen &codegen);
  };

  // The proxy around ValuePeeker::PeekDate()
  struct _PeekDate {
    static const std::string &GetFunctionName();
    static llvm::Function *GetFunction(CodeGen &codegen);
  };

  // The proxy around ValuePeeker::PeekTimestamp()
  struct _PeekTimestamp {
    static const std::string &GetFunctionName();
    static llvm::Function *GetFunction(CodeGen &codegen);
  };

  // The proxy around ValuePeeker::PeekVarchar()
  struct _PeekVarcharVal {
    static const std::string &GetFunctionName();
    static llvm::Function *GetFunction(CodeGen &codegen);
  };

  struct _PeekVarcharLen {
    static const std::string &GetFunctionName();
    static llvm::Function *GetFunction(CodeGen &codegen);
  };
};

}  // namespace codegen
}  // namespace peloton
