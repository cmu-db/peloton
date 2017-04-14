//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// value_proxy.h
//
// Identification: src/include/codegen/value_proxy.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/codegen.h"
#include "type/value.h"

namespace peloton {
namespace codegen {

class ValueProxy {
 public:
  // Get the LLVM Type for type::Value
  static llvm::Type* GetType(CodeGen& codegen) ;

  static type::Value *GetValue(type::Value *values, uint32_t offset) ;

  static void PutValue(type::Value *values, uint32_t offset,
                       type::Value *value);
  static bool CmpEqual(type::Value *val1, type::Value *val2);
  static bool CmpNotEqual(type::Value *val1, type::Value *val2);
  static bool CmpLess(type::Value *val1, type::Value *val2);
  static bool CmpLessEqual(type::Value *val1, type::Value *val2);
  static bool CmpGreater(type::Value *val1, type::Value *val2);
  static bool CmpGreaterEqual(type::Value *val1, type::Value *val2);

  static void OpPlus(type::Value *val1, type::Value *val2,
                     type::Value *values, uint32_t offset);
  static void OpMinus(type::Value *val1, type::Value *val2,
                      type::Value *values, uint32_t offset);
  static void OpMultiply(type::Value *val1, type::Value *val2,
                         type::Value *values, uint32_t offset);
  static void OpDevide(type::Value *val1, type::Value *val2,
                       type::Value *values, uint32_t offset);
  static void OpMod(type::Value *val1, type::Value *val2,
                    type::Value *values, uint32_t offset);

  struct _GetValue {
    // Return the symbol for the ValueProxy.GetValue() function
    static const std::string &GetFunctionName() ;
    // Return the LLVM-typed function definition for ValueProxy.GetValue()
    static llvm::Function *GetFunction(CodeGen &codegen) ;
  };

  struct _PutValue {
    // Return the symbol for the ValueProxy.GetValue() function
    static const std::string &GetFunctionName() ;
    // Return the LLVM-typed function definition for ValueProxy.GetValue()
    static llvm::Function *GetFunction(CodeGen &codegen) ;
  };

  struct _CmpEqual {
    // Return the symbol for the ValueProxy.GetValue() function
    static const std::string &GetFunctionName() ;
    // Return the LLVM-typed function definition for ValueProxy.GetValue()
    static llvm::Function *GetFunction(CodeGen &codegen) ;
  };

  struct _CmpNotEqual {
    // Return the symbol for the ValueProxy.GetValue() function
    static const std::string &GetFunctionName() ;
    // Return the LLVM-typed function definition for ValueProxy.GetValue()
    static llvm::Function *GetFunction(CodeGen &codegen) ;
  };

  struct _CmpLess {
    // Return the symbol for the ValueProxy.GetValue() function
    static const std::string &GetFunctionName() ;
    // Return the LLVM-typed function definition for ValueProxy.GetValue()
    static llvm::Function *GetFunction(CodeGen &codegen) ;
  };

  struct _CmpLessEqual {
    // Return the symbol for the ValueProxy.GetValue() function
    static const std::string &GetFunctionName() ;
    // Return the LLVM-typed function definition for ValueProxy.GetValue()
    static llvm::Function *GetFunction(CodeGen &codegen) ;
  };

  struct _CmpGreater {
    // Return the symbol for the ValueProxy.GetValue() function
    static const std::string &GetFunctionName() ;
    // Return the LLVM-typed function definition for ValueProxy.GetValue()
    static llvm::Function *GetFunction(CodeGen &codegen) ;
  };

  struct _CmpGreaterEqual {
    // Return the symbol for the ValueProxy.GetValue() function
    static const std::string &GetFunctionName() ;
    // Return the LLVM-typed function definition for ValueProxy.GetValue()
    static llvm::Function *GetFunction(CodeGen &codegen) ;
  };

  struct _OpPlus {
    // Return the symbol for the ValueProxy.GetValue() function
    static const std::string &GetFunctionName() ;
    // Return the LLVM-typed function definition for ValueProxy.GetValue()
    static llvm::Function *GetFunction(CodeGen &codegen) ;
  };

  struct _OpMinus {
    // Return the symbol for the ValueProxy.GetValue() function
    static const std::string &GetFunctionName() ;
    // Return the LLVM-typed function definition for ValueProxy.GetValue()
    static llvm::Function *GetFunction(CodeGen &codegen) ;
  };

  struct _OpMultiply {
    // Return the symbol for the ValueProxy.GetValue() function
    static const std::string &GetFunctionName() ;
    // Return the LLVM-typed function definition for ValueProxy.GetValue()
    static llvm::Function *GetFunction(CodeGen &codegen) ;
  };

  struct _OpDevide {
    // Return the symbol for the ValueProxy.GetValue() function
    static const std::string &GetFunctionName() ;
    // Return the LLVM-typed function definition for ValueProxy.GetValue()
    static llvm::Function *GetFunction(CodeGen &codegen) ;
  };

  struct _OpMod {
    // Return the symbol for the ValueProxy.GetValue() function
    static const std::string &GetFunctionName() ;
    // Return the LLVM-typed function definition for ValueProxy.GetValue()
    static llvm::Function *GetFunction(CodeGen &codegen) ;
  };
};

}  // namespace codegen
}  // namespace peloton