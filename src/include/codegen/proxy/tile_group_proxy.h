//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tile_group_proxy.h
//
// Identification: src/include/codegen/tile_group_proxy.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/codegen.h"

namespace peloton {
namespace codegen {

class TileGroupProxy {
 public:
  static llvm::Type *GetType(CodeGen &codegen);

  //===--------------------------------------------------------------------===//
  // LLVM proxy for storage::TileGroup::GetNextTupleSlot(...)
  //===--------------------------------------------------------------------===//
  struct _GetNextTupleSlot {
    // Get the symbol name
    static const std::string &GetFunctionName();
    // Get the actual function definition
    static llvm::Function *GetFunction(CodeGen &codegen);
  };

  //===--------------------------------------------------------------------===//
  // LLVM proxy for storage::TileGroup::GetTileGroupId(...)
  //===--------------------------------------------------------------------===//
  struct _GetTileGroupId {
    // Get the symbol name
    static const std::string &GetFunctionName();
    // Get the actual function definition
    static llvm::Function *GetFunction(CodeGen &codegen);
  };
};

}  // namespace codegen
}  // namespace peloton