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
  // LLVM wrapper/definition for storage::TileGroup::GetNextTupleSlot(...) to
  // get the number of tuples in a tile group
  //===--------------------------------------------------------------------===//
  struct _GetNextTupleSlot {
    // Get the symbol name
    static const std::string &GetFunctionName();
    // Get the actual function definition
    static llvm::Function *GetFunction(CodeGen &codegen);
  };
};

}  // namespace codegen
}  // namespace peloton