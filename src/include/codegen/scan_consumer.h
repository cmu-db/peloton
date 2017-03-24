//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// scan_consumer.h
//
// Identification: src/include/codegen/scan_consumer.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/codegen.h"
#include "codegen/tile_group.h"
#include "codegen/value.h"

namespace peloton {
namespace codegen {

//===----------------------------------------------------------------------===//
// An interface for consumers of table scans. Anytime a client wishes to
// generate code for a table scan, they must implement an instance of this
// interface. Various callback hooks are provided for when the scanner begins
// iterating over a new tile group, and when iteration over a tile group
// completes. In between these calls, ScanBody() will be called to allow the
// client to generate code that goes into the body/loop of the table scan.
//===----------------------------------------------------------------------===//
class ScanConsumer {
 public:
  // Virtual destructor
  virtual ~ScanConsumer() {}
  // Callback for when iteration begins over a new tile group. The second
  // parameter is a pointer to the tile group.
  virtual void TileGroupStart(CodeGen &codegen,
                              llvm::Value *tile_group_ptr) = 0;

  // Callback to generate the body of the scan loop
  virtual void ScanBody(CodeGen &codegen, llvm::Value *tid_start,
                        llvm::Value *tid_end,
                        TileGroup::TileGroupAccess &tile_group_access) = 0;

  // Callback for when iteration over the given tile group has completed
  virtual void TileGroupFinish(CodeGen &codegen,
                               llvm::Value *tile_group_ptr) = 0;
};

}  // namespace codegen
}  // namespace peloton