//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// table.h
//
// Identification: src/include/codegen/art_index.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/codegen.h"
#include "codegen/scan_callback.h"
#include "index/index.h"
#include "index/art_index.h"

namespace peloton {

namespace index {
class ArtIndex;
}

namespace codegen {

class ArtIndex {
 public:
  // Constructor
  ArtIndex(index::Index &index);

private:
  index::ArtIndex &art_index_;
};
}
}
