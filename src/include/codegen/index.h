//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// table.h
//
// Identification: src/include/codegen/index.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/codegen.h"
#include "codegen/scan_callback.h"

namespace peloton {
namespace index {
class Index;
}

namespace codegen {
class Index {
public:
  Index(index::Index &index);

private:
  index::Index &index_;
};
}
}
