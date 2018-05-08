//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// csv_scanner_proxy.cpp
//
// Identification: src/codegen/proxy/csv_scanner_proxy.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/proxy/csv_scanner_proxy.h"

#include "codegen/proxy/executor_context_proxy.h"
#include "codegen/proxy/runtime_functions_proxy.h"

namespace peloton {
namespace codegen {

DEFINE_TYPE(CSVScanner, "util::CSVScanner", MEMBER(opaque1), MEMBER(cols),
            MEMBER(opaque2));

DEFINE_TYPE(CSVScannerColumn, "util::CSVScanner::Column", MEMBER(type),
            MEMBER(ptr), MEMBER(len), MEMBER(is_null));

DEFINE_METHOD(peloton::codegen::util, CSVScanner, Init);
DEFINE_METHOD(peloton::codegen::util, CSVScanner, Destroy);
DEFINE_METHOD(peloton::codegen::util, CSVScanner, Produce);

}  // namespace codegen
}  // namespace peloton