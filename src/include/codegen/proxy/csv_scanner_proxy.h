//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// csv_scanner_proxy.h
//
// Identification: src/include/codegen/proxy/csv_scanner_proxy.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/proxy/proxy.h"
#include "codegen/proxy/type_builder.h"
#include "codegen/util/csv_scanner.h"

namespace peloton {
namespace codegen {

PROXY(CSVScannerColumn) {
  DECLARE_MEMBER(0, char[sizeof(type::Type)], type);
  DECLARE_MEMBER(1, char *, ptr);
  DECLARE_MEMBER(2, uint32_t, len);
  DECLARE_MEMBER(3, bool, is_null);
  DECLARE_TYPE;
};

PROXY(CSVScanner) {
  DECLARE_MEMBER(0, char[sizeof(std::string)], file_path);
  DECLARE_MEMBER(1, char[sizeof(util::CSVScanner::Callback)], callback);
  DECLARE_MEMBER(2, void *, opaque_callback_state);
  DECLARE_MEMBER(3, char[sizeof(std::vector<util::CSVScanner::Column>)], cols);
  DECLARE_MEMBER(4, util::CSVScanner::Column *, cols_view);
  DECLARE_TYPE;

  DECLARE_METHOD(Init);
  DECLARE_METHOD(Destroy);
  DECLARE_METHOD(Produce);
};

TYPE_BUILDER(CSVScanner, codegen::util::CSVScanner);
TYPE_BUILDER(CSVScannerColumn, codegen::util::CSVScanner::Column);

}  // namespace codegen
}  // namespace peloton