//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// csv_scanner.h
//
// Identification: src/include/codegen/util/csv_scanner.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <vector>

#include "codegen/type/type.h"

namespace peloton {
namespace codegen {
namespace util {

class CSVScanner {
 public:
  using Callback = void (*)(void *);

  struct Column {
    codegen::type::Type col_type;
    char *ptr;
    uint32_t len;
    bool is_null;
  };

  CSVScanner(const std::string &file_path, const codegen::type::Type *col_types,
             uint32_t num_cols, Callback func, void *opaque_state);

  ~CSVScanner();

  static void Init(CSVScanner &scanner, const char *file_path,
                   const codegen::type::Type *col_types, uint32_t num_cols,
                   Callback func, void *opaque_state);

  static void Destroy(CSVScanner &scanner);

  void Produce();

 private:
  void InitializeScan();

 private:
  // The file
  const std::string file_path_;

  // The callback function and opaque state
  Callback func_;
  void *opaque_state_;

  std::vector<Column> cols_;
  Column *cols_view_;
};

}  // namespace util
}  // namespace codegen
}  // namespace peloton