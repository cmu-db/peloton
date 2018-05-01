//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// csv_scanner.cpp
//
// Identification: src/codegen/util/csv_scanner.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/util/csv_scanner.h"

#include <boost/filesystem.hpp>

#include "common/exception.h"
#include "util/string_util.h"

namespace peloton {
namespace codegen {
namespace util {

CSVScanner::CSVScanner(const std::string &file_path,
                       const codegen::type::Type *col_types, uint32_t num_cols,
                       CSVScanner::Callback func, void *opaque_state)
    : file_path_(file_path), func_(func), opaque_state_(opaque_state) {
  // Initialize the columns
  cols_.resize(num_cols);
  for (uint32_t i = 0; i < num_cols; i++) {
    cols_[i].col_type = col_types[i];
    cols_[i].ptr = nullptr;
    cols_[i].is_null = false;
  }

  // Setup the view. Since the Column's vector will never be resized after this
  // point (it isn't possible to add or remove columns once the scan has been
  // constructed), grabbing a pointer to the underlying array is safe for the
  // lifetime of this scanner.
  cols_view_ = cols_.data();
}

CSVScanner::~CSVScanner() {}

void CSVScanner::Init(CSVScanner &scanner, const char *file_path,
                      const codegen::type::Type *col_types, uint32_t num_cols,
                      CSVScanner::Callback func, void *opaque_state) {
  new (&scanner) CSVScanner(file_path, col_types, num_cols, func, opaque_state);
}

void CSVScanner::Destroy(CSVScanner &scanner) { scanner.~CSVScanner(); }

void CSVScanner::Produce() { InitializeScan(); }

void CSVScanner::InitializeScan() {
  // Validity checks
  if (!boost::filesystem::exists(file_path_)) {
    throw ExecutorException{StringUtil::Format(
        "ERROR: input path '%s' does not exist", file_path_.c_str())};
  }

  if (!boost::filesystem::is_directory(file_path_)) {
    throw ExecutorException{StringUtil::Format(
        "ERROR: input '%s' is a directory, not a file", file_path_.c_str())};
  }

  if (!boost::filesystem::is_regular_file(file_path_)) {
    throw ExecutorException{StringUtil::Format(
        "ERROR: unable to read file '%s'", file_path_.c_str())};
  }
}

}  // namespace util
}  // namespace codegen
}  // namespace peloton