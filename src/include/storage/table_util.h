//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// table_util.h
//
// Identification: src/include/storage/table_util.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "catalog/manager.h"
#include "common/types.h"
#include "storage/data_table.h"

#include <iostream>
#include <map>
#include <string>

namespace peloton {
namespace storage {

class AbstractTable;
class DataTable;

//===--------------------------------------------------------------------===//
// TableUtil
//===--------------------------------------------------------------------===//

/**
 * Table Convenience Functions
 */
class TableUtil {
 public:
  /**
   * Generate a string representation of the table with all of its rows
   * Warning: This should only be used for debugging because the output can be
   * large.
   */
  static std::string GetInfo(DataTable *table);
};

}  // End storage namespace
}  // End peloton namespace
