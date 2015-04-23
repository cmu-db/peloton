/*-------------------------------------------------------------------------
 *
 * table.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/catalog/table.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "catalog/column.h"
#include "catalog/index.h"

namespace nstore {
namespace catalog {

//===--------------------------------------------------------------------===//
// Table
//===--------------------------------------------------------------------===//

class Table {

 public:

  Table(std::string name)
 : name(name) {
  }

  std::string GetName() {
    return name;
  }

  bool AddColumn(Column* column);
  Column* GetColumn(const std::string &column_name) const;
  bool RemoveColumn(const std::string &column_name);

  bool AddIndex(Index* index);
  Index* GetIndex(const std::string &index_name) const;
  bool RemoveIndex(const std::string &index_name);

 private:
  std::string name;

  // columns in table
  std::vector<Column*> columns;

  // indexes for table
  std::vector<Index*> indexes;
};

} // End catalog namespace
} // End nstore namespace

