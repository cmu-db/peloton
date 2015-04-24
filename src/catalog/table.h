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
#include "storage/table.h"

#include <iostream>

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

  ~Table(){

    // clean up columns
    for(auto col : columns)
      delete col;

    // clean up indices
    for(auto index : indexes)
      delete index;

    // clean up constraints
    for(auto constraint : constraints)
      delete constraint;

    // clean up underlying physical table
    delete physical_table;
  }

  std::string GetName() {
    return name;
  }

  std::vector<Column*> GetColumns() const {
    return columns;
  }

  std::vector<Index*> GetIndices() const {
    return indexes;
  }

  std::vector<Constraint*> GetConstraints() const {
    return constraints;
  }

  storage::Table *GetTable() const {
    return physical_table;
  }

  void SetPhysicalTable(storage::Table* table_) {
    physical_table = table_;
  }

  storage::Table *GetPhysicalTable() {
    return physical_table;
  }

  bool AddColumn(Column* column);
  Column* GetColumn(const std::string &column_name) const;
  bool RemoveColumn(const std::string &column_name);

  bool AddIndex(Index* index);
  Index* GetIndex(const std::string &index_name) const;
  bool RemoveIndex(const std::string &index_name);

  bool AddConstraint(Constraint* constraint);
  Constraint* GetConstraint(const std::string &constraint_name) const;
  bool RemoveConstraint(const std::string &constraint_name);

  // Get a string representation of this table
  friend std::ostream& operator<<(std::ostream& os, const Table& table);

 private:
  std::string name;

  // columns in table
  std::vector<Column*> columns;

  // indexes for table
  std::vector<Index*> indexes;

  // constraints for column
  std::vector<Constraint*> constraints;

  // underlying physical table
  storage::Table* physical_table = nullptr;
};

} // End catalog namespace
} // End nstore namespace

