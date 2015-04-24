/*-------------------------------------------------------------------------
 *
 * constraint.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/catalog/constraint.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include <string>
#include <vector>
#include <algorithm>
#include <iostream>

namespace nstore {
namespace catalog {

class Index;
class Table;
class Column;

//===--------------------------------------------------------------------===//
// Constraint
//===--------------------------------------------------------------------===//

class Constraint {

 public:

  enum ConstraintType{
    CONSTRAINT_TYPE_INVALID = 0, // invalid

    CONSTRAINT_TYPE_PRIMARY = 1, // primary key
    CONSTRAINT_TYPE_FOREIGN = 2  // foreign key
  };

  Constraint(std::string name,
             ConstraintType type,
             Index* index,
             Table* foreign_key_table,
             std::vector<Column*> columns,
             std::vector<Column*> foreign_columns)
 : name(name),
   type(type),
   index(index),
   foreign_key_table(foreign_key_table),
   columns(columns),
   foreign_columns(foreign_columns){
  }

  std::string GetName() const {
    return name;
  }

  ConstraintType GetType() const {
    return type;
  }

  Index *GetIndex() const {
    return index;
  }

  Table *GetForeignKeyTable() const {
    return foreign_key_table;
  }

  std::vector<Column*> GetColumns() const {
    return columns;
  }

  std::vector<Column*> GetForeignColumns() const {
    return foreign_columns;
  }

  // Get a string representation of this constraint
  friend std::ostream& operator<<(std::ostream& os, const Constraint& constraint);

 private:
  std::string name;

  // The type of constraint
  ConstraintType type = CONSTRAINT_TYPE_INVALID;

  // The index used by this constraint (if needed)
  Index* index = nullptr;

  // The table referenced by the foreign key (if needed)
  Table* foreign_key_table = nullptr;

  // The columns in the table referenced by the constraint (if needed)
  std::vector<Column*> columns;

  // The columns in the foreign table referenced by the constraint (if needed)
  std::vector<Column*> foreign_columns;

};

} // End catalog namespace
} // End nstore namespace
