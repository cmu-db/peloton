/*-------------------------------------------------------------------------
 *
 * database.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/catalog/database.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "catalog/table.h"
#include <iostream>

namespace nstore {
namespace catalog {

//===--------------------------------------------------------------------===//
// Database
//===--------------------------------------------------------------------===//

class Database {

 public:

  Database(std::string name)
 : name(name) {
  }

  std::string GetName() {
    return name;
  }

  bool AddTable(Table* table);
  Table* GetTable(const std::string &table_name) const;
  bool RemoveTable(const std::string &table_name);

  // Get a string representation of this database
  friend std::ostream& operator<<(std::ostream& os, const Database& database);

 private:
  std::string name;

  // tables in db
  std::vector<Table*> tables;

};

} // End catalog namespace
} // End nstore namespace
