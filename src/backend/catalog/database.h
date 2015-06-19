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

#include "backend/catalog/table.h"

#include <iostream>
#include <mutex>

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

  ~Database() {

    // clean up tables
    for(auto table : tables)
      delete table;

  }

  std::string GetName() {
    return name;
  }

  bool AddTable(Table* table);
  Table* GetTable(const std::string &table_name) const;
  bool RemoveTable(const std::string &table_name);

  // Get a string representation of this database
  friend std::ostream& operator<<(std::ostream& os, const Database& database);

  void Lock(){
    database_mtx.lock();
  }

  void Unlock(){
    database_mtx.unlock();
  }

 private:
  std::string name;

  // tables in db
  std::vector<Table*> tables;

  std::mutex database_mtx;
};

} // End catalog namespace
} // End nstore namespace
