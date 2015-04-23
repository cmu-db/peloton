/*-------------------------------------------------------------------------
 *
 * catalog.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/catalog/catalog.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "common/types.h"

namespace nstore {
namespace catalog {

Catalog& Catalog::GetInstance() {
  static Catalog catalog;
  return catalog;
}

bool Catalog::AddDatabase(Database* db) {
  if(std::find(databases.begin(), databases.end(), db) != databases.end())
    return false;
  databases.push_back(db);
  return true;
}

Database* Catalog::GetDatabase(const std::string &db_name) const {
  for(auto db : databases)
    if(db->GetName() == db_name)
      return db;
  return nullptr;
}

bool Catalog::RemoveDatabase(const std::string &db_name) {
  for(auto itr = databases.begin(); itr != databases.end() ; itr++)
    if((*itr)->GetName() == db_name) {
      databases.erase(itr);
      return true;
    }
  return false;
}

} // End catalog namespace
} // End nstore namespace
