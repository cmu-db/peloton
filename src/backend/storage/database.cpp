/*-------------------------------------------------------------------------
 *
 * database.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/storage/table.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "backend/storage/database.h"

#include <sstream>

namespace peloton {
namespace storage {

Database* Database::GetDatabaseById(oid_t database_oid){
  Database* db_address;

  try {
    db_address = database_address_locator.at( database_oid );
  }catch (const std::out_of_range& oor) {
    Database* db = new Database( database_oid ); 
    db_address = db;
    database_address_locator.insert( std::pair<oid_t,Database*>( database_oid,
          db_address ));
  }
  return db_address;
}

bool Database::AddTable(storage::DataTable* table){

  std::string table_name = table->GetName();
  oid_t table_oid = table->GetId();

  try {
    table_oid = table_oid_locator.at( table_name );
    return false; // table already exists
  }catch (const std::out_of_range& oor)  {
    table_oid_locator.insert( std::pair<std::string,oid_t>
        ( table_name, table_oid ));
    table_address_locator.insert( std::pair<oid_t, storage::DataTable*>
        ( table_oid, table ));
  }

  return true;
}

storage::DataTable* Database::GetTableByName(const std::string table_name) const{

  storage::DataTable* table = nullptr;
  oid_t table_oid;
  try {
    table_oid = table_oid_locator.at( table_name );
  } catch  (const std::out_of_range& oor) {
    return table; // return nullptr
  }
  table = table_address_locator.at( table_oid );

  return table;
}
storage::DataTable* Database::GetTableById(const oid_t table_oid) const{

  storage::DataTable* table = nullptr;
  try {
    table = table_address_locator.at( table_oid );
  } catch  (const std::out_of_range& oor) {
    table = nullptr;
  }
  return table;
}

storage::DataTable* Database::GetTableByPosition(const oid_t table_position) const{
  storage::DataTable* table = nullptr;
  oid_t curr_position = 0;

  if( table_position < 0 || table_position >= table_oid_locator.size() ){
    LOG_WARN("GetTableByPosition :: Out of range %u/%lu ", table_position, table_oid_locator.size() );
    return table; // out of range
  }

  std::for_each(begin(table_address_locator), end(table_address_locator),
      [&] (const std::pair<oid_t, storage::DataTable*>& curr_table){

      if( curr_position == table_position ){
        table =  curr_table.second; // find it
      }
      else
        curr_position++;
      });

  if( table == nullptr )
    LOG_WARN("GetTableByPosition :: Not exist here curr %u size %lu table posi %u ", curr_position,  table_oid_locator.size(),  table_position );

  return table;
}


std::ostream& operator<<(std::ostream& os, const Database& database) {
  os << "=====================================================\n";
  os << "DATABASE(" << database.GetDatabaseOid() << ") : \n";

  oid_t number_of_tables = database.GetTableCount();

  for (oid_t table_itr = 0 ; table_itr < number_of_tables ; table_itr++) {
    storage::DataTable* table = database.GetTableByPosition( table_itr );
    if( table != nullptr )
      std::cout << *(table->GetSchema()) << std::endl;
  }

  std::cout << "The number of tables : " << number_of_tables  << "\n";


  os << "=====================================================\n";

  return os;
}

} // End storage namespace
} // End peloton namespace

