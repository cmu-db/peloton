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
  assert( table_name != "");
  oid_t table_oid = table->GetId();
  assert( table_oid);

  try {
    table_oid = table_oid_locator.at( table_name );
    LOG_WARN("Table(%u) already exists in database(%u) ", table_oid, database_oid);
    return false; // table already exists
  }catch (const std::out_of_range& oor)  {
    table_oid_locator.insert( std::pair<std::string,oid_t> ( table_name, table_oid ));
    table_address_locator.insert( std::pair<oid_t, storage::DataTable*> ( table_oid, table ));
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
    LOG_WARN("GetTableByPosition :: Out of range %u/%hu ", table_position, table_oid_locator.size() );
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

  if( table == nullptr ){
    LOG_WARN("GetTableByPosition :: Not exist here");
  }
  

  return table;
}


std::ostream& operator<<(std::ostream& os, const Database& database) {
  os << "=====================================================\n";
  os << "DATABASE(" << database.GetDatabaseOid() << ") : \n";

  oid_t number_of_tables = database.GetTableCount();
  std::cout << "The number of tables : " << number_of_tables  << "\n";

  for (oid_t table_itr = 0 ; table_itr < number_of_tables ; table_itr++) {
    storage::DataTable* table = database.GetTableByPosition( table_itr );
    if( table != nullptr ){
      std::cout << "Table Name : " << table->GetName() << "\n" <<  *(table->GetSchema()) << std::endl;
      if( table->ishasPrimaryKey()  ){
        printf("print primary key index \n");
        std::cout<< *(table->GetPrimaryIndex()) << std::endl;
      }
      if ( table->ishasUnique()){
        printf("print unique index \n");
        for( int i =0 ; i<  table->GetUniqueIndexCount(); i++){
          std::cout << *(table->GetUniqueIndex(i)) << std::endl;
        }
      }
      if ( table->GetIndexCount() > 0 ){
        printf("print index \n");
        for( int i =0 ; i<  table->GetIndexCount(); i++){
          std::cout << *(table->GetIndex(i)) << std::endl;
        }
      }
      if ( table->ishasReferenceTable() ){
        printf("print foreign tables \n");
        for( int i =0 ; i<  table->GetReferenceTableCount(); i++){
          peloton::storage::DataTable *temp_table = table->GetReferenceTable(i);
          const peloton::catalog::Schema* temp_our_schema = temp_table->GetSchema();
          std::cout << "table name : " << temp_table->GetName() << " " << *temp_our_schema << std::endl;
        }
      }
    }
  }

  std::cout << "The number of tables : " << number_of_tables  << "\n";


  os << "=====================================================\n";

  return os;
}

} // End storage namespace
} // End peloton namespace

