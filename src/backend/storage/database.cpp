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
#include "backend/storage/table_factory.h"

#include <sstream>

namespace peloton {
namespace storage {


Database* Database::GetDatabaseById( oid_t database_oid ){
  Database* db_address;

  try {
    db_address = database_oid_to_address.at( database_oid );
  }catch (const std::out_of_range& oor) {
    Database* db = new Database( database_oid ); 
    db_address = db;
    database_oid_to_address.insert( std::pair<oid_t,Database*>( database_oid,
                                                                db_address ));
  }
  return db_address;
}

bool Database::DeleteDatabaseById( oid_t database_oid ){

  try {
    Database* db_address = database_oid_to_address.at( database_oid );
    delete db_address;
  }catch (const std::out_of_range& oor) {
    LOG_WARN("Not exist database(%u)", database_oid);
    return false;
  }
  return true;
}

bool Database::AddTable( storage::DataTable* table ){

  std::string table_name = table->GetName();
  assert( table_name != "");
  oid_t table_oid = table->GetId();
  assert( table_oid);

  try {
    table_oid = table_name_to_oid.at( table_name );
    LOG_WARN("Table(%u) already exists in database(%u) ", table_oid, database_oid);
    return false; // table already exists
  }catch (const std::out_of_range& oor)  {
    table_name_to_oid.insert( std::pair<std::string,oid_t> ( table_name, table_oid ));
    table_oid_to_name.insert( std::pair<oid_t,std::string> ( table_oid, table_name ));
    table_oid_to_address.insert( std::pair<oid_t, storage::DataTable*> ( table_oid, table ));
  }

  return true;
}


bool Database::DeleteTableById( oid_t table_oid )
{
  bool status;
  std::string table_name;

  try {
    table_name = table_oid_to_name.at( table_oid );
  } catch ( const std::out_of_range & oor ) {
    LOG_WARN("Table(%u) doesn't exist in database(%u) ", table_oid, database_oid );
    return false;
  }

  status = table_name_to_oid.unsafe_erase( table_name );
  if( status == false ){ // TODO :: SHOULD BE REMOVED NOW, IT'S JUST FOR DEBUGGING
    LOG_WARN("Failed to erase table(%u) in database(%u) ", table_oid, database_oid );
    return status;
  }

  status = table_oid_to_name.unsafe_erase( table_oid );
  if( status == false ){ // TODO :: SHOULD BE REMOVED NOW, IT'S JUST FOR DEBUGGING
    LOG_WARN("Failed to erase table(%u) in database(%u) ", table_oid, database_oid );
    return status;
  }

  status = table_oid_to_address.unsafe_erase( table_oid );
  if( status == false ){ // TODO :: SHOULD BE REMOVED NOW, IT'S JUST FOR DEBUGGING
    LOG_WARN("Failed to erase table(%u) in database(%u) ", table_oid, database_oid );
    return status;
  }

  status = storage::TableFactory::DropDataTable(database_oid, table_oid);
  if(status == false) {
    LOG_WARN("Failed to erase table(%u) in DropDataTable", table_oid );
    return status;
  }
  
  return status;
}

bool Database::DeleteTableByName( std::string table_name )
{
  bool status;
  oid_t table_oid;

  try {
    table_oid = table_name_to_oid.at( table_name );
  } catch ( const std::out_of_range & oor ) {
    LOG_WARN("Table(%s) doesn't exist in database(%u) ", table_name.c_str(), database_oid );
    return false;
  }
  
  status = DeleteTableById( table_oid );
  return status;
}

bool Database::DeleteAllTables()
{

  try {
    std::for_each(begin(table_name_to_oid), end(table_name_to_oid),
        [&] (const std::pair<std::string, oid_t>& curr_table){

        DeleteTableById( curr_table.second );

        });
  } catch ( const std::out_of_range & oor ) {
    LOG_WARN("Failed to delete all tables in database(%u) ", database_oid );
    return false;
  }

  return true;
}

storage::DataTable* Database::GetTableByName( const std::string table_name ) const{

  storage::DataTable* table = nullptr;
  oid_t table_oid;
  try {
    table_oid = table_name_to_oid.at( table_name );
  } catch  (const std::out_of_range& oor) {
    return table; // return nullptr
  }
  table = table_oid_to_address.at( table_oid );

  return table;
}
storage::DataTable* Database::GetTableById( const oid_t table_oid ) const{

  storage::DataTable* table = nullptr;
  try {
    table = table_oid_to_address.at( table_oid );
  } catch  (const std::out_of_range& oor) {
    table = nullptr;
  }
  return table;
}

storage::DataTable* Database::GetTableByOffset( const oid_t table_offset ) const{
  storage::DataTable* table = nullptr;
  oid_t curr_offset = 0;

  std::for_each(begin(table_oid_to_address), end(table_oid_to_address),
      [&] (const std::pair<oid_t, storage::DataTable*>& curr_table){

      if( curr_offset == table_offset ){
        table =  curr_table.second; // find it
      }

      curr_offset++;

      });

  if( table == nullptr ){
    LOG_WARN("GetTableByOffset :: Not exist here");
  }

  return table;
}

oid_t Database::GetTableIdByName( const std::string table_name ) const{

  oid_t table_oid = INVALID_OID;

  try {
    table_oid = table_name_to_oid.at( table_name );
  } catch  (const std::out_of_range& oor) {
    return table_oid ; 
  }

  return table_oid;
}

std::string Database::GetTableNameById( const oid_t table_oid ) const{

  std::string table_name = "";

  try {
    table_name = table_oid_to_name.at( table_oid );
  } catch  (const std::out_of_range& oor) {
    return table_name ; 
  }

  return table_name;
}


std::ostream& operator<<( std::ostream& os, const Database& database ) {
  os << "=====================================================\n";
  os << "DATABASE(" << database.GetDatabaseOid() << ") : \n";

  oid_t number_of_tables = database.GetTableCount();
  std::cout << "The number of tables : " << number_of_tables  << "\n";

  for (oid_t table_itr = 0 ; table_itr < number_of_tables ; table_itr++) {
    storage::DataTable* table = database.GetTableByOffset( table_itr );
    if( table != nullptr ){
      std::cout << "(" <<table_itr+1 <<"/" << number_of_tables<< ")Table Name : " << table->GetName() << "\n" <<  *(table->GetSchema()) << std::endl;

      std::cout << "index count : " <<  table->GetIndexCount() << std::endl;
      if ( table->GetIndexCount() > 0 ){
        for( int i =0 ; i<  table->GetIndexCount(); i++){
           index::Index* index = table->GetIndex(i);

           switch( index->GetIndexType() ){
             case INDEX_TYPE_PRIMARY_KEY:
               printf("print primary key index \n");
               break;
             case INDEX_TYPE_UNIQUE:
               printf("print unique index \n");
               break;
             default:
               printf("print index \n");
               break;
          }
           std::cout << *index << std::endl;
        }
      }

     if ( table->HasReferenceTable() ){
        printf("print foreign tables \n");
        for( int i =0 ; i<  table->GetForeignKeyCount(); i++){
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

