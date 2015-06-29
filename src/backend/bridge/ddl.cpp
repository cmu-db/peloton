/*-------------------------------------------------------------------------
 *
 * dll.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/bridge/ddl.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "bridge/bridge.h"

#include "backend/bridge/ddl.h"
#include "backend/catalog/catalog.h"
#include "backend/catalog/schema.h"
#include "backend/common/types.h"
#include "backend/index/index.h"
#include "backend/index/index_factory.h"
#include "backend/storage/backend_vm.h"
#include "backend/storage/table_factory.h"

#include <cassert>

namespace peloton {
namespace bridge {

/**
 * @brief Create table.
 * @param table_name Table name
 * @param column_info Information about the columns
 * @param num_columns Number of columns in the table
 * @param schema Schema for the table
 * @return true if we created a table, false otherwise
 */
bool DDL::CreateTable(std::string table_name,
                      ColumnInfo *column_info,
                      int num_columns, 
                      catalog::Schema* schema) {

  if( ( num_columns > 0 && column_info == NULL) && schema == NULL ) 
    return false;

  oid_t db_oid = GetCurrentDatabaseOid();
  if( db_oid == 0 )
    return false;

  // Construct schema with ddl_columnInfo
  if( schema == NULL ){
    std::vector<catalog::ColumnInfo> columnInfoVect;
    ValueType currentValueType;

    // TODO :: Do we need this kind of table??
    // create a table without columnInfo
    if( num_columns == 0 )
    {
          currentValueType = VALUE_TYPE_NULL;
          catalog::ColumnInfo *columnInfo = new catalog::ColumnInfo( currentValueType, 0, 0, "", true, true);
          columnInfoVect.push_back(*columnInfo);
    }else
    {
      for( int column_itr = 0; column_itr < num_columns; column_itr++ ){
  
  
        switch(column_info[column_itr].type ){
          // Could not find yet corresponding types in Postgres...
          // Also - check below types again to make sure..
          // TODO :: change the numbers to enum type
  
          /* BOOLEAN */
          case 16: // boolean, 'true'/'false'
            currentValueType = VALUE_TYPE_BOOLEAN;
            break;
  
            /* INTEGER */
          case 21: // -32 thousand to 32 thousand, 2-byte storage
            currentValueType = VALUE_TYPE_SMALLINT;
            column_info[column_itr].is_inlined = true;
            break;
          case 23: // -2 billion to 2 billion integer, 4-byte storage
            currentValueType = VALUE_TYPE_INTEGER;
            column_info[column_itr].is_inlined = true;
            break;
          case 20: // ~18 digit integer, 8-byte storage
            currentValueType = VALUE_TYPE_BIGINT;
            column_info[column_itr].is_inlined = true;
            break;
  
            /* DOUBLE */
          case 701: // double-precision floating point number, 8-byte storage
            currentValueType = VALUE_TYPE_DOUBLE;
            column_info[column_itr].is_inlined = true;
            break;
  
            /* CHAR */
          case 1014:
          case 1042: // char(length), blank-padded string, fixed storage length
            currentValueType = VALUE_TYPE_VARCHAR;
            column_info[column_itr].is_inlined = true;
            break;
            // !!! NEED TO BE UPDATED ...
          case 1015:
          case 1043: // varchar(length), non-blank-padded string, variable storage length;
            currentValueType = VALUE_TYPE_VARCHAR;
            column_info[column_itr].is_inlined = true;
            break;
  
            /* TIMESTAMPS */
          case 1114: // date and time
          case 1184: // date and time with time zone
            currentValueType = VALUE_TYPE_TIMESTAMP;
            column_info[column_itr].is_inlined = true;
            break;
  
            /* DECIMAL */
          case 1700: // numeric(precision, decimal), arbitrary precision number
            currentValueType = VALUE_TYPE_DECIMAL;
            break;
  
            /* INVALID VALUE TYPE */
          default:
            currentValueType = VALUE_TYPE_INVALID;
            printf("INVALID VALUE TYPE : %d \n", column_info[column_itr].type);
            break;
        }
  
        catalog::ColumnInfo *columnInfo = new catalog::ColumnInfo( currentValueType,
                                                                   column_info[column_itr].column_offset,
                                                                   column_info[column_itr].column_length,
                                                                   column_info[column_itr].name,
                                                                   column_info[column_itr].allow_null,
                                                                   column_info[column_itr].is_inlined );
        // Add current columnInfo into the columnInfoVect
        columnInfoVect.push_back(*columnInfo);
      }
    }

    // Construct schema from vector of ColumnInfo
    schema = new catalog::Schema(columnInfoVect);
  }

  // Construct backend
  storage::VMBackend *vmbackend = new storage::VMBackend;

  // Create a table from schema
  storage::DataTable *table;

  table =  storage::TableFactory::GetDataTable(db_oid, schema, table_name);

  // Store the schema for "CreateIndex"
  table->SetSchema(schema);

  //LOG_WARN("Created table : %s \n", table_name);
  return true;
}

/**
 * @brief Drop table.
 * @param table_oid Table id.
 * @return true if we dropped the table, false otherwise
 */
bool DDL::DropTable(unsigned int table_oid) {

  oid_t db_oid = GetCurrentDatabaseOid();
  if( db_oid == 0 || table_oid == 0 )
    return false;

  bool ret = storage::TableFactory::DropDataTable(db_oid, table_oid);
  if( !ret )
    return false;

  return true;
}

/**
 * @brief Create index.
 * @param index_name Index name
 * @param table_name Table name
 * @param type Type of the index
 * @param unique Index is unique or not ?
 * @param
 * @param column_info Information about the columns
 * @param num_columns Number of columns in the table
 * @param schema Schema for the table
 *
 * @return true if we dropped the table, false otherwise
 */
bool DDL::CreateIndex(std::string index_name, std::string table_name,
                      int type, bool unique,
                      ColumnInfo* key_column_info,
                      int num_columns_in_key) {

  /* Currently, we use only btree as an index method */
  IndexType currentIndexType = INDEX_TYPE_BTREE_MULTIMAP;
//  switch(type)
//  {
//    case 403: /* BTREE_AM_OID*/
//    currentIndexType = INDEX_TYPE_BTREE_MULTIMAP; // array
//      break;
//    case 405: /* HASH_AM_OID*/
//      break;
//    case 783: /* GIST_AM_OID*/
//      break;
//    case 2742: /* GINH_AM_OID*/
//      break;
//    case 4000: /* SPGIST_AM_OID*/
//      break;
//    case 3580: /* BRIN_AM_OID*/
//      break;
//    default:
//    currentIndexType = INDEX_TYPE_ORDERED_MAP;   // ordered map
//    currentIndexType = INDEX_TYPE_INVALID; 
//      break;
//  }

  // Get the database oid and table oid
  oid_t database_oid = GetCurrentDatabaseOid();
  oid_t table_oid = GetRelationOidFromRelationName(table_name.c_str());

  // Get the table location from manager
  storage::DataTable* table = (storage::DataTable*) catalog::Manager::GetInstance().GetLocation(database_oid, table_oid);

  // Bring the tuple schema from the table
  auto tuple_schema = table->GetSchema();

  // Print out tuple_schema just for debugging
  //std::cout << *tuple_schema << std::endl;

  // Make a vector to store selected column's oids
  std::vector<oid_t> selected_oids_for_KeySchema;

  // Based on the ColumnInfo of KeySchema, find out the given 'key' columns in the tuple schema and store it's oid 
  for(oid_t column_itr_for_KeySchema = 0;  column_itr_for_KeySchema < num_columns_in_key; column_itr_for_KeySchema++)
  {
    for( oid_t column_itr_for_TupleSchema = 0; column_itr_for_TupleSchema < tuple_schema->GetColumnCount(); column_itr_for_TupleSchema++)
    {
      // Get the current column info from tuple schema
      catalog::ColumnInfo colInfo = tuple_schema->GetColumnInfo(column_itr_for_TupleSchema);

      // Compare Key Schema's current column name and Tuple Schema's current column name
      if( strcmp( key_column_info[ column_itr_for_KeySchema].name , (colInfo.name).c_str() )== 0 )
        selected_oids_for_KeySchema.push_back(column_itr_for_TupleSchema);
    }
  }

  catalog::Schema * key_schema = catalog::Schema::CopySchema(tuple_schema, selected_oids_for_KeySchema);
  // TODO :: REMOVE :: Print out key schema just for debugging
  //std::cout << *key_schema << std::endl;

  // Create metadata and index 
  index::IndexMetadata* metadata = new index::IndexMetadata(index_name, currentIndexType, tuple_schema, key_schema, unique);
  index::Index* index = index::IndexFactory::GetInstance(metadata);

  // Add an index into the table
 table->AddIndex(index);

  return true;
}
 
/* ------------------------------------------------------------
 * C-style function declarations
 * ------------------------------------------------------------
 */

extern "C" {

bool DDLCreateTable(char* table_name,
                     ColumnInfo* column_info,
                     int num_columns) {
  return DDL::CreateTable(table_name, column_info, num_columns);
}

bool DDLDropTable(unsigned int table_oid) {
  return DDL::DropTable(table_oid);
}

bool DDLCreateIndex(char* index_name, char* table_name,
                     int type, bool unique,
                     ColumnInfo* key_column_info,
                     int num_columns_in_key) {

  return DDL::CreateIndex(index_name, table_name,
                          type, unique,
                          key_column_info,
                          num_columns_in_key ) ;

}

}

} // namespace bridge
} // namespace peloton
