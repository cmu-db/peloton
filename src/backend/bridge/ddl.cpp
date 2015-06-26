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

#include "backend/bridge/ddl.h"


namespace nstore {
namespace bridge {

bool DDL::CreateTable(std::string table_name,
                      DDL_ColumnInfo* ddl_columnInfo,
                      int num_columns, 
                      catalog::Schema* schema = NULL){
  if( ( num_columns > 0 && ddl_columnInfo == NULL) && schema == NULL ) 
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
  
  
        switch(ddl_columnInfo[column_itr].type ){
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
            ddl_columnInfo[column_itr].is_inlined = true;
            break;
          case 23: // -2 billion to 2 billion integer, 4-byte storage
            currentValueType = VALUE_TYPE_INTEGER;
            ddl_columnInfo[column_itr].is_inlined = true;
            break;
          case 20: // ~18 digit integer, 8-byte storage
            currentValueType = VALUE_TYPE_BIGINT;
            ddl_columnInfo[column_itr].is_inlined = true;
            break;
  
            /* DOUBLE */
          case 701: // double-precision floating point number, 8-byte storage
            currentValueType = VALUE_TYPE_DOUBLE;
            ddl_columnInfo[column_itr].is_inlined = true;
            break;
  
            /* CHAR */
          case 1042: // char(length), blank-padded string, fixed storage length
            currentValueType = VALUE_TYPE_VARCHAR;
            ddl_columnInfo[column_itr].is_inlined = true;
            break;
            // !!! NEED TO BE UPDATED ...
          case 1043: // varchar(length), non-blank-padded string, variable storage length;
            currentValueType = VALUE_TYPE_VARCHAR;
            ddl_columnInfo[column_itr].is_inlined = true;
            break;
  
            /* TIMESTAMPS */
          case 1114: // date and time
          case 1184: // date and time with time zone
            currentValueType = VALUE_TYPE_TIMESTAMP;
            ddl_columnInfo[column_itr].is_inlined = true;
            break;
  
            /* DECIMAL */
          case 1700: // numeric(precision, decimal), arbitrary precision number
            currentValueType = VALUE_TYPE_DECIMAL;
            break;
  
            /* INVALID VALUE TYPE */
          default:
            currentValueType = VALUE_TYPE_INVALID;
            printf("INVALID VALUE TYPE : %d \n", ddl_columnInfo[column_itr].type);
            break;
        }
  
        catalog::ColumnInfo *columnInfo = new catalog::ColumnInfo( currentValueType,
                                                                   ddl_columnInfo[column_itr].column_offset,
                                                                   ddl_columnInfo[column_itr].column_length,
                                                                   ddl_columnInfo[column_itr].name,
                                                                   ddl_columnInfo[column_itr].allow_null,
                                                                   ddl_columnInfo[column_itr].is_inlined );
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

bool DDL::DropTable(unsigned int table_oid)
{
  oid_t db_oid = GetCurrentDatabaseOid();
  if( db_oid == 0 || table_oid == 0 )
    return false;

  bool ret = storage::TableFactory::DropDataTable(db_oid, table_oid);
  if( !ret )
    return false;

  return true;
}

//TODO :: 
bool DDL::CreateIndex(std::string index_name,
                      std::string table_name,
                      std::string accessMethod,
                      bool unique, 
                      DDL_ColumnInfo* ddl_columnInfoForTupleSchema,
                      DDL_ColumnInfo* ddl_columnInfoForKeySchema,
                      int num_columns_of_TupleSchema,
                      int num_columns_of_KeySchema)
{
  IndexType currentIndexType = INDEX_TYPE_BTREE_MULTIMAP;

  // TODO :: Check
  if( strcmp( accessMethod.c_str(), "btree") == 0 )
    currentIndexType = INDEX_TYPE_BTREE_MULTIMAP; // array
  else if( strcmp( accessMethod.c_str(), "map") == 0 )
    currentIndexType = INDEX_TYPE_ORDERED_MAP;   // ordered map
  else
    currentIndexType = INDEX_TYPE_INVALID; 
  

  // Get the database oid and table oid
  oid_t database_oid = GetCurrentDatabaseOid();
  oid_t table_oid = GetRelationOidFromRelationName(table_name.c_str());

  // Get the table location from manager
  storage::DataTable* table = (storage::DataTable*) catalog::Manager::GetInstance().GetLocation(database_oid, table_oid);

  // Bring the schema of table
  catalog::Schema* table_schema = table->GetSchema();

  // Print out table_schema just for debugging
  std::cout << *table_schema << std::endl;

  // To store selected column's oid into the vector
  std::vector<oid_t> selected_oids_for_TupleSchema;
  std::vector<oid_t> selected_oids_for_KeySchema;

  // Based on ColumnInfo for TupleSchema, find out the given column names in the table schema and store it's column oid 
  for(oid_t column_itr_for_TupleSchema = 0;  column_itr_for_TupleSchema < num_columns_of_TupleSchema; column_itr_for_TupleSchema++)
  {
    for( oid_t column_itr_for_TableSchema = 0; column_itr_for_TableSchema < table_schema->GetColumnCount(); column_itr_for_TableSchema++)
    {
      // Get the current column info from table schema
      catalog::ColumnInfo colInfo = table_schema->GetColumnInfo(column_itr_for_TableSchema);

      // compare it with the given column name based on columnInfo for Tuple Schema
      if( strcmp( ddl_columnInfoForTupleSchema[ column_itr_for_TupleSchema].name , (colInfo.name).c_str() )== 0 )
        selected_oids_for_TupleSchema.push_back(column_itr_for_TableSchema);
    }
  }

  // Print out tuple schema just for debugging
  catalog::Schema * tuple_schema = catalog::Schema::CopySchema(table_schema, selected_oids_for_TupleSchema);
  std::cout << *tuple_schema << std::endl;


  // Do the same thing with KeySchema 
  // Based on ColumnInfo for KeySchema, find out the given column names in the table schema and store it's column oid 
  for(oid_t column_itr_for_KeySchema = 0;  column_itr_for_KeySchema < num_columns_of_KeySchema; column_itr_for_KeySchema++)
  {
    for( oid_t column_itr_for_TableSchema = 0; column_itr_for_TableSchema < table_schema->GetColumnCount(); column_itr_for_TableSchema++)
    {
      // Get the current column info from table schema
      catalog::ColumnInfo colInfo = table_schema->GetColumnInfo(column_itr_for_TableSchema);

      // compare it with the given column name based on columnInfo for Tuple Schema
      if( strcmp( ddl_columnInfoForKeySchema[ column_itr_for_KeySchema].name , (colInfo.name).c_str() )== 0 )
        selected_oids_for_KeySchema.push_back(column_itr_for_TableSchema);
    }
  }

  // Print out key schema just for debugging
  catalog::Schema * key_schema = catalog::Schema::CopySchema(table_schema, selected_oids_for_KeySchema);
  std::cout << *key_schema << std::endl;

  // Create metadata and index 
  index::IndexMetadata* metadata = new index::IndexMetadata(index_name, currentIndexType, tuple_schema, key_schema, unique);
  index::Index* index = index::IndexFactory::GetInstance(metadata);

  // Add an index into the table
   table->AddIndex(index);

  return true;
}
 

extern "C" {
bool DDL_CreateTable(char* table_name, DDL_ColumnInfo* ddl_columnInfo, int num_columns) {
  return DDL::CreateTable(table_name, ddl_columnInfo, num_columns);
}
bool DDL_DropTable(unsigned int table_oid) {
  return DDL::DropTable(table_oid);
}
bool DDL_CreateIndex(char* index_name, char* table_name, char* accessMethod, bool unique, DDL_ColumnInfo* ddl_columnInfoForTupleSchema, DDL_ColumnInfo* ddl_columnInfoForKeySchema, int num_columns, int num_columns2) {
  return DDL::CreateIndex(index_name, table_name, accessMethod, unique, ddl_columnInfoForTupleSchema, ddl_columnInfoForKeySchema, num_columns, num_columns2); }
}

} // namespace bridge
} // namespace nstore
