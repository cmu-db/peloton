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

    if( num_columns == 0 )
    {
          currentValueType = VALUE_TYPE_NULL;
          catalog::ColumnInfo *columnInfo = new catalog::ColumnInfo( currentValueType,
                          0, 
                          0,
                          "",
                          true,
                          true);
          // Add current columnInfo into the columnInfoVect
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
                      int type,
                      bool unique, 
                      DDL_ColumnInfo* ddl_columnInfoForTupleSchema,
                      DDL_ColumnInfo* ddl_columnInfoForKeySchema,
                      int num_columns)
{
 
  IndexType currentIndexType = INDEX_TYPE_BTREE_MULTIMAP;

/*
  // TODO :: I coudn't find any IndexType Information in IndexStmt
  switch(type){
    case:
    currentIndexType = INDEX_TYPE_BTREE_MULTIMAP; // array
    break;

    case:
    currentIndexType = INDEX_TYPE_ORDERED_MAP;   // ordered map
    break;

    default:
    printf("Something's wrong\n");
    break;
  }
*/

  // Get the database oid and table oid
  oid_t database_oid = GetCurrentDatabaseOid();
  oid_t table_oid = GetRelationOidFromRelationName(table_name.c_str());

  // Get the table pointer from manager
  storage::DataTable* table = (storage::DataTable*) catalog::Manager::GetInstance().GetLocation(database_oid, table_oid);

  // Bring the schema of table
  catalog::Schema* table_schema = table->GetSchema();

  std::cout << *table_schema << std::endl;

  // Make schema based on table_schema
  std::vector<oid_t> oid_t_vec;
  
/*
  for(oid_t column_itr = 0;  column_itr < num_columns; column_itr++)
  {
    // GetColumns and iterate it and compare with .. this.. one ..
    catalog::ColumnInfo columnInfo = table_schema->GetColumnInfo(column_itr);
    for( oid_t column_itr2 = 0; column_itr2 < table_schema->GetColumnCount(); column_itr2++)
    {
      // Look up the given column names in table_schema
      if( sddl_columnInfoForTupleSchema->name 
    }
    DDL_ColumnInfo* ddl_columnInfoForKeySchema)
}
  oid_t_vec.push_back(0);
  catalog::Schema * tuple_schema = catalog::Schema::CopySchema(table_schema, oid_t_vec);
  std::cout << *tuple_schema << std::endl;
*/

  /*
  catalog::Schema * key
  */

 
  // Create metadata and index 
  /*
  index::IndexMetadata* metadata(index_name, currentIndexType, tuple_schema, key_schema, unique);

  index::Index* index = index::IndexFactory::GetInstance(metadata);
  */

  // Add an index into the table
  // table->AddIndex(index);

  return true;
}
 

extern "C" {
bool DDL_CreateTable(char* table_name, DDL_ColumnInfo* ddl_columnInfo, int num_columns) {
  return DDL::CreateTable(table_name, ddl_columnInfo, num_columns);
}
bool DDL_DropTable(unsigned int table_oid) {
  return DDL::DropTable(table_oid);
}
bool DDL_CreateIndex(char* index_name, char* table_name, int type, bool unique, DDL_ColumnInfo* ddl_columnInfoForTupleSchema, DDL_ColumnInfo* ddl_columnInfoForKeySchema, int num_columns) {
  return DDL::CreateIndex(index_name, table_name, type, unique, ddl_columnInfoForTupleSchema, ddl_columnInfoForKeySchema, num_columns); }
}

} // namespace bridge
} // namespace nstore
