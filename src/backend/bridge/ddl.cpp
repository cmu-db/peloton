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


namespace peloton {
namespace bridge {

bool DDL::CreateTable(std::string table_name,
                      DDL_ColumnInfo* ddl_columnInfo,
                      int num_columns, 
                      int *num_of_constraints_of_each_column,
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
    ConstraintType currentConstraintType;
    std::vector<catalog::Constraint> constraint_vec;

    // TODO :: Do we need this kind of table??
    // create a table without columnInfo
    if( num_columns == 0 ){
          currentValueType = VALUE_TYPE_NULL;
          catalog::ColumnInfo *columnInfo = new catalog::ColumnInfo( currentValueType, 0, 0, "", true, true, constraint_vec);
          columnInfoVect.push_back(*columnInfo);
    }else{

      for( int column_itr = 0; column_itr < num_columns; column_itr++ ){
  
        // Set up ValueType
        switch(ddl_columnInfo[column_itr].valueType ){
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
          case 1014:
          case 1042: // char(length), blank-padded string, fixed storage length
            currentValueType = VALUE_TYPE_VARCHAR;
            ddl_columnInfo[column_itr].is_inlined = true;
            break;
            // !!! NEED TO BE UPDATED ...
          case 1015:
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
            printf("INVALID VALUE TYPE : %d \n", ddl_columnInfo[column_itr].valueType);
            break;
        }
 
        /*
         * Set up Constraints
         */
        constraint_vec.clear();
        for(int constraint_itr = 0 ; constraint_itr < num_of_constraints_of_each_column[column_itr]; constraint_itr++)
        {
          index::Index* index = nullptr;
          storage::DataTable* table = nullptr;

          // Matching the ConstraintType from Postgres to Peloton
          // TODO :: Do we need both constraint types ???
          // Make a function with followings..
          switch(ddl_columnInfo[column_itr].constraintType[constraint_itr] ){
            case CONSTR_CHECK:
              printf(" ConstraintNode->contype is CONSTR_CHECK\n");
              currentConstraintType = CONSTRAINT_TYPE_CHECK;
              break;

            case CONSTR_NOTNULL:
              printf(" ConstraintNode->contype is CONSTR_NOTNULL\n");
              currentConstraintType = CONSTRAINT_TYPE_NOTNULL;
              break;

            case CONSTR_UNIQUE:
              printf(" ConstraintNode->contype is CONSTR_UNIQUE\n");
              currentConstraintType = CONSTRAINT_TYPE_UNIQUE;
              break;

           case CONSTR_PRIMARY:
              printf(" ConstraintNode->contype is CONSTR_PRIMARY\n");
              currentConstraintType = CONSTRAINT_TYPE_PRIMARY;
              printf("conname %s \n",  ddl_columnInfo[column_itr].conname[constraint_itr]);
/*
              DDL_ColumnInfo ddl_columnInfoForKeySchema; 
              ddl_columnInfoForKeySchema.valueType = 0; 
              ddl_columnInfoForKeySchema.column_offset = 0;
              ddl_columnInfoForKeySchema.column_length = 0;
              strcpy(ddl_columnInfoForKeySchema.name, indexElem->name );
              ddl_columnInfoForKeySchema.allow_null = true;  
              ddl_columnInfoForKeySchema.is_inlined = false;
              ddl_columnInfoForKeySchema.constraintType = NULL; 
              ddl_columnInfoForKeySchema.conname = NULL; 
              column_itr_for_KeySchema++;
*/
              //CreateIndex(table_name+"primary_index", table_name, 0, true, 
              break;

            case CONSTR_FOREIGN:
              printf(" ConstraintNode->contype is CONST_FOREIGN\n");
              currentConstraintType = CONSTRAINT_TYPE_NOTNULL;
              break;

           case CONSTR_EXCLUSION:
              printf(" ConstraintNode->contype is CONSTR_EXCLUSION\n");
              currentConstraintType = CONSTRAINT_TYPE_NOTNULL;
              break;

           default:
              printf("INVALID CONSTRAINT TYPE : %d \n", ddl_columnInfo[column_itr].constraintType[constraint_itr]);
              break;
          }

          catalog::Constraint* constraint = nullptr;
          std::string tmp = ConstraintTypeToString(currentConstraintType);
          constraint = new catalog::Constraint( ddl_columnInfo[column_itr].conname[constraint_itr], currentConstraintType, NULL, NULL);

          constraint_vec.push_back(*constraint);
        }
      
        catalog::ColumnInfo *columnInfo = new catalog::ColumnInfo( currentValueType,
                                                                   ddl_columnInfo[column_itr].column_offset,
                                                                   ddl_columnInfo[column_itr].column_length,
                                                                   ddl_columnInfo[column_itr].name,
                                                                   ddl_columnInfo[column_itr].allow_null,
                                                                   ddl_columnInfo[column_itr].is_inlined,
                                                                   constraint_vec);
        // Add current columnInfo into the columnInfoVect
        columnInfoVect.push_back(*columnInfo);
      }
    }

    // Construct schema from vector of ColumnInfo
    schema = new catalog::Schema(columnInfoVect);

    // Just for debugging
    std::cout << "Print out Schema just for debugging of constraint" << std::endl;
    std::cout << *schema << std::endl;
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
                      DDL_ColumnInfo* ddl_columnInfoForKeySchema,
                      int num_columns_of_KeySchema)
{

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
  for(oid_t column_itr_for_KeySchema = 0;  column_itr_for_KeySchema < num_columns_of_KeySchema; column_itr_for_KeySchema++)
  {
    for( oid_t column_itr_for_TupleSchema = 0; column_itr_for_TupleSchema < tuple_schema->GetColumnCount(); column_itr_for_TupleSchema++)
    {
      // Get the current column info from tuple schema
      catalog::ColumnInfo colInfo = tuple_schema->GetColumnInfo(column_itr_for_TupleSchema);

      // Compare Key Schema's current column name and Tuple Schema's current column name
      if( strcmp( ddl_columnInfoForKeySchema[ column_itr_for_KeySchema].name , (colInfo.name).c_str() )== 0 )
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
 

extern "C" {
bool DDL_CreateTable(char* table_name, DDL_ColumnInfo* ddl_columnInfo, int num_columns, int* num_of_constraints_of_each_column) {
  return DDL::CreateTable(table_name, ddl_columnInfo, num_columns, num_of_constraints_of_each_column);
}
bool DDL_DropTable(unsigned int table_oid) {
  return DDL::DropTable(table_oid);
}
bool DDL_CreateIndex(char* index_name, char* table_name, int type, bool unique, DDL_ColumnInfo* ddl_columnInfoForKeySchema, int num_columns_of_KeySchema) {
  return DDL::CreateIndex(index_name, table_name, type, unique, ddl_columnInfoForKeySchema, num_columns_of_KeySchema ) ; }
}

} // namespace bridge
} // namespace peloton
