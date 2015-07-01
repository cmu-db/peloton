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

#include "postgres.h"
#include "c.h"

#include "bridge/bridge.h"
#include "nodes/parsenodes.h"

#include "backend/bridge/ddl.h"
#include "backend/catalog/catalog.h"
#include "backend/catalog/schema.h"
#include "backend/common/logger.h"
#include "backend/common/types.h"
#include "backend/index/index.h"
#include "backend/index/index_factory.h"
#include "backend/storage/backend_vm.h"
#include "backend/storage/table_factory.h"

#include <cassert>

namespace peloton {
namespace bridge {

//===--------------------------------------------------------------------===//
// Function definitions
//===--------------------------------------------------------------------===//

/**
 * @brief Create table.
 * @param table_name Table name
 * @param column_info Information about the columns
 * @param num_columns Number of columns in the table
 * @param schema Schema for the table
 * @return true if we created a table, false otherwise
 */
bool DDL::CreateTable(std::string table_name,
                      DDL_ColumnInfo* schema,
                      int num_columns,
                      int *constraints_per_column) {
  assert(num_columns >= 0);
  assert(schema);

  // Check db oid
  Oid database_oid = GetCurrentDatabaseOid();
  if(database_oid == InvalidOid)
    return false;

  // Construct schema with column information
  std::vector<catalog::ColumnInfo> column_infos;
  std::vector<catalog::Constraint> constraint_vec;

  ValueType column_type;
  ConstraintType currentConstraintType;

  if(num_columns > 0) {
    // Go over each column to get its information
    for( int column_itr = 0; column_itr < num_columns; column_itr++ ){

      switch(schema[column_itr].valueType){

        // Could not find yet corresponding types in Postgres.
        // FIXME: change the hardcoded constants to enum type

        //===--------------------------------------------------------------------===//
        // Column Type Information
        //===--------------------------------------------------------------------===//

        /* BOOLEAN */
        case 16: // boolean, 'true'/'false'
          column_type = VALUE_TYPE_BOOLEAN;
          break;

          /* INTEGER */
        case 21: // -32 thousand to 32 thousand, 2-byte storage
          column_type = VALUE_TYPE_SMALLINT;
          schema[column_itr].is_inlined = true;
          break;
        case 23: // -2 billion to 2 billion integer, 4-byte storage
          column_type = VALUE_TYPE_INTEGER;
          schema[column_itr].is_inlined = true;
          break;
        case 20: // ~18 digit integer, 8-byte storage
          column_type = VALUE_TYPE_BIGINT;
          schema[column_itr].is_inlined = true;
          break;

          /* DOUBLE */
        case 701: // double-precision floating point number, 8-byte storage
          column_type = VALUE_TYPE_DOUBLE;
          schema[column_itr].is_inlined = true;
          break;

          /* CHAR */
        case 1014:
        case 1042: // char(length), blank-padded string, fixed storage length
          column_type = VALUE_TYPE_VARCHAR;
          schema[column_itr].is_inlined = true;
          break;

          // !!! NEED TO BE UPDATED ...
        case 1015:
        case 1043: // varchar(length), non-blank-padded string, variable storage length;
          column_type = VALUE_TYPE_VARCHAR;
          schema[column_itr].is_inlined = true;
          break;

          /* TIMESTAMPS */
        case 1114: // date and time
        case 1184: // date and time with time zone
          column_type = VALUE_TYPE_TIMESTAMP;
          schema[column_itr].is_inlined = true;
          break;

          /* DECIMAL */
        case 1700: // numeric(precision, decimal), arbitrary precision number
          column_type = VALUE_TYPE_DECIMAL;
          break;

          /* INVALID VALUE TYPE */
        default:
          column_type = VALUE_TYPE_INVALID;
          LOG_ERROR("Invalid column type : %d \n", schema[column_itr].valueType);
          return false;
          break;
      }

      //===--------------------------------------------------------------------===//
      // Column Constraint Information
      //===--------------------------------------------------------------------===//

      constraint_vec.clear();

      // Create constraints if needed
      if(constraints_per_column != NULL) {

        for(int constraint_itr = 0 ; constraint_itr < constraints_per_column[column_itr]; constraint_itr++)
        {
          // Matching the ConstraintType from Postgres to Peloton
          // TODO :: Do we need both constraint types ???
          // Make a function with followings..
          // TODO :: Failed :: CREATE TABLE MULTI_COLUMNS_PRIMARY_KEY_EXAMPLE ( a integer, b integer, PRIMARY KEY (a, b) );

          switch(schema[column_itr].constraintType[constraint_itr] ){
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
              //key_column_info_vec.push_back(ddl
              //num_of_PrimaryKeys++;
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
              printf("INVALID CONSTRAINT TYPE : %d \n", schema[column_itr].constraintType[constraint_itr]);
              break;
          }

          catalog::Constraint* constraint = new catalog::Constraint( currentConstraintType,
                                                                     schema[column_itr].conname[constraint_itr]);

          constraint_vec.push_back(*constraint);
        }

      }

      catalog::ColumnInfo column_info(column_type,
                                      schema[column_itr].column_offset,
                                      schema[column_itr].column_length,
                                      schema[column_itr].name,
                                      schema[column_itr].allow_null,
                                      schema[column_itr].is_inlined,
                                      constraint_vec);

      // Add current column to column info vector
      column_infos.push_back(column_info);
    }
  }
  // SPECIAL CASE:: No columns in table
  else {
    column_type = VALUE_TYPE_NULL;
    catalog::ColumnInfo column_info(column_type, 0, 0, "",
                                    true, true, constraint_vec);
    column_infos.push_back(column_info);
  }

  // Construct our schema from vector of ColumnInfo
  auto our_schema = new catalog::Schema(column_infos);

  // FIXME: Construct table backend
  storage::VMBackend *backend = new storage::VMBackend();

  // Build a table from schema
  storage::DataTable *table = storage::TableFactory::GetDataTable(database_oid, our_schema, table_name);

  if(table != nullptr) {
    LOG_INFO("Created table : %s\n", table_name.c_str());
    return true;
  }

  return false;
}

/**
 * @brief Drop table.
 * @param table_oid Table id.
 * @return true if we dropped the table, false otherwise
 */
bool DDL::DropTable(Oid table_oid) {

  oid_t database_oid = GetCurrentDatabaseOid();

  if(database_oid == InvalidOid || table_oid == InvalidOid) {
    LOG_WARN("Could not drop table :: db oid : %u table oid : %u", database_oid, table_oid);
    return false;
  }

  bool status = storage::TableFactory::DropDataTable(database_oid, table_oid);
  if(status == true) {
    LOG_INFO("Dropped table with oid : %u\n", table_oid);
    return true;
  }

  return false;
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
bool DDL::CreateIndex(std::string index_name,
                      std::string table_name,
                      int index_type,
                      bool unique_keys,
                      char **key_column_names,
                      int num_columns_in_key) {

  assert( index_name != "" || table_name != "" || key_column_names != NULL || num_columns_in_key > 0 );

  // NOTE: We currently only support btree as our index implementation
  // FIXME: Support other types based on "type" argument
  IndexType our_index_type = INDEX_TYPE_BTREE_MULTIMAP;

  // Get the database oid and table oid
  oid_t database_oid = GetCurrentDatabaseOid();
  oid_t table_oid = GetRelationOid(table_name.c_str());

  // Get the table location from manager
  auto table = catalog::Manager::GetInstance().GetLocation(database_oid, table_oid);
  storage::DataTable* data_table = (storage::DataTable*) table;
  auto tuple_schema = data_table->GetSchema();

  // Construct key schema
  std::vector<oid_t> key_columns;

  // Based on the key column info, get the oid of the given 'key' columns in the tuple schema
  for(oid_t key_schema_column_itr = 0;  key_schema_column_itr < num_columns_in_key; key_schema_column_itr++) {
    for( oid_t tuple_schema_column_itr = 0; tuple_schema_column_itr < tuple_schema->GetColumnCount();
        tuple_schema_column_itr++){

      // Get the current column info from tuple schema
      catalog::ColumnInfo column_info = tuple_schema->GetColumnInfo(tuple_schema_column_itr);

      // Compare Key Schema's current column name and Tuple Schema's current column name
      if(strcmp(key_column_names[key_schema_column_itr], (column_info.name).c_str() )== 0 )
        key_columns.push_back(tuple_schema_column_itr);
    }
  }

  auto key_schema = catalog::Schema::CopySchema(tuple_schema, key_columns);

  // Create index metadata and physical index
  index::IndexMetadata* metadata = new index::IndexMetadata(index_name, our_index_type,
                                                            tuple_schema, key_schema,
                                                            unique_keys);
  index::Index* index = index::IndexFactory::GetInstance(metadata);

  // Record the built index in the table
  data_table->AddIndex(index);

  return true;
}

} // namespace bridge
} // namespace peloton
