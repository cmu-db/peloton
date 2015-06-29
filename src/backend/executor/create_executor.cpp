/*-------------------------------------------------------------------------
 *
 * create_executor.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/executor/create_executor.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "backend/catalog/catalog.h"
#include "backend/catalog/database.h"
#include "backend/common/logger.h"
#include "backend/common/types.h"
#include "backend/executor/create_executor.h"
#include "backend/index/index_factory.h"
#include "backend/storage/data_table.h"
#include "backend/storage/table_factory.h"

#include "bridge/bridge.h"

#include <cassert>
#include <algorithm>

namespace peloton {
namespace executor {

// TODO: Fix function
bool CreateExecutor::Execute(std::string name, CreateType createType) {

  // FIXME: Get default db
  catalog::Database* db = catalog::Catalog::GetInstance().GetDatabase(DEFAULT_DB_NAME);
  assert(db);

  bool ret = false;
  switch(createType) {
    case CREATE_TYPE_DB:
      ret = CreateDatabase(name);
      break;
//   case CREATE_TYPE_TABLE:
//    ret = CreateTable(db, name);
//      break;
//    case CREATE_TYPE_INDEX:
//     ret = CreateIndex(db, index_name, table_name, index_attrs, unique);
//      break;
    case CREATE_TYPE_CONSTRAINT:
      ret = CreateConstraint(db, name);
      break;
    default:
      // FIXME: This needs to use our logging infrastructure
      std::cout << "Unknown create statement type : " << createType << "\n";
  } // SWITCH

  return (ret);
}


// TODO :: Fix or Remove
bool CreateExecutor::CreateDatabase(std::string db_name) {
  catalog::Database *database = catalog::Catalog::GetInstance().GetDatabase(db_name);
  if (database != nullptr) {
    //LOG_ERROR("Database already exists  : %s \n", db_name.c_str());
    catalog::Catalog::GetInstance().Unlock();
    return false;
  }

  database = new catalog::Database(db_name.c_str());

  // lock catalog
  {
    catalog::Catalog::GetInstance().Lock();

    bool status = catalog::Catalog::GetInstance().AddDatabase(database);
    if (status == false) {
      //LOG_ERROR("Could not create database : %s \n", db_name.c_str());
      delete database;
      catalog::Catalog::GetInstance().Unlock();
      return false;
    }

    catalog::Catalog::GetInstance().Unlock();
  }

  //LOG_WARN("Created database : %s \n", db_name.c_str());
  return true;
}

// TODO :: Remove
bool CreateExecutor::CreateTable(catalog::Database* db,
                                 std::string table_name,
                                 DDL_ColumnInfo* ddl_columnInfo,
                                 int num_columns,
                                 catalog::Schema* schema = NULL) {
  //Either columns or schema is required to create a table
  assert( ddl_columnInfo == NULL && schema == NULL);

  bool isExist; 
  storage::DataTable *table = nullptr;

  // Check whether the 'table_name' table exists in the current database or not
  // It returns true if it exists
  isExist = IsThisTableExist(table_name.c_str());
  if( isExist )
  {
    //LOG_ERROR("Table already exists  : %s \n", table_name);
    printf("Table already exists  : %s \n", table_name.c_str());
    return false;
  }

  //Construct schema with ddl_columnInfo
  if( schema == NULL )
  {
    std::vector<catalog::ColumnInfo> columnInfoVect;

    //for( auto columnInfo_itr : columnInfo){
    for( int column_itr = 0; column_itr < num_columns; column_itr++ ){

      ValueType currentValueType;

      switch( ddl_columnInfo[column_itr].type ){
         // Could not find yet corresponding types in Postgres...
         // Also - check below types again to make sure..
         // TODO :: change the numbers to enum type

         //case ??:
         //currentValueType = VALUE_TYPE_NULL;
         //break;

         //case ??:
         //currentValueType = VALUE_TYPE_TINYINT;
         //ddl_columnInfo[column_itr].is_inlined = true;

         //break;
         //case ??:
         //currentValueType = VALUE_TYPE_ADDRESS;
         //break;

         //case ??:
         //currentValueType = VALUE_TYPE_VARBINARY;
         //break;

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
    //Add current columnInfo into the columnInfoVect
    columnInfoVect.push_back(*columnInfo);
    }

    // Construct schema from vector of ColumnInfo
    schema = new catalog::Schema(columnInfoVect);
  }

  storage::VMBackend *vmb = new storage::VMBackend;
  vmb->Allocate(sizeof(storage::DataTable)/*table size*/);

   //Create a table from schema
  //table = new storage::DataTable(schema, /*FIX IT*/backend, table_name, /*FIX*/tuples_per_tilegroup);

  /* TODO :: 
  // lock database
  {
    db->Lock();

    bool status = db->AddTable(table); // FIX
    if (status == false) {
      //LOG_ERROR("Could not create table : %s \n", table_name);
      printf("Could not create table : %s \n", table_name.c_str());
      delete table;
      db->Unlock();
      return false;
    }

    db->Unlock();
  }
  */

  //LOG_WARN("Created table : %s \n", table_name);
  printf("Created table : %s \n", table_name.c_str());
  return true;
}


bool CreateExecutor::CreateIndex(catalog::Database* db,
                                 std::string index_name,
                                 std::string table_name,
                                 std::vector<std::string> index_attrs,
                                 bool unique) {
  catalog::Table *table = db->GetTable(table_name);
  if (table == nullptr) {
    LOG_ERROR("Table does not exist  : %s \n", table_name.c_str());
    return false;
  }

  if (index_attrs.empty()) {
    LOG_ERROR("No index attributes defined for index : %s \n", index_name.c_str());
    return false;
  }

  std::vector<oid_t> key_attrs;
  std::vector<catalog::Column*> table_columns = table->GetColumns();
  std::vector<catalog::Column*> key_columns;

  for (auto key : index_attrs) {
    catalog::Column *column = table->GetColumn(key);
    if (column == nullptr) {
      LOG_ERROR("Index attribute does not exist in table : %s %s \n", key.c_str(), table_name.c_str());
      return false;
    }
    else {
      key_attrs.push_back(column->GetOffset());
      key_columns.push_back(column);
    }
  }

  //===--------------------------------------------------------------------===//
  // Physical index
  //===--------------------------------------------------------------------===//

  auto tuple_schema = table->GetTable()->GetSchema();
  catalog::Schema *key_schema = catalog::Schema::CopySchema(tuple_schema, key_attrs);

  index::IndexMetadata *index_metadata = new index::IndexMetadata(index_name,
                                                                  INDEX_TYPE_BTREE_MULTIMAP,
                                                                  tuple_schema,
                                                                  key_schema,
                                                                  unique);

  index::Index *physical_index = index::IndexFactory::GetInstance(index_metadata);

  catalog::Index *index = new catalog::Index(index_name,
                                             INDEX_TYPE_BTREE_MULTIMAP,
                                             unique,
                                             key_columns);

  // lock table
  {
    table->Lock();

    bool status = table->AddIndex(index);
    if (status == false) {
      LOG_ERROR("Could not create index : %s \n", index_name.c_str());
      delete physical_index;
      delete index;
      delete index_metadata;
      table->Unlock();
      return false;
    }

    table->Unlock();
  }

  index->SetPhysicalIndex(physical_index);

  LOG_WARN("Created index : %s \n", index_name.c_str());

  return true;
}

bool CreateExecutor::CreateConstraint(catalog::Database* db,
                                      std::string table_name) {

  catalog::Table *table = db->GetTable(table_name);
  if (table == nullptr) {
    LOG_ERROR("Table does not exist  : %s \n", table_name.c_str());
    return false;
  }

  // TODO : Need to work on this.
  // Constraints
  /*
  for (auto col : *stmt->columns) {
    if (col->type == parser::ColumnDefinition::PRIMARY) {
      auto primary_key_cols = col->primary_key;
      bool unique = col->unique;

      constraint_name = "PK_" + std::to_string(constraint_id++);
      index_name = "INDEX_" + std::to_string(index_id++);

      std::vector<catalog::Column*> columns;
      std::vector<catalog::Column*> sink_columns; // not set
      for (auto key : *primary_key_cols)
        columns.push_back(table->GetColumn(key));

      catalog::Index *index = new catalog::Index(index_name,
                                                 INDEX_TYPE_BTREE_MULTIMAP,
                                                 unique,
                                                 columns);

      catalog::Constraint *constraint = new catalog::Constraint(constraint_name,
                                                                catalog::Constraint::CONSTRAINT_TYPE_PRIMARY,
                                                                index,
                                                                nullptr,
                                                                columns,
                                                                sink_columns);

      status = table->AddConstraint(constraint);
      if (status == false) {
        LOG_ERROR("Could not create constraint : %s \n", constraint->GetName().c_str());
        delete index;
        delete constraint;
        delete table;
        return false;
      }

      status = table->AddIndex(index);
      if (status == false) {
        LOG_ERROR("Could not create index : %s \n", index->GetName().c_str());
        delete index;
        delete constraint;
        delete table;
        return false;
      }

    }
    // Foreign Keys
    else if (col->type == parser::ColumnDefinition::FOREIGN ) {
      auto foreign_key_source_cols = col->foreign_key_source;
      auto foreign_key_sink_cols = col->foreign_key_sink;

      constraint_name = "FK_" + std::to_string(constraint_id++);

      std::vector<catalog::Column*> source_columns;
      std::vector<catalog::Column*> sink_columns;
      catalog::Table *foreign_table = db->GetTable(col->name);

      for (auto key : *foreign_key_source_cols)
        source_columns.push_back(table->GetColumn(key));
      for (auto key : *foreign_key_sink_cols)
        sink_columns.push_back(foreign_table->GetColumn(key));

      catalog::Constraint *constraint = new catalog::Constraint(constraint_name,
                                                                catalog::Constraint::CONSTRAINT_TYPE_FOREIGN,
                                                                nullptr,
                                                                foreign_table,
                                                                source_columns,
                                                                sink_columns);

      status = table->AddConstraint(constraint);
      if (status == false) {
        LOG_ERROR("Could not create constraint : %s \n", constraint->GetName().c_str());
        delete constraint;
        delete table;
        return false;
      }
    }
    // Normal(?) Columns
    else {

      ValueType type = parser::ColumnDefinition::GetValueType(col->type);
      size_t col_len = GetTypeSize(type);

      if (col->type == parser::ColumnDefinition::CHAR)
        col_len = 1;
      if (type == VALUE_TYPE_VARCHAR || type == VALUE_TYPE_VARBINARY)
        col_len = col->varlen;

      catalog::Column *column = new catalog::Column(col->name,
                                                    type,
                                                    offset++,
                                                    col_len,
                                                    col->not_null);

      bool varlen = false;
      if (col->varlen != 0)
        varlen = true;

      catalog::ColumnInfo physical_column(type, col_len, col->name, !col->not_null, varlen);
      physical_columns.push_back(physical_column);

      status = table->AddColumn(column);
      if (status == false) {
        LOG_ERROR("Could not create column : %s \n", column->GetName().c_str());
        delete table;
        return false;
      }

    }
  } // FOR

  // In-Memory Data Table
  catalog::Schema *schema = new catalog::Schema(physical_columns);
  storage::DataTable *physical_table = storage::TableFactory::GetDataTable(DEFAULT_DB_ID, schema, table_name);
  table->SetPhysicalTable(physical_table);

  // lock database
  {
    db->Lock();

    bool status = db->AddTable(table);
    if (status == false) {
      LOG_ERROR("Could not create table : %s \n", table_name);
      delete table;
      db->Unlock();
      return false;
    }

    db->Unlock();
  }
  */

  LOG_WARN("Created table : %s \n", table_name.c_str());
  return true;
}

} // namespace executor
} // namespace peloton

