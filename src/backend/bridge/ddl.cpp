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
#include "parser/parse_utilcmd.h"
#include "parser/parse_type.h"
#include "access/htup_details.h"
#include "utils/resowner.h"
#include "utils/syscache.h"
#include "catalog/pg_type.h"

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
#include <iostream>

namespace peloton {
namespace bridge {

/**
 * @brief Process utility statement.
 * @param parsetree Parse tree
 */
void DDL::ProcessUtility(Node *parsetree,
                         const char *queryString){
  assert(parsetree != nullptr);
  assert(queryString != nullptr);

  // Process depending on type of utility statement
  switch (nodeTag(parsetree))
  {
    case T_CreateStmt:
    case T_CreateForeignTableStmt:
    {
      List     *stmts;
      ListCell   *l;

      return;
      /* Run parse analysis ... */
      stmts = transformCreateStmt((CreateStmt *) parsetree,
                                  queryString);

      /* ... and do it */
      foreach(l, stmts)
      {
        Node     *stmt = (Node *) lfirst(l);

        if (IsA(stmt, CreateStmt))
        {
          int column_itr=0;
          bool ret;
          CreateStmt* Cstmt = (CreateStmt*)stmt;
          List* schema = (List*)(Cstmt->tableElts);

          // Construct DDL_ColumnInfo with schema
          if( schema != NULL ){
            ListCell   *entry;

            // All column information will be stored in DDL_ColumnInfo, it will be delivered in Peloton via DDL_functions
            // Allocate ddl_columnInfo as much as number of columns of the current schema
            DDL_ColumnInfo* ddl_columnInfo = (DDL_ColumnInfo*) malloc( sizeof(DDL_ColumnInfo)*schema->length);

            // Let's assume that we have column A,B, and C
            // If column 'A' has two constraints and column 'B' doesn't have any constraint and 'C' has one,
            // then the values of num_of_constraints_of_each_column will be { 2, 0, 1 }
            int* num_of_constraints_of_each_column = (int*) malloc( sizeof(int) * schema->length);

            // Parse the CreateStmt and construct ddl_columnInfo
            foreach(entry, schema){
              int constNode_itr = 0;
              ColumnDef  *coldef = lfirst(entry);
              Type    tup;
              Form_pg_type typ;
              Oid      typoid;

              tup = typenameType(NULL, coldef->typeName, NULL);
              typ = (Form_pg_type) GETSTRUCT(tup);
              typoid = HeapTupleGetOid(tup);
              ReleaseSysCache(tup);

              ddl_columnInfo[column_itr].valueType = typoid;
              ddl_columnInfo[column_itr].column_offset = column_itr;
              ddl_columnInfo[column_itr].column_length = typ->typlen;
              strcpy(ddl_columnInfo[column_itr].name, coldef->colname);
              ddl_columnInfo[column_itr].allow_null = !coldef->is_not_null;
              ddl_columnInfo[column_itr].is_inlined = false;

              //  CONSTRAINTS
              if( coldef->constraints != NULL)
              {
                ListCell* constNodeEntry;

                // Allocate  constraints dynamically since a single column can have multiple constraints
                ddl_columnInfo[column_itr].constraintType = (int*) malloc(sizeof(int)*coldef->constraints->length);
                ddl_columnInfo[column_itr].conname = (char**) malloc(sizeof(char*)*coldef->constraints->length);

                foreach(constNodeEntry, coldef->constraints)
                {
                  Constraint* ConstraintNode = lfirst(constNodeEntry);
                  ddl_columnInfo[column_itr].constraintType[constNode_itr] = ConstraintNode->contype;
                  if( ConstraintNode->conname != NULL){
                    ddl_columnInfo[column_itr].conname[constNode_itr] = (char*) malloc(sizeof(char*)*strlen(ConstraintNode->conname));
                    strcpy(ddl_columnInfo[column_itr].conname[constNode_itr],ConstraintNode->conname);
                  }else
                    ddl_columnInfo[column_itr].conname[constNode_itr] = "";
                  constNode_itr++;
                }
              }
              else{
                ddl_columnInfo[column_itr].constraintType = NULL;
                ddl_columnInfo[column_itr].conname = NULL;
              }

              num_of_constraints_of_each_column[column_itr] = constNode_itr;
              column_itr++;
            }

            // Now, intercept the create table request from Postgres and create a table in Peloton
            ret = peloton::bridge::DDL::CreateTable( Cstmt->relation->relname, ddl_columnInfo, schema->length, num_of_constraints_of_each_column);
          }
          else {
            // Create Table without column info
            ret = peloton::bridge::DDL::CreateTable( Cstmt->relation->relname, NULL, 0 , 0);
          }

          fprintf(stderr, "DDL_CreateTable(%s) :: %d \n", Cstmt->relation->relname,ret);
        }
      }

    }
    break;

    case T_IndexStmt:  /* CREATE INDEX */
    {
      IndexStmt  *stmt = (IndexStmt *) parsetree;
      Oid      relid;

      // CreateIndex
      ListCell   *entry;
      int column_itr_for_KeySchema= 0;
      int type = 0;
      bool ret;

      char**ColumnNamesForKeySchema = (char**)malloc(sizeof(char*)*stmt->indexParams->length);

      // Parse the IndexStmt and construct ddl_columnInfo for TupleSchema and KeySchema
      foreach(entry, stmt->indexParams)
      {
        IndexElem *indexElem = lfirst(entry);

        //printf("index name %s \n", indexElem->name);
        //printf("Index column %s \n", indexElem->indexcolname) ;

        if( indexElem->name != NULL )
        {
          ColumnNamesForKeySchema[column_itr_for_KeySchema] = (char*) malloc( sizeof(char*)*strlen(indexElem->name));
          strcpy(ColumnNamesForKeySchema[column_itr_for_KeySchema], indexElem->name );
          column_itr_for_KeySchema++;
        }
      }

      // look up the access method, transform the method name into IndexType
      if (strcmp(stmt->accessMethod, "btree") == 0){
        type = BTREE_AM_OID;
      }
      else if (strcmp(stmt->accessMethod, "hash") == 0){
        type = HASH_AM_OID;
      }
      else if (strcmp(stmt->accessMethod, "rtree") == 0 || strcmp(stmt->accessMethod, "gist") == 0){
        type = GIST_AM_OID;
      }
      else if (strcmp(stmt->accessMethod, "gin") == 0){
        type = GIN_AM_OID;
      }
      else if (strcmp(stmt->accessMethod, "spgist") == 0){
        type = SPGIST_AM_OID;
      }
      else if (strcmp(stmt->accessMethod, "brin") == 0){
        type = BRIN_AM_OID;
      }
      else{
        type = 0;
      }

      ret = peloton::bridge::DDL::CreateIndex(stmt->idxname,
                                              stmt->relation->relname,
                                              type,
                                              stmt->unique,
                                              ColumnNamesForKeySchema,
                                              column_itr_for_KeySchema
      );

      fprintf(stderr, "DDLCreateIndex :: %d \n", ret);

    }
    break;

    case T_DropStmt:
    {
      DropStmt* drop;
      ListCell  *cell;
      int table_oid_itr = 0;
      bool ret;
      Oid* table_oid_list;
      drop = (DropStmt*) parsetree;
      table_oid_list = (Oid*) malloc ( sizeof(Oid)*(drop->objects->length));

      foreach(cell, drop->objects)
      {
        if (drop->removeType == OBJECT_TABLE )
        {
          List* names = ((List *) lfirst(cell));
          char* table_name = strVal(linitial(names));
          table_oid_list[table_oid_itr++] = GetRelationOid(table_name);
        }
      }

      while(table_oid_itr > 0)
      {
        ret  = peloton::bridge::DDL::DropTable(table_oid_list[--table_oid_itr]);
        fprintf(stderr, "DDLDropTable :: %d \n", ret);
      }

    }
    break;

    default:
      elog(LOG, "unrecognized node type: %d",
           (int) nodeTag(parsetree));
      break;
  }

}

/**
 * @brief Construct ColumnInfo vector from a create statement
 * @param Cstmt a create statement 
 * @return ColumnInfo vector 
 */
std::vector<catalog::ColumnInfo> ConstructColumnInfoByParsingCreateStmt( CreateStmt* Cstmt ){
  assert(Cstmt);

  // Get the column list from the create statement
  List* ColumnList = (List*)(Cstmt->tableElts);
  std::vector<catalog::ColumnInfo> column_infos;

  //===--------------------------------------------------------------------===//
  // Column Type Information
  //===--------------------------------------------------------------------===//

  // Parse the CreateStmt and construct ColumnInfo
  ListCell   *entry;
  foreach(entry, ColumnList){

    ColumnDef  *coldef = lfirst(entry);

    // Parsing the column value type
    Oid typeoid, typemod;
    int typelen;

    // Get the type oid and type mod with given typeName
    typeoid = typenameTypeId(NULL, coldef->typeName);

    typenameTypeIdAndMod(NULL, coldef->typeName, &typeoid, &typemod);

    // Get type length
    Type tup = typeidType(typeoid);
    typelen = typeLen(tup);
    ReleaseSysCache(tup);

    // For a fixed-size type, typlen is the number of bytes in the internal
    // representation of the type. But for a variable-length type, typlen is negative.
    if( typelen == -1 )
    {
      printf("%u\n", typemod);
      // we need to get the atttypmod from pg_attribute
    }
     

    ValueType column_valueType = PostgresValueTypeToPelotonValueType( (PostgresValueType) typeoid );
    int column_length = typelen;
    std::string column_name = coldef->colname;
    bool column_allow_null = !coldef->is_not_null;

    //===--------------------------------------------------------------------===//
    // Column Constraint Information
    //===--------------------------------------------------------------------===//
    std::vector<catalog::Constraint> column_constraints;

    if( coldef->constraints != NULL){
      ListCell* constNodeEntry;

      foreach(constNodeEntry, coldef->constraints)
      {
        Constraint* ConstraintNode = lfirst(constNodeEntry);
        ConstraintType contype;
        std::string conname;

        // Get constraint type
        contype = PostgresConstraintTypeToPelotonConstraintType( (PostgresConstraintType) ConstraintNode->contype );
        std::cout << ConstraintTypeToString(contype) << std::endl;

        // Get constraint name
        if( ConstraintNode->conname != NULL)
          conname = ConstraintNode->conname;

        catalog::Constraint* constraint = new catalog::Constraint( contype, conname );
        column_constraints.push_back(*constraint);
      }
    }// end of parsing constraint 

    catalog::ColumnInfo *column_info = new catalog::ColumnInfo( column_valueType, 
                                                                column_length, 
                                                                column_name, 
                                                                column_allow_null,
                                                                column_constraints);

    // Insert column_info into ColumnInfos
    column_infos.push_back(*column_info);
  }// end of parsing column list

  return column_infos;
}

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
  std::cout << "DEBUGGING : " << table_name << std::endl;

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

  std::cout << "Creating table is failed.. : " << table_name << std::endl;
  return false;
}


//TODO :: New CreateTable 
/**
 * @brief Create table.
 * @param table_name Table name
 * @param column_infos Information about the columns
 * @param schema Schema for the table
 * @return true if we created a table, false otherwise
 */
bool DDL::CreateTable2( std::string table_name,
                        std::vector<catalog::ColumnInfo> column_infos,
                        catalog::Schema *schema){

  //===--------------------------------------------------------------------===//
  // Check Parameters 
  //===--------------------------------------------------------------------===//
  assert( !table_name.empty() );
  assert( column_infos.size() > 0 || schema != NULL );

  Oid database_oid = GetCurrentDatabaseOid();
  if(database_oid == InvalidOid)
    return false;

  // Construct our schema from vector of ColumnInfo
  if( schema == NULL) 
    schema = new catalog::Schema(column_infos);

  // FIXME: Construct table backend
  storage::VMBackend *backend = new storage::VMBackend();

  // Build a table from schema
  storage::DataTable *table = storage::TableFactory::GetDataTable(database_oid, schema, table_name);

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
