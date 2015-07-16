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
#include "miscadmin.h"
#include "c.h"

#include "bridge/bridge.h"
#include "nodes/parsenodes.h"
#include "parser/parse_utilcmd.h"
#include "parser/parse_type.h"
#include "access/htup_details.h"
#include "utils/resowner.h"
#include "utils/syscache.h"
#include "catalog/pg_type.h"
#include "commands/dbcommands.h"

#include "backend/bridge/ddl.h"
#include "backend/catalog/catalog.h"
#include "backend/catalog/schema.h"
#include "backend/common/logger.h"
#include "backend/common/types.h"
#include "backend/index/index.h"
#include "backend/index/index_factory.h"
#include "backend/storage/backend_vm.h"
#include "backend/storage/table_factory.h"
#include "backend/storage/database.h"

#include <cassert>
#include <iostream>

#include "parser/parse_node.h" // make pstate cook default
#include "parser/parse_expr.h" // cook default
#include "parser/parse_expr.h" // cook default


namespace peloton {
namespace bridge {

#define COLOR_RED     "\x1b[31m"
#define COLOR_GREEN   "\x1b[32m"
#define COLOR_YELLOW  "\x1b[33m"
#define COLOR_BLUE    "\x1b[34m"
#define COLOR_MAGENTA "\x1b[35m"
#define COLOR_CYAN    "\x1b[36m"
#define COLOR_RESET   "\x1b[0m"

static std::vector<IndexInfo> index_infos;

//===--------------------------------------------------------------------===//
// Create Object
//===--------------------------------------------------------------------===//

/**
 * @brief Create database.
 * @param database_oid database id
 * @return true if we created a database, false otherwise
 */
bool DDL::CreateDatabase( Oid database_oid ){

  storage::Database* db = storage::Database::GetDatabaseById( database_oid );

  fprintf(stderr, "DDLCreateDatabase :: %u %p \n", database_oid, db);

  return true;
}

/**
 * @brief Create table.
 * @param table_name Table name
 * @param column_infos Information about the columns
 * @param schema Schema for the table
 * @return true if we created a table, false otherwise
 */
bool DDL::CreateTable( Oid relation_oid,
                       std::string table_name,
                       std::vector<catalog::Column> column_infos,
                       catalog::Schema *schema){

  assert( !table_name.empty() );

  Oid database_oid = GetCurrentDatabaseOid();
  if(database_oid == INVALID_OID || relation_oid == INVALID_OID )
    return false;

  // Get db oid
  storage::Database* db = storage::Database::GetDatabaseById( database_oid );

  // Construct our schema from vector of ColumnInfo
  if( schema == NULL) 
    schema = new catalog::Schema(column_infos);

  // Build a table from schema
  storage::DataTable *table = storage::TableFactory::GetDataTable(database_oid, relation_oid, schema, table_name);

  bool status = db->AddTable(table);

  if( status == false ){
    LOG_WARN("Could not add table :: db oid : %u table oid : %u", database_oid,  relation_oid);
  }

  if(table != nullptr) {
    LOG_INFO("Created table(%u) : %s", relation_oid, table_name.c_str());
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
 * @param key_column_names column names for the key table 
 * @return true if we create the index, false otherwise
 */
bool DDL::CreateIndex( IndexInfo index_info ){

  std::string index_name = index_info.GetIndexName();
  oid_t index_oid = index_info.GetIndexId();
  std::string table_name = index_info.GetTableName();
  IndexType index_type = index_info.GetType();
  bool unique_keys = index_info.IsUnique();
  std::vector<std::string> key_column_names = index_info.GetKeyColumnNames();

  assert( !index_name.empty() );
  assert( !table_name.empty() );
  assert( key_column_names.size() > 0  );

  LOG_INFO("Create index %s on %s.", index_name.c_str(), table_name.c_str());
  // TODO: We currently only support btree as our index implementation
  // NOTE: We currently only support btree as our index implementation
  // TODO : Support other types based on "type" argument
  IndexMethodType our_index_type = INDEX_METHOD_TYPE_BTREE_MULTIMAP;

  // Get the database oid and table oid
  oid_t database_oid = GetCurrentDatabaseOid();
  assert( database_oid );

  // Get the table location from db
  storage::Database* db = storage::Database::GetDatabaseById( database_oid );
  storage::DataTable* data_table = db->GetTableByName( table_name );

  catalog::Schema *tuple_schema = data_table->GetSchema();

  // Construct key schema
  std::vector<oid_t> key_columns;

  // Based on the key column info, get the oid of the given 'key' columns in the tuple schema
  for( auto key_column_name : key_column_names ){
    for( oid_t  tuple_schema_column_itr = 0; tuple_schema_column_itr < tuple_schema->GetColumnCount();
        tuple_schema_column_itr++){

      // Get the current column info from tuple schema
      catalog::Column column_info = tuple_schema->GetColumn(tuple_schema_column_itr);
      // Compare Key Schema's current column name and Tuple Schema's current column name
      if( key_column_name == column_info.GetName() ){
        key_columns.push_back(tuple_schema_column_itr);

        // NOTE :: Since pg_attribute doesn't have any information about primary key and unique key,
        //         I try to store these information when we create an unique and primary key index
        if( index_type == INDEX_TYPE_PRIMARY_KEY ){ 
          catalog::Constraint constraint( CONSTRAINT_TYPE_PRIMARY, index_name );
          tuple_schema->AddConstraint( tuple_schema_column_itr, constraint);
        }else if( index_type == INDEX_TYPE_UNIQUE ){ 
          catalog::Constraint constraint( CONSTRAINT_TYPE_UNIQUE, index_name );
          constraint.SetUniqueIndexPosition( data_table->GetUniqueIndexCount() );
          tuple_schema->AddConstraint( tuple_schema_column_itr, constraint);
        }

      }
    }
  }

  auto key_schema = catalog::Schema::CopySchema(tuple_schema, key_columns);

  // Create index metadata and physical index
  index::IndexMetadata* metadata = new index::IndexMetadata(index_name, our_index_type, index_type,
                                                            tuple_schema, key_schema,
                                                            unique_keys);
  index::Index* index = index::IndexFactory::GetInstance(metadata);

  // Record the built index in the table
  data_table->AddIndex(index, index_oid);

  return true;
}

//===--------------------------------------------------------------------===//
// Alter Object
//===--------------------------------------------------------------------===//

/**
 * @brief AlterTable with given AlterTableStmt
 * @param relation_oid relation oid 
 * @param Astmt AlterTableStmt 
 * @return true if we alter the table successfully, false otherwise
 */
bool DDL::AlterTable( Oid relation_oid, AlterTableStmt* Astmt ){

  ListCell* lcmd;
  foreach( lcmd, Astmt->cmds)
  {
    AlterTableCmd *cmd = (AlterTableCmd *) lfirst(lcmd);

    switch (cmd->subtype){
      //case AT_AddColumn:  /* add column */
      //case AT_DropColumn:  /* drop column */

      case AT_AddConstraint:	/* ADD CONSTRAINT */
      {
        bool status = bridge::DDL::AddConstraint( relation_oid, (Constraint*) cmd->def );
        fprintf(stderr, "DDLAddConstraint :: %d \n", status);
        break;
      }
      default:
        break;
    }
  }

  return true;
}


//===--------------------------------------------------------------------===//
// Drop Object
//===--------------------------------------------------------------------===//

/**
 * @brief Drop database.
 * @param database_oid database id.
 * @return true if we dropped the database, false otherwise
 */
bool DDL::DropDatabase( Oid database_oid ){
  storage::Database* db = storage::Database::GetDatabaseById( database_oid );
  bool status = db->DeleteDatabaseById( database_oid );
  return status;
}

/**
 * @brief Drop table.
 * @param table_oid Table id.
 * @return true if we dropped the table, false otherwise
 */
// FIXME :: Dependencies btw indexes and tables
bool DDL::DropTable(Oid table_oid) {

  oid_t database_oid = GetCurrentDatabaseOid();
  bool status;

  if(database_oid == InvalidOid || table_oid == InvalidOid) {
    LOG_WARN("Could not drop table :: db oid : %u table oid : %u", database_oid, table_oid);
    return false;
  }

  // Get db with current database oid
  storage::Database* db = storage::Database::GetDatabaseById( database_oid );
  status = db->DeleteTableById( table_oid );

  if(status == true) {
    LOG_INFO("Dropped table with oid : %u\n", table_oid);
    return true;
  }

  return false;
}

//===--------------------------------------------------------------------===//
// Misc. 
//===--------------------------------------------------------------------===//

/**
 * @brief Process utility statement.
 * @param parsetree Parse tree
 */
void DDL::ProcessUtility(Node *parsetree,
                         const char *queryString){
  assert( parsetree != nullptr );
  assert( queryString != nullptr );

  /* When we call a backend function from different thread, the thread's stack
   * is at a different location than the main thread's stack. so it sets up
   * reference point for stack depth checking
   */
  set_stack_base();

  printf(COLOR_RED "ProcessUtility %d" COLOR_RESET "\n", (int)nodeTag(parsetree));

  // Process depending on type of utility statement
  switch ( nodeTag( parsetree ))
  {
    case T_CreatedbStmt:
    {
      printf(COLOR_RED "T_Createdb" COLOR_RESET "\n");
      CreatedbStmt* CdbStmt = (CreatedbStmt*) parsetree;
      bridge::DDL::CreateDatabase( CdbStmt->database_id );
    }
    break;

    case T_CreateStmt:
    case T_CreateForeignTableStmt:
    {
      printf(COLOR_RED "T_CreateTable" COLOR_RESET "\n");

      /* Run parse analysis ... */
      List     *stmts = transformCreateStmt((CreateStmt *) parsetree,
                                            queryString);

      /* ... and do it */
      ListCell   *l;
      foreach(l, stmts)
      {
        Node     *stmt = (Node *) lfirst(l);
        if (IsA(stmt, CreateStmt)){
          CreateStmt* Cstmt = (CreateStmt*)stmt;
          List* schema = (List*)(Cstmt->tableElts);

          // relation name and oid
          char* relation_name = Cstmt->relation->relname;
          Oid relation_oid = ((CreateStmt *)parsetree)->relation_id;

<<<<<<< HEAD
          std::vector<catalog::Column> column_infos;
=======
          std::vector<catalog::ColumnInfo> column_infos;
>>>>>>> bridge
          std::vector<catalog::ForeignKeyInfo> reference_table_infos;

          bool status;

          //===--------------------------------------------------------------------===//
          // CreateStmt --> ColumnInfo --> CreateTable
          //===--------------------------------------------------------------------===//
          if( schema != NULL ){
            bridge::DDL::ParsingCreateStmt( Cstmt,
<<<<<<< HEAD
                                            column_infos,
                                            reference_table_infos );

            status = bridge::DDL::CreateTable( relation_oid,
                                               relation_name,
                                               column_infos );
          } else {
            // SPECIAL CASE : CREATE TABLE WITHOUT COLUMN INFO
            status = bridge::DDL::CreateTable( relation_oid,
                                               relation_name,
                                               column_infos );
=======
                                                     column_infos,
                                                     reference_table_infos );

            status = bridge::DDL::CreateTable( relation_oid,
                                                        relation_name, 
                                                        column_infos );
          } else {
            // SPECIAL CASE : CREATE TABLE WITHOUT COLUMN INFO
            status = bridge::DDL::CreateTable( relation_oid,
                                                        relation_name, 
                                                        column_infos );
>>>>>>> bridge
          }

          fprintf(stderr, "DDL_CreateTable(%s) :: Oid : %d Status : %d \n", relation_name, relation_oid, status);

          //===--------------------------------------------------------------------===//
          // Check Constraint
          //===--------------------------------------------------------------------===//
          // TODO : Cook the data..
          //          if( Cstmt->constraints != NULL){
          //            oid_t database_oid = GetCurrentDatabaseOid();
          //            assert( database_oid );
          //            auto table = catalog::Manager::GetInstance().GetLocation(database_oid, relation_oid);
          //            storage::DataTable* data_table = (storage::DataTable*) table;
          //
          //            ListCell* constraint;
          //            foreach(constraint, Cstmt->constraints)
          //            {
          //              Constraint* ConstraintNode = ( Constraint* ) lfirst(constraint);
          //
          //              // Or we can get cooked infomation from catalog
          //              //ex)
          //              //ConstrCheck *check = rel->rd_att->constr->check;
          //              //AttrDefault *defval = rel->rd_att->constr->defval;
          //
          //              if( ConstraintNode->raw_expr != NULL ){
          //                data_table->SetRawCheckExpr(ConstraintNode->raw_expr);
          //              }
          //            }
          //          }

          //===--------------------------------------------------------------------===//
          // Set Reference Tables
          //===--------------------------------------------------------------------===//
          status = bridge::DDL::SetReferenceTables( reference_table_infos,
<<<<<<< HEAD
                                                    relation_oid );
=======
                                                             relation_oid );
>>>>>>> bridge
          if( status == false ){
            LOG_WARN("Failed to set reference tables");
          } 


          //===--------------------------------------------------------------------===//
          // Add Primary Key and Unique Indexes to the table
          //===--------------------------------------------------------------------===//
          status = bridge::DDL::CreateIndexesWithIndexInfos( index_infos );
          if( status == false ){
            LOG_WARN("Failed to create primary key and unique index");
          }

        }
      }
    }
    break;

    case T_IndexStmt:  /* CREATE INDEX */
    {
      printf(COLOR_RED "T_IndexStmt" COLOR_RESET "\n");
      bool status;
      IndexStmt  *Istmt = (IndexStmt *) parsetree;

      // Construct IndexInfo 
      IndexInfo* index_info = ConstructIndexInfoByParsingIndexStmt( Istmt );

      // If table has not been created yet, skip the rest part of this function
      storage::Database* db = storage::Database::GetDatabaseById( GetCurrentDatabaseOid() );
      if( nullptr == db->GetTableByName( Istmt->relation->relname )){
        index_infos.push_back(*index_info);
        break;
      }

      status = bridge::DDL::CreateIndex( *index_info );


      fprintf(stderr, "DDLCreateIndex :: %d \n", status);
    }
    break;

    case T_AlterTableStmt:
    {
      printf(COLOR_RED "T_AlterTableStmt" COLOR_RESET "\n");

      break; // still working on here.... 

      AlterTableStmt *atstmt = (AlterTableStmt *) parsetree;
      Oid     relation_oid = atstmt->relation_id;
      List     *stmts = transformAlterTableStmt(relation_oid, atstmt, queryString);
      bool status;

      /* ... and do it */
      ListCell   *l;
      foreach(l, stmts){

        Node *stmt = (Node *) lfirst(l);

        if (IsA(stmt, AlterTableStmt)){
          status = bridge::DDL::AlterTable( relation_oid, (AlterTableStmt*)stmt );

          fprintf(stderr, "DDLAlterTable :: %d \n", status);

        }
      }
    }
    break;

    case T_DropdbStmt:
    {
      printf(COLOR_RED "T_DropdbStmt" COLOR_RESET "\n" );
      DropdbStmt *Dstmt = (DropdbStmt *) parsetree;

      Oid database_oid = get_database_oid( Dstmt->dbname, Dstmt->missing_ok );

      bool status = bridge::DDL::DropDatabase( database_oid );
      fprintf(stderr, "DDL Database :: %d \n", status);
    }
    break;

    case T_DropStmt:
    {
      printf(COLOR_RED "T_DropStmt" COLOR_RESET "\n");
      DropStmt* drop = (DropStmt*) parsetree; // TODO drop->behavior;		/* RESTRICT or CASCADE behavior */
      bool status;

      ListCell  *cell;
      foreach(cell, drop->objects){
        List* names = ((List *) lfirst(cell));


        switch( drop->removeType ){

          case OBJECT_DATABASE:
          {
            char* database_name = strVal(linitial(names));
            Oid database_oid = get_database_oid( database_name, true );

            status = bridge::DDL::DropDatabase( database_oid );
            fprintf(stderr, "DDL Database :: %d \n", status);
          }
          break;

          case OBJECT_TABLE:
          {
            char* table_name = strVal(linitial(names));
            Oid table_oid = GetRelationOid(table_name);

            status = bridge::DDL::DropTable( table_oid );
            fprintf(stderr, "DDL DropTable :: %d \n", status);
          }
          break;

          default:
          {
            LOG_WARN("Unsupported drop object %d ", drop->removeType);
          }
          break;
        }

      }
    }
    break;
    // End of DropStmt

    default:
    {
      LOG_WARN("unrecognized node type: %d", (int) nodeTag(parsetree));
    }
    break;
  }

  // TODO :: This is for debugging
  //storage::Database* db = storage::Database::GetDatabaseById( GetCurrentDatabaseOid() );
  //std::cout << *db << std::endl;

}



/**
 * @brief parsing create statement
 * @param Cstmt a create statement 
 * @param column_infos to create a table
 * @param refernce_table_infos to store reference table to the table
 */
void DDL::ParsingCreateStmt( CreateStmt* Cstmt,
                             std::vector<catalog::Column>& column_infos,
                             std::vector<catalog::ForeignKeyInfo>& reference_table_infos
) {
  assert(Cstmt);

  //===--------------------------------------------------------------------===//
  // Table Constraint
  //===--------------------------------------------------------------------===//
  /*
//TODO
  if( Cstmt->constraints != NULL){
    ListCell* constraint;

    foreach(constraint, Cstmt->constraints){
      //printf("\nPrint Multi column constraint\n");
      Constraint* ConstraintNode = lfirst(constraint);
      ConstraintType contype;
      std::string conname;
      std::string reference_table_name;

      // Get constraint type
      contype = PostgresConstraintTypeToPelotonConstraintType( (PostgresConstraintType) ConstraintNode->contype );
      printf("Constraint type %s \n", ConstraintTypeToString( contype ).c_str());

      // Get constraint name
      if( ConstraintNode->conname != NULL){
        conname = ConstraintNode->conname;
        printf("Constraint name %s \n", conname.c_str() );
      }

      // Get reference table name 
      if( ConstraintNode->pktable != NULL ){
        reference_table_name = ConstraintNode->pktable->relname;
        printf("Constraint name %s \n", reference_table_name.c_str() );
      }

      printf("\n");
    }
  }
   */

  //===--------------------------------------------------------------------===//
  // Column Infomation 
  //===--------------------------------------------------------------------===//

  // Get the column list from the create statement
  List* ColumnList = (List*)(Cstmt->tableElts);

  // Parse the CreateStmt and construct ColumnInfo
  ListCell   *entry;
  foreach(entry, ColumnList){

    ColumnDef  *coldef = static_cast<ColumnDef *>(lfirst(entry));

    // Get the type oid and type mod with given typeName
    Oid typeoid = typenameTypeId(NULL, coldef->typeName);
    int32 typemod;
    typenameTypeIdAndMod(NULL, coldef->typeName, &typeoid, &typemod);

    // Get type length
    Type tup = typeidType(typeoid);
    int typelen = typeLen(tup);
    ReleaseSysCache(tup);

    /* TODO :: Simple version, but need to check it is correct
    // Get the type oid
    typeoid = typenameTypeId(NULL, coldef->typeName);
    // type mod
    typemod = get_typmodin(typeoid);
    // Get type length
    typelen = get_typlen(typeoid);
     */

    // For a fixed-size type, typlen is the number of bytes in the internal
    // representation of the type. But for a variable-length type, typlen is negative.
    if( typelen == - 1 )
      typelen = typemod;

    ValueType column_valueType = PostgresValueTypeToPelotonValueType( (PostgresValueType) typeoid );
    int column_length = typelen;
    std::string column_name = coldef->colname;

    //===--------------------------------------------------------------------===//
    // Column Constraint
    //===--------------------------------------------------------------------===//

    std::vector<catalog::Constraint> column_constraints;

    if( coldef->raw_default != NULL){
      catalog::Constraint constraint(CONSTRAINT_TYPE_DEFAULT, "", coldef->raw_default);
      column_constraints.push_back(constraint);
    };

    if( coldef->constraints != NULL){
      ListCell* constNodeEntry;

      foreach(constNodeEntry, coldef->constraints)
      {
        Constraint* ConstraintNode = static_cast<Constraint*>(lfirst(constNodeEntry));
        ConstraintType contype;
        std::string conname;

        // CONSTRAINT TYPE
        contype = PostgresConstraintTypeToPelotonConstraintType( (PostgresConstraintType) ConstraintNode->contype );

        // CONSTRAINT NAME
        if( ConstraintNode->conname != NULL){
          conname = ConstraintNode->conname;
        }else{
          conname = "";
        }

        catalog::Constraint* constraint;

        switch( contype ){

          case CONSTRAINT_TYPE_NULL:
            constraint = new catalog::Constraint( contype, conname );
            break;
          case CONSTRAINT_TYPE_NOTNULL:
            constraint = new catalog::Constraint( contype, conname );
            break;
          case CONSTRAINT_TYPE_CHECK:
            constraint = new catalog::Constraint( contype, conname, ConstraintNode->raw_expr );
            break;
          case CONSTRAINT_TYPE_PRIMARY:
            constraint = new catalog::Constraint( contype, conname );
            break;
          case CONSTRAINT_TYPE_UNIQUE:
            continue;
          case CONSTRAINT_TYPE_FOREIGN:
          {
            // REFERENCE TABLE NAME AND ACTION OPTION
            if( ConstraintNode->pktable != NULL ){

<<<<<<< HEAD
              storage::Database* db = storage::Database::GetDatabaseById( GetCurrentDatabaseOid() );
=======
                storage::Database* db = storage::Database::GetDatabaseById( GetCurrentDatabaseOid() );
>>>>>>> bridge

              // PrimaryKey Table
              oid_t PrimaryKeyTableId = db->GetTableIdByName( ConstraintNode->pktable->relname );

              // Each table column names
              std::vector<std::string> pk_column_names;
              std::vector<std::string> fk_column_names;

              ListCell   *column;
              if( ConstraintNode->pk_attrs != NULL && ConstraintNode->pk_attrs->length > 0 ){
                foreach(column, ConstraintNode->pk_attrs ){
                  char* attname = strVal(lfirst(column));
                  pk_column_names.push_back(  attname  );
                }
              }
              if( ConstraintNode->fk_attrs != NULL && ConstraintNode->fk_attrs->length > 0 ){
                foreach(column, ConstraintNode->fk_attrs ){
                  char* attname = strVal(lfirst(column));
                  fk_column_names.push_back(  attname  );
                }
              }

              catalog::ForeignKeyInfo *reference_table_info = new catalog::ForeignKeyInfo( PrimaryKeyTableId,
                                                                                                   pk_column_names,
                                                                                                   fk_column_names,
                                                                                                   ConstraintNode->fk_upd_action,
                                                                                                   ConstraintNode->fk_del_action,
                                                                                                   conname );

              reference_table_infos.push_back( *reference_table_info );
            }
            continue;
          }
          default:
          {
            constraint = new catalog::Constraint( contype, conname );
            LOG_WARN("Unrecognized constraint type %d\n", (int) contype );
            break;
          }
        }


        column_constraints.push_back(*constraint);
      }
    }// end of parsing constraint 

    catalog::Column column_info( column_valueType,
                                 column_length,
                                 column_name,
                                 false);

    // Insert column_info into ColumnInfos
    column_infos.push_back(column_info);
  }// end of parsing column list
}

/**
 * @brief Construct IndexInfo from a index statement
 * @param Istmt an index statement 
 * @return IndexInfo  
 */
IndexInfo* DDL::ConstructIndexInfoByParsingIndexStmt( IndexStmt* Istmt ){
  std::string index_name;
  oid_t index_oid = Istmt->index_id;
  std::string table_name;
  IndexMethodType method_type;
  IndexType type = INDEX_TYPE_NORMAL;
  std::vector<std::string> key_column_names;

  // Table name
  table_name = Istmt->relation->relname;

  // Key column names
  ListCell   *entry;
  foreach(entry, Istmt->indexParams){
    IndexElem *indexElem = static_cast<IndexElem *>(lfirst(entry));
    if( indexElem->name != NULL ){
      key_column_names.push_back(indexElem->name);
    }
  }

  // Index name and index type
  if( Istmt->idxname == NULL ){
    if( Istmt->isconstraint ){
      if( Istmt->primary ) {
        index_name = table_name+"_pkey";
        type = INDEX_TYPE_PRIMARY_KEY;
      }else if( Istmt->unique ){
        index_name = table_name;
        for( auto column_name : key_column_names ){
          index_name += "_"+column_name+"_";
        }
        index_name += "key";
        type = INDEX_TYPE_UNIQUE;
      }
    }else{
      LOG_WARN("No index name");
    }
  }else{
    index_name = Istmt->idxname;
  }

  // Index method type
  // TODO :: More access method types need
  method_type = INDEX_METHOD_TYPE_BTREE_MULTIMAP;

  IndexInfo* index_info = new IndexInfo( index_name, 
                                         index_oid,
                                         table_name, 
                                         method_type,
                                         type,
                                         Istmt->unique,
                                         key_column_names);
  return index_info;
}

/**
 * @brief Set Reference Tables
 * @param reference table namees  reference table names 
 * @param relation_oid relation oid 
 * @return true if we set the reference tables, false otherwise
 */
bool DDL::SetReferenceTables( std::vector<catalog::ForeignKeyInfo>& reference_table_infos, 
                              oid_t relation_oid ){
  assert( relation_oid );
  oid_t database_oid = GetCurrentDatabaseOid();
  assert( database_oid );
  storage::Database* db = storage::Database::GetDatabaseById( database_oid );
  storage::DataTable* current_table = db->GetTableById( relation_oid );

  for( auto reference_table_info : reference_table_infos) {
<<<<<<< HEAD
    storage::DataTable* current_table = (storage::DataTable*) catalog::Manager::GetInstance().GetTable(database_oid, relation_oid);
=======
>>>>>>> bridge
    current_table->AddReferenceTable( &reference_table_info );
  }

  return true;
}

/**
 * @brief Create the indexes using IndexInfos and add it to the table
 * @param relation_oid relation oid 
 * @return true if we create all the indexes, false otherwise
 */
bool DDL::CreateIndexesWithIndexInfos( std::vector<IndexInfo> index_infos ){

  for( auto index_info : index_infos){
    bool status;
    status = bridge::DDL::CreateIndex( index_info );

    fprintf(stderr, "DDLCreateIndex %s :: %d \n", index_info.GetIndexName().c_str(), status);
  }
  index_infos.clear();
  return true;
}

/**
 * @brief Add new constraint to the table
 * @param relation_oid relation oid 
 * @param constraint constraint 
 * @return true if we add the constraint, false otherwise
 */
bool DDL::AddConstraint(Oid relation_oid, Constraint* constraint )
{

  ConstraintType contype = PostgresConstraintTypeToPelotonConstraintType( (PostgresConstraintType) constraint->contype );
  std::vector<catalog::ForeignKeyInfo> reference_table_infos;

  std::string conname;
  if( constraint->conname != NULL){
    conname = constraint->conname;
  }else{
    conname = "";
  }

  // FIXME
  // Create a new constraint 
  //catalog::Constraint* new_constraint;


  switch( contype ){
    std::cout << "const type : " << ConstraintTypeToString( contype ) << std::endl;

    case CONSTRAINT_TYPE_FOREIGN:
    {
      oid_t database_oid = GetCurrentDatabaseOid();
      assert( database_oid );
      storage::Database* db = storage::Database::GetDatabaseById( GetCurrentDatabaseOid() );

      // PrimaryKey Table
      oid_t PrimaryKeyTableId = db->GetTableIdByName( constraint->pktable->relname );

      // Each table column names
      std::vector<std::string> pk_column_names;
      std::vector<std::string> fk_column_names;

      ListCell   *column;
      if( constraint->pk_attrs != NULL && constraint->pk_attrs->length > 0 ){
        foreach(column, constraint->pk_attrs ){
          char* attname = strVal(lfirst(column));
          pk_column_names.push_back(  attname  );
        }
      }
      if( constraint->fk_attrs != NULL && constraint->fk_attrs->length > 0 ){
        foreach(column, constraint->fk_attrs ){
          char* attname = strVal(lfirst(column));
          fk_column_names.push_back(  attname  );
        }
      }

      catalog::ForeignKeyInfo *reference_table_info = new catalog::ForeignKeyInfo( PrimaryKeyTableId,
                                                                                           pk_column_names,  
                                                                                           fk_column_names,  
                                                                                           constraint->fk_upd_action,  
                                                                                           constraint->fk_del_action, 
                                                                                           conname );
      //new_constraint  = new catalog::Constraint( contype, conname);
      reference_table_infos.push_back( *reference_table_info );

    }
    break;
    default:
      LOG_WARN("Unrecognized constraint type %d\n", (int) contype );
      break;
  }

  // FIXME : 
  bool status = bridge::DDL::SetReferenceTables( reference_table_infos, relation_oid );
  if( status == false ){
    LOG_WARN("Failed to set reference tables");
  } 

  return true;
}

} // namespace bridge
} // namespace peloton
