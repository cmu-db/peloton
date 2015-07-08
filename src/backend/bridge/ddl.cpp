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

#include "parser/parse_node.h" // make pstate cook default
#include "parser/parse_expr.h" // cook default
#include "parser/parse_expr.h" // cook default


namespace peloton {
namespace bridge {

static std::vector<IndexInfo> index_infos;

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

      /* Run parse analysis ... */
      stmts = transformCreateStmt((CreateStmt *) parsetree,
                                  queryString);

      /* ... and do it */
      foreach(l, stmts)
      {
        Node     *stmt = (Node *) lfirst(l);
        if (IsA(stmt, CreateStmt))
        {
          CreateStmt* Cstmt = (CreateStmt*)stmt;
          List* schema = (List*)(Cstmt->tableElts);

          char* relation_name = Cstmt->relation->relname;
          Oid relation_oid = ((CreateStmt *)parsetree)->relation_id;

          std::vector<peloton::catalog::ColumnInfo> column_infos;
          std::vector<std::string> reference_table_names;

          bool status;

          //===--------------------------------------------------------------------===//
          // CreateStmt --> ColumnInfo --> CreateTable
          //===--------------------------------------------------------------------===//
          if( schema != NULL ){
            column_infos = peloton::bridge::DDL::ConstructColumnInfoByParsingCreateStmt( Cstmt, reference_table_names );
            status = peloton::bridge::DDL::CreateTable( relation_oid, relation_name, column_infos );
          }
          else {
            // Create Table without column info
            status = peloton::bridge::DDL::CreateTable( relation_oid, relation_name, column_infos );
          }

          fprintf(stderr, "DDL_CreateTable(%s) :: Oid : %d Status : %d \n", relation_name, relation_oid, status);

          //===--------------------------------------------------------------------===//
          // Check Constraint
          //===--------------------------------------------------------------------===//
          // TODO : Cook the data..
          if( Cstmt->constraints != NULL){
            oid_t database_oid = GetCurrentDatabaseOid();
            assert( database_oid );
            auto table = catalog::Manager::GetInstance().GetLocation(database_oid, relation_oid);
            storage::DataTable* data_table = (storage::DataTable*) table;

            ListCell* constraint;
            foreach(constraint, Cstmt->constraints)
            {
              Constraint* ConstraintNode = lfirst(constraint);

              // Or we can get cooked infomation from catalog
              //ex)
              //ConstrCheck *check = rel->rd_att->constr->check;
              //AttrDefault *defval = rel->rd_att->constr->defval;

              if( ConstraintNode->raw_expr != NULL ){
                data_table->SetRawCheckExpr(ConstraintNode->raw_expr);
              }
            }
          }

          //===--------------------------------------------------------------------===//
          // Foreign Keys 
          //===--------------------------------------------------------------------===//
          for( auto reference_table_name : reference_table_names ){
            oid_t database_oid = GetCurrentDatabaseOid();
            assert( database_oid );
            oid_t reference_table_oid = GetRelationOid(reference_table_name.c_str());
            assert(reference_table_oid);

            storage::DataTable* current_table = (storage::DataTable*) catalog::Manager::GetInstance().GetLocation(database_oid, relation_oid);
            storage::DataTable* reference_table = (storage::DataTable*) catalog::Manager::GetInstance().GetLocation(database_oid, reference_table_oid);
            current_table->AddReferenceTable(reference_table);
          }

          //===--------------------------------------------------------------------===//
          // Primary Key and Unique Indexes 
          //===--------------------------------------------------------------------===//
          for( auto index_info : index_infos){
            bool status;
            status = peloton::bridge::DDL::CreateIndex( index_info.GetIndexName(),
                                                        index_info.GetTableName(),
                                                        index_info.GetMethodType(), 
                                                        index_info.GetType(),
                                                        index_info.IsUnique(),
                                                        index_info.GetKeyColumnNames(),
                                                        relation_oid );
            fprintf(stderr, "DDLCreateIndex %s :: %d \n", index_info.GetIndexName().c_str(), status);
          }
          index_infos.clear();

         /*{
            // DEBUGGING CREATE TABLE
            oid_t database_oid = GetCurrentDatabaseOid();
            auto table = peloton::catalog::Manager::GetInstance().GetLocation(database_oid, relation_oid);
            peloton::storage::DataTable* data_table = (peloton::storage::DataTable*) table;
            auto tuple_schema = data_table->GetSchema();

            std::cout << "Relation Name : " << relation_name << "\n" <<  *tuple_schema << std::endl;

            if( data_table->ishasPrimaryKey()  ){
              printf("print primary key index \n");
              std::cout<< *(data_table->GetPrimaryIndex()) << std::endl;
            }
            if ( data_table->ishasUnique()){
              printf("print unique index \n");
              for( int i =0 ; i<  data_table->GetUniqueIndexCount(); i++){
                std::cout << *(data_table->GetUniqueIndex(i)) << std::endl;
              }
            }
            if ( data_table->GetIndexCount() > 0 ){
              printf("print index \n");
              for( int i =0 ; i<  data_table->GetIndexCount(); i++){
                std::cout << *(data_table->GetIndex(i)) << std::endl;
              }
            }
            if ( data_table->ishasReferenceTable() ){
              printf("print foreign tables \n");
              for( int i =0 ; i<  data_table->GetReferenceTableCount(); i++){
                peloton::storage::DataTable *temp_table = data_table->GetReferenceTable(i);
                const peloton::catalog::Schema* temp_our_schema = temp_table->GetSchema();
                std::cout << "table name : " << temp_table->GetName() << " " << *temp_our_schema << std::endl;
              }
            }
          }*/ // DEBUGGING CREATE TABLE
        }
      }
   }


    break;

    case T_IndexStmt:  /* CREATE INDEX */
    {
      bool status;
      IndexStmt  *Istmt = (IndexStmt *) parsetree;

      // Construct IndexInfo 
      IndexInfo* index_info = ConstructIndexInfoByParsingIndexStmt( Istmt );

      // If this index is either unique or primary key, store the index information and skip
      // the rest part since the table has not been created yet.
      if( Istmt->isconstraint ){
        index_infos.push_back(*index_info);
        break;
      }

      status = peloton::bridge::DDL::CreateIndex( index_info->GetIndexName(),
                                                  index_info->GetTableName(),
                                                  index_info->GetMethodType(), 
                                                  index_info->GetType(),
                                                  index_info->IsUnique(),
                                                  index_info->GetKeyColumnNames());

      fprintf(stderr, "DDLCreateIndex %s :: %d \n", index_info->GetIndexName().c_str(), status);

    }
    break;

    case T_AlterTableStmt:
    {
      AlterTableStmt *atstmt = (AlterTableStmt *) parsetree;
      Oid     relation_oid = GetRelationOid( atstmt->relation->relname );
      List     *stmts;
      ListCell   *l;

      /* Run parse analysis ... */
      stmts = transformAlterTableStmt(relation_oid, atstmt, queryString);

      /* ... and do it */
      foreach(l, stmts){
        Node *stmt = (Node *) lfirst(l);

        if (IsA(stmt, AlterTableStmt)){

          // lockmode control..
          /* Do the table alteration proper */
          //AlterTable(relation_oid, lockmode, (AlterTableStmt *) stmt);
          AlterTableStmt* Astmt = (AlterTableStmt*)stmt;

          ListCell* lcmd;
          foreach( lcmd, Astmt->cmds)
          {
            AlterTableCmd *cmd = (AlterTableCmd *) lfirst(lcmd);
            switch (cmd->subtype){
                case AT_AddConstraint:	/* ADD CONSTRAINT */
                printf("name %s \n",cmd->name);
                Constraint* constraint = cmd->def;
                ConstraintType contype = PostgresConstraintTypeToPelotonConstraintType( (PostgresConstraintType) constraint->contype );
                std::cout << "const type : " << ConstraintTypeToString( contype ) << std::endl;
                switch( contype )
                {
                  case CONSTRAINT_TYPE_FOREIGN:
                    oid_t database_oid = GetCurrentDatabaseOid();
                    assert( database_oid );
                    oid_t reference_table_oid = GetRelationOid( constraint->pktable->relname );
                    assert(reference_table_oid);

                    storage::DataTable* current_table = (storage::DataTable*) catalog::Manager::GetInstance().GetLocation(database_oid, relation_oid);
                    storage::DataTable* reference_table = (storage::DataTable*) catalog::Manager::GetInstance().GetLocation(database_oid, reference_table_oid);
                    const char* fk_del_action = nullptr;
                    if( constraint->fk_del_action )
                      fk_del_action = constraint->fk_del_action;

                    current_table->AddReferenceTable(reference_table, fk_del_action);

                    printf("successfully ...\n"); 
                  break;
                }
              break;
            }
          }
        }
      }
    }
    break;

    case T_DropStmt:
    {
      DropStmt* drop = (DropStmt*) parsetree;
      bool status;

      ListCell  *cell;
      foreach(cell, drop->objects){
        if (drop->removeType == OBJECT_TABLE ){
          List* names = ((List *) lfirst(cell));

          char* table_name = strVal(linitial(names));
          Oid table_oid = GetRelationOid(table_name);

          status = peloton::bridge::DDL::DropTable( table_oid );
          fprintf(stderr, "DDLDropTable :: %d \n", status);
        }
      }
    }
    break;
    // End of DropStmt

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
std::vector<catalog::ColumnInfo> DDL::ConstructColumnInfoByParsingCreateStmt( CreateStmt* Cstmt, std::vector<std::string>& reference_table_names){
  assert(Cstmt);

 //===--------------------------------------------------------------------===//
  // Table-level Constraint Information - multi columns check constraint,
  //                                      multi column foreign table references
  //===--------------------------------------------------------------------===//
/*
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
  std::vector<catalog::ColumnInfo> column_infos;

  // Parse the CreateStmt and construct ColumnInfo
  ListCell   *entry;
  foreach(entry, ColumnList){

    ColumnDef  *coldef = lfirst(entry);

    // Parsing the column value type
    Oid typeoid;
    int32 typemod;
    int typelen;


    // Get the type oid and type mod with given typeName
    typeoid = typenameTypeId(NULL, coldef->typeName);
    typenameTypeIdAndMod(NULL, coldef->typeName, &typeoid, &typemod);

    // Get type length
    Type tup = typeidType(typeoid);
    typelen = typeLen(tup);
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

    std::vector<catalog::Constraint> column_constraints;

    if( coldef->raw_default != NULL){
      catalog::Constraint* constraint = new catalog::Constraint( CONSTRAINT_TYPE_DEFAULT, coldef->raw_default );
      column_constraints.push_back(*constraint);
    };

    //===--------------------------------------------------------------------===//
    // Constraint Information
    //===--------------------------------------------------------------------===//

    if( coldef->constraints != NULL){
      ListCell* constNodeEntry;

      foreach(constNodeEntry, coldef->constraints)
      {
        Constraint* ConstraintNode = lfirst(constNodeEntry);
        ConstraintType contype;
        std::string conname;
        std::string reference_table_name;

        // Get constraint type
        contype = PostgresConstraintTypeToPelotonConstraintType( (PostgresConstraintType) ConstraintNode->contype );

        // Get constraint name
        if( ConstraintNode->conname != NULL){
          conname = ConstraintNode->conname;
        }

        // Get reference table name 
        if( ConstraintNode->pktable != NULL ){
          reference_table_name = ConstraintNode->pktable->relname;
          reference_table_names.push_back( reference_table_name );
        }

        catalog::Constraint* constraint = new catalog::Constraint( contype, conname, reference_table_name );
        column_constraints.push_back(*constraint);
      }
    }// end of parsing constraint 

    catalog::ColumnInfo *column_info = new catalog::ColumnInfo( column_valueType, 
                                                                column_length, 
                                                                column_name, 
                                                                column_constraints);

    // Insert column_info into ColumnInfos
    column_infos.push_back(*column_info);
  }// end of parsing column list

  return column_infos;
}

/**
 * @brief Construct IndexInfo from a index statement
 * @param Istmt an index statement 
 * @return IndexInfo  
 */
IndexInfo* DDL::ConstructIndexInfoByParsingIndexStmt( IndexStmt* Istmt ){
  std::string index_name;
  std::string table_name;
  IndexMethodType method_type;
  IndexType type;
  bool unique_keys;
  std::vector<std::string> key_column_names;

  // Table name
  table_name = Istmt->relation->relname;

  // Key column names
  ListCell   *entry;
  foreach(entry, Istmt->indexParams){
    IndexElem *indexElem = lfirst(entry);
    if( indexElem->name != NULL ){
      key_column_names.push_back(indexElem->name);
    }
  }

  // Index name and index type
  if( Istmt->idxname == NULL && Istmt->isconstraint ){
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
    type = INDEX_TYPE_NORMAL;
  }

  // Index method type
  // TODO :: More access method types need
  method_type = INDEX_METHOD_TYPE_BTREE_MULTIMAP;

  IndexInfo* index_info = new IndexInfo( index_name, 
                                         table_name, 
                                         method_type,
                                         type,
                                         Istmt->unique,
                                         key_column_names);
  return index_info;
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

printf("JWKIM DEBUG %s %d \n", __func__, __LINE__);
  // Build a table from schema
  storage::DataTable *table = storage::TableFactory::GetDataTable(database_oid, relation_oid, schema, table_name);

  if(table != nullptr) {
    LOG_INFO("Created table : %s", table_name.c_str());
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
 * @param key_column_names column names for the key table 
 * @return true if we create the index, false otherwise
 */
bool DDL::CreateIndex(std::string index_name,
                      std::string table_name,
                      IndexMethodType  index_method_type,
                      IndexType  index_type,
                      bool unique_keys,
                      std::vector<std::string> key_column_names,
                      Oid table_oid){

  assert( !index_name.empty() );
  assert( !table_name.empty() );
  assert( key_column_names.size() > 0  );

  // NOTE: We currently only support btree as our index implementation
  // TODO : Support other types based on "type" argument
  IndexMethodType our_index_type = INDEX_METHOD_TYPE_BTREE_MULTIMAP;

  // Get the database oid and table oid
  oid_t database_oid = GetCurrentDatabaseOid();
  assert( database_oid );

  if( table_oid == INVALID_OID )
    table_oid = GetRelationOid(table_name.c_str());
  
  assert( table_oid );

  // Get the table location from manager
  auto table = catalog::Manager::GetInstance().GetLocation(database_oid, table_oid);
  storage::DataTable* data_table = (storage::DataTable*) table;
  auto tuple_schema = data_table->GetSchema();

  // Construct key schema
  std::vector<oid_t> key_columns;

  // Based on the key column info, get the oid of the given 'key' columns in the tuple schema
  for( auto key_column_name : key_column_names ){
    for( oid_t  tuple_schema_column_itr = 0; tuple_schema_column_itr < tuple_schema->GetColumnCount();
        tuple_schema_column_itr++){

      // Get the current column info from tuple schema
      catalog::ColumnInfo column_info = tuple_schema->GetColumnInfo(tuple_schema_column_itr);
      // Compare Key Schema's current column name and Tuple Schema's current column name
      if( key_column_name == column_info.name ){
        key_columns.push_back(tuple_schema_column_itr);

        // NOTE :: Since pg_attribute doesn't have any information about primary key and unique key,
        //         I try to store these information when we create an unique and primary key index
        if( index_type == INDEX_TYPE_PRIMARY_KEY ){ 
          catalog::Constraint* constraint = new catalog::Constraint( CONSTRAINT_TYPE_PRIMARY );
          tuple_schema->AddConstraintInColumn( tuple_schema_column_itr, constraint); 
        }else if( index_type == INDEX_TYPE_UNIQUE ){ 
          catalog::Constraint* constraint = new catalog::Constraint( CONSTRAINT_TYPE_UNIQUE );
          tuple_schema->AddConstraintInColumn( tuple_schema_column_itr, constraint);
        }

      }
    }
  }

  auto key_schema = catalog::Schema::CopySchema(tuple_schema, key_columns);

  // Create index metadata and physical index
  index::IndexMetadata* metadata = new index::IndexMetadata(index_name, our_index_type,
                                                            tuple_schema, key_schema,
                                                            unique_keys);
  index::Index* index = index::IndexFactory::GetInstance(metadata);

  // Record the built index in the table
  switch( index_type ){
    case INDEX_TYPE_NORMAL:
      data_table->AddIndex(index);
      break;
    case INDEX_TYPE_PRIMARY_KEY:
      data_table->SetPrimaryIndex(index);
      break;
    case INDEX_TYPE_UNIQUE:
      data_table->AddUniqueIndex(index);
      break;
    default:
      elog(LOG, "unrecognized index type: %d", index_type);
  }

  return true;
}
/*bool DDL::AlterTable(std::string index_name,
                     std::string table_name,
                     IndexMethodType  index_method_type,
                     IndexType  index_type,
                     bool unique_keys,
                     std::vector<std::string> key_column_names,
                      Oid table_oid){
                      */



} // namespace bridge
} // namespace peloton
