/*-------------------------------------------------------------------------
 *
 * ddl.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/bridge/ddl.cpp
 *
 *-------------------------------------------------------------------------
 */

#include <cassert>
#include <iostream>

#include "backend/bridge/ddl_table.h"
#include "backend/bridge/ddl_index.h"
#include "backend/bridge/ddl_utils.h"
#include "backend/bridge/ddl_database.h"
#include "backend/bridge/ddl_transaction.h"
#include "backend/bridge/ddl.h"
#include "backend/bridge/bridge.h"
#include "backend/catalog/schema.h"
#include "backend/catalog/manager.h"
#include "backend/common/logger.h"
#include "backend/storage/database.h"

#include "postgres.h"
#include "miscadmin.h"
#include "c.h"
#include "nodes/parsenodes.h"
#include "parser/parse_utilcmd.h"
#include "parser/parse_type.h"
#include "access/htup_details.h"
#include "utils/resowner.h"
#include "utils/syscache.h"
#include "catalog/pg_type.h"
#include "commands/dbcommands.h"

#include "parser/parse_node.h" // make pstate cook default
#include "parser/parse_expr.h" // cook default

namespace peloton {
namespace bridge {

static std::vector<IndexInfo> index_infos;

//===--------------------------------------------------------------------===//
// Utilities.
//===--------------------------------------------------------------------===//

/**
 * @brief Process utility statement.
 * @param parsetree Parse tree
 */
void DDL::ProcessUtility(Node *parsetree,
                         const char *queryString,
                         TransactionId txn_id){
  assert(parsetree != nullptr);
  assert(queryString != nullptr);

  /* When we call a backend function from different thread, the thread's stack
   * is at a different location than the main thread's stack. so it sets up
   * reference point for stack depth checking
   */
  set_stack_base();

  // Process depending on type of utility statement
  switch (nodeTag(parsetree))
  {
    case T_CreatedbStmt:
    {
      CreatedbStmt* CdbStmt = (CreatedbStmt*) parsetree;
      DDLDatabase::CreateDatabase(CdbStmt->database_id);
    }
    break;

    case T_CreateStmt:
    case T_CreateForeignTableStmt:
    {

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

          std::vector<catalog::Column> column_infos;
          std::vector<catalog::ForeignKey> foreign_keys;

          bool status;

          //===--------------------------------------------------------------------===//
          // CreateStmt --> ColumnInfo --> CreateTable
          //===--------------------------------------------------------------------===//
          if(schema != NULL){
            DDLUtils::ParsingCreateStmt(Cstmt,
                                        column_infos,
                                        foreign_keys);

            DDLTable::CreateTable(relation_oid,
                                  relation_name,
                                  column_infos);
          } else {
            // SPECIAL CASE : CREATE TABLE WITHOUT COLUMN INFO
            DDLTable::CreateTable(relation_oid,
                                  relation_name,
                                  column_infos);
          }


          //===--------------------------------------------------------------------===//
          // Check Constraint
          //===--------------------------------------------------------------------===//
          // TODO : Cook the data..
          //          if(Cstmt->constraints != NULL){
          //            oid_t database_oid = Bridge::GetCurrentDatabaseOid();
          //            assert(database_oid);
          //            auto table = catalog::Manager::GetInstance().GetLocation(database_oid, relation_oid);
          //            storage::DataTable* data_table = (storage::DataTable*) table;
          //
          //            ListCell* constraint;
          //            foreach(constraint, Cstmt->constraints)
          //            {
          //              Constraint* ConstraintNode = (Constraint*) lfirst(constraint);
          //
          //              // Or we can get cooked infomation from catalog
          //              //ex)
          //              //ConstrCheck *check = rel->rd_att->constr->check;
          //              //AttrDefault *defval = rel->rd_att->constr->defval;
          //
          //              if(ConstraintNode->raw_expr != NULL){
          //                data_table->SetRawCheckExpr(ConstraintNode->raw_expr);
          //              }
          //            }
          //          }

          //===--------------------------------------------------------------------===//
          // Set Reference Tables
          //===--------------------------------------------------------------------===//
          status = DDLTable::SetReferenceTables(foreign_keys,
                                                relation_oid);
          if(status == false){
            LOG_WARN("Failed to set reference tables");
          }


          //===--------------------------------------------------------------------===//
          // Add Primary Key and Unique Indexes to the table
          //===--------------------------------------------------------------------===//
          status = DDLIndex::CreateIndexes(index_infos);
          if(status == false){
            LOG_WARN("Failed to create primary key and unique index");
          }

        }
      }
    }
    break;

    case T_IndexStmt:  /* CREATE INDEX */
    {
      IndexStmt  *Istmt = (IndexStmt *) parsetree;

      // Construct IndexInfo
      IndexInfo* index_info = DDLIndex::ConstructIndexInfoByParsingIndexStmt(Istmt);

      // If table has not been created yet, skip the rest part of this function
      auto& manager = catalog::Manager::GetInstance();
      storage::Database* db = manager.GetDatabaseWithOid(Bridge::GetCurrentDatabaseOid());

      if(nullptr == db->GetTableWithName(Istmt->relation->relname)){
        index_infos.push_back(*index_info);
        break;
      }

      DDLIndex::CreateIndex(*index_info);
    }
    break;

    case T_AlterTableStmt:
    {

      break; // still working on here....

      AlterTableStmt *atstmt = (AlterTableStmt *) parsetree;
      Oid     relation_oid = atstmt->relation_id;
      List     *stmts = transformAlterTableStmt(relation_oid, atstmt, queryString);

      /* ... and do it */
      ListCell   *l;
      foreach(l, stmts){

        Node *stmt = (Node *) lfirst(l);

        if (IsA(stmt, AlterTableStmt)){
          DDLTable::AlterTable(relation_oid, (AlterTableStmt*)stmt);

        }
      }
    }
    break;

    case T_DropdbStmt:
    {
      DropdbStmt *Dstmt = (DropdbStmt *) parsetree;

      Oid database_oid = get_database_oid(Dstmt->dbname, Dstmt->missing_ok);

      DDLDatabase::DropDatabase(database_oid);
    }
    break;

    case T_DropStmt:
    {
      DropStmt* drop = (DropStmt*) parsetree; // TODO drop->behavior;   /* RESTRICT or CASCADE behavior */

      ListCell  *cell;
      foreach(cell, drop->objects){
        List* names = ((List *) lfirst(cell));


        switch(drop->removeType){

          case OBJECT_DATABASE:
          {
            char* database_name = strVal(linitial(names));
            Oid database_oid = get_database_oid(database_name, true);

            DDLDatabase::DropDatabase(database_oid);
          }
          break;

          case OBJECT_TABLE:
          {
            char* table_name = strVal(linitial(names));
            Oid table_oid = Bridge::GetRelationOid(table_name);

            DDLTable::DropTable(table_oid);
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

    case T_TransactionStmt: {
      TransactionStmt *stmt = (TransactionStmt *) parsetree;

      DDLTransaction::ExecTransactionStmt(stmt, txn_id);
    }
    break;

    default: {
      LOG_WARN("unrecognized node type: %d", (int) nodeTag(parsetree));
    }
    break;
  }

  // TODO :: This is for debugging
  //storage::Database* db = storage::Database::GetDatabaseById(Bridge::GetCurrentDatabaseOid());
  //std::cout << *db << std::endl;

}

} // namespace bridge
} // namespace peloton

