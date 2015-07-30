/*-------------------------------------------------------------------------
 *
 * ddl_utilities.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/bridge/ddl_utilities.cpp
 *
 *-------------------------------------------------------------------------
 */

#include <cassert>
#include <iostream>

#include "backend/bridge/ddl/ddl_utils.h"
#include "backend/common/logger.h"
#include "backend/storage/database.h"

#include "miscadmin.h"
#include "parser/parse_utilcmd.h"
#include "parser/parse_type.h"
#include "access/htup_details.h"
#include "utils/resowner.h"
#include "utils/syscache.h"
#include "catalog/pg_type.h"
#include "commands/dbcommands.h"

namespace peloton {
namespace bridge {

//===--------------------------------------------------------------------===//
// DDL Utils
//===--------------------------------------------------------------------===//

/**
 * @brief parsing create statement
 * @param Cstmt a create statement
 * @param column_infos to create a table
 * @param refernce_table_infos to store reference table to the table
 */
void DDLUtils::ParsingCreateStmt(
    CreateStmt* Cstmt, std::vector<catalog::Column>& column_infos,
    std::vector<catalog::ForeignKey>& foreign_keys) {
  assert(Cstmt);

  //===--------------------------------------------------------------------===//
  // Column Infomation
  //===--------------------------------------------------------------------===//

  // Get the column list from the create statement
  List* ColumnList = (List*)(Cstmt->tableElts);

  // Parse the CreateStmt and construct ColumnInfo
  ListCell* entry;
  foreach (entry, ColumnList) {
    ColumnDef* coldef = static_cast<ColumnDef*>(lfirst(entry));

    // Get the type oid and type mod with given typeName
    Oid typeoid = typenameTypeId(NULL, coldef->typeName);
    int32 typemod;
    typenameTypeIdAndMod(NULL, coldef->typeName, &typeoid, &typemod);

    // Get type length
    Type tup = typeidType(typeoid);
    int typelen = typeLen(tup);
    ReleaseSysCache(tup);

    // For a fixed-size type, typlen is the number of bytes in the internal
    // representation of the type. But for a variable-length type, typlen is
    // negative.
    if (typelen == -1) typelen = typemod;

    ValueType column_valueType =
        PostgresValueTypeToPelotonValueType((PostgresValueType)typeoid);
    int column_length = typelen;
    std::string column_name = coldef->colname;

    //===--------------------------------------------------------------------===//
    // Column Constraint
    //===--------------------------------------------------------------------===//

    std::vector<catalog::Constraint> column_constraints;

    if (coldef->raw_default != NULL) {
      catalog::Constraint constraint(CONSTRAINT_TYPE_DEFAULT, "",
                                     coldef->raw_default);
      column_constraints.push_back(constraint);
    };

    if (coldef->constraints != NULL) {
      ListCell* constNodeEntry;

      foreach (constNodeEntry, coldef->constraints) {
        Constraint* ConstraintNode =
            static_cast<Constraint*>(lfirst(constNodeEntry));
        ConstraintType contype;
        std::string conname;

        // CONSTRAINT TYPE
        contype = PostgresConstraintTypeToPelotonConstraintType(
            (PostgresConstraintType)ConstraintNode->contype);

        // CONSTRAINT NAME
        if (ConstraintNode->conname != NULL) {
          conname = ConstraintNode->conname;
        } else {
          conname = "";
        }

        catalog::Constraint* constraint;

        switch (contype) {
          case CONSTRAINT_TYPE_NULL:
            constraint = new catalog::Constraint(contype, conname);
            break;
          case CONSTRAINT_TYPE_NOTNULL:
            constraint = new catalog::Constraint(contype, conname);
            break;
          case CONSTRAINT_TYPE_CHECK:
            constraint = new catalog::Constraint(contype, conname,
                                                 ConstraintNode->raw_expr);
            break;
          case CONSTRAINT_TYPE_PRIMARY:
            constraint = new catalog::Constraint(contype, conname);
            break;
          case CONSTRAINT_TYPE_UNIQUE:
            continue;
          case CONSTRAINT_TYPE_FOREIGN: {
            // REFERENCE TABLE NAME AND ACTION OPTION
            if (ConstraintNode->pktable != NULL) {
              auto& manager = catalog::Manager::GetInstance();
              storage::Database* db =
                  manager.GetDatabaseWithOid(Bridge::GetCurrentDatabaseOid());

              // PrimaryKey Table
              oid_t PrimaryKeyTableId =
                  db->GetTableWithName(ConstraintNode->pktable->relname)
                      ->GetOid();

              // Each table column names
              std::vector<std::string> pk_column_names;
              std::vector<std::string> fk_column_names;

              ListCell* column;
              if (ConstraintNode->pk_attrs != NULL &&
                  ConstraintNode->pk_attrs->length > 0) {
                foreach (column, ConstraintNode->pk_attrs) {
                  char* attname = strVal(lfirst(column));
                  pk_column_names.push_back(attname);
                }
              }
              if (ConstraintNode->fk_attrs != NULL &&
                  ConstraintNode->fk_attrs->length > 0) {
                foreach (column, ConstraintNode->fk_attrs) {
                  char* attname = strVal(lfirst(column));
                  fk_column_names.push_back(attname);
                }
              }

              catalog::ForeignKey* foreign_key = new catalog::ForeignKey(
                  PrimaryKeyTableId, pk_column_names, fk_column_names,
                  ConstraintNode->fk_upd_action, ConstraintNode->fk_del_action,
                  conname);

              foreign_keys.push_back(*foreign_key);
            }
            continue;
          }
          default: {
            constraint = new catalog::Constraint(contype, conname);
            LOG_WARN("Unrecognized constraint type %d\n", (int)contype);
            break;
          }
        }

        column_constraints.push_back(*constraint);
      }
    }  // end of parsing constraint

    catalog::Column column_info(column_valueType, column_length, column_name,
                                false);

    // Insert column_info into ColumnInfos
    column_infos.push_back(column_info);
  }  // end of parsing column list
}

}  // namespace bridge
}  // namespace peloton
