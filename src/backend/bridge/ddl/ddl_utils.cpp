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

        switch(contype){
          case CONSTRAINT_TYPE_UNIQUE:
          case CONSTRAINT_TYPE_FOREIGN:
            continue;

          case CONSTRAINT_TYPE_NULL:
          case CONSTRAINT_TYPE_NOTNULL:
          case CONSTRAINT_TYPE_PRIMARY:{
            catalog::Constraint constraint(contype, conname);
            column_constraints.push_back(constraint);
            break;
          }
          case CONSTRAINT_TYPE_CHECK:{
            catalog::Constraint constraint(contype, conname, ConstraintNode->raw_expr);
            column_constraints.push_back(constraint);
            break;
          }
          case CONSTRAINT_TYPE_DEFAULT:{
            catalog::Constraint constraint(contype, conname, coldef->raw_default);
            column_constraints.push_back(constraint);
            break;
          }
          default:
          {
            LOG_WARN("Unrecognized constraint type %d\n", (int) contype);
            break;
          }
        }
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
