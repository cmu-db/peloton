//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// ddl_utils.cpp
//
// Identification: src/backend/bridge/ddl/ddl_utils.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <iostream>

#include "backend/bridge/ddl/ddl_utils.h"
#include "backend/bridge/ddl/format_transformer.h"
#include "backend/bridge/ddl/bridge.h"
#include "backend/common/logger.h"
#include "backend/storage/database.h"

#include "postmaster/peloton.h"
#include "parser/parse_type.h"
#include "utils/syscache.h"
#include "miscadmin.h"
#include "parser/parse_utilcmd.h"
#include "access/htup_details.h"
#include "utils/resowner.h"
#include "catalog/pg_type.h"
#include "commands/dbcommands.h"

namespace peloton {
namespace bridge {

//===--------------------------------------------------------------------===//
// DDL Utils
//===--------------------------------------------------------------------===//

/**
 * @brief setting default constraint
 */
void DDLUtils::SetDefaultConstraint(ColumnDef *coldef, int column_itr,
                                    oid_t relation_oid) {
  Relation relation = heap_open(relation_oid, AccessShareLock);
  int num_defva = relation->rd_att->constr->num_defval;
  for (int def_itr = 0; def_itr < num_defva; def_itr++) {
    if (column_itr == relation->rd_att->constr->defval[def_itr].adnum) {
      char *default_expression =
          relation->rd_att->constr->defval[def_itr].adbin;
      coldef->cooked_default =
          static_cast<Node *>(stringToNode(default_expression));
    }
  }
  heap_close(relation, AccessShareLock);
}

/**
 * @brief parsing create statement
 * @param Cstmt a create statement
 * @param column_infos to create a table
 * @param refernce_table_infos to store reference table to the table
 */
void DDLUtils::ParsingCreateStmt(

    CreateStmt *Cstmt, std::vector<catalog::Column> &column_infos){

  assert(Cstmt);

  //===--------------------------------------------------------------------===//
  // Column Infomation
  //===--------------------------------------------------------------------===//

  // Get the column list from the create statement
  List *ColumnList = (List *)(Cstmt->tableElts);

  // Parse the CreateStmt and construct ColumnInfo
  ListCell *entry;
  foreach (entry, ColumnList) {
    ColumnDef *coldef = static_cast<ColumnDef *>(lfirst(entry));

    // Get the type oid and type mod with given typeName
    Oid typeoid = coldef->typeName->type_oid;
    int typelen = coldef->typeName->type_len;


    PostgresValueFormat postgresValueFormat( typeoid, typelen, typelen);
    PelotonValueFormat pelotonValueFormat = FormatTransformer::TransformValueFormat(postgresValueFormat);

    ValueType column_valueType = pelotonValueFormat.GetType();
    int column_length = pelotonValueFormat.GetLength();
    bool is_inlined = pelotonValueFormat.IsInlined();

    std::string column_name = coldef->colname;

    //===--------------------------------------------------------------------===//
    // Column Constraint
    //===--------------------------------------------------------------------===//

    std::vector<catalog::Constraint> column_constraints;

    if (coldef->constraints != NULL) {
      ListCell *constNodeEntry;

      foreach (constNodeEntry, coldef->constraints) {
        Constraint *ConstraintNode =
            static_cast<Constraint *>(lfirst(constNodeEntry));
        ConstraintType contype;

        // CONSTRAINT TYPE
        contype = PostgresConstraintTypeToPelotonConstraintType(
            (PostgresConstraintType)ConstraintNode->contype);

        // CONSTRAINT NAME
        std::string conname;
        if (ConstraintNode->conname != NULL) {
          conname = std::string(ConstraintNode->conname);
        }

        switch (contype) {
          case CONSTRAINT_TYPE_UNIQUE:
          case CONSTRAINT_TYPE_FOREIGN:
            continue;

          case CONSTRAINT_TYPE_NULL:
          case CONSTRAINT_TYPE_NOTNULL:
          case CONSTRAINT_TYPE_PRIMARY: {
            catalog::Constraint constraint(contype, conname);
            column_constraints.push_back(constraint);
            break;
          }
          case CONSTRAINT_TYPE_CHECK: {
            catalog::Constraint constraint(contype, conname,
                                           ConstraintNode->raw_expr);
            column_constraints.push_back(constraint);
            break;
          }
          case CONSTRAINT_TYPE_DEFAULT: {
            catalog::Constraint constraint(contype, conname,
                                           coldef->cooked_default);
            column_constraints.push_back(constraint);
            break;
          }
          default: {
            LOG_WARN("Unrecognized constraint type %d\n", (int)contype);
            break;
          }
        }
      }
    }  // end of parsing constraint

    catalog::Column column_info(column_valueType, column_length, column_name,
                                is_inlined);

    for (auto constraint : column_constraints)
      column_info.AddConstraint(constraint);

    // Insert column_info into ColumnInfos
    column_infos.push_back(column_info);
  }  // end of parsing column list
}

}  // namespace bridge
}  // namespace peloton
