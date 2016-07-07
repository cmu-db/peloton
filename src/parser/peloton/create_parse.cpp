
//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// create_parse.cpp
//
// Identification: /peloton/src/parser/peloton/create_parse.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
 

#include "parser/peloton/create_parse.h"

#include "common/types.h"
#include "common/logger.h"
#include "catalog/column.h"
#include "catalog/schema.h"

namespace peloton {
namespace parser {

CreateParse::CreateParse(CreateStmt *create_node) {
    columns_name.clear();

    // Get Table name
    RangeVar* rv = create_node->relation;
    entity_name = std::string(rv->relname);

    // Get Table Elements from Parse node
    List *object_list = create_node->tableElts;
    ListCell *object_item;

    foreach(object_item, object_list)
    {

      LOG_INFO("Got Something from tableElts");
      ::Value *value = (::Value *) lfirst(object_item);

      LOG_INFO("Column Name: %s ", strVal(value));
      std::string column_name = std::string(strVal(value));

      // Cast to ColumnDef to access the Column type IDs
      ColumnDef *element = (ColumnDef *) lfirst(object_item);

      // Get the Name
      char* type_name = strVal(linitial(element->typeName->names));

      // Flag to discard using type "pg_catalog"
      if (strcmp(type_name, "pg_catalog") == 0) {
        LOG_INFO("pg_catalog type detected .... switching to next type");

        type_name = strVal(llast(element->typeName->names));
      }

      LOG_INFO("Column type is: %s", type_name);


      int type_size;
      if (element->typeName->typmods == NIL)
        type_size = element->typeName->typemod;

      else {
        ListCell* item;
        foreach(item, element->typeName->typmods)
        {
          Node *tm = (Node *) lfirst(item);


          if (IsA(tm, A_Const)) {
            A_Const *ac = (A_Const *) tm;

            if (IsA(&ac->val, Integer)) {

              type_size = (int) ac->val.val.ival;

              LOG_INFO("Type size: -----> %d", type_size);

            } else if (IsA(&ac->val, Float) || IsA(&ac->val, String)) {

              LOG_INFO("Either Float or String");
            }
          }

        }
      }

      //TODO: Convert to ValueType
      ValueType vType = PostgresStringToValueType(std::string(type_name));


      //TODO: Get Constraints
      std::vector<catalog::Constraint> column_contraints;
      if (element->constraints != NULL) {
        List* con_list = element->constraints;
        ListCell* con_item;

        foreach(con_item, con_list)
        {

          Constraint* con_node = static_cast<Constraint *>lfirst(con_item);
          ConstraintType con_type;

          // Get the constraint Type
              con_type = PostgresConstraintTypeToPelotonConstraintType(
                  (PostgresConstraintType)con_node->contype);

              //Get Constraint Name
              std::string con_name;
              if (con_node->conname != NULL) {
                con_name = std::string(con_node->conname);
              }

              switch (con_type) {
                case CONSTRAINT_TYPE_UNIQUE:
                case CONSTRAINT_TYPE_FOREIGN:
                continue;

                case CONSTRAINT_TYPE_NULL:
                case CONSTRAINT_TYPE_NOTNULL:
                case CONSTRAINT_TYPE_PRIMARY: {
                  catalog::Constraint constraint(con_type, con_name);
                  column_contraints.push_back(constraint);
                  break;
                }
                case CONSTRAINT_TYPE_CHECK: {
                  catalog::Constraint constraint(con_type, con_name,
                      con_node->raw_expr);
                  column_contraints.push_back(constraint);
                  break;
                }
                case CONSTRAINT_TYPE_DEFAULT: {
                  catalog::Constraint constraint(con_type, con_name,
                      element->cooked_default);
                  column_contraints.push_back(constraint);
                  break;
                }
                default: {
                  LOG_TRACE("Unrecognized constraint type %d", (int)con_type);
                  break;
                }
              }

            }
          }
        auto col = catalog::Column(vType, GetTypeSize(vType), column_name, false);

        for(auto constraint : column_contraints)
          col.AddConstraint(constraint);

        columns_name.push_back(col);
        }

}

}  // namespace parser
}  // namespace peloton


