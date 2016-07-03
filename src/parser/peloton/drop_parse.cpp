
//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// drop_pasre.cpp
//
// Identification: /peloton/src/parser/peloton/drop_pasre.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
 
#include "parser/peloton/drop_parse.h"

#include "common/types.h"
#include "common/logger.h"

namespace peloton {
namespace parser {

DropParse::DropParse(DropStmt* drop_node) {
    entity_type = ENTITY_TYPE_TABLE;

    ListCell *object_item;
    List *object_list = drop_node->objects;
    ListCell *subobject_item;
    List *subobject_list;

    // Value
    foreach(object_item, object_list){
      subobject_list = (List *)lfirst(object_item);

      foreach(subobject_item, subobject_list) {
        ::Value *value = (::Value *)lfirst(subobject_item);
        LOG_INFO("Table : %s ", strVal(value));
        entity_name = std::string(strVal(value));
        missing = drop_node->missing_ok;
      }
    }
}

}  // namespace parser
}  // namespace peloton
