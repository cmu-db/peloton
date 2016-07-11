
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
 

#include "parser/create_parse.h"

#include "common/types.h"
#include "common/logger.h"
#include "catalog/column.h"
#include "catalog/schema.h"

namespace peloton {
namespace parser {

CreateParse::CreateParse(UNUSED_ATTRIBUTE CreateStatement *create_node) {
}

}  // namespace parser
}  // namespace peloton


