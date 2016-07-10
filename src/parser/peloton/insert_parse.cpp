//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// insert_parse.cpp
//
// Identification: /peloton/src/parser/peloton/insert_parse.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "parser/insert_parse.h"

#include "common/logger.h"
#include "catalog/column.h"
#include "common/value.h"
#include "common/value_factory.h"

namespace peloton {
namespace parser {
InsertParse::InsertParse(UNUSED_ATTRIBUTE InsertStatement* insert_node) {
}
}
}
