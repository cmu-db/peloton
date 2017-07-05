//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// table_ref.cpp
//
// Identification: src/parser/table_ref.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "parser/table_ref.h"
#include "parser/select_statement.h"

namespace peloton {
namespace parser {

TableRef::~TableRef() {
  delete select;
}

}  // End parser namespace
}  // End peloton namespace
