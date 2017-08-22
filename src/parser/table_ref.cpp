//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// table_ref.cpp
//
// Identification: src/parser/table_ref.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "parser/table_ref.h"
#include "parser/select_statement.h"

namespace peloton {
namespace parser {

TableRef::~TableRef() {
  delete table_info_;
  delete[] alias;
  delete[] schema;

  delete select;
  delete join;

  if (list) {
    for (auto ref : (*list)) delete ref;
    delete list;
  }
}

}  // namespace parser
}  // namespace peloton
