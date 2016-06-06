//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// table_ref.h
//
// Identification: src/include/parser/table_ref.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <stdio.h>
#include <vector>

#include "expression/abstract_expression.h"
#include "common/types.h"

namespace peloton {
namespace parser {

struct SelectStatement;
struct JoinDefinition;

//  Holds reference to tables.
// Can be either table names or a select statement.
struct TableRef {
  TableRef(TableReferenceType type)
      : type(type),
        schema(NULL),
        name(NULL),
        alias(NULL),
        select(NULL),
        list(NULL),
        join(NULL) {}

  virtual ~TableRef();

  TableReferenceType type;

  char* schema;
  char* name;
  char* alias;

  SelectStatement* select;
  std::vector<TableRef*>* list;
  JoinDefinition* join;

  // Convenience accessor methods
  inline bool HasSchema() { return schema != NULL; }

  inline char* GetName() {
    if (alias != NULL)
      return alias;
    else
      return name;
  }
};

// Definition of a join table
struct JoinDefinition {
  JoinDefinition()
      : left(NULL), right(NULL), condition(NULL), type(JOIN_TYPE_INNER) {}

  virtual ~JoinDefinition() {
    delete left;
    delete right;
    delete condition;
  }

  TableRef* left;
  TableRef* right;
  expression::AbstractExpression* condition;

  PelotonJoinType type;
};

}  // End parser namespace
}  // End peloton namespace
