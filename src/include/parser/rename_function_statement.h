//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// rename_function_statement.h
//
// Identification: src/include/parser/rename_function_statement.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/sql_statement.h"
#include "common/sql_node_visitor.h"

namespace peloton {
namespace parser {
/** @brief Rename statement
 *  The statement that used for transform from postgress statement
 *  to Peloton statement
 */
class RenameFuncStatement : public TableRefStatement {
 public:
  enum class ObjectType { INVALID = 0, COLUMN = 1 };
  RenameFuncStatement(ObjectType type)
      : TableRefStatement(StatementType::RENAME),
        type(type),
        oldName(nullptr),
        newName(nullptr) {}

  virtual ~RenameFuncStatement() {
    if (oldName) delete oldName;
    if (newName) delete newName;
  }

  virtual void Accept(SqlNodeVisitor *v) override { v->Visit(this); }

  const std::string GetInfo(UNUSED_ATTRIBUTE int num_indent) const override {
    return std::string{""};
  }

  const std::string GetInfo() const override { return std::string{""}; }

  ObjectType type;

  // the name that needs to be changed
  char *oldName;
  char *newName;
};
}
}