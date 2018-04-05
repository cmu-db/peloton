//
// Created by Nevermore on 01/04/2018.
//

#pragma once

#include "parser/sql_statement.h"
#include "common/sql_node_visitor.h"

namespace peloton {
namespace parser {
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