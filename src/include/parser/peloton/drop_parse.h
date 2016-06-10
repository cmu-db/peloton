//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// insert_parse.h
//
// Identification: src/include/parser/insert_parse.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "abstract_parse.h"

namespace peloton {
namespace parser {

class DropParse : public AbstractParse {
 public:
  DropParse() = delete;
  DropParse(const DropParse &) = delete;
  DropParse &operator=(const DropParse &) = delete;
  DropParse(DropParse &&) = delete;
  DropParse &operator=(DropParse &&) = delete;

  explicit DropParse(DropStmt *drop_node) {
  }

  inline ParseNodeType GetParseNodeType() const { return PARSE_NODE_TYPE_DROP; }

  const std::string GetInfo() const { return "DropParse"; }

 private:

};

}  // namespace parser
}  // namespace peloton
