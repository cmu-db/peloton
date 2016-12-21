//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// parse_tree_transformer.h
//
// Identification: src/include/parser/parse_tree_transformer.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

#include "type/types.h"

struct Node;

namespace peloton {
namespace parser {

class AbstractParse;

//===--------------------------------------------------------------------===//
// Parse Tree Transformer
//===--------------------------------------------------------------------===//

class ParseTreeTransformer {
 public:
  ParseTreeTransformer(const ParseTreeTransformer &) = delete;
  ParseTreeTransformer &operator=(const ParseTreeTransformer &) = delete;
  ParseTreeTransformer(ParseTreeTransformer &&) = delete;
  ParseTreeTransformer &operator=(ParseTreeTransformer &&) = delete;

  ParseTreeTransformer(){};

  static void
  PrintParseTree(const std::unique_ptr<parser::AbstractParse>& parse_tree,
                 std::string prefix = "");

  static std::unique_ptr<parser::AbstractParse>
  BuildParseTree(Node *postgres_parse_tree);

};

}  // namespace parser
}  // namespace peloton
