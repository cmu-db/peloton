//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// abstract_parser.h
//
// Identification: src/include/parser/abstract_parser.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "parser/abstract_parse.h"

namespace peloton {
namespace parser {

//===--------------------------------------------------------------------===//
// Abstract Parser
//===--------------------------------------------------------------------===//

class AbstractParser {
 public:
  AbstractParser(const AbstractParser &) = delete;
  AbstractParser &operator=(const AbstractParser &) = delete;
  AbstractParser(AbstractParser &&) = delete;
  AbstractParser &operator=(AbstractParser &&) = delete;

  AbstractParser();
  virtual ~AbstractParser();

  virtual std::unique_ptr<parser::AbstractParse> BuildParseTree(
      const std::string &query_string) = 0;
};

}  // End parser namespace
}  // End peloton namespace
