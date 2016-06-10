//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// postgres_parser.h
//
// Identification: src/include/parser/postgres_parser.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "parser/peloton/abstract_parse.h"

namespace peloton {
namespace parser {

//===--------------------------------------------------------------------===//
// Postgres Parser
//===--------------------------------------------------------------------===//

class PostgresParser {

 public:
  PostgresParser(const PostgresParser &) = delete;
  PostgresParser &operator=(const PostgresParser &) = delete;
  PostgresParser(PostgresParser &&) = delete;
  PostgresParser &operator=(PostgresParser &&) = delete;

  PostgresParser();
  virtual ~PostgresParser();

  PostgresParser &GetInstance();

  std::unique_ptr<parser::AbstractParse> BuildParseTree(const std::string& query_string);

};

}  // End parser namespace
}  // End peloton namespace
