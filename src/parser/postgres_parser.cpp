//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// postgres_parser.cpp
//
// Identification: src/parser/postgres_parser.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "parser/postgres_parser.h"

#include "common/types.h"
#include "common/logger.h"

#include "parser/parser/pg_query.h"

namespace peloton {
namespace parser {

PostgresParser::PostgresParser(){

  // Initialize the postgresult memory context
  pg_query_init();

}

PostgresParser::~PostgresParser(){

  // Destroy the postgresult memory context
  pg_query_destroy();

}

PostgresParser &PostgresParser::GetInstance(){
  static PostgresParser postgres_parser;
  return postgres_parser;
}

std::unique_ptr<parser::AbstractParse> PostgresParser::BuildParseTree(
    const std::string& query_string){
  std::unique_ptr<parser::AbstractParse> parse_tree;

  // TODO: Build parse tree

  return parse_tree;
}


}  // End parser namespace
}  // End peloton namespace
