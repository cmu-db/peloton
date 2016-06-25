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
#include "parser/pg_query_internal.h"
#include "parser/pg_query_json.h"

#include "parser/parser/parser.h"
#include "parser/parser/scanner.h"
#include "parser/parser/scansup.h"

#include "parser/parse_tree_transformer.h"

namespace peloton {
namespace parser {

PostgresParser::PostgresParser(){

  // Initialize the postgres memory context
  pg_query_init();

}

PostgresParser::~PostgresParser(){

  // Destroy the postgres memory context
  pg_query_destroy();

}

PostgresParser &PostgresParser::GetInstance(){
  static PostgresParser postgres_parser;
  return postgres_parser;
}

std::unique_ptr<parser::AbstractParse> PostgresParser::BuildParseTree(
    const std::string& query_string){
  std::unique_ptr<parser::AbstractParse> parse_tree;

  // Get postgres parse tree
  MemoryContext ctx = NULL;
  PgQueryInternalParsetreeAndError internal_result;
  PgQueryParseResult result = {0};

  // Enter the temporary query parsing memory context
  ctx = pg_query_enter_memory_context("pg_query_parse");

  LOG_INFO("Query string : %s", query_string.c_str());

  internal_result = pg_query_raw_parse(query_string.c_str());

  // Convert parse tree to string representation
  if (internal_result.tree != NULL) {
    char *tree_json;
    tree_json = pg_query_nodes_to_json(internal_result.tree);
    result.parse_tree = strdup(tree_json);
    pfree(tree_json);
  } else {
    result.parse_tree = strdup("[]");
  }

  result.stderr_buffer = internal_result.stderr_buffer;
  result.error = internal_result.error;

  // Parsing error
  if (result.error != nullptr) {
    LOG_INFO("input: %s", query_string.c_str());
    LOG_ERROR("error: %s at %d", result.error->message,
              result.error->cursorpos);
    return parse_tree;
  }

  LOG_INFO("Parse Tree : %s %s", result.parse_tree, result.stderr_buffer);

  // Transform parse tree to our representation
  ListCell *parsetree_item;
  List *parsetree_list = internal_result.tree;

  foreach(parsetree_item, parsetree_list){
    Node *parsetree = (Node *) lfirst(parsetree_item);

    parse_tree = std::move(ParseTreeTransformer::BuildParseTree(parsetree));

    // Ignore other statements in list
    break;
  }

  // Exit and clean the temporary query parsing memory context
  pg_query_exit_memory_context(ctx);

  // Clean up the result
  pg_query_free_parse_result(result);

  return parse_tree;
}


}  // End parser namespace
}  // End peloton namespace
