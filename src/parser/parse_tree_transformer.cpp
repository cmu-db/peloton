//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// parse_tree_transformer.cpp
//
// Identification: src/parser/parse_tree_transformer.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include <vector>
#include <memory>

#include "common/logger.h"
#include "parser/postgres.h"
#include "parser/parse_tree_transformer.h"
#include "parser/nodes/nodes.h"
#include "parser/nodes/parsenodes.h"

#include "parser/peloton/abstract_parse.h"
#include "parser/peloton/insert_parse.h"
#include "parser/peloton/drop_parse.h"
#include "parser/peloton/create_parse.h"
#include "parser/peloton/select_parse.h"

namespace peloton {
namespace parser {

/**
 * @brief Pretty print the parse tree.
 * @param The parse tree
 * @return none.
 */
void ParseTreeTransformer::PrintParseTree(
    const std::unique_ptr<parser::AbstractParse> &parse_tree,
    std::string prefix) {
  if (parse_tree.get() == nullptr) {
    LOG_INFO("Done printing");
    return;
  }

  prefix += "  ";

  LOG_INFO("%s->Parse Type :: %d ", prefix.c_str(),
           parse_tree->GetParseNodeType());

  auto &children = parse_tree->GetChildren();

  for (auto &child : children) {
    PrintParseTree(child, prefix);
  }
}

/**
 * @brief Build the parser tree.
 * @param The current peloton parse tree
 * @param The postgres parse tree
 * @return The updated peloton parse tree.
 */
std::unique_ptr<parser::AbstractParse> TransformParseTree(
    std::unique_ptr<parser::AbstractParse> &root,
    const Node *postgres_parse_tree) {

  LOG_INFO("Transform Parse Tree : %p ", postgres_parse_tree);

  // Base case
  if (postgres_parse_tree == nullptr) {
    return std::move(root);
  }

  std::unique_ptr<parser::AbstractParse> child_parse_tree;

  auto parse_node_type = postgres_parse_tree->type;
  LOG_INFO("Parse node type : %d", parse_node_type);

  switch (parse_node_type) {

    case T_Invalid:
      LOG_ERROR("Invalid parse node type ");
      break;

    case T_CreateStmt:
	LOG_INFO("Move on Up");
    	child_parse_tree.reset(new parser::CreateParse((CreateStmt *) postgres_parse_tree));
      break;

    case T_DropStmt:
      child_parse_tree.reset(
          new parser::DropParse((DropStmt *)postgres_parse_tree));
      break;

    case T_CreatedbStmt:
      break;

    case T_DropdbStmt:
      break;

    case T_SelectStmt:
      child_parse_tree.reset(
          new parser::SelectParse((SelectStmt *)postgres_parse_tree));
      break;

    case T_InsertStmt:
      break;

    case T_UpdateStmt:
      break;

    case T_DeleteStmt:
      break;

    case T_PrepareStmt:
      break;

    case T_ExecuteStmt:
      break;

    default:
      LOG_ERROR("Unsupported parse node type : %d ", parse_node_type);
      break;
  }

  // Base case
  if (child_parse_tree.get() != nullptr) {

    if (root.get() != nullptr) {
      LOG_INFO("Attach child");
      root->AddChild(std::move(child_parse_tree));
    } else {
      LOG_INFO("Set root");
      root = std::move(child_parse_tree);
    }
  }

  return std::move(root);
}

std::unique_ptr<parser::AbstractParse> ParseTreeTransformer::BuildParseTree(
    Node *postgres_parse_tree) {

  std::unique_ptr<parser::AbstractParse> parse_tree;

  // Transform the postgres_parsetree and place it in parse tree
  parse_tree = TransformParseTree(parse_tree, postgres_parse_tree);

  // Print parse tree
  PrintParseTree(parse_tree);

  return parse_tree;
}

}  // namespace parser
}  // namespace peloton
