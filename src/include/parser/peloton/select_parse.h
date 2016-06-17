//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// select_parse.h
//
// Identification: src/include/parser/select_parse.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/peloton/abstract_parse.h"
#include "parser/peloton/parse_node_visitor.h"

#include "common/types.h"
#include "common/logger.h"

namespace peloton {
namespace parser {

class SelectParse : public AbstractParse {
 public:
  SelectParse() = delete;
  SelectParse(const SelectParse &) = delete;
  SelectParse &operator=(const SelectParse &) = delete;
  SelectParse(SelectParse &&) = delete;
  SelectParse &operator=(SelectParse &&) = delete;

  explicit SelectParse(SelectStmt *select_node) {
    entity_type = ENTITY_TYPE_TABLE;

    ListCell *object_item;
    List *object_list = drop_node->objects;
    ListCell *subobject_item;
    List *subobject_list;

    // Value
    foreach(object_item, object_list) {
      subobject_list = lfirst(object_item);

      foreach(subobject_item, subobject_list) {
        ::Value *value = (::Value *)lfirst(subobject_item);
        LOG_INFO("Table : %s ", strVal(value));
        entity_name = std::string(strVal(value));
      }
    }

    // Convert range table list into vector for easy access
    ListCell *o_target;
    foreach(o_target, pg_query->rtable) {
      RangeTblEntry *rte = static_cast<RangeTblEntry *>(lfirst(o_target));
      rte_entries.push_back(rte);
    }

    // Cannot handle other case right now
    assert(list_length(select_node->fromClause) == 1);

    // Convert join tree
    LOG_DEBUG("Converting parse jointree");
    auto join_and_where_pred = ConvertFromExpr(pg_query->jointree);
    std::unique_ptr<QueryJoinNode> join_tree(join_and_where_pred.first);
    std::unique_ptr<QueryExpression> where_predicate(
        join_and_where_pred.second);
    if (join_tree == nullptr) return query;

    std::vector<std::unique_ptr<Attribute>> output_list;
    // Convert target list
    LOG_DEBUG("Converting Query targetList");
    foreach(o_target, pg_query->targetList) {
      TargetEntry *tle = static_cast<TargetEntry *>(lfirst(o_target));

      // Can ignore for now?
      if (tle->resjunk) continue;

      std::unique_ptr<Attribute> attribute{ConvertTargetEntry(tle)};
      if (attribute == nullptr) return query;

      output_list.push_back(std::move(attribute));
    }

    // Convert sort clauses
    LOG_DEBUG("Converting Query sortClauses");
    std::vector<std::unique_ptr<OrderBy>> orderings;
    if (pg_query->sortClause) {
      List *sort_list = pg_query->sortClause;
      ListCell *list_item;

      foreach(list_item, sort_list) {
        SortGroupClause *sort_clause = (SortGroupClause *)lfirst(list_item);
        OrderBy *order =
            ConvertSortGroupClause(sort_clause, pg_query->targetList);
        orderings.emplace_back(order);
      }
    }

    {
      std::vector<Attribute *> attrs;
      for (std::unique_ptr<Attribute> &at : output_list) {
        attrs.push_back(at.release());
      }
      std::vector<OrderBy *> ords;
      for (std::unique_ptr<OrderBy> &ord : orderings) {
        ords.push_back(ord.release());
      }
      query = new Select(join_tree.release(), where_predicate.release(), attrs,
                         ords);
    }
  }

  inline ParseNodeType GetParseNodeType() const {
    return PARSE_NODE_TYPE_SELECT;
  }

  const std::string GetInfo() const { return "SelectParse"; }
  void accept(ParseNodeVisitor *v) const;

 private:
  AbstractParse *join_tree;
  AbstractExpressionParse *where_predicate;
  std::vector<Attribute *> output_list;
  std::vector<OrderBy *> orderings;
  // Type of entity
  EntityType entity_type = ENTITY_TYPE_INVALID;

  // Name of entity
  std::string entity_name;
};

}  // namespace parser
}  // namespace peloton
