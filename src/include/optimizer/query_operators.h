//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// query_operators.h
//
// Identification: src/include/optimizer/query_operators.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "optimizer/operator_node.h"

#include "catalog/schema.h"
#include "common/types.h"
#include "common/value.h"
#include "storage/data_table.h"

#include <memory>
#include <vector>

namespace peloton {
namespace optimizer {

class QueryNodeVisitor;

enum class QueryJoinNodeType {
  TABLE,
  JOIN,
};

//===--------------------------------------------------------------------===//
// QueryExpression
//===--------------------------------------------------------------------===//
struct QueryExpression {
  QueryExpression(const QueryExpression &) = delete;
  QueryExpression &operator=(const QueryExpression &) = delete;
  QueryExpression(QueryExpression &&) = delete;
  QueryExpression &operator=(QueryExpression &&) = delete;

  QueryExpression();

  virtual ~QueryExpression();

  //===--------------------------------------------------------------------===//
  // Parent Helpers
  //===--------------------------------------------------------------------===//

  const QueryExpression *GetParent() const;

  //===--------------------------------------------------------------------===//
  // Accessors
  //===--------------------------------------------------------------------===//

  virtual ExpressionType GetExpressionType() const = 0;

  virtual void accept(QueryNodeVisitor *v) const = 0;

 private:
  QueryExpression *parent_ = nullptr;
};

//===--------------------------------------------------------------------===//
// QueryJoinNode
//===--------------------------------------------------------------------===//
struct QueryJoinNode {
  QueryJoinNode(const QueryJoinNode &) = delete;
  QueryJoinNode &operator=(const QueryJoinNode &) = delete;
  QueryJoinNode(QueryJoinNode &&) = delete;
  QueryJoinNode &operator=(QueryJoinNode &&) = delete;

  QueryJoinNode();

  virtual ~QueryJoinNode();

  //===--------------------------------------------------------------------===//
  // Parent Helpers
  //===--------------------------------------------------------------------===//

  const QueryJoinNode *GetParent() const;

  //===--------------------------------------------------------------------===//
  // Accessors
  //===--------------------------------------------------------------------===//

  virtual QueryJoinNodeType GetPlanNodeType() const = 0;

  virtual void accept(QueryNodeVisitor *v) const = 0;

 private:
  QueryJoinNode *parent_ = nullptr;
};

//===--------------------------------------------------------------------===//
// Get tuples from a single table
//===--------------------------------------------------------------------===//
struct Table : QueryJoinNode {
  Table(storage::DataTable *data_table);

  virtual QueryJoinNodeType GetPlanNodeType() const override;

  virtual void accept(QueryNodeVisitor *v) const override;

  storage::DataTable *data_table;
};

//===--------------------------------------------------------------------===//
// Join
//===--------------------------------------------------------------------===//
struct Join : QueryJoinNode {
  Join(PelotonJoinType join_type, QueryJoinNode *left_node,
       QueryJoinNode *right_node, QueryExpression *predicate,
       const std::vector<Table *> &left_tables,
       const std::vector<Table *> &right_tables);

  virtual QueryJoinNodeType GetPlanNodeType() const override;

  virtual void accept(QueryNodeVisitor *v) const override;

  PelotonJoinType join_type;

  QueryJoinNode *left_node;

  QueryJoinNode *right_node;

  QueryExpression *predicate;

  // List of all base relations in left node
  std::vector<Table *> left_node_tables;

  // List of all base relations in right node
  std::vector<Table *> right_node_tables;
};

//===--------------------------------------------------------------------===//
// Group By
//===--------------------------------------------------------------------===//
// struct GroupBy : OperatorNode<GroupBy> {
//   static Operator make(Operator a);

//   Operator a;
// };

//===--------------------------------------------------------------------===//
// Order By
//===--------------------------------------------------------------------===//
struct OrderBy {
  OrderBy(int output_list_index, std::vector<bool> equality_fn,
          std::vector<bool> sort_fn, bool hashable, bool nulls_first,
          bool reverse);

  void accept(QueryNodeVisitor *v) const;

  int output_list_index;
  std::vector<bool> equality_fn;
  std::vector<bool> sort_fn;
  bool hashable;
  bool nulls_first;

  bool reverse;
};

//===--------------------------------------------------------------------===//
// Select
//===--------------------------------------------------------------------===//
struct Select {
  Select(QueryJoinNode *join_tree, QueryExpression *where_predicate,
         const std::vector<Attribute *> &output_list,
         const std::vector<OrderBy *> &orderings);

  void accept(QueryNodeVisitor *v) const;

  QueryJoinNode *join_tree;
  // LM: I keep this information as Alex did. However, if we want to reuse the
  // logical transformation across different quries, we have to update this
  // field and any other query specific field when new query comes. Another way
  // to do it is to exclude this information in logical plans and pass metadata
  // to physical plans with real query.
  QueryExpression *where_predicate;
  std::vector<Attribute *> output_list;
  std::vector<OrderBy *> orderings;
};

//===--------------------------------------------------------------------===//
// Aggregate Functions
//===--------------------------------------------------------------------===//
// struct MaxFn : OperatorNode<MaxFn> {
//   static Operator make(Operator a);

//   Operator a;
// };

// struct MinFn : OperatorNode<MinFn> {
//   static Operator make(Operator a);

//   Operator a;
// };

// struct SumFn : OperatorNode<SumFn> {
//   static Operator make(Operator a);

//   Operator a;
// };

// struct AverageFn : OperatorNode<AverageFn> {
//   static Operator make(Operator a);

//   Operator a;
// }

// struct CountFn : OperatorNode<CountFn> {
//   static Operator make(Operator a);

//   Operator a;
// }

//===--------------------------------------------------------------------===//
// Scalar Function
//===--------------------------------------------------------------------===//

// Boolean
// struct AndFn : OperatorNode<AndOperator> {
//   static Operator make(Operator a, Operator b);

//   Operator a;
//   Operator b;
// };

// struct OrFn : OperatorNode<OrOperator> {
//   static Operator make(Operator a, Operator b);

//   Operator a;
//   Operator b;
// };

// struct EqualFn : OperatorNode<EqualFn> {
//   static Operator make(Operator a, Operator b);

//   Operator a;
//   Operator b;
// };

// struct NotEqualFn : OperatorNode<NotEqualFn> {
//   static Operator make(Operator a, Operator b);

//   Operator a;
//   Operator b;
// };

// struct LessFn : OperatorNode<LessFn> {
//   static Operator make(Operator a, Operator b);

//   Operator a;
//   Operator b;
// };

// struct GreaterFn : OperatorNode<GreaterFn> {
//   static Operator make(Operator a, Operator b);

//   Operator a;
//   Operator b;
// };

// struct LessEqualFn : OperatorNode<LessEqualFn> {
//   static Operator make(Operator a, Operator b);

//   Operator a;
//   Operator b;
// };

// struct GreaterEqualFn : OperatorNode<GreaterEqualFn> {
//   static Operator make(Operator a, Operator b);

//   Operator a;
//   Operator b;
// };

} /* namespace optimizer */
} /* namespace peloton */
