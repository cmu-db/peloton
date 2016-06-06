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

#include "common/types.h"
#include "common/value.h"
#include "storage/data_table.h"
#include "catalog/schema.h"

#include <vector>
#include <memory>

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
// Variable
//===--------------------------------------------------------------------===//
struct Variable : QueryExpression {
  Variable(oid_t base_table_oid, oid_t column_index, catalog::Column col);

  virtual ExpressionType GetExpressionType() const override;

  virtual void accept(QueryNodeVisitor *v) const override;

  oid_t base_table_oid;
  oid_t column_index;
  catalog::Column column;
};

//===--------------------------------------------------------------------===//
// Constant
//===--------------------------------------------------------------------===//
struct Constant : QueryExpression {
  Constant(Value value);

  virtual ExpressionType GetExpressionType() const override;

  virtual void accept(QueryNodeVisitor *v) const override;

  Value value;
};

//===--------------------------------------------------------------------===//
// OperatorExpression - matches with Peloton's operator_expression.h
//===--------------------------------------------------------------------===//
struct OperatorExpression : QueryExpression {
  OperatorExpression(peloton::ExpressionType type, ValueType value_type,
                     const std::vector<QueryExpression *> &args);

  virtual ExpressionType GetExpressionType() const override;

  virtual void accept(QueryNodeVisitor *v) const override;

  peloton::ExpressionType type;
  ValueType value_type;
  std::vector<QueryExpression *> args;
};

//===--------------------------------------------------------------------===//
// Logical Operators
//===--------------------------------------------------------------------===//
struct AndOperator : QueryExpression {
  AndOperator(const std::vector<QueryExpression *> args);

  virtual ExpressionType GetExpressionType() const override;

  virtual void accept(QueryNodeVisitor *v) const override;

  std::vector<QueryExpression *> args;
};

struct OrOperator : QueryExpression {
  OrOperator(const std::vector<QueryExpression *> args);

  virtual ExpressionType GetExpressionType() const override;

  virtual void accept(QueryNodeVisitor *v) const override;

  std::vector<QueryExpression *> args;
};

struct NotOperator : QueryExpression {
  NotOperator(QueryExpression *arg);

  virtual ExpressionType GetExpressionType() const override;

  virtual void accept(QueryNodeVisitor *v) const override;

  QueryExpression *arg;
};

//===--------------------------------------------------------------------===//
// Attribute
//===--------------------------------------------------------------------===//
struct Attribute : QueryExpression {
  Attribute(QueryExpression *expression, std::string name, bool intermediate);

  virtual ExpressionType GetExpressionType() const override;

  void accept(QueryNodeVisitor *v) const override;

  QueryExpression *expression;
  std::string name;
  bool intermediate;
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
// Table
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
