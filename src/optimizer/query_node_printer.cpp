//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// query_node_printer.cpp
//
// Identification: src/optimizer/query_node_printer.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "optimizer/query_node_printer.h"
#include "optimizer/query_operators.h"
#include "common/types.h"

namespace peloton {
namespace optimizer {

QueryNodePrinter::QueryNodePrinter(Select *op)
  : op_(op), depth_(0), new_line_(false) {}

std::string QueryNodePrinter::print() {
  if (printed_op_.empty()) {
    op_->accept(this);
  }
  return printed_op_;
}

void QueryNodePrinter::visit(const Variable *op) {
  append("Variable: ");
  append("base_table_oid " + std::to_string(op->base_table_oid));
  append(", column_offset " + std::to_string(op->column_index));
  append(", type " + ValueTypeToString(op->column.GetType()));
}

void QueryNodePrinter::visit(const Constant *op) {
  append("Constant: " + op->value.GetInfo());
  (void)op;
}

void QueryNodePrinter::visit(const OperatorExpression* op) {
  push_header("OperatorExpression, type: " +
              ExpressionTypeToString(op->type) +
              ", return_type: " + ValueTypeToString(op->value_type));
  for (QueryExpression *e : op->args) {
    e->accept(this);
    append_line();
  }
  pop(); // OperatorExpression
}

void QueryNodePrinter::visit(const AndOperator *op) {
  push_header("And");
  for (QueryExpression *e : op->args) {
    e->accept(this);
    append_line();
  }
  pop(); // And
}

void QueryNodePrinter::visit(const OrOperator *op) {
  push_header("Or");
  for (QueryExpression *e : op->args) {
    e->accept(this);
    append_line();
  }
  pop(); // Or
}

void QueryNodePrinter::visit(const NotOperator *op) {
  push_header("Not");
  op->arg->accept(this);
  pop();
}

void QueryNodePrinter::visit(const Attribute *op) {
  push_header("Attribute:  " + (op->name != "" ? op->name : "<NO_NAME>") +
              std::string(op->intermediate ? "(intermediate)" : ""));
  op->expression->accept(this);
  pop();
}

void QueryNodePrinter::visit(const Table *op) {
  append("Table: oid " + std::to_string(op->data_table->GetOid()));
}

void QueryNodePrinter::visit(const Join *op) {
  push_header("Join: type " + PelotonJoinTypeToString(op->join_type));
  push_header("Left child");
  op->left_node->accept(this);
  append_line();
  pop(); // Left child
  push_header("Right child");
  op->right_node->accept(this);
  append_line();
  pop(); // Right child
  push_header("Predicate");
  op->predicate->accept(this);
  append_line();
  pop(); // Join
}

void QueryNodePrinter::visit(const OrderBy *op) {
  append("OrderBy: ");
  append("output_column_index " + std::to_string(op->output_list_index));
  append(", ");
  append("equalify_fn " + ExpressionTypeToString(op->equality_fn.exprtype));
  append(", ");
  append("sort_fn " + ExpressionTypeToString(op->sort_fn.exprtype));
  append(", ");
  append("hashable " + std::to_string(op->hashable));
  append(", ");
  append("nulls_first " + std::to_string(op->nulls_first));
  append(", ");
  append("reverse " + std::to_string(op->reverse));

}

void QueryNodePrinter::visit(const Select *op) {
  push_header("Select");

  push_header("Join Tree");
  if (op->join_tree) {
    op->join_tree->accept(this);
    append_line();
  }
  pop(); // Join Tree

  push_header("Where Predicate");
  if (op->where_predicate) {
    op->where_predicate->accept(this);
    append_line();
  }
  pop(); // Where predicate

  push_header("Output list");
  for (size_t i = 0; i < op->output_list.size(); ++i) {
    Attribute *attr = op->output_list[i];
    attr->accept(this);
    append_line();
  }
  pop(); // Output list

  push_header("Orderings");
  for (size_t i = 0; i < op->orderings.size(); ++i) {
    OrderBy *ordering = op->orderings[i];
    ordering->accept(this);
    append_line();
  }
  pop();

  pop(); // Select
}

void QueryNodePrinter::append(const std::string &string) {
  if (new_line_) {
    for (int i = 0; i < depth_; ++i) {
      printed_op_ += "  ";
    }
    new_line_ = false;
  }
  printed_op_ += string;
}

void QueryNodePrinter::append_line(const std::string &string) {
  if (!new_line_ || string != "") {
    append(string + "\n");
    new_line_ = true;
  }
}

void QueryNodePrinter::append_line() {
  append_line("");
}

void QueryNodePrinter::push() {
  depth_++;
}

void QueryNodePrinter::push_header(const std::string &string) {
  append_line(string);
  push();
}

void QueryNodePrinter::pop() {
  depth_--;
  assert(depth_ >= 0);
}

std::string PrintQuery(Select *op) {
  QueryNodePrinter printer(op);
  return printer.print();
}

} /* namespace optimizer */
} /* namespace peloton */
