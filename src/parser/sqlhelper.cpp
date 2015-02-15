
#include "sqlhelper.h"
#include <stdio.h>
#include <string>

namespace hsql {

void printOperatorExpression(Expr* expr, uint num_indent);

const char* indent(uint num_indent) { return std::string(num_indent, '\t').c_str(); }
void inprint(int64_t val, uint num_indent) { printf("%s%ld  \n", indent(num_indent), val); }
void inprint(float val, uint num_indent) { printf("%s%f\n", indent(num_indent), val); }
void inprint(const char* val, uint num_indent) { printf("%s%s\n", indent(num_indent), val); }
void inprint(const char* val, const char* val2, uint num_indent) { printf("%s%s->%s\n", indent(num_indent), val, val2); }
void inprintC(char val, uint num_indent) { printf("%s%c\n", indent(num_indent), val); }
void inprintU(uint64_t val, uint num_indent) { printf("%s%lu\n", indent(num_indent), val); }

void printTableRefInfo(TableRef* table, uint num_indent) {
  switch (table->type) {
    case kTableName:
      inprint(table->name, num_indent);
      break;
    case kTableSelect:
      printSelectStatementInfo(table->select, num_indent);
      break;
    case kTableJoin:
      inprint("Join Table", num_indent);
      inprint("Left", num_indent+1);
      printTableRefInfo(table->join->left, num_indent+2);
      inprint("Right", num_indent+1);
      printTableRefInfo(table->join->right, num_indent+2);
      inprint("Join Condition", num_indent+1);
      printExpression(table->join->condition, num_indent+2);
      break;
    case kTableCrossProduct:
      for (TableRef* tbl : *table->list) printTableRefInfo(tbl, num_indent);
      break;
  }
  if (table->alias != NULL) {
    inprint("Alias", num_indent+1);
    inprint(table->alias, num_indent+2);
  }
}

void printOperatorExpression(Expr* expr, uint num_indent) {
  if (expr == NULL) { inprint("null", num_indent); return; }

  switch (expr->op_type) {
    case Expr::SIMPLE_OP: inprintC(expr->op_char, num_indent); break;
    case Expr::AND: inprint("AND", num_indent); break;
    case Expr::OR: inprint("OR", num_indent); break;
    case Expr::NOT: inprint("NOT", num_indent); break;
    default: inprintU(expr->op_type, num_indent); break;
  }
  printExpression(expr->expr, num_indent+1);
  if (expr->expr2 != NULL) printExpression(expr->expr2, num_indent+1);
}

void printExpression(Expr* expr, uint num_indent) {
  switch (expr->type) {
    case kExprStar: inprint("*", num_indent); break;
    case kExprColumnRef: inprint(expr->name, num_indent); break;
    // case kExprTableColumnRef: inprint(expr->table, expr->name, num_indent); break;
    case kExprLiteralFloat: inprint(expr->fval, num_indent); break;
    case kExprLiteralInt: inprint(expr->ival, num_indent); break;
    case kExprLiteralString: inprint(expr->name, num_indent); break;
    case kExprFunctionRef: inprint(expr->name, num_indent); inprint(expr->expr->name, num_indent+1); break;
    case kExprOperator: printOperatorExpression(expr, num_indent); break;
    default: fprintf(stderr, "Unrecognized expression type %d\n", expr->type); return;
  }
  if (expr->alias != NULL) {
    inprint("Alias", num_indent+1); inprint(expr->alias, num_indent+2);
  }
}

void printSelectStatementInfo(SelectStatement* stmt, uint num_indent) {
  inprint("SelectStatement", num_indent);
  inprint("Fields:", num_indent+1);
  for (Expr* expr : *stmt->select_list) printExpression(expr, num_indent+2);

  inprint("Sources:", num_indent+1);
  printTableRefInfo(stmt->from_table, num_indent+2);

  if (stmt->where_clause != NULL) {
    inprint("Search Conditions:", num_indent+1);
    printExpression(stmt->where_clause, num_indent+2);
  }


  if (stmt->union_select != NULL) {
    inprint("Union:", num_indent+1);
    printSelectStatementInfo(stmt->union_select, num_indent+2);
  }

  if (stmt->order != NULL) {
    inprint("OrderBy:", num_indent+1);
    printExpression(stmt->order->expr, num_indent+2);
    if (stmt->order->type == kOrderAsc) inprint("ascending", num_indent+2);
    else inprint("descending", num_indent+2);
  }

  if (stmt->limit != NULL) {
    inprint("Limit:", num_indent+1);
    inprint(stmt->limit->limit, num_indent+2);
  }
}



void printImportStatementInfo(ImportStatement* stmt, uint num_indent) {
  inprint("ImportStatment", num_indent);
  inprint(stmt->file_path, num_indent+1);
  inprint(stmt->table_name, num_indent+1);
}

void printCreateStatementInfo(CreateStatement* stmt, uint num_indent) {
  inprint("CreateStatment", num_indent);
  inprint(stmt->table_name, num_indent+1);
  inprint(stmt->file_path, num_indent+1);
}

void printInsertStatementInfo(InsertStatement* stmt, uint num_indent) {
  inprint("InsertStatment", num_indent);
  inprint(stmt->table_name, num_indent+1);
  if (stmt->columns != NULL) {
    inprint("Columns", num_indent+1);
    for (char* col_name : *stmt->columns) {
      inprint(col_name, num_indent+2);
    }
  }
  switch (stmt->type) {
    case InsertStatement::kInsertValues:
      inprint("Values", num_indent+1);
      for (Expr* expr : *stmt->values) {
        printExpression(expr, num_indent+2);
      }
      break;
    case InsertStatement::kInsertSelect:
      printSelectStatementInfo(stmt->select, num_indent+1);
      break;
  }

}


} // namespace hsql