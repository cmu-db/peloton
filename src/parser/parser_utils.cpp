
#include "parser_utils.h"

#include <stdio.h>
#include <string>
#include <iostream>

namespace nstore {
namespace parser {

const char* indent(uint num_indent) {
  return std::string(num_indent, '\t').c_str();
}

void inprint(int64_t val, uint num_indent) {
  printf("%s%ld  \n", indent(num_indent), val);
}

void inprint(float val, uint num_indent) {
  printf("%s%f\n", indent(num_indent), val);
}

void inprint(const char* val, uint num_indent) {
  printf("%s%s\n", indent(num_indent), val);
}

void inprint(const char* val, const char* val2, uint num_indent) {
  printf("%s%s->%s\n", indent(num_indent), val, val2);
}

void inprintC(char val, uint num_indent) {
  printf("%s%c\n", indent(num_indent), val);
}

void inprintU(uint64_t val, uint num_indent) {
  printf("%s%lu\n", indent(num_indent), val);
}

void PrintTableRefInfo(TableRef* table, uint num_indent) {
  switch (table->type) {
    case TABLE_REFERENCE_TYPE_NAME:
      inprint(table->name, num_indent);
      break;

    case TABLE_REFERENCE_TYPE_SELECT:
      GetSelectStatementInfo(table->select, num_indent);
      break;

    case TABLE_REFERENCE_TYPE_JOIN:
      inprint("-> Join Table", num_indent);
      inprint("-> Left", num_indent+1);
      PrintTableRefInfo(table->join->left, num_indent+2);
      inprint("-> Right", num_indent+1);
      PrintTableRefInfo(table->join->right, num_indent+2);
      inprint("-> Join Condition", num_indent+1);
      GetExpressionInfo(table->join->condition, num_indent+2);
      break;

    case TABLE_REFERENCE_TYPE_CROSS_PRODUCT:
      for (TableRef* tbl : *table->list) PrintTableRefInfo(tbl, num_indent);
      break;

    case TABLE_REFERENCE_TYPE_INVALID:
    default:
      printf("invalid table ref type \n");
      break;
  }

  if (table->alias != NULL) {
    inprint("Alias", num_indent+1);
    inprint(table->alias, num_indent+2);
  }
}

void PrintOperatorExpression(const expression::AbstractExpression* expr, uint num_indent) {
  if (expr == NULL) {
    inprint("null", num_indent);
    return;
  }

  GetExpressionInfo(expr->GetLeft(), num_indent+1);
  if (expr->GetRight() != NULL)
    GetExpressionInfo(expr->GetRight(), num_indent+1);

}

void GetExpressionInfo(const expression::AbstractExpression* expr, uint num_indent) {

  if(expr == NULL) {
      inprint("null", num_indent);
      return;
  }

  printf("%s", indent(num_indent));
  std::cout << "Expr Type :: " << ExpressionToString(expr->GetExpressionType()) << "\n";

  switch (expr->GetExpressionType()) {
    case EXPRESSION_TYPE_STAR:
      inprint("*", num_indent);
      break;
    case EXPRESSION_TYPE_COLUMN_REF:
      inprint(((expression::ParserExpression*)expr)->GetName(), num_indent);
      break;
    case EXPRESSION_TYPE_VALUE_CONSTANT:
      printf("%s", indent(num_indent));
      std::cout << ((expression::ConstantValueExpression*)expr)->GetValue();
      printf("\n");
      break;
    case EXPRESSION_TYPE_FUNCTION_REF:
      inprint(((expression::ParserExpression*)expr)->GetName(), num_indent);
      break;
    default:
      PrintOperatorExpression(expr, num_indent);
      break;
  }

  if (expr->alias != NULL) {
    inprint("Alias", num_indent+1); inprint(expr->alias, num_indent+2);
  }
}

void GetSelectStatementInfo(SelectStatement* stmt, uint num_indent) {
  inprint("SelectStatement", num_indent);
  inprint("-> Fields:", num_indent+1);
  for (expression::AbstractExpression* expr : *(stmt->select_list))
    GetExpressionInfo(expr, num_indent+2);

  inprint("-> Sources:", num_indent+1);
  PrintTableRefInfo(stmt->from_table, num_indent+2);

  if (stmt->where_clause != NULL) {
    inprint("-> Search Conditions:", num_indent+1);
    GetExpressionInfo(stmt->where_clause, num_indent+2);
  }


  if (stmt->union_select != NULL) {
    inprint("-> Union:", num_indent+1);
    GetSelectStatementInfo(stmt->union_select, num_indent+2);
  }

  if (stmt->order != NULL) {
    inprint("-> OrderBy:", num_indent+1);
    GetExpressionInfo(stmt->order->expr, num_indent+2);
    if (stmt->order->type == kOrderAsc) inprint("ascending", num_indent+2);
    else inprint("descending", num_indent+2);
  }

  if (stmt->limit != NULL) {
    inprint("-> Limit:", num_indent+1);
    inprint(stmt->limit->limit, num_indent+2);
    inprint(stmt->limit->offset, num_indent+2);
  }
}



void GetImportStatementInfo(ImportStatement* stmt, uint num_indent) {
  inprint("ImportStatment", num_indent);
  inprint(stmt->file_path, num_indent+1);
  inprint(stmt->table_name, num_indent+1);
}

void GetCreateStatementInfo(CreateStatement* stmt, uint num_indent) {
  inprint("CreateStatment", num_indent);
  inprint(stmt->table_name, num_indent+1);
  inprint(stmt->file_path, num_indent+1);

  if(stmt->columns != nullptr){
    for(ColumnDefinition *col : *(stmt->columns)) {
      printf("%s", indent(num_indent));
      printf("%s %d \n", col->name, col->type);
    }
  }

}

void GetInsertStatementInfo(InsertStatement* stmt, uint num_indent) {
  inprint("InsertStatment", num_indent);
  inprint(stmt->table_name, num_indent+1);
  if (stmt->columns != NULL) {
    inprint("-> Columns", num_indent+1);
    for (char* col_name : *stmt->columns) {
      inprint(col_name, num_indent+2);
    }
  }
  switch (stmt->type) {
    case INSERT_TYPE_VALUES:
      inprint("-> Values", num_indent+1);
      for (expression::AbstractExpression* expr : *stmt->values) {
        GetExpressionInfo(expr, num_indent+2);
      }
      break;
    case INSERT_TYPE_SELECT:
      GetSelectStatementInfo(stmt->select, num_indent+1);
      break;
    default:
      break;
  }

}

} // End parser namespace
} // End nstore namespace
