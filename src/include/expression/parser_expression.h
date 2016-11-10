//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// parser_value_expression.h
//
// Identification: src/include/expression/parser_expression.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "expression/abstract_expression.h"

#include <string>

namespace peloton {
namespace expression {

//===--------------------------------------------------------------------===//
// Parser Expression
//===--------------------------------------------------------------------===//

class ParserExpression : public AbstractExpression {
 public:
  ParserExpression(ExpressionType type, char* name_)
      : AbstractExpression(type) {
    name = name_;
  }

  ParserExpression(ExpressionType type, char* name_, char* column_or_database_)
      : AbstractExpression(type) {
    name = name_;
    if (type == peloton::EXPRESSION_TYPE_COLUMN_REF) {
      column = column_or_database_;
    } else if (type == peloton::EXPRESSION_TYPE_TABLE_REF) {
      database = column_or_database_;
    }
  }

  ParserExpression(ExpressionType type) : AbstractExpression(type) {}

  ParserExpression(ExpressionType type, char* func_name_,
                   AbstractExpression* expr_, bool distinct_)
      : AbstractExpression(type) {
    name = func_name_;
    expr = expr_;
    distinct = distinct_;
  }

  ParserExpression(ExpressionType type, int placeholder)
      : AbstractExpression(type) {
    ival = placeholder;
  }

  virtual ~ParserExpression() {
    delete expr;
    delete[] name;
    delete[] column;
    delete[] database;
    delete[] alias;
  }

  Value Evaluate(UNUSED_ATTRIBUTE const AbstractTuple* tuple1,
                 UNUSED_ATTRIBUTE const AbstractTuple* tuple2,
                 UNUSED_ATTRIBUTE executor::ExecutorContext* context) const
      override {
    return ValueFactory::GetBooleanValue(1);
  }

  AbstractExpression* Copy() const override {
    std::string str(name);
    char* new_cstr = new char[str.length() + 1];
    std::strcpy(new_cstr, str.c_str());

    switch (this->GetExpressionType()) {
      case EXPRESSION_TYPE_STAR:
      case EXPRESSION_TYPE_FUNCTION_REF: {
        return new ParserExpression(this->GetExpressionType(), new_cstr);
      }
      case EXPRESSION_TYPE_PLACEHOLDER: {
        delete new_cstr;
        return new ParserExpression(this->GetExpressionType(), ival);
      }
      case EXPRESSION_TYPE_COLUMN_REF: {
        if (column == nullptr) {
          return new ParserExpression(this->GetExpressionType(), new_cstr);
        }
        std::string ref(column);
        char* new_ref = new char[ref.length() + 1];
        std::strcpy(new_ref, ref.c_str());
        return new ParserExpression(this->GetExpressionType(), new_cstr,
                                    new_ref);
      }
      case EXPRESSION_TYPE_TABLE_REF: {
        if (database == nullptr) {
          return new ParserExpression(this->GetExpressionType(), new_cstr);
        }
        std::string ref(database);
        char* new_ref = new char[ref.length() + 1];
        std::strcpy(new_ref, ref.c_str());
        return new ParserExpression(this->GetExpressionType(), new_cstr,
                                    new_ref);
      }
      default: { LOG_ERROR("Unexpected type"); }
    }
    return nullptr;
  }
};

}  // End peloton namespace
}  // End nstore namespace
