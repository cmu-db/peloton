//===----------------------------------------------------------------------===//
//
//                         Peloton
//
//create_function_statement.h
//
// Identification: src/include/parser/create_function_statement.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "expression/abstract_expression.h"
#include "type/types.h"
#include "common/sql_node_visitor.h"
#include "parser/sql_statement.h"

namespace peloton {
namespace parser {


enum FuncParamMode {

	FUNC_PARAM_IN = 'i',		/* input only */
	FUNC_PARAM_OUT = 'o',		/* output only */
	FUNC_PARAM_INOUT = 'b',		/* both */
	FUNC_PARAM_VARIADIC = 'v',	/* variadic (always input) */
	FUNC_PARAM_TABLE = 't'	

};

struct FuncParameter {
  enum DataType {
	INT,
	INTEGER,
	TINYINT,
	SMALLINT,
	BIGINT,
  VARCHAR,
  TEXT
  };

  FuncParameter(DataType type): type(type) {};	

  FuncParameter(std::string name, DataType type):name(name),type(type){};

    virtual ~FuncParameter(){
      //delete name
      // delete type? 	 		
    }	

    std::string name;
    DataType type; 
    FuncParamMode mode;

    static type::Type::TypeId GetValueType(DataType type) {
    switch (type) {
    case INT:
    case INTEGER:
      return type::Type::INTEGER;
      break;

    case TINYINT:
      return type::Type::TINYINT;
      break;
    case SMALLINT:
      return type::Type::SMALLINT;
      break;
    case BIGINT:
      return type::Type::BIGINT;
      break;
    case TEXT:
    case VARCHAR:
      return type::Type::VARCHAR;
      break;
     // add other types as necessary 
      }
  }	
};

//move it to types.h
enum PLType {
	
   PL_PGSQL=0,
   PL_C=1
	
};

/*
enum ASclause {
    
    EXECUTABLE=0,
    QUERY_STRING=1	  	
  
};
*/

//might want to change it to char* instead of string
struct CreateFunctionStatement : public SQLStatement {

  CreateFunctionStatement()
      :SQLStatement(StatementType::CREATE_FUNC){
   }; 

  virtual ~CreateFunctionStatement() {
  //insert delete code for attributes later 
  }

  virtual void Accept(SqlNodeVisitor* v) const override {
    v->Visit(this);
  }
	
  PLType language;
  //ASclause as_type;
  std::string function_body;
  //DataType return_type;
  std::vector<FuncParameter*>* func_parameters;	
  std::string function_name;
  bool replace = false;

};

}  // End parser namespace
}  // End peloton namespace
