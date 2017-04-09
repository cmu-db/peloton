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

  struct Parameter {



    enum FuncParamMode {

      FUNC_PARAM_IN = 'i',		/* input only */
      FUNC_PARAM_OUT = 'o',		/* output only */
      FUNC_PARAM_INOUT = 'b',		/* both */
      FUNC_PARAM_VARIADIC = 'v',	/* variadic (always input) */
      FUNC_PARAM_TABLE = 't'	

    };


    enum DataType {
      INT,
      INTEGER,
      TINYINT,
      SMALLINT,
      BIGINT,
      VARCHAR,
      TEXT
    };

    Parameter(DataType type): type(type) {};	

   
    virtual ~Parameter(){
      //delete name
      // delete type? 	 		
    }	

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

struct ReturnType : Parameter {
  
  ReturnType(DataType type):Parameter(type){};
  virtual ~ReturnType(){}
 
};

struct FuncParameter : Parameter {
  std::string name;
  FuncParameter(std::string name, DataType type):Parameter(type),name(name){};
 
  virtual ~FuncParameter(){}

};

//move it to types.h
enum PLType {
	
   PL_PGSQL=0,
   PL_C=1
	
};


//might want to change it to char* instead of string
struct CreateFunctionStatement : public SQLStatement {

  enum ASclause {

    EXECUTABLE=0,
    QUERY_STRING=1	  	

  };


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
  ASclause as_type;
  std::vector<std::string> function_body;
  ReturnType *return_type;
  std::vector<FuncParameter*>* func_parameters;	
  std::string function_name;
  bool replace = false;

  void set_as_type(){
    if(function_body.size() > 1)
      as_type = EXECUTABLE;
    else
      as_type = QUERY_STRING;
  }

};

}  // End parser namespace
}  // End peloton namespace
