#pragma once

#include "parser/sql_statement.h"
#include "parser/table_ref.h"

namespace peloton {
namespace parser {

/**
 * @struct OrderDescription
 * @brief Description of the order by clause within a select statement
 * 
 * TODO: hold multiple expressions to be sorted by
 */
typedef enum {
	kOrderAsc,
	kOrderDesc
} OrderType;

struct OrderDescription {
	OrderDescription(OrderType type, expression::AbstractExpression* expr) :
		type(type),
		expr(expr) {}
		
	virtual ~OrderDescription() {
		delete expr;
	}

	OrderType type;
	expression::AbstractExpression* expr;
};

/**
 * @struct LimitDescription
 * @brief Description of the limit clause within a select statement
 */
const int64_t kNoLimit = -1;
const int64_t kNoOffset = -1;
struct LimitDescription {
	LimitDescription(int64_t limit, int64_t offset) :
		limit(limit),
		offset(offset) {
	}

	int64_t limit;
	int64_t offset;	
};

/**
 * @struct GroupByDescription
 */
struct GroupByDescription {
	GroupByDescription() : 
		columns(NULL),
		having(NULL) {}

	~GroupByDescription() {

    if(columns) {
      for(auto col : *columns)
        delete col;
      delete columns;
    }

		delete having;
	}

	std::vector<expression::AbstractExpression*>* columns;
	expression::AbstractExpression* having;
};

/**
 * @struct SelectStatement
 * @brief Representation of a full select statement.
 * 
 * TODO: add union_order and union_limit
 */
struct SelectStatement : SQLStatement {
	SelectStatement() : 
		SQLStatement(STATEMENT_TYPE_SELECT),
		from_table(NULL),
		select_distinct(false),
		select_list(NULL),
		where_clause(NULL),
		group_by(NULL),
		union_select(NULL),
		order(NULL),
		limit(NULL) {};

	virtual ~SelectStatement() {
		delete from_table;

		if(select_list) {
		  for(auto expr : *select_list)
		    delete expr;
		  delete select_list;
		}

		delete where_clause;
		delete group_by;

		delete union_select;
		delete order;
		delete limit;
	}

	TableRef* from_table;
	bool select_distinct;
	std::vector<expression::AbstractExpression*>* select_list;
	expression::AbstractExpression* where_clause;
	GroupByDescription* group_by;

	SelectStatement* union_select;
	OrderDescription* order;
	LimitDescription* limit;
};

} // End parser namespace
} // End peloton namespace
