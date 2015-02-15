#ifndef __SELECT_STATEMENT_H__
#define __SELECT_STATEMENT_H__

#include "SQLStatement.h"
#include "Expr.h"
#include "Table.h"

namespace hsql {



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
	OrderDescription(OrderType type, Expr* expr) :
		type(type),
		expr(expr) {}
		
	virtual ~OrderDescription() {
		delete expr;
	}

	OrderType type;
	Expr* expr;	
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
		offset(offset) {}

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
		delete columns;
		delete having;
	}

	std::vector<Expr*>* columns;
	Expr* having;
};

/**
 * @struct SelectStatement
 * @brief Representation of a full select statement.
 * 
 * TODO: add union_order and union_limit
 */
struct SelectStatement : SQLStatement {
	SelectStatement() : 
		SQLStatement(kStmtSelect),
		from_table(NULL),
		select_list(NULL),
		where_clause(NULL),
		group_by(NULL),
		union_select(NULL),
		order(NULL),
		limit(NULL) {};

	virtual ~SelectStatement() {
		delete from_table;
		delete select_list;
		delete where_clause;
		delete group_by;
		delete order;
		delete limit;
	}

	TableRef* from_table;
	bool select_distinct;
	std::vector<Expr*>* select_list;
	Expr* where_clause;	
	GroupByDescription* group_by;

	SelectStatement* union_select;
	OrderDescription* order;
	LimitDescription* limit;
};


} // namespace hsql

#endif