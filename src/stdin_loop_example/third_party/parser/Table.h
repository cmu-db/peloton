#ifndef __TABLEREF_H__
#define __TABLEREF_H__

#include "Expr.h"
#include <stdio.h>
#include <vector>

namespace hsql {

struct SelectStatement;
struct JoinDefinition;
struct TableRef;


/**
 * @enum TableRefType
 * Types table references
 */
typedef enum {
	kTableName,
	kTableSelect,
	kTableJoin,
	kTableCrossProduct
} TableRefType;


/**
 * @struct TableRef
 * @brief Holds reference to tables. Can be either table names or a select statement.
 */
struct TableRef {
	TableRef(TableRefType type) :
		type(type),
		schema(NULL),
		name(NULL),
		alias(NULL),
		select(NULL),
		list(NULL),
		join(NULL) {}
		
	virtual ~TableRef();

	TableRefType type;

	char* schema;
	char* name;
	char* alias;

	SelectStatement* select;
	std::vector<TableRef*>* list;
	JoinDefinition* join;


	/**
	 * Convenience accessor methods
	 */
	inline bool hasSchema() { return schema != NULL; }

	inline char* getName() {
		if (alias != NULL) return alias;
		else return name;
	}
};


/**
 * @enum JoinType
 * Types of joins
 */ 
typedef enum {
	kJoinInner,
	kJoinOuter,
	kJoinLeft,
	kJoinRight,
} JoinType;


/**
 * @struct JoinDefinition
 * @brief Definition of a join table
 */
struct JoinDefinition {
	JoinDefinition() :
		left(NULL),
		right(NULL),
		condition(NULL),
		type(kJoinInner) {}

	virtual ~JoinDefinition() {
		delete left;
		delete right;
		delete condition;
	}

	TableRef* left;
	TableRef* right;
	Expr* condition;

	JoinType type;
};



} // namespace hsql
#endif
