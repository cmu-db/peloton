#pragma once

#include <stdlib.h>

namespace nstore {
namespace parser {

// Helper function
char* substr(const char* source, int from, int to);

typedef enum {
	kExprLiteralFloat,
	kExprLiteralString,
	kExprLiteralInt,
	kExprStar,
	kExprPlaceholder,
	kExprColumnRef,
	kExprFunctionRef,
	kExprOperator
} ExprType;

typedef struct Expr Expr;

/** 
 * @class Expr
 * @brief Represents SQL expressions (i.e. literals, operators, column_refs)
 *
 * TODO: When destructing a placeholder expression, we might need to alter the placeholder_list
 */
struct Expr {

	/**
	 * Operator types. These are important for expressions of type kExprOperator
	 * Trivial types are those that can be described by a single character e.g:
	 * + - * / < > = %
	 * Non-trivial are:
	 * <> <= >= LIKE ISNULL NOT
	 */
	typedef enum {
		SIMPLE_OP,
		// Binary
		NOT_EQUALS,
		LESS_EQ,
		GREATER_EQ,
		LIKE,
		NOT_LIKE,
		AND,
		OR,
		// Unary
		NOT,
		UMINUS,
		ISNULL
	} OperatorType;



	Expr(ExprType type) :
		type(type),
		expr(NULL),
		expr2(NULL),
		name(NULL),
		table(NULL),
		alias(NULL) {};

	// Interesting side-effect:
	// Making the destructor virtual used to cause segmentation faults
	~Expr();
	
	ExprType type;

	Expr* expr;
	Expr* expr2;
	char* name;
	char* table;
	char* alias;
	float fval;
	int64_t ival;
	int64_t ival2;

	OperatorType op_type;
	char op_char;
	bool distinct;

	/**
	 * Convenience accessor methods
	 */
	inline bool IsType(ExprType e_type) {
	  return e_type == type;
	}

	inline bool IsLiteral() {
	  return IsType(kExprLiteralInt) || IsType(kExprLiteralFloat) || IsType(kExprLiteralString) || IsType(kExprPlaceholder);
	}

	inline bool HasAlias() {
	  return alias != NULL;
	}

	inline bool HasTable() {
	  return table != NULL;
	}

	inline char* GetName() {
		if (alias != NULL)
		  return alias;
		else
		  return name;
	}

	inline bool IsSimpleOp() {
	  return op_type == SIMPLE_OP;
	}

	inline bool IsSimpleOp(char op) {
	  return IsSimpleOp() && op_char == op;
	}

	/**
	 * Static expression constructors
	 */
	static Expr* MakeOpUnary(OperatorType op, Expr* expr);
	static Expr* MakeOpBinary(Expr* expr1, char op, Expr* expr2);
	static Expr* MakeOpBinary(Expr* expr1, OperatorType op, Expr* expr2);

	static Expr* MakeLiteral(int64_t val);
	static Expr* MakeLiteral(double val);
	static Expr* MakeLiteral(char* val);

	static Expr* MakeColumnRef(char* name);
	static Expr* MakeColumnRef(char* table, char* name);
	static Expr* MakeFunctionRef(char* func_name, Expr* expr, bool distinct);

	static Expr* MakePlaceholder(int id);
};

// Zero initializes an Expr object and assigns it to a space in the heap
// For Hyrise we still had to put in the explicit NULL constructor
// http://www.ex-parrot.com/~chris/random/initialise.html
// Unused
#define ALLOC_EXPR(var, type) 		\
	Expr* var;						\
	do {							\
		Expr zero = {type};			\
		var = (Expr*)malloc(sizeof *var);	\
		*var = zero;				\
	} while(0);
#undef ALLOC_EXPR

} // End parser namespace
} // End nstore namespace
