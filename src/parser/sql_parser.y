%{
/**
 * sql_parser.y
 * defines sql_parser.h
 * 
 * Grammar File Spec: http://dinosaur.compilertools.net/bison/bison_6.html
 * Based on : https://github.com/hyrise/sql-parser (Feb 2015)
 *
 */
/*********************************
 ** Section 1: C Declarations
 *********************************/

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>

#include "common/types.h"
#include "common/value.h"
#include "common/value_factory.h"
#include "expression/abstract_expression.h"
#include "expression/operator_expression.h"
#include "expression/comparison_expression.h"
#include "expression/conjunction_expression.h"
#include "expression/constant_value_expression.h"
#include "expression/function_expression.h"

#include "parser/statements.h"
#include "parser/sql_parser.h"

using namespace std;
using namespace peloton::parser;

extern int parser_lex (YYSTYPE *lvalp, YYLTYPE *llocp, yyscan_t scanner);

int yyerror(YYLTYPE* llocp, peloton::parser::SQLStatementList** result, __attribute__((unused)) yyscan_t scanner, const char *msg) {

	peloton::parser::SQLStatementList* list = new peloton::parser::SQLStatementList();
	list->is_valid = false;
	list->parser_msg = strdup(msg);
	list->error_line = llocp->first_line;
	list->error_col = llocp->first_column;

	*result = list;
	return 0;
}


%}
/*********************************
 ** Section 2: Bison Parser Declarations
 *********************************/


// Specify code that is included in the generated .h and .c files
%code requires {
// %code requires block	
#ifndef PARSER_TYPEDEF_
#define PARSER_TYPEDEF_
#include <vector>

#ifndef YYtypeDEF_YY_SCANNER_T
#define YYtypeDEF_YY_SCANNER_T
typedef void* yyscan_t;
#endif

#define YYSTYPE PARSER_STYPE
#define YYLTYPE PARSER_LTYPE

struct PARSER_CUST_LTYPE {
  int first_line;
  int first_column;
  int last_line;
  int last_column;

  int total_column;

  // Placeholder
  int placeholder_id;
  std::vector<void*> placeholder_list;
};

#define PARSER_LTYPE PARSER_CUST_LTYPE
#define PARSER_LTYPE_IS_DECLARED 1

#endif

// Auto update column and line number
#define YY_USER_ACTION \
    yylloc->first_line = yylloc->last_line; \
    yylloc->first_column = yylloc->last_column; \
    for(int i = 0; yytext[i] != '\0'; i++) { \
    	yylloc->total_column++; \
        if(yytext[i] == '\n') { \
            yylloc->last_line++; \
            yylloc->last_column = 0; \
        } \
        else { \
            yylloc->last_column++; \
        } \
    }
}

// Define the names of the created files
// Defined by the build system
/* Output location is defined by the build system */
/* %output  "parser/sql_parser.cpp" */
%defines "include/parser/sql_parser.h"

// Tell bison to create a reentrant parser
%define api.pure full

// Prefix the parser
%define api.prefix {parser_}
%define api.token.prefix {SQL_}

%define parse.error verbose
%locations

%initial-action {
	// Initialize
	@$.first_column = 0;
	@$.last_column = 0;
	@$.first_line = 0;
	@$.last_line = 0;
	@$.total_column = 0;
	@$.placeholder_id = 0;
};


// Define additional parameters for yylex (http://www.gnu.org/software/bison/manual/html_node/Pure-Calling.html)
%lex-param   { yyscan_t scanner }

// Define additional parameters for yyparse
%parse-param { peloton::parser::SQLStatementList** result }
%parse-param { yyscan_t scanner }


/*********************************
 ** Define all data-types (http://www.gnu.org/software/bison/manual/html_node/Union-Decl.html)
 *********************************/
%union {
	double fval;
	int64_t ival;
	char* sval;
	uint uval;
	bool bval;

	peloton::parser::SQLStatement* statement;
	peloton::parser::SelectStatement*  	  select_stmt;
	peloton::parser::CreateStatement* 	  create_stmt;
	peloton::parser::InsertStatement* 	  insert_stmt;
	peloton::parser::DeleteStatement* 	  delete_stmt;
	peloton::parser::UpdateStatement* 	  update_stmt;
	peloton::parser::DropStatement*   	  drop_stmt;
	peloton::parser::PrepareStatement*     prep_stmt;
	peloton::parser::ExecuteStatement*     exec_stmt;
	peloton::parser::TransactionStatement* txn_stmt;

	peloton::parser::TableRef* table;
	peloton::expression::AbstractExpression* expr;
	peloton::parser::OrderDescription* order;
	peloton::parser::OrderType order_type;
	peloton::parser::LimitDescription* limit;
	peloton::parser::ColumnDefinition* column_t;
	peloton::parser::GroupByDescription* group_t;
	peloton::parser::UpdateClause* update_t;

	peloton::parser::SQLStatementList* stmt_list;

	std::vector<char*>* str_vec;
	std::vector<peloton::parser::TableRef*>* table_vec;
	std::vector<peloton::parser::ColumnDefinition*>* column_vec;
	std::vector<peloton::parser::UpdateClause*>* update_vec;
	std::vector<peloton::expression::AbstractExpression*>* expr_vec;
}



/*********************************
 ** Token Definition
 *********************************/
%token <sval> IDENTIFIER STRING
%token <fval> FLOATVAL
%token <ival> INTVAL
%token <uval> NOTEQUALS LESSEQ GREATEREQ

/* SQL Keywords */
%token TRANSACTION
%token REFERENCES DEALLOCATE PARAMETERS INTERSECT TEMPORARY TIMESTAMP
%token VARBINARY ROLLBACK DISTINCT NVARCHAR RESTRICT TRUNCATE ANALYZE BETWEEN BOOLEAN ADDRESS
%token DATABASE SMALLINT VARCHAR FOREIGN TINYINT CASCADE COLUMNS CONTROL DEFAULT EXECUTE EXPLAIN
%token HISTORY INTEGER NATURAL PREPARE PRIMARY SCHEMAS DECIMAL
%token SPATIAL VIRTUAL BEFORE COLUMN CREATE DELETE DIRECT 
%token BIGINT DOUBLE ESCAPE EXCEPT EXISTS GLOBAL HAVING
%token INSERT ISNULL OFFSET RENAME SCHEMA SELECT SORTED
%token COMMIT TABLES UNIQUE UNLOAD UPDATE VALUES AFTER ALTER CROSS
%token FLOAT BEGIN DELTA GROUP INDEX INNER LIMIT LOCAL MERGE MINUS ORDER
%token OUTER RIGHT TABLE UNION USING WHERE CHAR CALL DATE DESC
%token DROP FILE FROM FULL HASH HINT INTO JOIN LEFT LIKE
%token LOAD NULL PART PLAN SHOW TEXT TIME VIEW WITH ADD ALL
%token AND ASC CSV FOR INT KEY NOT OFF SET TOP AS BY IF
%token IN IS OF ON OR TO


/*********************************
 ** Non-Terminal types (http://www.gnu.org/software/bison/manual/html_node/Type-Decl.html)
 *********************************/
%type <stmt_list>	statement_list
%type <statement> 	statement preparable_statement
%type <exec_stmt>	execute_statement
%type <prep_stmt>	prepare_statement
%type <select_stmt> select_statement select_with_paren select_no_paren select_clause
%type <create_stmt> create_statement
%type <insert_stmt> insert_statement
%type <delete_stmt> delete_statement truncate_statement
%type <update_stmt> update_statement
%type <drop_stmt>	drop_statement
%type <txn_stmt>    transaction_statement
%type <sval> 		table_name opt_alias alias
%type <bval> 		opt_not_exists opt_distinct opt_notnull opt_primary opt_unique
%type <uval>		opt_join_type column_type opt_column_width
%type <table> 		from_clause table_ref table_ref_atomic table_ref_name
%type <table>		join_clause join_table table_ref_name_no_alias
%type <expr> 		expr scalar_expr unary_expr binary_expr function_expr star_expr expr_alias placeholder_expr
%type <expr> 		column_name literal int_literal num_literal string_literal
%type <expr> 		comp_expr opt_where join_condition opt_having
%type <order>		opt_order
%type <limit>		opt_limit
%type <order_type>	opt_order_type
%type <column_t>	column_def
%type <update_t>	update_clause
%type <group_t>		opt_group

%type <str_vec>		ident_commalist opt_column_list
%type <expr_vec> 	expr_list select_list literal_list
%type <table_vec> 	table_ref_commalist
%type <update_vec>	update_clause_commalist
%type <column_vec>	column_def_commalist 

/******************************
 ** Token Precedence and Associativity
 ** Precedence: lowest to highest
 ******************************/
%left		OR
%left		AND
%right		NOT
%right		'=' EQUALS NOTEQUALS LIKE
%nonassoc	'<' '>' LESS GREATER LESSEQ GREATEREQ

%nonassoc	NOTNULL
%nonassoc	ISNULL
%nonassoc	IS				/* sets precedence for IS NULL, etc */
%left		'+' '-'
%left		'*' '/' '%'
%left		'^'

/* Unary Operators */
%right 		UMINUS
%left		'[' ']'
%left		'(' ')'
%left		'.'

%%
/*********************************
 ** Section 3: Grammar Definition
 *********************************/

// Defines our general input.
input:
		statement_list opt_semicolon {
			*result = $1;
		}
	;


statement_list:
		statement { $$ = new SQLStatementList($1); }
	|	statement_list ';' statement { $1->AddStatement($3); $$ = $1; }
	;

statement:
		prepare_statement {
			$1->setPlaceholders(yyloc.placeholder_list);
			yyloc.placeholder_list.clear();
			$$ = $1;
		}
	|	preparable_statement
	;


preparable_statement:
		select_statement { $$ = $1; }
	|	create_statement { $$ = $1; }
	|	insert_statement { $$ = $1; }
	|	delete_statement { $$ = $1; }
	|	truncate_statement { $$ = $1; }
	|	update_statement { $$ = $1; }
	|	drop_statement { $$ = $1; }
	|	execute_statement { $$ = $1; }
	|	transaction_statement { $$ = $1; }	
	;


/******************************
 * Prepared Statement
 ******************************/
prepare_statement:
		PREPARE IDENTIFIER ':' preparable_statement {
			$$ = new PrepareStatement();
			$$->name = $2;
			$$->query = new SQLStatementList($4);
		}
	|	PREPARE IDENTIFIER '{' statement_list opt_semicolon '}' {
			$$ = new PrepareStatement();
			$$->name = $2;
			$$->query = $4;
		}
	;

execute_statement:
		EXECUTE IDENTIFIER {
			$$ = new ExecuteStatement();
			$$->name = $2;
		}
	|	EXECUTE IDENTIFIER '(' literal_list ')' {
			$$ = new ExecuteStatement();
			$$->name = $2;
			$$->parameters = $4;
		}
	;


/******************************
 * Create Statement
 * CREATE TABLE students (name TEXT, student_number INTEGER, city TEXT, grade DOUBLE)
 * CREATE INDEX i_security ON security (s_co_id, s_issue)
 * CREATE DATABASE my_db
 ******************************/
create_statement:
		CREATE TABLE opt_not_exists table_name '(' column_def_commalist ')' {
			$$ = new CreateStatement(CreateStatement::kTable);
			$$->if_not_exists = $3;
			$$->name = $4;
			$$->columns = $6;
		}
		|	CREATE DATABASE opt_not_exists IDENTIFIER {
			$$ = new CreateStatement(CreateStatement::kDatabase);
			$$->if_not_exists = $3;
			$$->name = $4;
		}
		|	CREATE opt_unique INDEX IDENTIFIER ON table_name '(' ident_commalist ')' {
			$$ = new CreateStatement(CreateStatement::kIndex);
			$$->unique = $2;
			$$->name = $4;
			$$->table_name = $6;
			$$->index_attrs = $8;			
		}
	;

opt_not_exists:
		IF NOT EXISTS { $$ = true; }
	|	/* empty */ { $$ = false; }
	;

column_def_commalist:
		column_def { $$ = new std::vector<ColumnDefinition*>(); $$->push_back($1); }
	|	column_def_commalist ',' column_def { $1->push_back($3); $$ = $1; }
	;
	
column_def:
		IDENTIFIER column_type opt_column_width opt_notnull opt_primary opt_unique {
			$$ = new ColumnDefinition($1, (ColumnDefinition::DataType) $2);
			$$->varlen = $3;
			$$->not_null = $4;
			$$->primary = $5;
			$$->unique = $6;
		}
		|
		PRIMARY KEY '(' ident_commalist ')' {
			$$ = new ColumnDefinition(ColumnDefinition::DataType::PRIMARY);
			$$->primary_key = $4;
		}
		|
		FOREIGN KEY '(' ident_commalist ')' REFERENCES table_name '(' ident_commalist ')' {
			$$ = new ColumnDefinition(ColumnDefinition::DataType::FOREIGN);
			$$->foreign_key_source = $4;
			$$->name = $7;
			$$->foreign_key_sink = $9;
		}
		;

opt_column_width:
	'(' int_literal ')' { $$ = $2->ival; delete $2; }
	| /* empty */ { $$ = 0; }
	;    

opt_notnull:
	NOT NULL { $$ = true; }
	|  /* empty */ { $$ = false; }
	;

opt_primary:
	PRIMARY KEY { $$ = true; }
	|  /* empty */ { $$ = false; }
	;

opt_unique:
	UNIQUE { $$ = true; }
	|  /* empty */ { $$ = false; }
	;

column_type:
		INT { $$ = ColumnDefinition::INT; }
	|	INTEGER { $$ = ColumnDefinition::INT; }
	|	DOUBLE { $$ = ColumnDefinition::DOUBLE; }
	|	TEXT { $$ = ColumnDefinition::TEXT; }
	|   CHAR { $$ = ColumnDefinition::CHAR; }
	|   TINYINT { $$ = ColumnDefinition::TINYINT; }
	|   SMALLINT { $$ = ColumnDefinition::SMALLINT; }
	|	BIGINT { $$ = ColumnDefinition::BIGINT; }
	|	FLOAT { $$ = ColumnDefinition::FLOAT; }
	|	DECIMAL { $$ = ColumnDefinition::DECIMAL; }
	|	BOOLEAN { $$ = ColumnDefinition::BOOLEAN; }
	|	TIMESTAMP { $$ = ColumnDefinition::TIMESTAMP; }
	|	VARCHAR { $$ = ColumnDefinition::VARCHAR; }
    |   VARBINARY { $$ = ColumnDefinition::VARBINARY; }
	;

/******************************
 * Drop Statement
 * DROP TABLE student_table;
 * DROP DATABASE student_db;
 * DROP INDEX student_index ON student_table;
 * DEALLOCATE PREPARE stmt;
 ******************************/

drop_statement:
		DROP TABLE table_name {
			$$ = new DropStatement(DropStatement::kTable);
			$$->name = $3;
		}
		|
		DROP DATABASE IDENTIFIER {
			$$ = new DropStatement(DropStatement::kDatabase);
			$$->name = $3;
		}
		|	
		DROP INDEX IDENTIFIER ON table_name  {
			$$ = new DropStatement(DropStatement::kIndex);
			$$->name = $3;
			$$->table_name = $5;
		}
		|	
		DEALLOCATE PREPARE IDENTIFIER {
			$$ = new DropStatement(DropStatement::kPreparedStatement);
			$$->name = $3;
		}
	;

/******************************
 * Transaction Statement
 * BEGIN [ TRANSACTION ]
 * COMMIT [ TRANSACTION ]
 * ROLLBACK [ TRANSACTION ]
 ******************************/

transaction_statement:
	BEGIN opt_transaction { 
		$$ = new TransactionStatement(TransactionStatement::kBegin);
	}
	| COMMIT opt_transaction {
		$$ = new TransactionStatement(TransactionStatement::kCommit);
	}
	| ROLLBACK opt_transaction {
		$$ = new TransactionStatement(TransactionStatement::kRollback);
	}
	;
	
opt_transaction:
	| TRANSACTION
	;

/******************************
 * Delete Statement / Truncate statement
 * DELETE FROM students WHERE grade > 3.0
 * DELETE FROM students <=> TRUNCATE students
 ******************************/
delete_statement:
		DELETE FROM table_name opt_where {
			$$ = new DeleteStatement();
			$$->table_name = $3;
			$$->expr = $4;
		}
	;

truncate_statement:
		TRUNCATE table_name {
			$$ = new DeleteStatement();
			$$->table_name = $2;
		}
	;

/******************************
 * Insert Statement
 * INSERT INTO students VALUES ('Max', 1112233, 'Musterhausen', 2.3)
 * INSERT INTO employees SELECT * FROM stundents
 ******************************/
insert_statement:
		INSERT INTO table_name opt_column_list VALUES '(' literal_list ')' {
			$$ = new InsertStatement(peloton::INSERT_TYPE_VALUES);
			$$->table_name = $3;
			$$->columns = $4;
			$$->values = $7;
		}
	|	INSERT INTO table_name opt_column_list select_no_paren {
			$$ = new InsertStatement(peloton::INSERT_TYPE_SELECT);
			$$->table_name = $3;
			$$->columns = $4;
			$$->select = $5;
		}
	;


opt_column_list:
		'(' ident_commalist ')' { $$ = $2; }
	|	/* empty */ { $$ = NULL; }
	;


/******************************
 * Update Statement
 * UPDATE students SET grade = 1.3, name='Felix Fürstenberg' WHERE name = 'Max Mustermann';
 ******************************/

update_statement:
	UPDATE table_ref_name_no_alias SET update_clause_commalist opt_where {
		$$ = new UpdateStatement();
		$$->table = $2;
		$$->updates = $4;
		$$->where = $5;
	}
	;

update_clause_commalist:
		update_clause { $$ = new std::vector<UpdateClause*>(); $$->push_back($1); }
	|	update_clause_commalist ',' update_clause { $1->push_back($3); $$ = $1; }
	;

update_clause:
		IDENTIFIER '=' literal {
			$$ = new UpdateClause();
			$$->column = $1;
			$$->value = $3;
		}
	;

/******************************
 * Select Statement
 ******************************/

select_statement:
		select_with_paren
	|	select_no_paren
	;

select_with_paren:
		'(' select_no_paren ')' { $$ = $2; }
	|	'(' select_with_paren ')' { $$ = $2; }
	;

select_no_paren:
		select_clause opt_order opt_limit {
			$$ = $1;
			$$->order = $2;
			$$->limit = $3;
		}
	|	select_clause set_operator select_clause opt_order opt_limit {
			// TODO: allow multiple unions (through linked list)
			// TODO: capture type of set_operator
			// TODO: might overwrite order and limit of first select here
			$$ = $1;
			$$->union_select = $3;
			$$->order = $4;
			$$->limit = $5;
		}
	;

set_operator:
		UNION
	|	INTERSECT
	|	EXCEPT
	;

select_clause:
		SELECT opt_distinct select_list from_clause opt_where opt_group {
			$$ = new SelectStatement();
			$$->select_distinct = $2;
			$$->select_list = $3;
			$$->from_table = $4;
			$$->where_clause = $5;
			$$->group_by = $6;
		}
	;

opt_distinct:
		DISTINCT { $$ = true; }
	|	/* empty */ { $$ = false; }
	;

select_list:
		expr_list
	;


from_clause:
		FROM table_ref { $$ = $2; }
	;


opt_where:
		WHERE expr { $$ = $2; }
	|	/* empty */ { $$ = NULL; }
	;

opt_group:
		GROUP BY expr_list opt_having {
			$$ = new GroupByDescription();
			$$->columns = $3;
			$$->having = $4;
		}
	|	/* empty */ { $$ = NULL; }
	;

opt_having:
		HAVING expr { $$ = $2; }
	|	/* empty */ { $$ = NULL; }

opt_order:
		ORDER BY expr opt_order_type { $$ = new OrderDescription($4, $3); }
	|	/* empty */ { $$ = NULL; }
	;

opt_order_type:
		ASC { $$ = kOrderAsc; }
	|	DESC { $$ = kOrderDesc; }
	|	/* empty */ { $$ = kOrderAsc; }
	;


opt_limit:
		LIMIT int_literal { $$ = new LimitDescription($2->ival, kNoOffset); delete $2; }
	|	LIMIT int_literal OFFSET int_literal { $$ = new LimitDescription($2->ival, $4->ival); delete $2; delete $4; }
	|	/* empty */ { $$ = NULL; }
	;

/******************************
 * Expressions 
 ******************************/
expr_list:
		expr_alias { $$ = new std::vector<peloton::expression::AbstractExpression*>(); $$->push_back($1); }
	|	expr_list ',' expr_alias { $1->push_back($3); $$ = $1; }
	;

literal_list:
		literal { $$ = new std::vector<peloton::expression::AbstractExpression*>(); $$->push_back($1); }
	|	literal_list ',' literal { $1->push_back($3); $$ = $1; }
	;

expr_alias:
		expr opt_alias {
			$$ = $1;
			$$->alias = $2;
		}
	;

expr:
		'(' expr ')' { $$ = $2; }
	|	scalar_expr
	|	unary_expr
	|	binary_expr
	|	function_expr
	;

scalar_expr:
		column_name
	|	star_expr
	|	literal
	;

unary_expr:
		'-' expr { $$ = new peloton::expression::OperatorUnaryMinusExpression($2); }
	|	NOT expr { $$ = new peloton::expression::OperatorUnaryNotExpression($2); }
	;

binary_expr:
		comp_expr
	|	expr '-' expr	{ $$ = new peloton::expression::OperatorExpression<peloton::expression::OpMinus>(peloton::EXPRESSION_TYPE_OPERATOR_MINUS, $1->GetValueType(), $1, $3); }
	|	expr '+' expr	{ $$ = new peloton::expression::OperatorExpression<peloton::expression::OpPlus>(peloton::EXPRESSION_TYPE_OPERATOR_PLUS, $1->GetValueType(), $1, $3); }
	|	expr '/' expr	{ $$ = new peloton::expression::OperatorExpression<peloton::expression::OpDivide>(peloton::EXPRESSION_TYPE_OPERATOR_DIVIDE, $1->GetValueType(), $1, $3); }
	|	expr '*' expr	{ $$ = new peloton::expression::OperatorExpression<peloton::expression::OpMultiply>(peloton::EXPRESSION_TYPE_OPERATOR_MULTIPLY, $1->GetValueType(), $1, $3); }
	|	expr AND expr	{ $$ = new peloton::expression::ConjunctionExpression<peloton::expression::ConjunctionAnd>(peloton::EXPRESSION_TYPE_CONJUNCTION_AND, $1, $3); }
	|	expr OR expr	{ $$ = new peloton::expression::ConjunctionExpression<peloton::expression::ConjunctionOr>(peloton::EXPRESSION_TYPE_CONJUNCTION_OR, $1, $3); }
	|	expr LIKE expr	{ $$ = new peloton::expression::ComparisonExpression<peloton::expression::CmpEq>(peloton::EXPRESSION_TYPE_COMPARE_LIKE, $1, $3); }
	|	expr NOT LIKE expr	{ $$ = new peloton::expression::ComparisonExpression<peloton::expression::CmpNe>(peloton::EXPRESSION_TYPE_COMPARE_LIKE, $1, $4); }
	;


comp_expr:
		expr '=' expr		{ $$ = new peloton::expression::ComparisonExpression<peloton::expression::CmpEq>(peloton::EXPRESSION_TYPE_COMPARE_EQUAL, $1, $3); }
	|	expr NOTEQUALS expr	{ $$ = new peloton::expression::ComparisonExpression<peloton::expression::CmpNe>(peloton::EXPRESSION_TYPE_COMPARE_NOTEQUAL, $1, $3); }
	|	expr '<' expr		{ $$ = new peloton::expression::ComparisonExpression<peloton::expression::CmpLt>(peloton::EXPRESSION_TYPE_COMPARE_LESSTHAN, $1, $3);; }
	|	expr '>' expr		{ $$ = new peloton::expression::ComparisonExpression<peloton::expression::CmpGt>(peloton::EXPRESSION_TYPE_COMPARE_GREATERTHAN, $1, $3); }
	|	expr LESSEQ expr	{ $$ = new peloton::expression::ComparisonExpression<peloton::expression::CmpLte>(peloton::EXPRESSION_TYPE_COMPARE_LESSTHANOREQUALTO, $1, $3); }
	|	expr GREATEREQ expr	{ $$ = new peloton::expression::ComparisonExpression<peloton::expression::CmpGte>(peloton::EXPRESSION_TYPE_COMPARE_GREATERTHANOREQUALTO, $1, $3); }
	;

function_expr:
		IDENTIFIER '(' opt_distinct expr ')' { /* TODO: $$ = new peloton::expression::AbstractExpression(peloton::EXPRESSION_TYPE_FUNCTION_REF, $1, $4, $3); */ }
	;

column_name:
		IDENTIFIER { /* TODO: $$ = new peloton::expression::AbstractExpression(peloton::EXPRESSION_TYPE_COLUMN_REF, $1); */ }
	|	IDENTIFIER '.' IDENTIFIER { /* TODO: $$ = new peloton::expression::AbstractExpression(peloton::EXPRESSION_TYPE_COLUMN_REF, $1, $3); */ }
	;

literal:
		string_literal
	|	num_literal
	|	placeholder_expr
	;

string_literal:
		STRING { $$ = new peloton::expression::ConstantValueExpression(peloton::ValueFactory::GetStringValue($1)); free($1);}
	;


num_literal:
		FLOATVAL { $$ = new peloton::expression::ConstantValueExpression(peloton::ValueFactory::GetDoubleValue($1)); }
	|	int_literal
	;

int_literal:
		INTVAL { $$ = new peloton::expression::ConstantValueExpression(peloton::ValueFactory::GetIntegerValue($1)); $$->ival = $1; }
	;

star_expr:
		'*' { /* TODO: $$ = new peloton::expression::AbstractExpression(peloton::EXPRESSION_TYPE_STAR); */ } 
	;


placeholder_expr:
		'?' {
			/* TODO: $$ = new peloton::expression::AbstractExpression(peloton::EXPRESSION_TYPE_PLACEHOLDER, yylloc.total_column) */ ;
			yyloc.placeholder_list.push_back($$);
		}
	;


/******************************
 * Table 
 ******************************/
table_ref:
		table_ref_atomic
	|	table_ref_atomic ',' table_ref_commalist {
			$3->push_back($1);
			auto tbl = new TableRef(peloton::TABLE_REFERENCE_TYPE_CROSS_PRODUCT);
			tbl->list = $3;
			$$ = tbl;
		}
	;


table_ref_atomic:
		table_ref_name
	|	'(' select_statement ')' alias {
			auto tbl = new TableRef(peloton::TABLE_REFERENCE_TYPE_SELECT);
			tbl->select = $2;
			tbl->alias = $4;
			$$ = tbl;
		}
	|	join_clause
	;

table_ref_commalist:
		table_ref_atomic { $$ = new std::vector<TableRef*>(); $$->push_back($1); }
	|	table_ref_commalist ',' table_ref_atomic { $1->push_back($3); $$ = $1; }
	;


table_ref_name:
		table_name opt_alias {
			auto tbl = new TableRef(peloton::TABLE_REFERENCE_TYPE_NAME);
			tbl->name = $1;
			tbl->alias = $2;
			$$ = tbl;
		}
	;


table_ref_name_no_alias:
		table_name {
			$$ = new TableRef(peloton::TABLE_REFERENCE_TYPE_NAME);
			$$->name = $1;
		}
	;


table_name:
		IDENTIFIER
	|	IDENTIFIER '.' IDENTIFIER
	;


alias:	
		AS IDENTIFIER { $$ = $2; }
	|	IDENTIFIER
	;

opt_alias:
		alias
	|	/* empty */ { $$ = NULL; }


/******************************
 * Join Statements
 ******************************/

join_clause:
		join_table opt_join_type JOIN join_table ON join_condition
		{ 
			$$ = new TableRef(peloton::TABLE_REFERENCE_TYPE_JOIN);
			$$->join = new JoinDefinition();
			$$->join->type = (peloton::PelotonJoinType) $2;
			$$->join->left = $1;
			$$->join->right = $4;
			$$->join->condition = $6;
		}
		;

opt_join_type:
		INNER 	{ $$ = peloton::JOIN_TYPE_INNER; }
	|	OUTER 	{ $$ = peloton::JOIN_TYPE_OUTER; }
	|	LEFT 	{ $$ = peloton::JOIN_TYPE_LEFT; }
	|	RIGHT 	{ $$ = peloton::JOIN_TYPE_RIGHT; }
	|	/* empty, default */ 	{ $$ = peloton::JOIN_TYPE_INNER; }
	;



join_table:
		'(' select_statement ')' alias {
			auto tbl = new TableRef(peloton::TABLE_REFERENCE_TYPE_SELECT);
			tbl->select = $2;
			tbl->alias = $4;
			$$ = tbl;
		}
	|	table_ref_name;


join_condition:
		expr
		;


/******************************
 * Misc
 ******************************/

opt_semicolon:
		';'
	|	/* empty */
	;


ident_commalist:
		IDENTIFIER { $$ = new std::vector<char*>(); $$->push_back($1); }
	|	ident_commalist ',' IDENTIFIER { $1->push_back($3); $$ = $1; }
	;

%%
/*********************************
 ** Section 4: Additional C code
 *********************************/

/* empty */

