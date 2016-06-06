/******************************************************************
*
* uSQL for C++
*
* UnQL.g
*
* Copyright (C) Satoshi Konno 2011
*
* This is licensed under BSD-style license, see file COPYING.
*
******************************************************************/

grammar SQL;

options
{
    language = C;
}

@lexer::includes
{
}

@parser::includes
{
	#include "parser/parser/antlr/SQLParser.h"
	#include "parser/SQLParser.h"
	#define CG_ANTLR3_STRING_2_UTF8(str) ((const char *)str->chars)
	#define CG_ANTLR3_STRING_2_INT(str) (str->chars ? atoi((const char *)str->chars) : 0)
	inline void CG_ANTLR3_SQLNODE_ADDNODES(uSQL::SQLNode *parentNode, uSQL::SQLNodeList *sqlNodes) {
		for (uSQL::SQLNodeList::iterator node = sqlNodes->begin(); node != sqlNodes->end(); node++)
			parentNode->addChildNode(*node);
	}
	
	void uSQLDisplayRecognitionError (pANTLR3_BASE_RECOGNIZER rec, pANTLR3_UINT8 * tokenNames);
}

@parser::context
{
	void *uSqlParser;
}

@parser::apifuncs
 {
 	RECOGNIZER->displayRecognitionError = uSQLDisplayRecognitionError;
 	PARSER->super = (void *)ctx;
 }
 
/*------------------------------------------------------------------
 *
 * PARSER RULES
 *
 *------------------------------------------------------------------*/

statement_list [uSQL::SQLParser *sqlParser]
	: statement[sqlParser] (SEMICOLON statement[sqlParser])*
	;	

statement [uSQL::SQLParser *sqlParser]
	@init {
		uSQL::SQLStatement *stmt = new uSQL::SQLStatement();
		sqlParser->addStatement(stmt);
	}
	: show_stmt[stmt]
	| use_stmt[stmt]
	| create_collection_stmt[stmt]
	| create_index_stmt[stmt]
	| drop_collection_stmt[stmt]
	| drop_index_stmt[stmt]
	| select_stmt[stmt]
	| insert_stmt[stmt]
	| update_stmt[stmt]
	| delete_stmt[stmt]
	;	

/******************************************************************
*
* SHOW
*
******************************************************************/

show_stmt [uSQL::SQLStatement *sqlStmt]
	@init {	
	}
	: SHOW collectionNode=collection_section
	{
		// SHOW
		uSQL::SQLShow *sqlCmd = new uSQL::SQLShow();
		sqlStmt->addChildNode(sqlCmd);
		
		// Collection
		sqlCmd->addChildNode(collectionNode);
	}
	;

/******************************************************************
*
* USE
*
******************************************************************/

use_stmt [uSQL::SQLStatement *sqlStmt]
	@init {	
	}
	: USE collectionNode=collection_section
	{
		// USE
		uSQL::SQLUse *sqlCmd = new uSQL::SQLUse();
		sqlStmt->addChildNode(sqlCmd);
		
		// Collection
		sqlCmd->addChildNode(collectionNode);
	}
	;
	
/******************************************************************
*
* CREATE
*
******************************************************************/

create_collection_stmt [uSQL::SQLStatement *sqlStmt]
	@init {	
		uSQL::SQLOption *sqlOpt = new uSQL::SQLOption();
	}
	: CREATE COLLECTION collectionNode=collection_section (OPTIONS expression[sqlOpt])? {
		// CREATE
		uSQL::SQLCreate *sqlCmd = new uSQL::SQLCreate();
		sqlStmt->addChildNode(sqlCmd);
		
		// Collection
		sqlCmd->addChildNode(collectionNode);

		// Option 
		if (sqlOpt->hasExpressions())
			sqlCmd->addChildNode(sqlOpt);
		else 
			delete sqlOpt;
	  }
	;

/******************************************************************
*
* DROP
*
******************************************************************/

drop_collection_stmt [uSQL::SQLStatement *sqlStmt]
	@init {	
	}
	: DROP COLLECTION collectionNode=collection_section
	{
		// DROP
		uSQL::SQLDrop *sqlCmd = new uSQL::SQLDrop();
		sqlStmt->addChildNode(sqlCmd);
		
		// Collection
		sqlCmd->addChildNode(collectionNode);
	}
	;

/******************************************************************
*
* CREATE INDEX
*
******************************************************************/

create_index_stmt [uSQL::SQLStatement *sqlStmt]
	@init {	
	}
	: CREATE COLLECTION_INDEX indexNode=index_section
	{
		// DROP
		uSQL::SQLCreateIndex *sqlCmd = new uSQL::SQLCreateIndex();
		sqlStmt->addChildNode(sqlCmd);
		
		// Collection
		sqlCmd->addChildNode(indexNode);
	}
	;

/******************************************************************
*
* DROP INDEX
*
******************************************************************/

drop_index_stmt [uSQL::SQLStatement *sqlStmt]
	@init {	
	}
	: DROP COLLECTION_INDEX indexNode=index_section
	{
		// DROP
		uSQL::SQLDropIndex *sqlCmd = new uSQL::SQLDropIndex();
		sqlStmt->addChildNode(sqlCmd);
		
		// Collection
		sqlCmd->addChildNode(indexNode);
	}
	;

/******************************************************************
*
* SELECT
*
******************************************************************/

select_stmt [uSQL::SQLStatement *sqlStmt]
	@init {
		// SELECT
		uSQL::SQLSelect *sqlSelect = new uSQL::SQLSelect();
		sqlStmt->addChildNode(sqlSelect);

		sortingSection = NULL;
		limitSection = NULL;
		offsetSection = NULL;
	}
	: select_core[sqlStmt] (sortingSection=sorting_section)? (limitSection=limit_section)? (offsetSection=offset_section)? 
	{
		// ORDER BY		
		if (sortingSection)
			sqlStmt->addChildNode(sortingSection);
			
		// LIMIT		
		if (limitSection)
			sqlStmt->addChildNode(limitSection);

		// OFFSET
		if (offsetSection)
			sqlStmt->addChildNode(offsetSection);
	}
	;

select_core [uSQL::SQLStatement *sqlStmt]
	@init {
		columnSection = NULL;
		fromSection = NULL;
		whereSection = NULL;
		groupSection = NULL;
		havingSection = NULL;
	}
	: SELECT (DISTINCT | ALL)?
	  (columnSection = result_column_section)?
	  (fromSection = from_section)//? 
	  (whereSection = where_section)?
	  (groupSection = grouping_section)? 
	  (havingSection = having_section)? 
	  {
	  
		if (columnSection) {
			if (columnSection->hasExpressions())
				sqlStmt->addChildNode(columnSection);
			else 
				delete columnSection;
		}
		// FROM
		if (fromSection)		
			sqlStmt->addChildNode(fromSection);
			
		// WHERE
		if (whereSection)		
			sqlStmt->addChildNode(whereSection);

		// GROUP BY
		if (groupSection)		
			sqlStmt->addChildNode(groupSection);

		// HAVING
		if (havingSection)		
			sqlStmt->addChildNode(havingSection);
	  }
	;

result_column_section returns [uSQL::SQLColumns *sqlColumns]
	@init {
		sqlColumns = new uSQL::SQLColumns();
	}
	: ASTERISK {
		uSQL::SQLAsterisk *sqlAsterisk = new uSQL::SQLAsterisk();
		sqlColumns->addExpression(sqlAsterisk);
	  }
	| column_section[sqlColumns] (',' column_section[sqlColumns])* {
	  }
	;

from_section returns [uSQL::SQLFrom *sqlCollections]
	@init {
		sqlCollections = new uSQL::SQLFrom();
	}
	: (FROM table_name[sqlCollections]) (COMMA table_name[sqlCollections])*
	;

table_name [uSQL::SQLCollections *SQLCollections]
	: dataSource = data_source {
		SQLCollections->addChildNode(dataSource);
	  }
	;

data_source returns [uSQL::SQLCollection *sqlDataSource]
	@init {
		sqlDataSource = new uSQL::SQLCollection();
	}
	: collection_name {
		// Collection
		sqlDataSource->setValue(CG_ANTLR3_STRING_2_UTF8($collection_name.text));
	  }
	;

grouping_section returns [uSQL::SQLGroupBy *sqlGroupBy]
	@init {
		sqlGroupBy = new uSQL::SQLGroupBy();
	}
	: GROUP BY expression[sqlGroupBy] (COMMA expression[sqlGroupBy])*
	;
	
having_section returns [uSQL::SQLHaving *sqlHaving]
	@init {
		sqlHaving = new uSQL::SQLHaving();
	}
	: HAVING expression[sqlHaving]
	;

sorting_section returns [uSQL::SQLOrderBy *sqlOrders]
	@init {
		sqlOrders = new uSQL::SQLOrderBy();
	}
	: ORDER BY sorting_item[sqlOrders] (COMMA sorting_item[sqlOrders])*
	;
		
sorting_item [uSQL::SQLOrderBy *sqlOrders]
	: property (sorting_specification)? {
		uSQL::SQLOrder *sqlOrder = new uSQL::SQLOrder();
		sqlOrder->setValue(CG_ANTLR3_STRING_2_UTF8($property.text));
		if (sorting_specification)
			sqlOrder->setOrder(CG_ANTLR3_STRING_2_UTF8($sorting_specification.text));
		sqlOrders->addChildNode(sqlOrder);
	  }
	;

sorting_specification
	: ASC
	| DESC
	;

limit_section returns [uSQL::SQLLimit *sqlLimit]
	@init {
		sqlLimit = new uSQL::SQLLimit();
		offsetExpr = NULL;
	}
	: LIMIT (offsetExpr=expression_literal COMMA)? countExpr=expression_literal {
		if (offsetExpr)
			sqlLimit->addChildNode(offsetExpr);
		sqlLimit->addChildNode(countExpr);
	  }
	;

offset_section returns [uSQL::SQLOffset *sqlOffset]
	@init {
		sqlOffset = new uSQL::SQLOffset();
	}
	: offsetExpr=expression_literal {
		sqlOffset->addChildNode(offsetExpr);
	  }
	;

/******************************************************************
*
* INSERT
*
******************************************************************/

insert_stmt [uSQL::SQLStatement *sqlStmt]
	@init {
		isAsync = false;
		columnNode = NULL;
	}
	: (isAsync=sync_operator)? INSERT INTO collectionNode=collection_section (columnNode=insert_columns_section)? sqlValue=insert_values_section
	{
		// INSERT
		uSQL::SQLInsert *sqlCmd = new uSQL::SQLInsert();
		sqlCmd->setAsyncEnabled(isAsync);
		sqlStmt->addChildNode(sqlCmd);

		// Collection
		uSQL::SQLCollections *sqlCollections = new uSQL::SQLCollections();
		sqlStmt->addChildNode(sqlCollections);
		sqlCollections->addChildNode(collectionNode);
		
		// Column
		if (columnNode)
			sqlStmt->addChildNode(columnNode);
		
		// Value
		sqlStmt->addChildNode(sqlValue);
	}
	;

insert_columns_section returns [uSQL::SQLColumns *sqlColumns]
	@init {
		sqlColumns = new uSQL::SQLColumns();
	}
	: '(' column_section[sqlColumns] (',' column_section[sqlColumns])* ')' {
	  }
	;

insert_values_section returns [uSQL::SQLValues *sqlValues]
	@init {
		sqlValues = new uSQL::SQLValues();
	}
	: VALUE expression[sqlValues] {
	  }
	| VALUES expression[sqlValues] {
	  }
	;

/******************************************************************
*
* UPDATE
*
******************************************************************/

update_stmt [uSQL::SQLStatement *sqlStmt]
	@init {
		uSQL::SQLSets *sqlSet = new uSQL::SQLSets();
		isAsync = false;
		whereSection = NULL;
	}
	: (isAsync=sync_operator)? UPDATE collectionNode=collection_section SET property_section[sqlSet] (COMMA property_section[sqlSet])* (whereSection = where_section)?

	{
		// INSERT
		uSQL::SQLUpdate *sqlCmd = new uSQL::SQLUpdate();
		sqlCmd->setAsyncEnabled(isAsync);
		sqlStmt->addChildNode(sqlCmd);

		// Collection
		uSQL::SQLCollections *sqlCollections = new uSQL::SQLCollections();
		sqlStmt->addChildNode(sqlCollections);
		sqlCollections->addChildNode(collectionNode);
		
		// Set
		sqlStmt->addChildNode(sqlSet);

		// WHERE
		if (whereSection)		
			sqlStmt->addChildNode(whereSection);
	}
	;

property_section [uSQL::SQLSets *sqlSet]
	@init {
		
	}
	: property SINGLE_EQ exprRight=expression_literal

	{
		uSQL::SQLSet *sqlDict = new uSQL::SQLSet();
		sqlDict->setName(CG_ANTLR3_STRING_2_UTF8($property.text));
		sqlDict->setValue(exprRight);
		sqlSet->addChildNode(sqlDict);
	}
	;

/******************************************************************
*
* DELETE
*
******************************************************************/

delete_stmt [uSQL::SQLStatement *sqlStmt]
	@init {
		isAsync = false;
		whereSection = NULL;
	}
	: (isAsync=sync_operator)? DELETE_TOKEN FROM collectionNode=collection_section (whereSection = where_section)?

	{
		// DELETE
		uSQL::SQLDelete *sqlCmd = new uSQL::SQLDelete();
		sqlCmd->setAsyncEnabled(isAsync);
		sqlStmt->addChildNode(sqlCmd);

		// Collection
		uSQL::SQLCollections *sqlCollections = new uSQL::SQLCollections();
		sqlStmt->addChildNode(sqlCollections);
		sqlCollections->addChildNode(collectionNode);
		
		// WHERE
		if (whereSection)		
			sqlStmt->addChildNode(whereSection);
	}
	;

/******************************************************************
*
* Expression
*
******************************************************************/

expression [uSQL::SQLNode *parentNode]
	@init {
		uSQL::SQLNodeList sqlNodeList;
	}
 	: expression_list[sqlNodeList] {
 		CG_ANTLR3_SQLNODE_ADDNODES(parentNode, &sqlNodeList);
	  }
	;

expression_list [uSQL::SQLNodeList &sqlNodeList]
 	: sqlExpr=expression_literal {
		sqlNodeList.push_back(sqlExpr);
	  }
	| sqlFunc=expression_function {
		sqlNodeList.push_back(sqlFunc);
	  }
	| expression_binary_operator[sqlNodeList] (expression_logic_operator[sqlNodeList] expression_binary_operator[sqlNodeList])* {
		sqlNodeList.sort();
	  }
	| '(' expression_array[sqlNodeList] (COMMA expression_array[sqlNodeList] )* ')'
	| '{' (expression_dictionary[sqlNodeList]) (COMMA expression_dictionary[sqlNodeList])* '}'
	| '[' expression_array[sqlNodeList] (COMMA expression_array[sqlNodeList] )* ']'
	;

expression_literal returns [uSQL::SQLExpression *sqlExpr]
	@init {
		sqlExpr = new uSQL::SQLExpression();
	}
 	: expression_literal_value[sqlExpr]
 	;

expression_literal_value [uSQL::SQLExpression *sqlExpr]
 	: property_literal {
		sqlExpr->setLiteralType(uSQL::SQLExpression::PROPERTY);
		sqlExpr->setValue(CG_ANTLR3_STRING_2_UTF8($property_literal.text));
	  }
	| integer_literal {
		sqlExpr->setLiteralType(uSQL::SQLExpression::INTEGER);
		sqlExpr->setValue(CG_ANTLR3_STRING_2_UTF8($integer_literal.text));
	  }
	| real_literal {
		sqlExpr->setLiteralType(uSQL::SQLExpression::REAL);
		sqlExpr->setValue(CG_ANTLR3_STRING_2_UTF8($real_literal.text));
	  }
	| string_literal {
		sqlExpr->setLiteralType(1/*uSQL::SQLExpression::STRING*/);
		sqlExpr->setValue(CG_ANTLR3_STRING_2_UTF8($string_literal.text));
	  }
	| true_literal {
		sqlExpr->setLiteralType(uSQL::SQLExpression::BOOLEAN);
		sqlExpr->setValue(CG_ANTLR3_STRING_2_UTF8($true_literal.text));
	  }
	| false_literal {
		sqlExpr->setLiteralType(uSQL::SQLExpression::BOOLEAN);
		sqlExpr->setValue(CG_ANTLR3_STRING_2_UTF8($false_literal.text));
	  }
	| NIL {
		sqlExpr->setLiteralType(10/*uSQL::SQLExpression::NIL*/);
	  }
	| CURRENT_TIME {
		sqlExpr->setLiteralType(11/*uSQL::SQLExpression::CURRENT_TIME*/);
	  }
	| CURRENT_DATE {
		sqlExpr->setLiteralType(12/*uSQL::SQLExpression::CURRENT_DATE*/);
	  }
	| CURRENT_TIMESTAMP {
		sqlExpr->setLiteralType(13/*uSQL::SQLExpression::CURRENT_TIMESTAMP*/);
	  }
	;

expression_dictionary [uSQL::SQLNodeList &sqlNodeList]
	: name ':' sqlExpr=expression_literal {
		uSQL::SQLSet *dictNode = new uSQL::SQLSet();
		dictNode->set(sqlExpr);
		dictNode->setName(CG_ANTLR3_STRING_2_UTF8($name.text));
		sqlNodeList.push_back(dictNode);
		delete sqlExpr;
	  }
	;

dictionary_literal [uSQL::SQLExpression *parentSqlExpr]
	: name ':' sqlExpr=expression_literal {
		uSQL::SQLSet *dictNode = new uSQL::SQLSet();
		dictNode->set(sqlExpr);
		dictNode->setName(CG_ANTLR3_STRING_2_UTF8($name.text));
		parentSqlExpr->addExpression(dictNode);
		delete sqlExpr;
	  }
	;

expression_array [uSQL::SQLNodeList &sqlNodeList]
	: sqlExpr=expression_literal {
		sqlNodeList.push_back(sqlExpr);
	  }
	;

array_literal [uSQL::SQLExpression *parentSqlExpr]
	: sqlExpr=expression_literal {
		parentSqlExpr->addExpression(sqlExpr);
	  }
	;

expression_logic_operator[uSQL::SQLNodeList &sqlNodeList]
	: logicOper=logical_operator {
		sqlNodeList.push_back(logicOper);
	  }
	;

expression_binary_operator [uSQL::SQLNodeList &sqlNodeList]
	: binOper=expression_operator {
		sqlNodeList.push_back(binOper);
	  }
	;


expression_function returns [uSQL::SQLFunction *sqlFunc]
	@init {
		sqlFunc = new uSQL::SQLFunction();
		sqlFunc->setLiteralType(uSQL::SQLExpression::FUNCTION);
	}
	: ID '(' (function_value[sqlFunc])? ')' {
		sqlFunc->setValue(CG_ANTLR3_STRING_2_UTF8($ID.text));
	  }
	;

function_name returns [uSQL::SQLFunction *sqlFunc]
	@init {
		sqlFunc = new uSQL::SQLFunction();
		sqlFunc->setLiteralType(uSQL::SQLExpression::FUNCTION);
	}
	: ID {
		sqlFunc->setValue(CG_ANTLR3_STRING_2_UTF8($ID.text));
	  }
	;

function_value [uSQL::SQLFunction *sqlFunc]
	: expression[sqlFunc] (COMMA expression[sqlFunc])*
	| ASTERISK {
		uSQL::SQLExpression *sqlExpr = new uSQL::SQLExpression();
		sqlExpr->setLiteralType(1/*uSQL::SQLExpression::STRING*/);
		sqlExpr->setValue("*");
		sqlExpr->addExpression(sqlFunc);
	}
	;

expression_operator returns [uSQL::SQLOperator *sqlOpeExpr]
	: leftExpr=expression_literal sqlBinOpeExpr=binary_operator rightExpr=expression_literal {
		sqlOpeExpr = sqlBinOpeExpr;
		sqlOpeExpr->addExpression(leftExpr);
		sqlOpeExpr->addExpression(rightExpr);
	  }
	;

binary_operator returns [uSQL::SQLOperator *sqlOper]
	@init {
		sqlOper = new uSQL::SQLOperator();
		sqlOper->setLiteralType(uSQL::SQLExpression::OPERATOR);
	}
	: SINGLE_EQ {
		sqlOper->setValue(1/*uSQL::SQLOperator::SEQ*/);
	  }
	| DOUBLE_EQ {
		sqlOper->setValue(2/*uSQL::SQLOperator::DEQ*/);
	  }
	| OP_LT {
		sqlOper->setValue(3/*uSQL::SQLOperator::LT*/);
	  }
	| LE {
		sqlOper->setValue(4/*uSQL::SQLOperator::LE*/);
	  }
	| GT {
		sqlOper->setValue(5/*uSQL::SQLOperator::GT*/);
	  }
	| GE {
		sqlOper->setValue(6/*uSQL::SQLOperator::GE*/);
	  }
	| NOTEQ {
		sqlOper->setValue(7/*uSQL::SQLOperator::NOTEQ*/);
	  }
	;

logical_operator returns [uSQL::SQLOperator *sqlOper]
	@init {
		sqlOper = new uSQL::SQLOperator();
		sqlOper->setLiteralType(uSQL::SQLExpression::OPERATOR);
	}
	: AND {
		sqlOper->setValue(8/*uSQL::SQLOperator::AND*/);
	  }
	| OR {
		sqlOper->setValue(9/*uSQL::SQLOperator::OR*/);
	  }
	;

property_literal
	: ID 
	;

integer_literal
	: NUMBER
	;

real_literal
	: FLOAT
	;

string_literal
	: STRING
	;

true_literal
	: T R U E
	;
	
false_literal
	: F A L S E
	;

/******************************************************************
*
* COMMON
*
******************************************************************/

sync_operator returns [bool isAync]
	: SYNC {
		isAync = false;
	  }
	| ASYNC {
		isAync = true;
	  }
	;

compound_operator
	: UNION (ALL)?
	| INTERSECT
	| EXCEPT
	;

condition_operator
	: SINGLE_EQ
	| DOUBLE_EQ
	| OP_LT
	| LE
	| GT
	| GE
	| NOTEQ
	;

property
	: ID 
	;

value 	
	: ID	
	;

name
	: ID
	;

collection_section returns [uSQL::SQLCollection *sqlCollection]
	@init {
		sqlCollection = new uSQL::SQLCollection();
	}
	: collection_name {
		sqlCollection->setValue(CG_ANTLR3_STRING_2_UTF8($collection_name.text));
	  }
	;

collection_name
	: ID
	| string_literal
	;

column_section [uSQL::SQLColumns *sqlColumns]
	: ((expression[sqlColumns]) (AS name)?) {
	  }
	;

index_section returns [uSQL::SQLIndex *sqlIndex]
	@init {
		sqlIndex = new uSQL::SQLIndex();
	}
	: index_name {
		sqlIndex->setValue(CG_ANTLR3_STRING_2_UTF8($index_name.text));
	  }
	;

index_name
	: ID
	| string_literal
	;

where_section returns [uSQL::SQLWhere *sqlWhere]
	@init {
		sqlWhere = new uSQL::SQLWhere();
	}
	: WHERE expression[sqlWhere]
	;

/*------------------------------------------------------------------
 * LEXER RULES
 *------------------------------------------------------------------*/


ASTERISK
	: '*'
	;
	
SINGLE_EQ
	: '='
	;

DOUBLE_EQ
	: '=='
	;

OP_LT
	: '<'
	;
	
LE
	: '<='
	;
	
GT
	: '>'
	;

GE	
	: '>='
	;
	
NOTEQ
	: '!='
	;

AND
	: A N D
	| '&'
	;

OR
	: O R
	| '|'
	;

COMMA
	: ','
	;

SEMICOLON
	: ';'
	;
	
fragment 
A	: 'A'
	| 'a'
	;

fragment 
B	: 'B'
	| 'b'
	;

fragment 
C	: 'C'
	| 'c'
	;

fragment 
D	: 'D'
	| 'd'
	;

fragment 
E	: 'E'
	| 'e'
	;

fragment 
F	: 'F'
	| 'f'
	;

fragment 
G	: 'G'
	| 'g'
	;

fragment 
H	: 'H'
	| 'h'
	;

fragment 
I	: 'I'
	| 'i'
	;

fragment 
J	: 'J'
	| 'j'
	;

fragment 
K	: 'K'
	| 'k'
	;

fragment 
L	: 'L'
	| 'l'
	;

fragment 
M	: 'M'
	| 'm'
	;

fragment 
N	: 'N'
	| 'n'
	;

fragment 
O	: 'O'
	| 'o'
	;

fragment 
P	: 'P'
	| 'p'
	;

fragment 
Q	: 'Q'
	| 'q'
	;

fragment 
R	: 'R'
	| 'r'
	;

fragment 
S	: 'S'
	| 's'
	;

fragment 
T	: 'T'
	| 't'
	;

fragment 
U	: 'U'
	| 'u'
	;

fragment 
V	: 'V'
	| 'v'
	;

fragment 
W	: 'W'
	| 'w'
	;

fragment 
X	: 'X'
	| 'x'
	;

fragment 
Y	: 'Y'
	| 'y'
	;

fragment 
Z	: 'Z'
	| 'z'
	;

ALL
	: A L L
	;
	
ANCESTOR
	: A N C E S T O R
	;

AS
	: A S
	;

ASC
	: A S C
	;
	
ASYNC
	: A S Y N C
	;
	
BY
	: B Y
	;

CREATE
	: C R E A T E
	;

COLLECTION
	: C O L L E C T I O N
	;

CURRENT_DATE
	: C U R R E N T '_' D A T E
	;

CURRENT_TIME
	: C U R R E N T '_' T I M E
	;

CURRENT_TIMESTAMP
	: C U R R E N T '_' T I M E S T A M P
	;

DESC
	: D E S C
	;

DELETE_TOKEN
	: D E L E T E
	;

DISTINCT
	: D I S T I N C T
	;

DROP
	: D R O P
	;

EACH
	: E A C H
	;
		
EXCEPT
	: E X C E P T
	;

FLATTEN
	: F L A T T E N
	;
		
FROM
	: F R O M
	;

GROUP
	: G R O U P
	;
		
HAVING
	: H A V I N G
	;
	
IN
	: I N
	;

COLLECTION_INDEX
	: I N D E X
	;

INSERT
	: I N S E R T
	;

INTERSECT
	: I N T E R S E C T
	;

INTO
	: I N T O
	;

IS
	: I S
	;

LIMIT
	: L I M I T
	;

NIL
	: N U L L
	;
	
OFFSET
	: O F F S E T
	;

OPTIONS
	: O P T I O N S
	;

ORDER
	: O R D E R
	;
	
SELECT
	: S E L E C T
	;

SET
	: S E T
	;

SHOW
	: S H O W
	;

SYNC
	: S Y N C
	;

USE
	: U S E
	;

UNION
	: U N I O N
	;

UPDATE
	: U P D A T E
	;

WHERE
	: W H E R E
	;

VALUE
	: V A L U E
	;

VALUES
	: V A L U E S
	;

/******************************************************************
*
* COMMON
*
******************************************************************/

WS  :   ( ' '
        | '\t'
        | '\r'
        | '\n'
        ) {$channel=HIDDEN;}
    ;
	
ID  
	//: ('a'..'z'|'A'..'Z'|'_') ('a'..'z'|'A'..'Z'|'0'..'9'|'_')*
	: ('a'..'z'|'A'..'Z'|'_'|'/')('a'..'z'|'A'..'Z'|'_'|'-'|'/'|'.'|'0'..'9')*
    	;

NUMBER 
	: '0'..'9'+
	;

FLOAT
	:   ('0'..'9')+ '.' ('0'..'9')* EXPONENT?
	|   '.' ('0'..'9')+ EXPONENT?
	|   ('0'..'9')+ EXPONENT
	;

STRING
	:  '"' ( EscapeSequence | ~('\\'| '"') )* '"' 
	;

fragment
EscapeSequence
	:   '\\' ('\"'|'\''|'\\')
	;

/*
NAME
   :   ('a'..'z'|'A'..'Z'|'_')('a'..'z'|'A'..'Z'|'_'|'-'|'0'..'9')*
   ;
*/

CHAR:  '\'' ( ESC_SEQ | ~('\''|'\\') ) '\''
    ;

fragment
EXPONENT : ('e'|'E') ('+'|'-')? ('0'..'9')+ ;

fragment
HEX_DIGIT : ('0'..'9'|'a'..'f'|'A'..'F') ;

fragment
ESC_SEQ
    :   '\\' ('b'|'t'|'n'|'f'|'r'|'\"'|'\''|'\\')
    |   UNICODE_ESC
    |   OCTAL_ESC
    ;

fragment
OCTAL_ESC
    :   '\\' ('0'..'3') ('0'..'7') ('0'..'7')
    |   '\\' ('0'..'7') ('0'..'7')
    |   '\\' ('0'..'7')
    ;

fragment
UNICODE_ESC
    :   '\\' 'u' HEX_DIGIT HEX_DIGIT HEX_DIGIT HEX_DIGIT
    ;
