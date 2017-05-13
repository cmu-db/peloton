%{
/*-------------------------------------------------------------------------
 *
 * pl_gram.y			- Parser for the User Defined Function
 *
 *
 */

#include <stdio.h>
#include <string>
#include "udf.h"
#include "udf_helper.h"


namespace peloton{
namespace udf{

static UDF_SQL_Expr *make_return_stmt(const char *);
static UDF_IFELSE_Stmt *make_ifelse_stmt(const char *, UDF_SQL_Expr *, UDF_SQL_Expr *);
void yyerror(std::string);
int yylex(void);
%}

%union
{
  char  *keyword;
  class UDF_Stmt *udf;
  class UDF_SQL_Expr *udf_sql_expr;
  class UDF_IFELSE_Stmt *udf_ifelse_stmt;

} 

%token K_RETURN K_BEGIN K_END FUNC_BODY K_IF K_THEN K_ELSE K_SEMICOLON

%type<keyword> K_RETURN K_BEGIN K_END FUNC_BODY K_IF K_THEN K_ELSE K_SEMICOLON
%type<udf> pl_function pl_block proc_stmt
%type<udf_sql_expr> stmt_return
%type<udf_ifelse_stmt> ifelse_stmt 

%%

pl_function		: pl_block
					{
            printf("pl_func\n");
						udf_parsed_result = $1;
					}
				;

pl_block		:  K_BEGIN proc_stmt K_END
					{
            printf("pl_block\n");
						$$ = $2;
					}
				;

proc_stmt		: stmt_return
						{
              printf("proc_stmt: return \n");
              $$ = (UDF_Stmt*)$1;
            }
            | ifelse_stmt
						{
              printf("proc_stmt: ifelse \n");
              $$ = (UDF_Stmt*)$1;
            }
				;

ifelse_stmt	: K_IF FUNC_BODY K_THEN stmt_return K_ELSE stmt_return K_END K_IF
             {
              std::string str = $2;
              size_t then_pos = str.find("THEN");
              printf("if else statement %s\n", str.substr(0, then_pos).c_str());
						  $$ = make_ifelse_stmt(str.substr(0, then_pos).c_str(), $4, $6);
             }

stmt_return		: K_RETURN FUNC_BODY K_SEMICOLON
					{
            printf("stmt_return\n");
            printf("stmt_return %s\n", $2);
						$$ = make_return_stmt((const char *)$2);
					}
				;

%%

static UDF_SQL_Expr *
make_return_stmt(const char *func_body)
{
	UDF_SQL_Expr	*expr = new UDF_SQL_Expr(func_body);
	return expr;
}

static UDF_IFELSE_Stmt *
make_ifelse_stmt(const char *cond_str, UDF_SQL_Expr * true_expr_, UDF_SQL_Expr * false_expr_)
{
	UDF_IFELSE_Stmt	*expr = new UDF_IFELSE_Stmt(cond_str, true_expr_, false_expr_);
	return expr;
}

void yyerror(std::string s) {
  fprintf(stderr, "xxxx%sxxxx\n", s.c_str());
}

}
}
