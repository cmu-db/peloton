%{
/*-------------------------------------------------------------------------
 *
 * pl_gram.y			- Parser for the User Defined Function
 *
 *
 */

#include <stdio.h>
#include "udf.h"
#include "udf_helper.h"

static UDF_SQL_Expr *make_return_stmt(const char *);
  void yyerror(char *);
%}

%union
{
  char  *keyword;
  class UDF_Stmt *udf;
} 

%token K_RETURN K_BEGIN K_END FUNC_BODY

%type<keyword> K_RETURN K_BEGIN K_END FUNC_BODY
%type<udf> pl_function pl_block proc_stmt stmt_return

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
              printf("proc_stmt\n");
              $$ = $1;
            }
				;

stmt_return		: K_RETURN FUNC_BODY
					{
            printf("stmt_return\n");
						$$ = (UDF_Stmt*)make_return_stmt((const char *)$2);
					}
				;

%%

/* Convenience routine to read an expression with one possible terminator */

//static UDFSQL_expr *
//read_sql_construct(int stop_char,
//				   const char *expected,
//				   const char *sqlstart,
//				   bool isexpression,
//				   bool valid_sql,
//				   bool trim,
//				   int *startloc,
//				   int *endtoken)
//{
//	int					tok;
//  std::string query_str;
//	IdentifierLookup	save_IdentifierLookup;
//	int					startlocation = -1;
//	int					parenlevel = 0;
//
//  // Haoran: add select before anything else
//  query_str += sqlstart;
//
//  // TODO(Haoran): Not sure whether we need to call yylex() here
//	for (;;)
//	{
//		tok = yylex();
//		if (startlocation < 0)			/* remember loc of first token */
//			startlocation = yylloc;
//		if (tok == stop_char && parenlevel == 0)
//			break;
//	}
//
//  // Assert this returns false
//	if (startloc)
//		*startloc = startlocation;
//
//  // TODO: rewrite this one
//	plpgsql_append_source_text(&ds, startlocation, yylloc);
//
//  // TODO: do we need to do trim or not?
//	//if (trim)
//	//{
//	//	while (ds.len > 0 && scanner_isspace(ds.data[ds.len - 1]))
//	//		ds.data[--ds.len] = '\0';
//	//}
//
//  // what does the ns do?
//	// expr->ns			= plpgsql_ns_top();
//
//  // Initialize this later
//	UDFSQL_expr		*expr = new UDFSQL_expr(query_str, PLPGSQL_DTYPE_EXPR);
//
//  // No validation yet. No raw_parser in Peloton
//
//	return expr;
//}

static UDF_SQL_Expr *
make_return_stmt(const char *func_body)
{
	UDF_SQL_Expr	*expr = new UDF_SQL_Expr(func_body);
	return (UDF_SQL_Expr *) expr;
}

void yyerror(char *s) {
  fprintf(stderr, "xxxx%sxxxx\n", s);
}
