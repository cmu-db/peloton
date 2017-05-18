
%{
  #include <stdlib.h>
  void yyerror(char *);
  #include "udf_gram.tab.h"
%}

%option noyyunput

%%

[ \t] ;

BEGIN   {
            printf("match begin\n");
            return K_BEGIN;
          }

END     {
            printf ("math end\n");
            return K_END;
        }

RETURN  { 
            printf ("math return\n");
            return K_RETURN;
        }


[^; \t]*; { 
            yylval.keyword = yytext;
            printf("func_body is: %s\n", yytext);
            return FUNC_BODY;
          }


%%

int yywrap(void) {
  return 1;
}
