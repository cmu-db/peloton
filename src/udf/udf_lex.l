
%{
  #include <stdlib.h>
  #include <string>
  #include "udf_gram.tab.h"
  namespace peloton{
  namespace udf{
  void yyerror(std::string);
%}

digit [0-9]
letter [A-Za-z]

%%

[ \t] ;

; {
            printf("match semi-colon\n");
            return K_SEMICOLON;
        }

BEGIN   {
            printf("match begin\n");
            return K_BEGIN;
          }

END     {
            printf ("match end\n");
            return K_END;
        }

IF  {
            printf ("match IF\n");
            return K_IF;
        }

THEN  {
            printf ("match then\n");
            return K_THEN;
        }

ELSE  {
            printf ("match else\n");
            return K_ELSE;
        }

RETURN {
            printf ("match return\n");
            return K_RETURN;
        }


[^ \t;]* {
            yylval.keyword = yytext;
            printf("func_body is: %s\n", yytext);
            return FUNC_BODY;
          }


%%

int yywrap(void) {
  return 1;
}

}
}
