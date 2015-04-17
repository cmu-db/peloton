#pragma once

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
