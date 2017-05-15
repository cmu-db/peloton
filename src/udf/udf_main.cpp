#include <include/udf/udf_main.h>

namespace peloton {
namespace udf {

// Reference the interface from udf_gram.tab.cpp and udf_lex.cpp
struct yy_buffer_state;
struct yy_buffer_state* yy_scan_string (const char *yy_str  );
int yylex_destroy (void );
int yyparse();

bool UDFHandle::Compile() {
  // TODO: May require lock here.
  if (stmt_)
    return false;
  yy_scan_string(body_.c_str());
  yyparse();
  yylex_destroy();
  if (!udf_parsed_result)
    return false;
  stmt_.reset(udf_parsed_result);
  udf_parsed_result = NULL;
  return true;
}

arg_value UDFHandle::Execute(std::vector<arg_value> values) {
  Compile();
  return stmt_.get()->Execute(values, args_name_);
}

}  // namespace udf
}  // namespace peloton
