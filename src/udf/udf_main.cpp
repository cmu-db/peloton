#include <include/udf/udf_main.h>

namespace peloton {
namespace udf {

// Reference the interface from udf_gram.tab.c
int yy_scan_string(const char *);
int yyparse();

bool UDFHandle::Compile() {
  // TODO: May require lock here.
  if (stmt_)
    return false;
  yy_scan_string(body_.c_str());
  yyparse();
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
