#include <include/udf/udf_main.h>

namespace peloton {
namespace udf {

// Reference the interface from udf_gram.tab.c
int yy_scan_string(const char *);
int yyparse();

bool UDFHandle::compile() {
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

int UDFHandle::execute(std::vector<arg_value> values) {
  return stmt_.get()->Execute(values);
}

}  // namespace udf
}  // namespace peloton
