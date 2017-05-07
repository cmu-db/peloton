#include <include/udf/udf_helper.h>

namespace peloton {
namespace udf {

arg_value UDF_SQL_Expr::Execute(std::vector<arg_value>) {
  // TODO: The place to call the executor for udf in Peloton
  return ::peloton::type::ValueFactory::GetIntegerValue(0);
}

}
}
