#include <string>
#include <vector>
#include "tcop/tcop.h"
#include "catalog/catalog.h"
#include "type/type.h"
#include "type/value.h"
#include "type/value_factory.h"

#ifndef UDF_HELP
#define UDF_HELP

namespace peloton {
namespace udf {

using arg_type=type::Type::TypeId;
using arg_value=type::Value;
using arg_tuple=std::tuple<std::string, arg_type>;

class UDF_Stmt {
 public:
  virtual arg_value Execute(std::vector<arg_value>)=0;
};

class UDF_SQL_Expr final: UDF_Stmt {
 public:
  UDF_SQL_Expr(std::string query, int dtype = 0):query_(query), dtype_(dtype)
  {}
  arg_value Execute(std::vector<arg_value>);
  static tcop::TrafficCop traffic_cop_;
 private:
  std::string query_;
  int dtype_;
};

}
}

#endif // UDF_HELP
