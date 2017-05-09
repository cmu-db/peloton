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
  virtual arg_value Execute(std::vector<arg_value>, std::vector<std::string>)=0;
};

class UDF_SQL_Expr final: UDF_Stmt {
 public:
  UDF_SQL_Expr(std::string query, int dtype = 0):query_(query), dtype_(dtype)
  {}
  arg_value Execute(std::vector<arg_value>, std::vector<std::string>);
  static tcop::TrafficCop traffic_cop_;
  std::string query_;
 private:
  int dtype_;
};

class UDF_IFELSE_Stmt final: UDF_Stmt {
 public:
  UDF_IFELSE_Stmt(std::string cond, std::string query1,std::string query2 = "", int dtype = 0):cond_exp_{cond}, true_exp_{query1}, false_exp_{query2}, dtype_(dtype) {}
  arg_value Execute(std::vector<arg_value>, std::vector<std::string>);
 private:
  UDF_SQL_Expr cond_exp_;
  UDF_SQL_Expr true_exp_;
  UDF_SQL_Expr false_exp_;
  int dtype_;
};

}
}

#endif // UDF_HELP
