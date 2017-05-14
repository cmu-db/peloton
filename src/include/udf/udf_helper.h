#include <string>
#include <vector>
#include "tcop/tcop.h"
#include "catalog/catalog.h"
#include "type/type.h"
#include "type/value.h"
#include "type/value_factory.h"
#include "common/logger.h"

#ifndef UDF_HELP
#define UDF_HELP

namespace peloton {
namespace udf {

using arg_type=type::Type::TypeId;
using arg_value=type::Value;
using arg_tuple=std::tuple<std::string, arg_type>;

class UDF_Stmt {
 public:
  virtual ~UDF_Stmt() {}
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
  UDF_IFELSE_Stmt(std::string cond, UDF_SQL_Expr *true_exp, UDF_SQL_Expr *false_exp, int dtype = 0): true_exp_{true_exp}, false_exp_{false_exp}, dtype_(dtype) {
    auto cond_exp = new UDF_SQL_Expr(cond);
    cond_exp_.reset(cond_exp);
  }
  arg_value Execute(std::vector<arg_value>, std::vector<std::string>);

 private:
  std::unique_ptr<UDF_SQL_Expr> cond_exp_, true_exp_, false_exp_;
  int dtype_;
};

}
}

#endif // UDF_HELP
