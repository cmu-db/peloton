#include <string>
#include <vector>

#ifndef UDF_HELP
#define UDF_HELP

namespace peloton {
namespace udf {

using arg_type=int;
using arg_value=int;
using arg_tuple=std::tuple<std::string, arg_type>;

class UDF_Stmt {
 public:
  virtual int Execute(std::vector<arg_value>)=0;
};

class UDF_SQL_Expr final: UDF_Stmt {
 public:
  UDF_SQL_Expr(std::string query, int dtype = 0):query_(query), dtype_(dtype)
  {}
  int Execute(std::vector<arg_value>);
 private:
  std::string query_;
  int dtype_;
};

}
}

#endif // UDF_HELP
