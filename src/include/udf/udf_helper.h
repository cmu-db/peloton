#include <string>

#ifndef UDF_HELP
#define UDF_HELP

namespace peloton {
namespace udf {

class UDF_Stmt {
  void Execute();
};

class UDF_SQL_Expr final: UDF_Stmt {
 public:
  UDF_SQL_Expr(std::string query, int dtype = 0):query_(query), dtype_(dtype) {
  }
  void Execute();
 private:
  std::string query_;
  int dtype_;
};

}
}

#endif // UDF_HELP
