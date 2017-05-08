#include <include/udf/udf_helper.h>

namespace peloton {
namespace udf {

arg_value UDF_SQL_Expr::Execute(std::vector<arg_value>) {
  // TODO: not sure we need this or not
  auto catalog = catalog::Catalog::GetInstance();
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  std::vector<StatementResult> result;
  std::vector<FieldInfo> tuple_descriptor;
  std::string error_message;
  int rows_changed;

  // TODO: How to handle with the return value
  traffic_cop_.ExecuteStatement(query_, result, tuple_descriptor,
                                rows_changed, error_message);

  // TODO: not sure we need this or not
  txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);

  std::string value(result[0].second.begin(), result[0].second.end());
  return ::peloton::type::ValueFactory::GetIntegerValue(atoi(value.c_str()));
}

tcop::TrafficCop UDF_SQL_Expr::traffic_cop_;

}
}
