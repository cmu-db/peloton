#include <include/udf/udf_helper.h>

namespace peloton {
namespace udf {

arg_value UDF_SQL_Expr::Execute(std::vector<arg_value> inputs) {
  // TODO: not sure we need this or not
  // auto catalog = catalog::Catalog::GetInstance();
  // auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  // auto txn = txn_manager.BeginTransaction();
  // catalog->CreateDatabase(DEFAULT_DB_NAME, txn);
  // txn_manager.CommitTransaction(txn);

  std::vector<StatementResult> result;
  std::vector<FieldInfo> tuple_descriptor;
  std::string error_message;
  int rows_changed;

  auto query_with_value = query_;

  // TODO: This is an adhoc fix here. Should pass argnames along with the name values
  query_with_value.replace(query_.find("i", 0), 1,
      std::to_string(inputs[0].GetAs<int32_t>()));
  query_with_value.insert(0, "SELECT ");

  // TODO: How to handle with the return status
  traffic_cop_.ExecuteStatement(query_with_value, result, tuple_descriptor,
                                rows_changed, error_message);

  // TODO: not sure we need this or not
  // txn = txn_manager.BeginTransaction();
  // catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  // txn_manager.CommitTransaction(txn);

  std::string value(result[0].second.begin(), result[0].second.end());
  return ::peloton::type::ValueFactory::GetIntegerValue(atoi(value.c_str()));
}

tcop::TrafficCop UDF_SQL_Expr::traffic_cop_;

}
}
