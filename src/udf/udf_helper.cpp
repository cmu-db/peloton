#include <include/udf/udf_helper.h>

namespace peloton {
namespace udf {

arg_value UDF_SQL_Expr::Execute(std::vector<arg_value> inputs, std::vector<std::string> names) {
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
  for (size_t i=0; i < names.size(); ++i) {
    size_t pos ;
    while ((pos= query_with_value.find(names[i], 0)) != std::string::npos)
      query_with_value.replace(pos, names[i].size(),
        std::to_string(inputs[i].GetAs<int32_t>()));
  }
  query_with_value.insert(0, "SELECT ");

  // TODO: How to handle with the return status
  traffic_cop_.ExecuteStatement(query_with_value, result, tuple_descriptor,
                                rows_changed, error_message);

  // TODO: not sure we need this or not
  // txn = txn_manager.BeginTransaction();
  // catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  // txn_manager.CommitTransaction(txn);

  std::string value(result[0].second.begin(), result[0].second.end());
  if (value.compare("true") == 0)
    return ::peloton::type::ValueFactory::GetIntegerValue(1);
  else if (value.compare("false") == 0)
    return ::peloton::type::ValueFactory::GetIntegerValue(0);

  return ::peloton::type::ValueFactory::GetIntegerValue(atoi(value.c_str()));
}

arg_value UDF_IFELSE_Stmt::Execute(std::vector<arg_value> inputs, std::vector<std::string> names) {
  auto cond_res = cond_exp_.Execute(inputs, names);
  if (cond_res.GetAs<int32_t>() == 1)
    return true_exp_.Execute(inputs, names);
  else
    return false_exp_.Execute(inputs, names);
}

tcop::TrafficCop UDF_SQL_Expr::traffic_cop_;

}
}
