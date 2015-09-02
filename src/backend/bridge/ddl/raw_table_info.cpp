#include "backend/bridge/ddl/raw_table_info.h"
#include "backend/bridge/ddl/ddl_table.h"

namespace peloton {
namespace bridge {

bool raw_table_info::CreateTable(void) const {

  std::vector<catalog::Column> columns;

  for(auto raw_column : raw_columns){
    auto column = raw_column.CreateColumn();
    columns.push_back(column);
  }
  bool status = DDLTable::CreateTable(table_oid,
                                      table_name, 
                                      columns);
  if (status == false) {
    elog(ERROR, "Could not create table \"%s\" in Peloton", table_name.c_str());
  }
  return status;
}

}  // namespace bridge
}  // namespace peloton
