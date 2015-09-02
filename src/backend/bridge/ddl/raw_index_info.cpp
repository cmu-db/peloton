#include "backend/bridge/ddl/raw_index_info.h"

namespace peloton {
namespace bridge {

bool raw_index_info::CreateIndex(void) const {

  IndexInfo index_info(index_name, index_oid,
                       table_name, method_type,
                       constraint_type, unique_keys,
                       key_column_names);

  bool status = DDLIndex::CreateIndex(index_info);

  if (status == false) {
    elog(ERROR, "Could not create index \"%s\" in Peloton", index_name.c_str());
  }

  return status;
}

}  // namespace bridge
}  // namespace peloton
