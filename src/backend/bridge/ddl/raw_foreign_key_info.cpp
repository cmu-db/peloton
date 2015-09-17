#include "backend/bridge/ddl/raw_foreign_key_info.h"

#include "backend/catalog/foreign_key.h"
#include "backend/storage/data_table.h"
#include "backend/catalog/manager.h"
#include "backend/bridge/ddl/bridge.h"
#include "backend/catalog/column.h"

namespace peloton {
namespace bridge {

void raw_foreign_key_info::CreateForeignkey(void) const{

  // Get source table, sink table, and database oid
  oid_t source_table_oid = source_table_id;
  assert(source_table_oid);
  oid_t sink_table_oid = sink_table_id;
  assert(sink_table_oid);
  oid_t database_oid = Bridge::GetCurrentDatabaseOid();

  // get source, sink tables
  auto &manager = catalog::Manager::GetInstance();
  auto source_table = manager.GetTableWithOid(database_oid, source_table_oid);
  assert(source_table);
  auto sink_table = manager.GetTableWithOid(database_oid, sink_table_oid);
  assert(sink_table);

  // extract column_names
  std::vector<std::string> sink_column_names;
  std::vector<std::string> source_column_names;

  auto source_table_schema = source_table->GetSchema();
  auto sink_table_schema = sink_table->GetSchema();

  // Populate primary key column names
  for (auto sink_offset : sink_column_offsets){
    catalog::Column column = sink_table_schema->GetColumn(sink_offset - 1);
    sink_column_names.push_back(column.GetName());
  }

  // Populate source key column names
  for (auto source_offset : source_column_offsets){
    catalog::Column column = source_table_schema->GetColumn(source_offset - 1);
    source_column_names.push_back(column.GetName());
  }

  catalog::ForeignKey *foreign_key 
    = new catalog::ForeignKey(sink_table_oid,
        sink_column_names, 
        source_column_names, 
        update_action,
        delete_action,
        fk_name);

  source_table->AddForeignKey(foreign_key);
}

}  // namespace bridge
}  // namespace peloton
