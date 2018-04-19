//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// foreign_key.cpp
//
// Identification: src/catalog/foreign_key.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "catalog/foreign_key.h"
#include "common/internal_types.h"
#include "type/serializeio.h"

namespace peloton {
namespace catalog {

// Serialize this foreign key constraint
void ForeignKey::SerializeTo(SerializeOutput &out) {
  // foreign key constraint basic information
  out.WriteTextString(fk_name);
  out.WriteInt((int)update_action);
  out.WriteInt((int)delete_action);

  // source table information
  out.WriteInt(source_table_id);
  out.WriteLong(source_col_ids.size());
  for (oid_t source_col_id : source_col_ids) {
    out.WriteInt(source_col_id);
  }

  // sink table information
  out.WriteInt(sink_table_id);
  out.WriteLong(sink_col_ids.size());
  for (oid_t source_col_id : sink_col_ids) {
    out.WriteInt(source_col_id);
  }
}

// Deserialize this foreign key constraint
ForeignKey *ForeignKey::DeserializeFrom(SerializeInput &in) {
  // foreign key constraint basic information
  std::string fk_name = in.ReadTextString();
  FKConstrActionType update_action = (FKConstrActionType)in.ReadInt();
  FKConstrActionType delete_action = (FKConstrActionType)in.ReadInt();

  // source table information
  oid_t source_table_id = in.ReadInt();
  size_t source_column_count = in.ReadLong();
  std::vector<oid_t> source_col_ids;
  for (oid_t src_col_idx = 0; src_col_idx < source_column_count;
       src_col_idx++) {
    source_col_ids.push_back(in.ReadInt());
  }

  // sink table information
  oid_t sink_table_id = in.ReadInt();
  size_t sink_column_count = in.ReadLong();
  std::vector<oid_t> sink_col_ids;
  for (oid_t snk_col_idx = 0; snk_col_idx < sink_column_count; snk_col_idx++) {
    sink_col_ids.push_back(in.ReadInt());
  }

  return new ForeignKey(source_table_id, sink_table_id, sink_col_ids,
                        source_col_ids, update_action, delete_action, fk_name);
}

}  // namespace catalog
}  // namespace peloton
