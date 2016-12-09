
//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// temp_table.cpp
//
// Identification: /peloton/src/storage/temp_table.cpp
//
// Copyright (c) 2015, 2016 Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/temp_table.h"

#include "catalog/schema.h"
#include "common/exception.h"
#include "common/logger.h"

namespace peloton {
namespace storage {

TempTable::TempTable(const oid_t &table_oid, catalog::Schema *schema,
                     const bool own_schema)
    : AbstractTable(table_oid, schema, own_schema) {
  // Nothing to see, nothing to do
}

ItemPointer TempTable::InsertTuple(const Tuple *tuple,
                                   concurrency::Transaction *transaction,
                                   ItemPointer **index_entry_ptr = nullptr) {
  // WIP
}
ItemPointer TempTable::InsertTuple(const Tuple *tuple) {
  // WIP
}

}  // End storage namespace
}  // End peloton namespace
