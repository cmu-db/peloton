#pragma once
namespace peloton {
namespace stats {
enum class StatInsertionPoint {
  TXN_BEGIN,
  TXN_COMMIT,
  TXN_ABORT,
  TUPLE_READ,
  TUPLE_UPDATE,
  TUPLE_INSERT,
  TUPLE_DELETE,
  INDEX_READ,
  INDEX_UPDATE,
  INDEX_INSERT,
  INDEX_DELETE,
  QUERY_BEGIN,
  QUERY_END
};
};
}
