#pragma once
namespace peloton {
namespace stats {
enum class stats_event_type {
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
  MEMORY_ALLOC,
  MEMORY_FREE,
  QUERY_BEGIN,
  QUERY_END
};
};
}
