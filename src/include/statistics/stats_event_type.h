//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// stats_event_type.h
//
// Identification: src/include/statistics/stats_event_type.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once
namespace peloton {
namespace stats {
enum class StatsEventType {
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
  TABLE_MEMORY_ALLOC,
  TABLE_MEMORY_FREE,
  INDEX_MEMORY_ALLOC,
  INDEX_MEMORY_FREE,
  INDEX_MEMORY_USAGE,
  INDEX_MEMORY_RECLAIM,
  QUERY_BEGIN,
  QUERY_END,
  TEST  // Testing event
};
};
}
