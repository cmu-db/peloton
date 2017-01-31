//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// index_factory.cpp
//
// Identification: src/index/index_factory.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>

#include "common/logger.h"
#include "common/macros.h"
#include "index/bwtree_index.h"
#include "index/index_factory.h"
#include "index/index_key.h"
#include "index/skiplist_index.h"
#include "type/types.h"

namespace peloton {
namespace index {

Index *IndexFactory::GetIndex(IndexMetadata *metadata) {
  LOG_TRACE("Creating index %s", metadata->GetName().c_str());

  // The size of the key in bytes
  const auto key_size = metadata->key_schema->GetLength();
  LOG_TRACE("key_size : %d", key_size);
  LOG_TRACE("Indexed column count: %ld",
            metadata->key_schema->GetIndexedColumns().size());

  // Check whether the index key only has ints.
  // If so, then we can rock a specialized IntsKeyComparator
  // that is faster than the FastGenericComparator
  bool ints_only = true;
  for (auto column : metadata->key_schema->GetColumns()) {
    auto col_type = column.GetType();
    if (col_type != type::Type::TINYINT && col_type != type::Type::SMALLINT &&
        col_type != type::Type::INTEGER && col_type != type::Type::BIGINT) {
      ints_only = false;
      break;
    }
  }  // FOR

  // If we want to use a specialized IntsKey template, make sure that
  // we are not longer than the largest IntsKey that we have
  if (ints_only && key_size > sizeof(uint64_t) * INTSKEY_MAX_SLOTS) {
    ints_only = false;
  }

  auto index_type = metadata->GetIndexType();
  Index *index = nullptr;
  LOG_TRACE("Index type : %s", IndexTypeToString(index_type).c_str());

  // -----------------------
  // BW-TREE
  // -----------------------
  if (index_type == IndexType::BWTREE) {
    if (ints_only) {
      index = IndexFactory::GetBwTreeIntsKeyIndex(metadata);
    } else {
      index = IndexFactory::GetBwTreeGenericKeyIndex(metadata);
    }

  // -----------------------
  // SKIP-LIST
  // -----------------------
  } else if (index_type == IndexType::SKIPLIST) {
    if (ints_only) {
      index = IndexFactory::GetSkipListIntsKeyIndex(metadata);
    } else {
      index = IndexFactory::GetSkipListGenericKeyIndex(metadata);
    }

  // -----------------------
  // ERROR
  // -----------------------
  } else {
    throw IndexException("Unsupported index scheme.");
  }
  PL_ASSERT(index != nullptr);

  return (index);
}

Index *IndexFactory::GetBwTreeIntsKeyIndex(IndexMetadata *metadata) {
  // Our new Index!
  Index *index = nullptr;

  // The size of the key in bytes
  const auto key_size = metadata->key_schema->GetLength();

// Debug Output
#ifdef LOG_TRACE_ENABLED
  std::string comparatorType;
#endif

  if (key_size <= sizeof(uint64_t)) {
#ifdef LOG_TRACE_ENABLED
    comparatorType = "CompactIntsKey<1>";
#endif
    index =
        new BWTreeIndex<CompactIntsKey<1>, ItemPointer *,
                        CompactIntsComparator<1>, CompactIntsEqualityChecker<1>,
                        CompactIntsHasher<1>, ItemPointerComparator,
                        ItemPointerHashFunc>(metadata);
  } else if (key_size <= sizeof(uint64_t) * 2) {
#ifdef LOG_TRACE_ENABLED
    comparatorType = "CompactIntsKey<2>";
#endif
    index =
        new BWTreeIndex<CompactIntsKey<2>, ItemPointer *,
                        CompactIntsComparator<2>, CompactIntsEqualityChecker<2>,
                        CompactIntsHasher<2>, ItemPointerComparator,
                        ItemPointerHashFunc>(metadata);
  } else if (key_size <= sizeof(uint64_t) * 3) {
#ifdef LOG_TRACE_ENABLED
    comparatorType = "CompactIntsKey<3>";
#endif
    index =
        new BWTreeIndex<CompactIntsKey<3>, ItemPointer *,
                        CompactIntsComparator<3>, CompactIntsEqualityChecker<3>,
                        CompactIntsHasher<3>, ItemPointerComparator,
                        ItemPointerHashFunc>(metadata);
  } else if (key_size <= sizeof(uint64_t) * 4) {
#ifdef LOG_TRACE_ENABLED
    comparatorType = "CompactIntsKey<4>";
#endif
    index =
        new BWTreeIndex<CompactIntsKey<4>, ItemPointer *,
                        CompactIntsComparator<4>, CompactIntsEqualityChecker<4>,
                        CompactIntsHasher<4>, ItemPointerComparator,
                        ItemPointerHashFunc>(metadata);
  } else {
    throw IndexException("Unsupported IntsKey scheme");
  }

#ifdef LOG_TRACE_ENABLED
  LOG_TRACE("%s", IndexFactory::GetInfo(metadata, comparatorType).c_str());
#endif
  return (index);
}

Index *IndexFactory::GetBwTreeGenericKeyIndex(IndexMetadata *metadata) {
  // Our new Index!
  Index *index = nullptr;

  // The size of the key in bytes
  const auto key_size = metadata->key_schema->GetLength();

// Debug Output
#ifdef LOG_TRACE_ENABLED
  std::string comparatorType;
#endif

  if (key_size <= 4) {
#ifdef LOG_TRACE_ENABLED
    comparatorType = "GenericKey<4>";
#endif
    index =
        new BWTreeIndex<GenericKey<4>, ItemPointer *, FastGenericComparator<4>,
                        GenericEqualityChecker<4>, GenericHasher<4>,
                        ItemPointerComparator, ItemPointerHashFunc>(metadata);
  } else if (key_size <= 8) {
#ifdef LOG_TRACE_ENABLED
    comparatorType = "GenericKey<8>";
#endif
    index =
        new BWTreeIndex<GenericKey<8>, ItemPointer *, FastGenericComparator<8>,
                        GenericEqualityChecker<8>, GenericHasher<8>,
                        ItemPointerComparator, ItemPointerHashFunc>(metadata);
  } else if (key_size <= 16) {
#ifdef LOG_TRACE_ENABLED
    comparatorType = "GenericKey<16>";
#endif
    index =
        new BWTreeIndex<GenericKey<16>, ItemPointer *,
                        FastGenericComparator<16>, GenericEqualityChecker<16>,
                        GenericHasher<16>, ItemPointerComparator,
                        ItemPointerHashFunc>(metadata);
  } else if (key_size <= 64) {
#ifdef LOG_TRACE_ENABLED
    comparatorType = "GenericKey<64>";
#endif
    index =
        new BWTreeIndex<GenericKey<64>, ItemPointer *,
                        FastGenericComparator<64>, GenericEqualityChecker<64>,
                        GenericHasher<64>, ItemPointerComparator,
                        ItemPointerHashFunc>(metadata);
  } else if (key_size <= 256) {
#ifdef LOG_TRACE_ENABLED
    comparatorType = "GenericKey<256>";
#endif
    index =
        new BWTreeIndex<GenericKey<256>, ItemPointer *,
                        FastGenericComparator<256>, GenericEqualityChecker<256>,
                        GenericHasher<256>, ItemPointerComparator,
                        ItemPointerHashFunc>(metadata);
  } else {
#ifdef LOG_TRACE_ENABLED
    comparatorType = "TupleKey";
#endif
    index =
        new BWTreeIndex<TupleKey, ItemPointer *, TupleKeyComparator,
                        TupleKeyEqualityChecker, TupleKeyHasher,
                        ItemPointerComparator, ItemPointerHashFunc>(metadata);
  }

#ifdef LOG_TRACE_ENABLED
  LOG_TRACE("%s", IndexFactory::GetInfo(metadata, comparatorType).c_str());
#endif
  return (index);
}

Index *IndexFactory::GetSkipListIntsKeyIndex(IndexMetadata *metadata) {
  // Our new Index!
  Index *index = nullptr;

  // The size of the key in bytes
  const auto key_size = metadata->key_schema->GetLength();

// Debug Output
#ifdef LOG_TRACE_ENABLED
  std::string comparatorType;
#endif

  if (key_size <= sizeof(uint64_t)) {
#ifdef LOG_TRACE_ENABLED
    comparatorType = "CompactIntsKey<1>";
#endif
    index =
        new SkipListIndex<CompactIntsKey<1>, ItemPointer *,
                          CompactIntsComparator<1>,
                          CompactIntsEqualityChecker<1>, ItemPointerComparator>(
            metadata);
  } else if (key_size <= sizeof(uint64_t) * 2) {
#ifdef LOG_TRACE_ENABLED
    comparatorType = "CompactIntsKey<2>";
#endif
    index =
        new SkipListIndex<CompactIntsKey<2>, ItemPointer *,
                          CompactIntsComparator<2>,
                          CompactIntsEqualityChecker<2>, ItemPointerComparator>(
            metadata);
  } else if (key_size <= sizeof(uint64_t) * 3) {
#ifdef LOG_TRACE_ENABLED
    comparatorType = "CompactIntsKey<3>";
#endif
    index =
        new SkipListIndex<CompactIntsKey<3>, ItemPointer *,
                          CompactIntsComparator<3>,
                          CompactIntsEqualityChecker<3>, ItemPointerComparator>(
            metadata);
  } else if (key_size <= sizeof(uint64_t) * 4) {
#ifdef LOG_TRACE_ENABLED
    comparatorType = "CompactIntsKey<4>";
#endif
    index =
        new SkipListIndex<CompactIntsKey<4>, ItemPointer *,
                          CompactIntsComparator<4>,
                          CompactIntsEqualityChecker<4>, ItemPointerComparator>(
            metadata);
  } else {
    throw IndexException("Unsupported IntsKey scheme");
  }

#ifdef LOG_TRACE_ENABLED
  LOG_TRACE("%s", IndexFactory::GetInfo(metadata, comparatorType).c_str());
#endif
  return (index);
}

Index *IndexFactory::GetSkipListGenericKeyIndex(IndexMetadata *metadata) {
  // Our new Index!
  Index *index = nullptr;

  // The size of the key in bytes
  const auto key_size = metadata->key_schema->GetLength();

// Debug Output
#ifdef LOG_TRACE_ENABLED
  std::string comparatorType;
#endif

  if (key_size <= 4) {
#ifdef LOG_TRACE_ENABLED
    comparatorType = "GenericKey<4>";
#endif
    index =
        new SkipListIndex<GenericKey<4>, ItemPointer *,
                          FastGenericComparator<4>, GenericEqualityChecker<4>,
                          ItemPointerComparator>(metadata);
  } else if (key_size <= 8) {
#ifdef LOG_TRACE_ENABLED
    comparatorType = "GenericKey<8>";
#endif
    index =
        new SkipListIndex<GenericKey<8>, ItemPointer *,
                          FastGenericComparator<8>, GenericEqualityChecker<8>,
                          ItemPointerComparator>(metadata);
  } else if (key_size <= 16) {
#ifdef LOG_TRACE_ENABLED
    comparatorType = "GenericKey<16>";
#endif
    index =
        new SkipListIndex<GenericKey<16>, ItemPointer *,
                          FastGenericComparator<16>, GenericEqualityChecker<16>,
                          ItemPointerComparator>(metadata);
  } else if (key_size <= 64) {
#ifdef LOG_TRACE_ENABLED
    comparatorType = "GenericKey<64>";
#endif
    index =
        new SkipListIndex<GenericKey<64>, ItemPointer *,
                          FastGenericComparator<64>, GenericEqualityChecker<64>,
                          ItemPointerComparator>(metadata);
  } else if (key_size <= 256) {
#ifdef LOG_TRACE_ENABLED
    comparatorType = "GenericKey<256>";
#endif
    index =
        new SkipListIndex<GenericKey<256>, ItemPointer *,
                          FastGenericComparator<256>,
                          GenericEqualityChecker<256>, ItemPointerComparator>(
            metadata);
  } else {
#ifdef LOG_TRACE_ENABLED
    comparatorType = "TupleKey";
#endif
    index = new SkipListIndex<TupleKey, ItemPointer *, TupleKeyComparator,
                              TupleKeyEqualityChecker, ItemPointerComparator>(
        metadata);
  }

#ifdef LOG_TRACE_ENABLED
  LOG_TRACE("%s", IndexFactory::GetInfo(metadata, comparatorType).c_str());
#endif
  return (index);
}

std::string IndexFactory::GetInfo(IndexMetadata *metadata,
                                  std::string comparatorType) {
  std::ostringstream os;
  os << "Index '" << metadata->GetName() << "' => "
     << IndexTypeToString(metadata->GetIndexType()) << "::" << comparatorType
     << "(";
  bool first = true;
  for (auto column : metadata->key_schema->GetColumns()) {
    if (first == false) os << ", ";
    os << column.GetName();
    first = false;
  }
  os << ")";
  return (os.str());
}

}  // End index namespace
}  // End peloton namespace
