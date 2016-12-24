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
#include "index/btree_index.h"
#include "index/bwtree_index.h"
#include "index/index_factory.h"
#include "index/index_key.h"
#include "type/types.h"

namespace peloton {
namespace index {

Index *IndexFactory::GetInstance(IndexMetadata *metadata) {
  LOG_INFO("Creating index %s", metadata->GetName().c_str());

  // The size of the key in bytes
  const auto key_size = metadata->key_schema->GetLength();
  LOG_TRACE("key_size : %d", key_size);
  LOG_TRACE("Indexed column count: %ld",
            metadata->key_schema->GetIndexedColumns().size());

  // Check whether the index key only has ints.
  // If so, then we can rock a specialized IntsKeyComparator
  // that is faster than the GenericComparator
  bool ints_only = true;
  for (auto column : metadata->key_schema->GetColumns()) {
    // TODO: We need to see whether we can also support
    // TINYINT and SMALLINT keys
    auto col_type = column.GetType();
    if (col_type != type::Type::INTEGER) {
      ints_only = false;
      break;
    }
  }  // FOR

  auto index_type = metadata->GetIndexMethodType();
  Index *index = nullptr;
  LOG_TRACE("Index type : %d", index_type);

#ifdef LOG_DEBUG_ENABLED
  std::string comparatorType;
#endif

  // -----------------------
  // B+TREE
  // -----------------------
  if (index_type == INDEX_TYPE_BTREE) {
    if (key_size <= 4) {
      if (ints_only) {
#ifdef LOG_DEBUG_ENABLED
        comparatorType = "IntsKey<1>";
#endif
        index = new BTreeIndex<IntsKey<1>, ItemPointer *, IntsComparator<1>,
                               IntsEqualityChecker<1>>(metadata);
      } else {
#ifdef LOG_DEBUG_ENABLED
        comparatorType = "GenericKey<4>";
#endif
        index =
            new BTreeIndex<GenericKey<4>, ItemPointer *, GenericComparator<4>,
                           GenericEqualityChecker<4>>(metadata);
      }
    } else if (key_size <= 8) {
      if (ints_only) {
#ifdef LOG_DEBUG_ENABLED
        comparatorType = "IntsKey<2>";
#endif
        index = new BTreeIndex<IntsKey<2>, ItemPointer *, IntsComparator<2>,
                               IntsEqualityChecker<2>>(metadata);
      } else {
#ifdef LOG_DEBUG_ENABLED
        comparatorType = "GenericKey<8>";
#endif
        index =
            new BTreeIndex<GenericKey<8>, ItemPointer *, GenericComparator<8>,
                           GenericEqualityChecker<8>>(metadata);
      }
    } else if (key_size <= 12) {
      if (ints_only) {
#ifdef LOG_DEBUG_ENABLED
        comparatorType = "IntsKey<3>";
#endif
        index = new BTreeIndex<IntsKey<3>, ItemPointer *, IntsComparator<3>,
                               IntsEqualityChecker<3>>(metadata);
      } else {
#ifdef LOG_DEBUG_ENABLED
        comparatorType = "GenericKey<12>";
#endif
        index =
            new BTreeIndex<GenericKey<12>, ItemPointer *, GenericComparator<12>,
                           GenericEqualityChecker<12>>(metadata);
      }
    } else if (key_size <= 16) {
      if (ints_only) {
#ifdef LOG_DEBUG_ENABLED
        comparatorType = "IntsKey<4>";
#endif
        index = new BTreeIndex<IntsKey<4>, ItemPointer *, IntsComparator<4>,
                               IntsEqualityChecker<4>>(metadata);
      } else {
#ifdef LOG_DEBUG_ENABLED
        comparatorType = "GenericKey<16>";
#endif
        index =
            new BTreeIndex<GenericKey<16>, ItemPointer *, GenericComparator<16>,
                           GenericEqualityChecker<16>>(metadata);
      }
    } else if (key_size <= 24) {
#ifdef LOG_DEBUG_ENABLED
      comparatorType = "GenericKey<24>";
#endif
      index =
          new BTreeIndex<GenericKey<24>, ItemPointer *, GenericComparator<24>,
                         GenericEqualityChecker<24>>(metadata);
    } else if (key_size <= 32) {
#ifdef LOG_DEBUG_ENABLED
      comparatorType = "GenericKey<32>";
#endif
      index =
          new BTreeIndex<GenericKey<32>, ItemPointer *, GenericComparator<32>,
                         GenericEqualityChecker<32>>(metadata);
    } else if (key_size <= 64) {
#ifdef LOG_DEBUG_ENABLED
      comparatorType = "GenericKey<64>";
#endif
      index =
          new BTreeIndex<GenericKey<64>, ItemPointer *, GenericComparator<64>,
                         GenericEqualityChecker<64>>(metadata);
    } else if (key_size <= 256) {
#ifdef LOG_DEBUG_ENABLED
      comparatorType = "GenericKey<256>";
#endif
      index =
          new BTreeIndex<GenericKey<256>, ItemPointer *, GenericComparator<256>,
                         GenericEqualityChecker<256>>(metadata);
    } else {
#ifdef LOG_DEBUG_ENABLED
      comparatorType = "TupleKey";
#endif
      index = new BTreeIndex<TupleKey, ItemPointer *, TupleKeyComparator,
                             TupleKeyEqualityChecker>(metadata);
    }

    // -----------------------
    // BW-TREE
    // -----------------------
  } else if (index_type == INDEX_TYPE_BWTREE) {
    if (key_size <= 4) {
      if (ints_only) {
#ifdef LOG_DEBUG_ENABLED
        comparatorType = "IntsKey<1>";
#endif
        index = new BWTreeIndex<IntsKey<1>, ItemPointer *, IntsComparator<1>,
                                IntsEqualityChecker<1>, IntsHasher<1>,
                                ItemPointerComparator, ItemPointerHashFunc>(
            metadata);
      } else {
#ifdef LOG_DEBUG_ENABLED
        comparatorType = "GenericKey<4>";
#endif
        index =
            new BWTreeIndex<GenericKey<4>, ItemPointer *, GenericComparator<4>,
                            GenericEqualityChecker<4>, GenericHasher<4>,
                            ItemPointerComparator, ItemPointerHashFunc>(
                metadata);
      }
    } else if (key_size <= 8) {
      if (ints_only) {
#ifdef LOG_DEBUG_ENABLED
        comparatorType = "IntsKey<2>";
#endif
        index = new BWTreeIndex<IntsKey<2>, ItemPointer *, IntsComparator<2>,
                                IntsEqualityChecker<2>, IntsHasher<2>,
                                ItemPointerComparator, ItemPointerHashFunc>(
            metadata);
      } else {
#ifdef LOG_DEBUG_ENABLED
        comparatorType = "GenericKey<8>";
#endif
        index =
            new BWTreeIndex<GenericKey<8>, ItemPointer *, GenericComparator<8>,
                            GenericEqualityChecker<8>, GenericHasher<8>,
                            ItemPointerComparator, ItemPointerHashFunc>(
                metadata);
      }
    } else if (key_size <= 12) {
      if (ints_only) {
#ifdef LOG_DEBUG_ENABLED
        comparatorType = "IntsKey<3>";
#endif
        index = new BWTreeIndex<IntsKey<3>, ItemPointer *, IntsComparator<3>,
                                IntsEqualityChecker<3>, IntsHasher<3>,
                                ItemPointerComparator, ItemPointerHashFunc>(
            metadata);
      } else {
#ifdef LOG_DEBUG_ENABLED
        comparatorType = "GenericKey<12>";
#endif
        index =
            new BWTreeIndex<GenericKey<12>, ItemPointer *,
                            GenericComparator<12>, GenericEqualityChecker<12>,
                            GenericHasher<12>, ItemPointerComparator,
                            ItemPointerHashFunc>(metadata);
      }
    } else if (key_size <= 16) {
      if (ints_only) {
#ifdef LOG_DEBUG_ENABLED
        comparatorType = "IntsKey<4>";
#endif
        index = new BWTreeIndex<IntsKey<4>, ItemPointer *, IntsComparator<4>,
                                IntsEqualityChecker<4>, IntsHasher<4>,
                                ItemPointerComparator, ItemPointerHashFunc>(
            metadata);
      } else {
#ifdef LOG_DEBUG_ENABLED
        comparatorType = "GenericKey<16>";
#endif
        index =
            new BWTreeIndex<GenericKey<16>, ItemPointer *,
                            GenericComparator<16>, GenericEqualityChecker<16>,
                            GenericHasher<16>, ItemPointerComparator,
                            ItemPointerHashFunc>(metadata);
      }
    } else if (key_size <= 64) {
#ifdef LOG_DEBUG_ENABLED
      comparatorType = "GenericKey<64>";
#endif
      index =
          new BWTreeIndex<GenericKey<64>, ItemPointer *, GenericComparator<64>,
                          GenericEqualityChecker<64>, GenericHasher<64>,
                          ItemPointerComparator, ItemPointerHashFunc>(metadata);
    } else if (key_size <= 256) {
#ifdef LOG_DEBUG_ENABLED
      comparatorType = "GenericKey<256>";
#endif
      index =
          new BWTreeIndex<GenericKey<256>, ItemPointer *,
                          GenericComparator<256>, GenericEqualityChecker<256>,
                          GenericHasher<256>, ItemPointerComparator,
                          ItemPointerHashFunc>(metadata);
    } else {
#ifdef LOG_DEBUG_ENABLED
      comparatorType = "TupleKey";
#endif
      index =
          new BWTreeIndex<TupleKey, ItemPointer *, TupleKeyComparator,
                          TupleKeyEqualityChecker, TupleKeyHasher,
                          ItemPointerComparator, ItemPointerHashFunc>(metadata);
    }
  } else {
    throw IndexException("Unsupported index scheme.");
  }
#ifdef LOG_DEBUG_ENABLED
  std::ostringstream os;
  os << "Index '" << metadata->GetName() << "' => "
     << IndexTypeToString(index_type) << "::" << comparatorType << "(";
  bool first = true;
  for (auto column : metadata->key_schema->GetColumns()) {
    if (first == false) os << ", ";
    os << column.GetName();
    first = false;
  }
  os << ")";
  LOG_DEBUG("%s", os.str().c_str());
#endif

  return (index);
}

}  // End index namespace
}  // End peloton namespace
