/*-------------------------------------------------------------------------
 *
 * index_factory.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/index/index_factory.cpp
 *
 *-------------------------------------------------------------------------
 */

#include <cassert>
#include <iostream>

#include "index/index_factory.h"

#include "common/types.h"
#include "common/exception.h"
#include "catalog/index.h"

#include "index/index.h"
#include "index/index_key.h"
#include "index/array_unique_index.h"
#include "index/binary_tree_unique_index.h"
#include "index/binary_tree_multimap_index.h"
#include "index/hash_table_unique_index.h"
#include "index/hash_table_multimap_index.h"

namespace nstore {
namespace index {

TableIndex *TableIndexFactory::getInstance(const TableIndexScheme &scheme) {
  int colCount = (int)scheme.columnIndices.size();
  bool unique = scheme.unique;
  bool ints_only = scheme.intsOnly;
  IndexType type = scheme.type;
  std::vector<int32_t> columnIndices = scheme.columnIndices;
  catalog::Schema *tupleSchema = scheme.tupleSchema;

  std::vector<ValueType> keyColumnTypes;
  std::vector<id_t> keyColumnLengths;
  std::vector<std::string> keyColumnNames(colCount, "INDEX_ATTR");
  std::vector<bool> keyColumnAllowNull(colCount, true);
  std::vector<bool> keyColumnIsInlined(colCount, true);
  for (int i = 0; i < colCount; ++i) {
    keyColumnTypes.push_back(tupleSchema->GetType(columnIndices[i]));
    keyColumnLengths.push_back(tupleSchema->GetLength(columnIndices[i]));
  }

  catalog::Schema *keySchema = catalog::Schema::CreateTupleSchema(keyColumnTypes,
                                                                  keyColumnLengths,
                                                                  keyColumnNames,
                                                                  keyColumnAllowNull,
                                                                  keyColumnIsInlined);
  TableIndexScheme schemeCopy(scheme);

  schemeCopy.keySchema = keySchema;
  LOG_TRACE("Creating index for %s.\n%s", scheme.name.c_str(), keySchema->debug().c_str());
  const int keySize = keySchema->GetLength();

  // no int specialization beyond this point
  if (keySize > sizeof(int64_t) * 4) {
    ints_only = false;
  }

  // a bit of a hack, this should be improved later
  if ((ints_only) && (unique) && (type == ARRAY_INDEX)) {
    return new ArrayUniqueIndex(schemeCopy);
  }
  if ((ints_only) && (type == BALANCED_TREE_INDEX) && (unique)) {
    if (keySize <= sizeof(uint64_t)) {
      return new BinaryTreeUniqueIndex<IntsKey<1>, IntsComparator<1>, IntsEqualityChecker<1> >(schemeCopy);
    } else if (keySize <= sizeof(int64_t) * 2) {
      return new BinaryTreeUniqueIndex<IntsKey<2>, IntsComparator<2>, IntsEqualityChecker<2> >(schemeCopy);
    } else if (keySize <= sizeof(int64_t) * 3) {
      return new BinaryTreeUniqueIndex<IntsKey<3>, IntsComparator<3>, IntsEqualityChecker<3> >(schemeCopy);
    } else if (keySize <= sizeof(int64_t) * 4) {
      return new BinaryTreeUniqueIndex<IntsKey<4>, IntsComparator<4>, IntsEqualityChecker<4> >(schemeCopy);
    } else {
      throw NotImplementedException("We currently only support tree index on unique integer keys of size 32 bytes or smaller...");
    }
  }

  if ((ints_only) && (type == BALANCED_TREE_INDEX) && (!unique)) {
    if (keySize <= sizeof(uint64_t)) {
      return new BinaryTreeMultiMapIndex<IntsKey<1>, IntsComparator<1>, IntsEqualityChecker<1> >(schemeCopy);
    } else if (keySize <= sizeof(int64_t) * 2) {
      return new BinaryTreeMultiMapIndex<IntsKey<2>, IntsComparator<2>, IntsEqualityChecker<2> >(schemeCopy);
    } else if (keySize <= sizeof(int64_t) * 3) {
      return new BinaryTreeMultiMapIndex<IntsKey<3>, IntsComparator<3>, IntsEqualityChecker<3> >(schemeCopy);
    } else if (keySize <= sizeof(int64_t) * 4) {
      return new BinaryTreeMultiMapIndex<IntsKey<4>, IntsComparator<4>, IntsEqualityChecker<4> >(schemeCopy);
    } else {
      throw NotImplementedException( "We currently only support tree index on non-unique integer keys of size 32 bytes or smaller..." );
    }
  }

  if ((ints_only) && (type == HASH_TABLE_INDEX) && (unique)) {
    if (keySize <= sizeof(uint64_t)) {
      return new HashTableUniqueIndex<IntsKey<1>, IntsHasher<1>, IntsEqualityChecker<1> >(schemeCopy);
    } else if (keySize <= sizeof(int64_t) * 2) {
      return new HashTableUniqueIndex<IntsKey<2>, IntsHasher<2>, IntsEqualityChecker<2> >(schemeCopy);
    } else if (keySize <= sizeof(int64_t) * 3) {
      return new HashTableUniqueIndex<IntsKey<3>, IntsHasher<3>, IntsEqualityChecker<3> >(schemeCopy);
    } else if (keySize <= sizeof(int64_t) * 4) {
      return new HashTableUniqueIndex<IntsKey<4>, IntsHasher<4>, IntsEqualityChecker<4> >(schemeCopy);
    } else {
      throw NotImplementedException( "We currently only support hash index on unique integer keys of size 32 bytes or smaller..." );
    }
  }

  if ((ints_only) && (type == HASH_TABLE_INDEX) && (!unique)) {
    if (keySize <= sizeof(uint64_t)) {
      return new HashTableMultiMapIndex<IntsKey<1>, IntsHasher<1>, IntsEqualityChecker<1> >(schemeCopy);
    } else if (keySize <= sizeof(int64_t) * 2) {
      return new HashTableMultiMapIndex<IntsKey<2>, IntsHasher<2>, IntsEqualityChecker<2> >(schemeCopy);
    } else if (keySize <= sizeof(int64_t) * 3) {
      return new HashTableMultiMapIndex<IntsKey<3>, IntsHasher<3>, IntsEqualityChecker<3> >(schemeCopy);
    } else if (keySize <= sizeof(int64_t) * 4) {
      return new HashTableMultiMapIndex<IntsKey<4>, IntsHasher<4>, IntsEqualityChecker<4> >(schemeCopy);
    } else {
      throw NotImplementedException( "We currently only support hash index on non-unique integer keys of size 32 bytes of smaller..." );
    }
  }

  if (/*(type == BALANCED_TREE_INDEX) &&*/ (unique)) {
    if (type == HASH_TABLE_INDEX) {
      LOG_INFO("Producing a tree index for %s: "
          "hash index not currently supported for this index key.\n",
          scheme.name.c_str());
    }

    if (keySize <= 4) {
      return new BinaryTreeUniqueIndex<GenericKey<4>, GenericComparator<4>, GenericEqualityChecker<4> >(schemeCopy);
    } else if (keySize <= 8) {
      return new BinaryTreeUniqueIndex<GenericKey<8>, GenericComparator<8>, GenericEqualityChecker<8> >(schemeCopy);
    } else if (keySize <= 12) {
      return new BinaryTreeUniqueIndex<GenericKey<12>, GenericComparator<12>, GenericEqualityChecker<12> >(schemeCopy);
    } else if (keySize <= 16) {
      return new BinaryTreeUniqueIndex<GenericKey<16>, GenericComparator<16>, GenericEqualityChecker<16> >(schemeCopy);
    } else if (keySize <= 24) {
      return new BinaryTreeUniqueIndex<GenericKey<24>, GenericComparator<24>, GenericEqualityChecker<24> >(schemeCopy);
    } else if (keySize <= 32) {
      return new BinaryTreeUniqueIndex<GenericKey<32>, GenericComparator<32>, GenericEqualityChecker<32> >(schemeCopy);
    } else if (keySize <= 48) {
      return new BinaryTreeUniqueIndex<GenericKey<48>, GenericComparator<48>, GenericEqualityChecker<48> >(schemeCopy);
    } else if (keySize <= 64) {
      return new BinaryTreeUniqueIndex<GenericKey<64>, GenericComparator<64>, GenericEqualityChecker<64> >(schemeCopy);
    } else if (keySize <= 96) {
      return new BinaryTreeUniqueIndex<GenericKey<96>, GenericComparator<96>, GenericEqualityChecker<96> >(schemeCopy);
    } else if (keySize <= 128) {
      return new BinaryTreeUniqueIndex<GenericKey<128>, GenericComparator<128>, GenericEqualityChecker<128> >(schemeCopy);
    } else if (keySize <= 256) {
      return new BinaryTreeUniqueIndex<GenericKey<256>, GenericComparator<256>, GenericEqualityChecker<256> >(schemeCopy);
    } else if (keySize <= 512) {
      return new BinaryTreeUniqueIndex<GenericKey<512>, GenericComparator<512>, GenericEqualityChecker<512> >(schemeCopy);
    } else {
      throw NotImplementedException( "We currently only support keys of up to 512 bytes when anti-caching is enabled..." );

      //return new BinaryTreeUniqueIndex<TupleKey, TupleKeyComparator, TupleKeyEqualityChecker>(schemeCopy);
    }
  }

  if (/*(type == BALANCED_TREE_INDEX) &&*/ (!unique)) {
    if (type == HASH_TABLE_INDEX) {
      LOG_INFO("Producing a tree index for %s: "
          "hash index not currently supported for this index key.\n",
          scheme.name.c_str());
    }

    if (keySize <= 4) {
      return new BinaryTreeMultiMapIndex<GenericKey<4>, GenericComparator<4>, GenericEqualityChecker<4> >(schemeCopy);
    } else if (keySize <= 8) {
      return new BinaryTreeMultiMapIndex<GenericKey<8>, GenericComparator<8>, GenericEqualityChecker<8> >(schemeCopy);
    } else if (keySize <= 12) {
      return new BinaryTreeMultiMapIndex<GenericKey<12>, GenericComparator<12>, GenericEqualityChecker<12> >(schemeCopy);
    } else if (keySize <= 16) {
      return new BinaryTreeMultiMapIndex<GenericKey<16>, GenericComparator<16>, GenericEqualityChecker<16> >(schemeCopy);
    } else if (keySize <= 24) {
      return new BinaryTreeMultiMapIndex<GenericKey<24>, GenericComparator<24>, GenericEqualityChecker<24> >(schemeCopy);
    } else if (keySize <= 32) {
      return new BinaryTreeMultiMapIndex<GenericKey<32>, GenericComparator<32>, GenericEqualityChecker<32> >(schemeCopy);
    } else if (keySize <= 48) {
      return new BinaryTreeMultiMapIndex<GenericKey<48>, GenericComparator<48>, GenericEqualityChecker<48> >(schemeCopy);
    } else if (keySize <= 64) {
      return new BinaryTreeMultiMapIndex<GenericKey<64>, GenericComparator<64>, GenericEqualityChecker<64> >(schemeCopy);
    } else if (keySize <= 96) {
      return new BinaryTreeMultiMapIndex<GenericKey<96>, GenericComparator<96>, GenericEqualityChecker<96> >(schemeCopy);
    } else if (keySize <= 128) {
      return new BinaryTreeMultiMapIndex<GenericKey<128>, GenericComparator<128>, GenericEqualityChecker<128> >(schemeCopy);
    } else if (keySize <= 256) {
      return new BinaryTreeMultiMapIndex<GenericKey<256>, GenericComparator<256>, GenericEqualityChecker<256> >(schemeCopy);
    } else if (keySize <= 512) {
      return new BinaryTreeMultiMapIndex<GenericKey<512>, GenericComparator<512>, GenericEqualityChecker<512> >(schemeCopy);
    } else {
      throw NotImplementedException( "We currently only support keys of up to 512 bytes when anti-caching is enabled..." );

      //return new BinaryTreeMultiMapIndex<TupleKey, TupleKeyComparator, TupleKeyEqualityChecker>(schemeCopy);
    }
  }


  throw NotImplementedException("Unsupported index scheme..." );
  return NULL;
}

} // End index namespace
} // End nstore namespace

