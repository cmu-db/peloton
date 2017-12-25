//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// copy_executor.cpp
//
// Identification: src/index/art_index.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/container_tuple.h"
#include "storage/tuple_iterator.h"
#include "index/art_index.h"

#include "common/logger.h"
#include "index/index_key.h"
#include "index/scan_optimizer.h"
#include "statistics/stats_aggregator.h"
#include "storage/tuple.h"
#include "storage/tile_group.h"
#include "storage/tile.h"
#include "common/logger.h"
#include "catalog/column.h"

namespace peloton {
namespace index {

/*
 * LoadKey() - Given a value tid which is the pointer to tuple, the key of
 *             that tuple will be recovered in the second parameter; the
 *             index metadata knows which columns in the tuple are needed;
 *             each column will be transformed into binary comparable bytes.
 *
 * ART uses lazy expansion technique and adopts a hybrid path compression
 * approach, both optimistic and pessimistic. For example, when doing a
 * lookup for 8 bytes key, a 6 bytes prefix (same as the given key) is found
 * and reach the leaf node, LoadKey will be called to reconstruct the key
 * using the value stored in leaf node in order to ensure that the key found
 * in ART is the same as the given key.
 *
 */
void LoadKey(TID tid, ARTKey &key, IndexMetadata *metadata) {
  // Store the key of the tuple into the key vector
  // Implementation is database specific

  // use the first value in the value list
  MultiValues *value_list = reinterpret_cast<MultiValues *>(tid);

  int columnCount = metadata->GetColumnCount();

  std::vector<oid_t> indexedColumns =
      metadata->GetKeySchema()->GetIndexedColumns();

  std::vector<catalog::Column> columns = metadata->GetKeySchema()->GetColumns();

  auto &manager = catalog::Manager::GetInstance();
  ItemPointer *tuple_pointer = (ItemPointer *)(value_list->tid);
  ItemPointer tuple_location = *tuple_pointer;
  auto tile_group = manager.GetTileGroup(tuple_location.block);
  ContainerTuple<storage::TileGroup> tuple(tile_group.get(),
                                           tuple_location.offset);

  std::vector<type::Value> values;
  int total_bytes = 0;
  for (int i = 0; i < columnCount; i++) {
    values.push_back(tuple.GetValue(indexedColumns[i]));

    // TODO: use values[i].GetLength() to save some space for varchar
    total_bytes += columns[i].GetLength();
  }

  char *c = new char[total_bytes];

  // write values to char array
  int offset = 0;
  for (int i = 0; i < columnCount; i++) {
    ArtIndex::WriteValueInBytes(values[i], c, offset, columns[i].GetLength());
    offset += columns[i].GetLength();
  }

  key.set(c, total_bytes);
  key.setKeyLen(total_bytes);

  delete[] c;
}

ArtIndex::ArtIndex(IndexMetadata *metadata)
    :  // Base class
      Index{metadata},
      art_(LoadKey) {
  art_.SetIndexMetadata(metadata);
  LOG_INFO("Art Index is created for %s\n", (metadata->GetName()).c_str());
  return;
}

// this constructor is used for testing
ArtIndex::ArtIndex(IndexMetadata *metadata,
                   AdaptiveRadixTree::LoadKeyFunction loadKeyForTest)
    : Index{metadata}, art_(loadKeyForTest) {
  LOG_INFO("Art Index is created for testing\n");
}

ArtIndex::~ArtIndex() {}

/*
 * InsertEntry() - insert a key-value pair
 *
 * If the key value pair already exists in the map, just return false;
 * by default, non-unque key is allowed
 *
 */
bool ArtIndex::InsertEntry(const storage::Tuple *key, ItemPointer *value) {
  bool ret = true;

  ARTKey index_key;

  WriteIndexedAttributesInKey(key, index_key);

  auto &t = art_.GetThreadInfo();
  art_.Insert(index_key, reinterpret_cast<TID>(value), t, ret);

  return ret;
}

/*
 * ScanKey() - return all the values for a given key
 *
 */
void ArtIndex::ScanKey(const storage::Tuple *key,
                       std::vector<ItemPointer *> &result) {
  ARTKey index_key;
  WriteIndexedAttributesInKey(key, index_key);

  auto &t = art_.GetThreadInfo();
  art_.Lookup(index_key, t, result);
  return;
}

/*
 * DeleteEntry() - Removes a key-value pair
 *
 * If the key-value pair does not exists yet, return false
 */
bool ArtIndex::DeleteEntry(const storage::Tuple *key, ItemPointer *value) {
  ARTKey index_key;
  WriteIndexedAttributesInKey(key, index_key);

  TID tid = reinterpret_cast<TID>(value);

  auto &t = art_.GetThreadInfo();
  return art_.Remove(index_key, tid, t);
}

/*
 * ConditionalInsert() - Insert a key-value pair only if a given
 *                       predicate fails for all values with a key
 *
 * If return true then the value has been inserted
 * If return false then the value is not inserted. The reason could be
 * predicates returning true for one of the values of a given key
 * or because the value is already in the index
 *
 * NOTE: We first test the predicate, and then test for duplicated values
 * so predicate test result is always available
 */
bool ArtIndex::CondInsertEntry(const storage::Tuple *key, ItemPointer *value,
                               std::function<bool(const void *)> predicate) {
  ARTKey index_key;
  WriteIndexedAttributesInKey(key, index_key);
  TID tid = reinterpret_cast<TID>(value);
  auto &t = art_.GetThreadInfo();
  return art_.ConditionalInsert(index_key, tid, t, predicate);
}

/*
 * Scan() - Scans a range inside the index using index scan optimizer
 *
 */
void ArtIndex::Scan(
    UNUSED_ATTRIBUTE const std::vector<type::Value> &value_list,
    UNUSED_ATTRIBUTE const std::vector<oid_t> &tuple_column_id_list,
    UNUSED_ATTRIBUTE const std::vector<ExpressionType> &expr_list,
    UNUSED_ATTRIBUTE ScanDirectionType scan_direction,
    std::vector<ItemPointer *> &result, const ConjunctionScanPredicate *csp_p) {
  if (csp_p->IsPointQuery() == true) {
    const storage::Tuple *point_query_key_p = csp_p->GetPointQueryKey();
    ScanKey(point_query_key_p, result);
  } else if (csp_p->IsFullIndexScan() == true) {
    if (scan_direction == ScanDirectionType::FORWARD) {
      ScanAllKeys(result);
    } else if (scan_direction == ScanDirectionType::BACKWARD) {
      // TODO
    }
  } else {
    // range scan
    const storage::Tuple *low_key_p = csp_p->GetLowKey();
    const storage::Tuple *high_key_p = csp_p->GetHighKey();

    ARTKey index_low_key, index_high_key, continue_key;
    WriteIndexedAttributesInKey(low_key_p, index_low_key);
    WriteIndexedAttributesInKey(high_key_p, index_high_key);

    // check whether they are the same key; if so, use lookup
    bool is_same_key = true;
    if (index_low_key.getKeyLen() != index_high_key.getKeyLen()) {
      is_same_key = false;
    } else {
      for (unsigned int i = 0; i < index_low_key.getKeyLen(); i++) {
        if (index_low_key.data[i] != index_high_key.data[i]) {
          is_same_key = false;
          break;
        }
      }
    }

    if (is_same_key) {
      ScanKey(low_key_p, result);
      return;
    }

    // the range specify the maximum number of tuples retrieved in this scan
    // the range parameter is unused so far, but it's useful in code gen
    std::size_t range = 1000;
    std::size_t actual_result_length = 0;

    auto &t = art_.GetThreadInfo();
    art_.LookupRange(index_low_key, index_high_key, continue_key, result, range,
                     actual_result_length, t);
  }
}

/*
 * ScanLimit() - Scan the index with predicate and limit/offset
 *
 */
void ArtIndex::ScanLimit(
    UNUSED_ATTRIBUTE const std::vector<type::Value> &value_list,
    UNUSED_ATTRIBUTE const std::vector<oid_t> &tuple_column_id_list,
    UNUSED_ATTRIBUTE const std::vector<ExpressionType> &expr_list,
    UNUSED_ATTRIBUTE ScanDirectionType scan_direction,
    UNUSED_ATTRIBUTE std::vector<ItemPointer *> &result,
    UNUSED_ATTRIBUTE const ConjunctionScanPredicate *csp_p,
    UNUSED_ATTRIBUTE uint64_t limit, UNUSED_ATTRIBUTE uint64_t offset) {
  // TODO
  return;
}

/*
 * ScanAllKeys() - A complete scan of the entire tree; return all tuples
 *
 */
void ArtIndex::ScanAllKeys(std::vector<ItemPointer *> &result) {
  auto &t = art_.GetThreadInfo();
  std::size_t actual_result_length = 0;
  art_.FullScan(result, actual_result_length, t);
  return;
}

std::string ArtIndex::GetTypeName() const { return "ART"; }

/*
 * WriteValueInBytes() - Given a value and its type, the value will be
 *                       transformed into binary comparable bytes array.
 *
 */
void ArtIndex::WriteValueInBytes(type::Value value, char *c, int offset,
                                 UNUSED_ATTRIBUTE int length) {
  switch (value.GetTypeId()) {
    case type::TypeId::BOOLEAN: {
      c[offset] = value.GetAs<int8_t>();
      break;
    }
    case type::TypeId::TINYINT: {
      int8_t v = value.GetAs<int8_t>();
      uint8_t unsigned_value = (uint8_t)v;
      unsigned_value ^= (1u << 7);
      c[offset] = unsigned_value & 0xFF;
      break;
    }
    case type::TypeId::SMALLINT: {
      int16_t v = value.GetAs<int16_t>();
      uint16_t unsigned_value = (uint16_t)v;
      unsigned_value ^= (1u << 15);
      c[offset] = (unsigned_value >> 8) & 0xFF;
      c[offset + 1] = unsigned_value & 0xFF;
      break;
    }
    case type::TypeId::INTEGER: {
      int32_t v = value.GetAs<int32_t>();
      uint32_t unsigned_value = (uint32_t)v;
      unsigned_value ^= (1u << 31);
      c[offset] = (unsigned_value >> 24) & 0xFF;
      c[offset + 1] = (unsigned_value >> 16) & 0xFF;
      c[offset + 2] = (unsigned_value >> 8) & 0xFF;
      c[offset + 3] = unsigned_value & 0xFF;
      break;
    }
    case type::TypeId::BIGINT: {
      int64_t v = value.GetAs<int64_t>();
      uint64_t unsigned_value = (uint64_t)v;
      unsigned_value ^= (1lu << 63);
      c[offset] = (unsigned_value >> 56) & 0xFF;
      c[offset + 1] = (unsigned_value >> 48) & 0xFF;
      c[offset + 2] = (unsigned_value >> 40) & 0xFF;
      c[offset + 3] = (unsigned_value >> 32) & 0xFF;
      c[offset + 4] = (unsigned_value >> 24) & 0xFF;
      c[offset + 5] = (unsigned_value >> 16) & 0xFF;
      c[offset + 6] = (unsigned_value >> 8) & 0xFF;
      c[offset + 7] = unsigned_value & 0xFF;
      break;
    }
    case type::TypeId::DECIMAL: {
      // double
      double v = value.GetAs<double>();
      uint64_t bits = (uint64_t)v;
      bits ^= (1lu << 63);

      // bits distribution in double:
      // 0: sign bit; 1-11: exponent; 12-63: fraction
      c[offset] = (bits >> 56) & 0xFF;
      c[offset + 1] = (bits >> 48) & 0xFF;
      c[offset + 2] = (bits >> 40) & 0xFF;
      c[offset + 3] = (bits >> 32) & 0xFF;
      c[offset + 4] = (bits >> 24) & 0xFF;
      c[offset + 5] = (bits >> 16) & 0xFF;
      c[offset + 6] = (bits >> 8) & 0xFF;
      c[offset + 7] = bits & 0xFF;
      break;
    }
    case type::TypeId::DATE: {
      uint32_t unsigned_value = value.GetAs<uint32_t>();
      c[offset] = (unsigned_value >> 24) & 0xFF;
      c[offset + 1] = (unsigned_value >> 16) & 0xFF;
      c[offset + 2] = (unsigned_value >> 8) & 0xFF;
      c[offset + 3] = unsigned_value & 0xFF;
      break;
    }
    case type::TypeId::TIMESTAMP: {
      uint64_t unsigned_value = value.GetAs<uint64_t>();
      c[offset] = (unsigned_value >> 56) & 0xFF;
      c[offset + 1] = (unsigned_value >> 48) & 0xFF;
      c[offset + 2] = (unsigned_value >> 40) & 0xFF;
      c[offset + 3] = (unsigned_value >> 32) & 0xFF;
      c[offset + 4] = (unsigned_value >> 24) & 0xFF;
      c[offset + 5] = (unsigned_value >> 16) & 0xFF;
      c[offset + 6] = (unsigned_value >> 8) & 0xFF;
      c[offset + 7] = unsigned_value & 0xFF;
      break;
    }
    case type::TypeId::VARCHAR:
    case type::TypeId::VARBINARY:
    case type::TypeId::ARRAY: {
      int len =
          ((uint32_t)length < value.GetLength()) ? length : value.GetLength();
      memcpy(c + offset, value.GetData(), len);
      if (len < length) {
        for (int i = len; i < length; i++) {
          c[offset + i] = 0;
        }
      }
      break;
    }

    default:
      break;
  }
  return;
}

/*
 * WriteIndexedAttributesInKey() - Given a tuple, the data in each column
 *                                 will be transform into binary comparable
 *                                 bytes array.
 *
 */
void ArtIndex::WriteIndexedAttributesInKey(const storage::Tuple *tuple,
                                           ARTKey &index_key) {
  int columns_in_key = tuple->GetColumnCount();

  const catalog::Schema *tuple_schema = tuple->GetSchema();
  std::vector<catalog::Column> columns = tuple_schema->GetColumns();
  int total_bytes = 0;
  for (int i = 0; i < columns_in_key; i++) {
    total_bytes += columns[i].GetLength();
  }

  char *c = new char[total_bytes];
  int offset = 0;
  for (int i = 0; i < columns_in_key; i++) {
    WriteValueInBytes(tuple->GetValue(i), c, offset, columns[i].GetLength());
    offset += columns[i].GetLength();
  }

  index_key.set(c, total_bytes);
  index_key.setKeyLen(total_bytes);

  delete[] c;
}

}  // namespace index
}  // namespace peloton