//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// art_index.cpp
//
// Identification: src/index/art_index.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "index/art_index.h"

#include "common/container_tuple.h"
#include "index/scan_optimizer.h"
#include "settings/settings_manager.h"
#include "statistics/backend_stats_context.h"
#include "storage/data_table.h"
#include "storage/storage_manager.h"
#include "util/portable_endian.h"

namespace peloton {
namespace index {

namespace {

// Load the key for the given TID (i.e., an ItemPointer), storing the key into
// the provided art::Key output argument. The provided opaque 'ctx' argument
// is the index object.
void LoadKey(void *ctx, TID tid, art::Key &key) {
  auto *art_index = reinterpret_cast<const ArtIndex *>(ctx);
  auto *index_meta = art_index->GetMetadata();

  // Get physical table instance (to load appropriate tile group)
  auto *table = storage::StorageManager::GetInstance()->GetTableWithOid(
      index_meta->GetDatabaseOid(), index_meta->GetTableOid());

  // Get physical tile group
  auto *item_pointer = reinterpret_cast<ItemPointer *>(tid);
  auto tile_group = table->GetTileGroupById(item_pointer->block);

  // Construct tuple, project only indexed columns
  const auto &indexed_cols = index_meta->GetKeyAttrs();
  ContainerTuple<storage::TileGroup> tuple{tile_group.get(),
                                           item_pointer->offset, &indexed_cols};

  // Construct the index key
  art_index->ConstructArtKey(tuple, key);
}

}  // namespace

ArtIndex::ArtIndex(IndexMetadata *metadata)
    : Index(metadata),
      container_(LoadKey, this),
      key_constructor_(*GetKeySchema()) {}

bool ArtIndex::InsertEntry(const storage::Tuple *key, ItemPointer *value) {
  // Construct the key for the tree
  art::Key tree_key;
  ConstructArtKey(*key, tree_key);

  // Perform insert (always succeeds)
  auto thread_info = container_.getThreadInfo();
  container_.insert(tree_key, reinterpret_cast<TID>(value), thread_info);

  if (static_cast<StatsType>(settings::SettingsManager::GetInt(
          settings::SettingId::stats_mode)) != StatsType::INVALID) {
    stats::BackendStatsContext::GetInstance()->IncrementIndexInserts(
        GetMetadata());
  }

  // Update stats
  IncreaseNumberOfTuplesBy(1);

  return true;
}

bool ArtIndex::DeleteEntry(const storage::Tuple *key, ItemPointer *value) {
  // Construct the key for the tree
  art::Key tree_key;
  ConstructArtKey(*key, tree_key);

  // Perform deletion
  auto thread_info = container_.getThreadInfo();
  bool removed =
      container_.remove(tree_key, reinterpret_cast<TID>(value), thread_info);

  if (removed) {
    // Update stats
    DecreaseNumberOfTuplesBy(1);
    if (static_cast<StatsType>(settings::SettingsManager::GetInt(
            settings::SettingId::stats_mode)) != StatsType::INVALID) {
      stats::BackendStatsContext::GetInstance()->IncrementIndexDeletes(
          1, GetMetadata());
    }
  }

  return removed;
}

bool ArtIndex::CondInsertEntry(const storage::Tuple *key, ItemPointer *value,
                               std::function<bool(const void *)> predicate) {
  // Construct the key for the tree
  art::Key tree_key;
  ConstructArtKey(*key, tree_key);

  // Perform conditional insertion
  auto thread_info = container_.getThreadInfo();
  bool inserted = container_.conditionalInsert(
      tree_key, reinterpret_cast<TID>(value), predicate, thread_info);

  if (inserted) {
    // Update stats
    IncreaseNumberOfTuplesBy(1);
    if (static_cast<StatsType>(settings::SettingsManager::GetInt(
            settings::SettingId::stats_mode)) != StatsType::INVALID) {
      stats::BackendStatsContext::GetInstance()->IncrementIndexInserts(
          GetMetadata());
    }
  }

  return inserted;
}

void ArtIndex::ScanRange(const storage::Tuple *start, const storage::Tuple *end,
                         std::vector<ItemPointer *> &result) {
  // Build boundary keys
  art::Key start_key, end_key;
  ConstructArtKey(*start, start_key);
  ConstructArtKey(*end, end_key);

  // Perform scan
  ScanRange(start_key, end_key, result);
}

void ArtIndex::Scan(
    UNUSED_ATTRIBUTE const std::vector<type::Value> &values,
    UNUSED_ATTRIBUTE const std::vector<oid_t> &key_column_ids,
    UNUSED_ATTRIBUTE const std::vector<ExpressionType> &expr_types,
    UNUSED_ATTRIBUTE ScanDirectionType scan_direction,
    std::vector<ItemPointer *> &result,
    const ConjunctionScanPredicate *scan_predicate) {
  // Perform the appropriate scan based on the scan predicate
  if (scan_predicate->IsFullIndexScan()) {
    ScanAllKeys(result);
  } else if (scan_predicate->IsPointQuery()) {
    ScanKey(scan_predicate->GetPointQueryKey(), result);
  } else {
    ScanRange(scan_predicate->GetLowKey(), scan_predicate->GetHighKey(),
              result);
  }

  // Update stats
  if (static_cast<StatsType>(settings::SettingsManager::GetInt(
          settings::SettingId::stats_mode)) != StatsType::INVALID) {
    stats::BackendStatsContext::GetInstance()->IncrementIndexReads(
        result.size(), GetMetadata());
  }
}

void ArtIndex::ScanLimit(const std::vector<type::Value> &values,
                         const std::vector<oid_t> &key_column_ids,
                         const std::vector<ExpressionType> &expr_types,
                         ScanDirectionType scan_direction,
                         std::vector<ItemPointer *> &result,
                         const ConjunctionScanPredicate *scan_predicate,
                         uint64_t limit, uint64_t offset) {
  (void)values;
  (void)key_column_ids;
  (void)expr_types;
  (void)scan_direction;
  (void)result;
  (void)scan_predicate;
  (void)limit;
  (void)offset;
  // This fucking function takes seven fucking arguments ... The fuck?
}

void ArtIndex::ScanAllKeys(std::vector<ItemPointer *> &result) {
  // Build boundary keys
  art::Key min_key, max_key;
  key_constructor_.ConstructMinMaxKey(min_key, max_key);

  // Scan range
  ScanRange(min_key, max_key, result);
}

void ArtIndex::ScanKey(const storage::Tuple *key,
                       std::vector<ItemPointer *> &result) {
  art::Key tree_key;
  ConstructArtKey(*key, tree_key);

  std::vector<TID> tmp_result;
  auto thread_info = container_.getThreadInfo();
  container_.lookup(tree_key, tmp_result, thread_info);
  for (const auto &tid : tmp_result) {
    result.push_back(reinterpret_cast<ItemPointer *>(tid));
  }
}

void ArtIndex::ScanRange(const art::Key &start, const art::Key &end,
                         std::vector<ItemPointer *> &result) {
  const uint32_t batch_size = 1000;
  std::vector<TID> tmp_result;

  art::Key start_key;
  start_key.setFrom(start);

  bool has_more = true;
  while (has_more) {
    art::Key next_start_key;
    auto thread_info = container_.getThreadInfo();
    has_more = container_.lookupRange(start_key, end, next_start_key,
                                      tmp_result, batch_size, thread_info);

    // Copy the results to the vector
    for (const auto &tid : tmp_result) {
      result.push_back(reinterpret_cast<ItemPointer *>(tid));
    }

    // Set the next key
    start_key.setFrom(next_start_key);
  }

  // Update stats
  if (static_cast<StatsType>(settings::SettingsManager::GetInt(
          settings::SettingId::stats_mode)) != StatsType::INVALID) {
    stats::BackendStatsContext::GetInstance()->IncrementIndexReads(
        result.size(), GetMetadata());
  }
}

void ArtIndex::SetLoadKeyFunc(art::Tree::LoadKeyFunction load_func, void *ctx) {
  container_.setLoadKeyFunc(load_func, ctx);
}

//===----------------------------------------------------------------------===//
//
// KeyConstructor
//
//===----------------------------------------------------------------------===//

/// Here we define a four utility functions to help with conversion of native
/// integral types (i.e., 1-, 2-, 4-, and 8-byte integers) to their big-endian
/// versions.
namespace {

uint8_t ToBigEndian(int8_t data) { return static_cast<uint8_t>(data); }

uint16_t ToBigEndian(int16_t data) {
  return htobe16(static_cast<uint16_t>(data));
}

uint32_t ToBigEndian(int32_t data) {
  return htobe32(static_cast<uint32_t>(data));
}

uint64_t ToBigEndian(int64_t data) {
  return htobe64(static_cast<uint64_t>(data));
}

}  // namespace

template <typename NativeType>
NativeType ArtIndex::KeyConstructor::FlipSign(NativeType val) {
  // This sets 1 on the MSB of the corresponding type
  auto mask = static_cast<NativeType>(1) << (sizeof(NativeType) * 8ul - 1);
  return val ^ mask;
}

template <typename NativeType>
void ArtIndex::KeyConstructor::WriteValue(uint8_t *data, NativeType val) {
  auto *casted_data = reinterpret_cast<NativeType *>(data);
  *casted_data = ToBigEndian(FlipSign(val));
}

// This is specialized for ASCII strings ...
void ArtIndex::KeyConstructor::WriteAsciiString(uint8_t *data, const char *val,
                                                uint32_t len) {
  // Write out the main payload, then tack on the NULL byte terminator
  PELOTON_MEMCPY(data, val, len);
  data[len] = '\0';
}

// Constructing an ART tree key from a Peloton input key involves converting it
// to a binary-comparable format. This method handles the conversion process.
//   1. For fixed-width integral types, we need to flip the sign and convert to
//      big-endian format. If the host system is big-endian, the conversion is
//      elided entirely.
//   2. For variable length strings, we write out the string and append a NULL
//      byte indicating the end of the string. The assumption is that NULL
//      cannot appear in normal strings. True for ASCII, not so for UTF-8.
//      TODO(pmenon): For UTF-8 strings, we need a third-party library to
//      generate sort-able text from UTF-8 string.
//   3. Floats/doubles are more complicated, not done here.
//   4. NULL is handled using special values.
void ArtIndex::KeyConstructor::ConstructKey(const AbstractTuple &input_key,
                                            art::Key &tree_key) const {
  // First calculate length of this key
  uint32_t key_len = 0;
  for (uint32_t i = 0; i < key_schema_.GetColumnCount(); i++) {
    auto col_info = key_schema_.GetColumn(i);
    if (col_info.IsInlined()) {
      key_len += col_info.GetFixedLength();
    } else {
      key_len += input_key.GetValue(i).GetLength();
      if (col_info.GetType() == type::TypeId::VARCHAR) {
        // Need to append NULL character
        key_len++;
      }
    }
  }

  // Set the tree key's length and grab a pointer the data area
  tree_key.setKeyLen(key_len);
  uint8_t *data = &tree_key[0];

  uint32_t offset = 0;
  for (uint32_t i = 0; i < key_schema_.GetColumnCount(); i++) {
    auto column = key_schema_.GetColumn(i);
    switch (column.GetType()) {
      case type::TypeId::BOOLEAN:
      case type::TypeId::TINYINT: {
        auto raw = type::ValuePeeker::PeekTinyInt(input_key.GetValue(i));
        WriteValue<int8_t>(data + offset, raw);
        offset += sizeof(int8_t);
        break;
      }
      case type::TypeId::SMALLINT: {
        auto raw = type::ValuePeeker::PeekSmallInt(input_key.GetValue(i));
        WriteValue<int16_t>(data + offset, raw);
        offset += sizeof(int16_t);
        break;
      }
      case type::TypeId::DATE:
      case type::TypeId::INTEGER: {
        auto raw = type::ValuePeeker::PeekInteger(input_key.GetValue(i));
        WriteValue<int32_t>(data + offset, raw);
        offset += sizeof(int32_t);
        break;
      }
      case type::TypeId::TIMESTAMP:
      case type::TypeId::BIGINT: {
        auto raw = type::ValuePeeker::PeekBigInt(input_key.GetValue(i));
        WriteValue<int64_t>(data + offset, raw);
        offset += sizeof(int64_t);
        break;
      }
      case type::TypeId::VARCHAR: {
        auto varchar_val = input_key.GetValue(i);
        auto raw = type::ValuePeeker::PeekVarchar(varchar_val);
        auto raw_len = varchar_val.GetLength();
        WriteAsciiString(data + offset, raw, raw_len);
        offset += raw_len;
        break;
      }
      default: {
        auto error =
            StringUtil::Format("Column type '%s' not supported in ART index",
                               TypeIdToString(column.GetType()).c_str());
        LOG_ERROR("%s", error.c_str());
        throw IndexException{error};
      }
    }
  }
}

void ArtIndex::KeyConstructor::ConstructMinMaxKey(art::Key &min_key,
                                                  art::Key &max_key) const {
  min_key.setKeyLen(1);
  max_key.setKeyLen(1);
  min_key[0] = 0u;
  max_key[0] = 255u;
}

}  // namespace index
}  // namespace peloton