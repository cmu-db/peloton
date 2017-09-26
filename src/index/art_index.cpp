//
// Created by Min Huang on 9/20/17.
//

#include "index/art_index.h"

#include "common/logger.h"
#include "index/index_key.h"
#include "index/scan_optimizer.h"
#include "statistics/stats_aggregator.h"
#include "storage/tuple.h"

namespace peloton {
namespace index {

void loadKey(UNUSED_ATTRIBUTE TID tid, UNUSED_ATTRIBUTE Key &key) {
  // Store the key of the tuple into the key vector
  // Implementation is database specific

  // TODO: recover key from tuple_pointer (recover a storage::Tuple from ItemPointer)
//  auto &manager = catalog::Manager::GetInstance();
//  ItemPointer *tuple_pointer = (ItemPointer *) tid;
//  ItemPointer tuple_location = *tuple_pointer;
//  auto tile_group = manager.GetTileGroup(tuple_location.block);
//  auto tile_group_header = tile_group.get()->GetHeader();
//  size_t chain_length = 0;
//
//
//  key.setKeyLen(sizeof(tid));
//  reinterpret_cast<uint64_t *>(&key[0])[0] = __builtin_bswap64(tid);
}

ArtIndex::ArtIndex(IndexMetadata *metadata)
  :  // Base class
  Index{metadata}, artTree(loadKey) {
  return;
}

ArtIndex::~ArtIndex() {}

/*
 * InsertEntry() - insert a key-value pair into the map
 *
 * If the key value pair already exists in the map, just return false
 */
bool ArtIndex::InsertEntry(
  const storage::Tuple *key,
  ItemPointer *value) {
  bool ret = true;

  Key index_key;
  index_key.set(key->GetData(), key->GetLength());
  index_key.setKeyLen(key->GetLength());
  printf("[DEBUG] ART insert: \n");
  for (unsigned int i = 0; i < index_key.getKeyLen(); i++) {
    printf("%d ", index_key.data[i]);
  }
  printf("\n");

  TID tid =  reinterpret_cast<TID>(value);
  printf("tid = %llu\n", tid);

  auto t = artTree.getThreadInfo();
  artTree.insert(index_key, reinterpret_cast<TID>(value), t);

  return ret;
}

void ArtIndex::ScanKey(
  const storage::Tuple *key,
  std::vector<ItemPointer *> &result) {

  Key index_key;
  index_key.set(key->GetData(), key->GetLength());
  index_key.setKeyLen(key->GetLength());

  printf("[DEBUG] ART scan: \n");
  for (unsigned int i = 0; i < index_key.getKeyLen(); i++) {
    printf("%d ", index_key.data[i]);
  }
  printf("\n");

  auto t = artTree.getThreadInfo();
  TID value = artTree.lookup(index_key, t);
  if (value != 0) {
    ItemPointer *value_pointer = (ItemPointer *) value;
    result.push_back(value_pointer);
  }
  return;
}

/*
 * DeleteEntry() - Removes a key-value pair
 *
 * If the key-value pair does not exists yet in the map return false
 */
bool ArtIndex::DeleteEntry(
  const storage::Tuple *key,
  ItemPointer *value) {
  bool ret = true;

  Key index_key;
  index_key.set(key->GetData(), key->GetLength());
  index_key.setKeyLen(key->GetLength());
  printf("[DEBUG] ART delete: \n");
  for (unsigned int i = 0; i < index_key.getKeyLen(); i++) {
    printf("%d ", index_key.data[i]);
  }
  printf("\n");

  TID tid =  reinterpret_cast<TID>(value);
  printf("tid = %llu\n", tid);

  auto t = artTree.getThreadInfo();
  artTree.remove(index_key, tid, t);

  return ret;
}

bool ArtIndex::CondInsertEntry(
  UNUSED_ATTRIBUTE const storage::Tuple *key,
  UNUSED_ATTRIBUTE ItemPointer *value,
  UNUSED_ATTRIBUTE std::function<bool(const void *)> predicate) {
  bool ret = false;
  // TODO: Add your implementation here
  return ret;
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
  UNUSED_ATTRIBUTE std::vector<ItemPointer *> &result,
  UNUSED_ATTRIBUTE const ConjunctionScanPredicate *csp_p) {

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

    Key index_low_key, index_high_key, continue_key;
    index_low_key.set(low_key_p->GetData(), low_key_p->GetLength());
    index_low_key.setKeyLen(low_key_p->GetLength());
    index_high_key.set(high_key_p->GetData(), high_key_p->GetLength());
    index_high_key.setKeyLen(high_key_p->GetLength());

    // TODO: how do I know the result length before scanning?
//    std::size_t expected_result_length = -1;
//    std::size_t actual_result_length = 0;
//
//    auto t = artTree.getThreadInfo();
//    artTree.lookupRange(index_low_key, index_high_key, continue_key, result, expected_result_length,
//      actual_result_length, t);

  }
  return;
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
  // TODO: Add your implementation here
  return;
}

void ArtIndex::ScanAllKeys(
  UNUSED_ATTRIBUTE std::vector<ItemPointer *> &result) {
  // TODO: Add your implementation here
  return;
}

std::string ArtIndex::GetTypeName() const { return "ART"; }

}  // namespace index
}  // namespace peloton