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

void loadKey(TID tid, Key &key) {
  // Store the key of the tuple into the key vector
  // Implementation is database specific
  key.setKeyLen(sizeof(tid));
  reinterpret_cast<uint64_t *>(&key[0])[0] = __builtin_bswap64(tid);
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
  // TODO: Add your implementation here
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