//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// art_index.h
//
// Identification: src/include/index/art_index.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "adaptive_radix_tree/Tree.h"
#include "index/index.h"

namespace peloton {
namespace index {

//===----------------------------------------------------------------------===//
//
// An Adaptive Radix Tree (ART) based index.
//
//===----------------------------------------------------------------------===//
class ArtIndex : public Index {
  friend class IndexFactory;

 public:
  explicit ArtIndex(IndexMetadata *metadata);

  // Forward declare iterator class for later
  class Iterator;

  bool InsertEntry(const storage::Tuple *key, ItemPointer *value) override;

  bool DeleteEntry(const storage::Tuple *key, ItemPointer *value) override;

  bool CondInsertEntry(const storage::Tuple *key, ItemPointer *value,
                       std::function<bool(const void *)> predicate) override;

  /**
   * Perform a range scan of keys between [start,end] inclusive.
   *
   * @param start The start key
   * @param end The end key
   * @param[out] result Where the results of the scan are stored
   * @param scan_predicate
   */
  void ScanRange(const storage::Tuple *start, const storage::Tuple *end,
                 std::vector<ItemPointer *> &result);

  /**
   * ArtIndex throws away the first three arguments and only uses the conjuncts
   * from the scan predicate.
   *
   * @param scan_predicate The only predicate that's actually used.
   * @param[out] result Where the results of the scan are stored
   */
  void Scan(const std::vector<type::Value> &values,
            const std::vector<oid_t> &key_column_ids,
            const std::vector<ExpressionType> &expr_types,
            ScanDirectionType scan_direction,
            std::vector<ItemPointer *> &result,
            const ConjunctionScanPredicate *scan_predicate) override;

  /**
   * ArtIndex throws away the first three arguments and only uses the conjuncts
   * from the scan predicate.
   *
   * @param scan_predicate The only parameter that's used
   * @param[out] result Where the results of the scan are stored
   * @param limit How many results to actually return
   * @param offset How many items to exclude from the results
   */
  void ScanLimit(const std::vector<type::Value> &values,
                 const std::vector<oid_t> &key_column_ids,
                 const std::vector<ExpressionType> &expr_types,
                 ScanDirectionType scan_direction,
                 std::vector<ItemPointer *> &result,
                 const ConjunctionScanPredicate *scan_predicate, uint64_t limit,
                 uint64_t offset) override;

  void ScanAllKeys(std::vector<ItemPointer *> &result) override;

  void ScanKey(const storage::Tuple *key,
               std::vector<ItemPointer *> &result) override;

  /// Return the index type
  std::string GetTypeName() const override {
    return IndexTypeToString(GetIndexMethodType());
  }

  // TODO(pmenon): Implement me
  size_t GetMemoryFootprint() override { return 0; }

  // TODO(pmenon): Implement me
  bool NeedGC() override { return false; }

  // TODO(pmenon): Implement me
  void PerformGC() override {}

  /**
   * Configure the load-key function for this index. The load-key function
   * retrieves the key associated with a given value in the tree. This is
   * necessary because an ART index does not always store the whole key in the
   * tree due to prefix compression.
   *
   * @param load_func The loading key function pointer
   * @param ctx An opaque pointer to some user-provided context. This context is
   * passed as the first argument to the load-key function on each invocation.
   */
  void SetLoadKeyFunc(art::Tree::LoadKeyFunction load_func, void *ctx);

  /**
   * Convert the provided Peloton key into an ART-based key
   *
   * @param tuple The input key we'd like to convert
   * @param[out] key Where the art-compatible key is written to
   */
  void ConstructArtKey(const AbstractTuple &tuple, art::Key &key) const {
    key_constructor_.ConstructKey(tuple, key);
  }

 private:
  void ScanRange(const art::Key &start, const art::Key &end,
                 std::vector<ItemPointer *> &result);

  //===--------------------------------------------------------------------===//
  //
  // Helper class to construct an art::Key from a Peloton key.
  //
  //===--------------------------------------------------------------------===//
  class KeyConstructor {
   public:
    explicit KeyConstructor(const catalog::Schema &key_schema)
        : key_schema_(key_schema) {}

    /**
     * Given an input Peloton key, construct an equivalent art::Key
     *
     * @param input_key The input (i.e., Peloton key)
     * @param[out] tree_key Where the tree-compatible key is written
     */
    void ConstructKey(const AbstractTuple &input_key, art::Key &tree_key) const;

    /**
     * Generate the minimum and maximum key, storing the results in the provided
     * min_key and max_key function parameters
     *
     * @param[out] min_key Where the minimum key is written
     * @param[out] max_key Where the maximum key is written
     */
    void ConstructMinMaxKey(art::Key &min_key, art::Key &max_key) const;

   private:
    template <typename NativeType>
    static NativeType FlipSign(NativeType val);

    // Write the provided integral data type into the buffer
    template <typename NativeType>
    static void WriteValue(uint8_t *data, NativeType val);

    // Write a specified number of ASCII characters into the output buffer
    static void WriteAsciiString(uint8_t *data, const char *val, uint32_t len);

   private:
    // The index's key schema
    const catalog::Schema &key_schema_;
  };

 private:
  // ART
  art::Tree container_;

  // Key constructor
  KeyConstructor key_constructor_;
};

}  // namespace index
}  // namespace peloton