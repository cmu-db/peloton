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

  /// Insert the given key-value pair into the index
  bool InsertEntry(const storage::Tuple *key, ItemPointer *value) override;

  /// Delete the given key-value pair from the index
  bool DeleteEntry(const storage::Tuple *key, ItemPointer *value) override;

  /// Insert the given key-value pair into the index, but only if the predicate
  /// returns false for every instance of the given key in the index
  bool CondInsertEntry(const storage::Tuple *key, ItemPointer *value,
                       std::function<bool(const void *)> predicate) override;

  /// Scan the range of keys between [start,end] inclusive, placing results in
  /// the provided result vector
  void ScanRange(const storage::Tuple *start, const storage::Tuple *end,
                 std::vector<ItemPointer *> &result);

  /// Perform a scan, using the predicate, and place the results into the
  /// provided result vector
  void Scan(const std::vector<type::Value> &values,
            const std::vector<oid_t> &key_column_ids,
            const std::vector<ExpressionType> &expr_types,
            ScanDirectionType scan_direction,
            std::vector<ItemPointer *> &result,
            const ConjunctionScanPredicate *scan_predicate) override;

  /// Perform a limited scan
  void ScanLimit(const std::vector<type::Value> &values,
                 const std::vector<oid_t> &key_column_ids,
                 const std::vector<ExpressionType> &expr_types,
                 ScanDirectionType scan_direction,
                 std::vector<ItemPointer *> &result,
                 const ConjunctionScanPredicate *scan_predicate, uint64_t limit,
                 uint64_t offset) override;

  /// Scan the entire index, placing results in the provided result vector
  void ScanAllKeys(std::vector<ItemPointer *> &result) override;

  /// Scan all values for the given key, placing results in the provided vector
  void ScanKey(const storage::Tuple *key,
               std::vector<ItemPointer *> &result) override;

  /// Return the index type
  std::string GetTypeName() const override {
    return IndexTypeToString(GetIndexMethodType());
  }

  // TODO(pmenon): Implement me
  size_t GetMemoryFootprint() override { return 0; }

  /// Does the index need GC
  // TODO(pmenon): Implement me
  bool NeedGC() override { return false; }

  /// Perform any necessary GC on the index
  // TODO(pmenon): Implement me
  void PerformGC() override {}

  /// These two functions are for testing so we can inject custom key loading
  /// functions without bringing up a full-blown database/table.
  void SetLoadKeyFunc(art::Tree::LoadKeyFunction load_func, void *ctx);

  /// Construct an ART key from a Peloton key
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

    /// Given an input Peloton key, construct an equivalent art::Key
    void ConstructKey(const AbstractTuple &input_key, art::Key &tree_key) const;

    /// Generate the minimum and maximum key
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