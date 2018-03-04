//
// Created by florian on 18.11.15.
//

#pragma once

#include "Node.h"

namespace art {

class Tree {
 public:
  using LoadKeyFunction = void (*)(void *ctx, TID tid, Key &key);

 public:
  /// Constructor
  explicit Tree(LoadKeyFunction loadKey, void *arg);

  ~Tree();

  /// No copying or moving
  Tree(const Tree &) = delete;
  Tree(Tree &&t) = delete;
  Tree &operator=(const Tree &) = delete;
  Tree &operator=(Tree &&) = delete;

  ThreadInfo getThreadInfo();

  /// Lookup TID mapping to the given full key
  bool lookup(const Key &k, std::vector<TID> &results,
              ThreadInfo &threadEpochInfo) const;

  /// Looks up all key-value pairs between the provided start and end keys.
  /// Results are placed in the provided result vector (of the provided size).
  /// The actual number of results that were inserted is in the output parameter
  /// 'resultLen' and the continuation key is provided for subsequent range
  /// lookups.
  bool lookupRange(const Key &start, const Key &end, Key &continueKey,
                   std::vector<TID> &results, uint32_t softMaxResults,
                   ThreadInfo &threadEpochInfo) const;

  /// Inserts the given key-value pair into the tree
  bool insert(const Key &k, TID tid, ThreadInfo &epochInfo);

  /// Insert the given key-value pair only if the predicate returns false for
  /// all existing entries with the given key.
  bool conditionalInsert(const Key &k, TID tid,
                         std::function<bool(const void *)> predicate,
                         ThreadInfo &epochInfo);

  /// Remove the provided key-value pair from the tree
  bool remove(const Key &k, TID tid, ThreadInfo &epochInfo);

  void setLoadKeyFunc(LoadKeyFunction loadKey, void *ctx);

 private:
  // Class to help loading the key for a given TID
  class KeyLoader {
   private:
    LoadKeyFunction loadKey;
    void *ctx;

   public:
    KeyLoader(LoadKeyFunction _loadKey, void *_ctx)
        : loadKey(nullptr), ctx(nullptr) {
      reset(_loadKey, _ctx);
    }

    void reset(LoadKeyFunction _loadKey, void *_ctx) {
      assert(_loadKey != nullptr);
      loadKey = _loadKey;
      ctx = _ctx;
    }

    void load(TID tid, Key &key) const { loadKey(ctx, tid, key); }
  };

  /// Function to check that the key for the provided TID is correct. This is
  /// done by loading the key and performing a comparison with the provided key.
  TID checkKey(TID tid, const Key &k) const;

  /// Optimistic prefix check
  enum class CheckPrefixResult : uint8_t { Match, NoMatch, OptimisticMatch };
  static CheckPrefixResult checkPrefix(Node *n, const Key &k, uint32_t &level);

  /// Pessimistic prefix check
  enum class CheckPrefixPessimisticResult : uint8_t {
    Match,
    NoMatch,
  };
  static CheckPrefixPessimisticResult checkPrefixPessimistic(
      Node *n, const Key &k, uint32_t &level, uint8_t &nonMatchingKey,
      Prefix &nonMatchingPrefix, KeyLoader keyLoader, bool &needRestart);

  enum class PCCompareResults : uint8_t {
    Smaller,
    Equal,
    Bigger,
  };
  static PCCompareResults checkPrefixCompare(const Node *n, const Key &k,
                                             uint8_t fillKey, uint32_t &level,
                                             KeyLoader keyLoader,
                                             bool &needRestart);

  enum class PCEqualsResults : uint8_t { BothMatch, Contained, NoMatch };
  static PCEqualsResults checkPrefixEquals(const Node *n, uint32_t &level,
                                           const Key &start, const Key &end,
                                           KeyLoader keyLoader,
                                           bool &needRestart);

 private:
  // The root of the tree
  Node *const root;

  // A callback function to load a key given a TID
  KeyLoader keyLoader;

  // GC
  Epoch epoch;
};

}  // namespace art
