//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// copy_executor.cpp
//
// Identification: src/include/index/art.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#ifndef PELOTON_ART_H
#define PELOTON_ART_H

#pragma once

#include "index/art_node.h"
#include "index/index.h"

namespace peloton {
namespace index {

class AdaptiveRadixTree {
public:
  using LoadKeyFunction = void (*)(TID tid, ARTKey &key, IndexMetadata *metadata);

private:
  N *const root_;

  TID CheckKey(const TID tid, const ARTKey &k) const;

  LoadKeyFunction load_key_;

  ARTEpochManager epoch_manager_;

  IndexMetadata *metadata_;

public:
  enum class CheckPrefixResult : uint8_t {
    Match,
    NoMatch,
    OptimisticMatch
  };

  enum class CheckPrefixPessimisticResult : uint8_t {
    Match,
    NoMatch,
  };

  enum class PCCompareResults : uint8_t {
    Smaller,
    Equal,
    Bigger,
  };
  enum class PCEqualsResults : uint8_t {
    BothMatch,
    Contained,
    NoMatch
  };
  static CheckPrefixResult CheckPrefix(N* n, const ARTKey &k, uint32_t &level);

  static CheckPrefixPessimisticResult CheckPrefixPessimistic(N *n, const ARTKey &k, uint32_t &level,
                                                             uint8_t &nonMatchingKey,
                                                             Prefix &nonMatchingPrefix,
                                                             LoadKeyFunction loadKey, bool &needRestart, IndexMetadata *metadata);

  static PCCompareResults CheckPrefixCompare(const N* n, const ARTKey &k, uint8_t fillKey, uint32_t &level, LoadKeyFunction loadKey, bool &needRestart, IndexMetadata *metadata);

  static PCEqualsResults CheckPrefixEquals(const N* n, uint32_t &level, const ARTKey &start, const ARTKey &end, LoadKeyFunction loadKey, bool &needRestart, IndexMetadata *metadata);

public:

  AdaptiveRadixTree(LoadKeyFunction loadKey);

  AdaptiveRadixTree(const AdaptiveRadixTree &) = delete;

  AdaptiveRadixTree(AdaptiveRadixTree &&t) : root_(t.root_), load_key_(t.load_key_), epoch_manager_(256) { }

  ~AdaptiveRadixTree();

  ThreadInfo &GetThreadInfo();

  TID Lookup(const ARTKey &k, ThreadInfo &threadEpocheInfo) const;

  bool LookupRange(const ARTKey &start, const ARTKey &end, ARTKey &continueKey, std::vector<ItemPointer *> &result, std::size_t resultLen,
                   std::size_t &resultCount, ThreadInfo &threadEpocheInfo) const;

  void FullScan(std::vector<ItemPointer *> &result, std::size_t &resultCount, ThreadInfo &threadEpocheInfo) const;

  void ScanAllLeafNodes(const N* node, std::vector<ItemPointer *> &result, std::size_t &resultCount) const;

  void Insert(const ARTKey &k, TID tid, ThreadInfo &epocheInfo, bool &insertSuccess);

  void Remove(const ARTKey &k, TID tid, ThreadInfo &epocheInfo);

  bool ConditionalInsert(const ARTKey &k, TID tid, ThreadInfo &epocheInfo, std::function<bool(const void *)> predicate);

  void SetIndexMetadata(IndexMetadata *metadata);
};


}
}

#endif //PELOTON_ART_H
