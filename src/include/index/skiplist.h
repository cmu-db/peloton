//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// skiplist.h
//
// Identification: src/include/index/skiplist.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#ifndef _SKIPLIST_H
#define _SKIPLIST_H

#pragma once

#include <atomic>
#include <functional>
#include <thread>
#include <tuple>

#include "index/index.h"

namespace peloton {
namespace index {

#define GET_DELETE(addr) ((addr)&1)
#define GET_MARK(addr) ((addr)&2)
#define SET_DELETE(addr, bit) ((addr) & ~1 | (bit))
#define SET_MARK(addr, bit) ((addr) & ~2 | ((bit) << 1))

/*
 * SKIPLIST_TEMPLATE_ARGUMENTS - Save some key strokes
 */
#define SKIPLIST_TEMPLATE_ARGUMENTS                                       \
  template <typename KeyType, typename ValueType, typename KeyComparator, \
            typename KeyEqualityChecker, typename ValueEqualityChecker>
template <typename KeyType, typename ValueType, typename KeyComparator,
    typename KeyEqualityChecker, typename ValueEqualityChecker>
class SkipList {
  class NodeManager;
  class EpochManager;
  class ForwardIterator;
  class ReversedIterator;
  class OperationContext;

 private:
  ///////////////////////////////////////////////////////////////////
  // Core components
  ///////////////////////////////////////////////////////////////////
  SkipListBaseNode *skip_list_head_;
  EpochManager epoch_manager_;
  NodeManager node_manager_;
  bool duplicate_support_;
  int GC_Interval_;

  /*
   * Get() - Search a key in the skip-list and fill in the node_list
   *
   * The return value is a indicator of the success get
   */
  bool Get(const KeyType &key,  std::vector<SkipListBaseNode *> &node_list, OperationContext &ctx) {
    return GetFrom(key, skip_list_head_, node_list, ctx);
  }

  /*
   * GetFrom() - Search a key start from a given node and fill in the node list
   *
   * The return value is a indicator of the success get from
   */
  bool GetFrom(const KeyType &key, const SkipListBaseNode *Node, std::vector<SkipListBaseNode *> &node_list, OperationContext &ctx) {
    return false;
  }

  /*
   * InsertNode() - Insert key value tuple to the skip-list
   *
   * The return value is a indicator of success or not
   */
  bool InsertNode(const KeyType &key, const ValueType &value,
                  OperationContext &ctx) {
    return false;
  }

  /*
   * DeleteNode() - Delete certain key from the skip-list
   *
   * The return value is the node deleted or NULL if failed to delete
   */
  SkipListBaseNode *DeleteNode(const KeyType &key, OperationContext &ctx) {
    return nullptr;
  }

  /*
   * HelpDeleted() Attempts to physically delete the del_node and unflag
   * prev_node
   */
  void HelpDeleted(SkipListBaseNode *prev_node, SkipListBaseNode *del_node,
                   OperationContext &ctx) {}

  /*
   * HelpFlagged() - Attempts to mark and physically delete del_node
   */
  void HelpFlagged(SkipListBaseNode *prev_node, SkipListBaseNode *del_node,
                   OperationContext &ctx) {}

  /*
   * TryDelete() Attempts to mark the node del node.
   */
  void TryDelete(SkipListBaseNode *del_node, OperationContext &ctx) {}

  /*
   * TryFlag() - Attempts to flag the prev_node, which is the last node known to
   *be the predecessor of target_node
   *
   * The return value is a tuple of deleted node and the success indicator
   */
  std::tuple<SkipListBaseNode *, bool> TryFlag(SkipListBaseNode *prev_node,
                                               SkipListBaseNode *target_node,
                                               OperationContext &ctx) {
    return std::tuple<SkipListBaseNode *, bool>{};
  }

 public:
  SkipList(bool duplicate, int GC_Interval,
           KeyComparator key_cmp_obj = KeyComparator{},
           KeyEqualityChecker key_eq_obj = KeyEqualityChecker{},
           ValueEqualityChecker value_eq_obj = ValueEqualityChecker{})
      : duplicate_support_(duplicate),
        GC_Interval_(GC_Interval_),
      // Key comparator, equality checker and hasher
        key_cmp_obj_{key_cmp_obj},
        key_eq_obj_{key_eq_obj},

      // Value equality checker and hasher
        value_eq_obj_{value_eq_obj} {
    LOG_TRACE("SkipList constructed!");
  }

  ~SkipList() {
    // TODO: deconstruct all nodes in the skip list
    LOG_TRACE("SkipList deconstructed!");

    return;
  }
  /*
   * struct shouldn't exceed 64 bytes -- cache line
   * possible optimization: add a direct link to the root of the skiplist
   */
  template <typename KeyType, typename ValueType>
  class SkipListBaseNode {
   public:
    std::atomic<SkipListBaseNode *> next_, down_, back_link_;
    KeyType key_;
    bool isHead_;
    SkipListBaseNode(SkipListBaseNode *next, SkipListBaseNode *down,
                     SkipListBaseNode *back_link, KeyType key, bool isHead)
        : next_(next),
          down_(down),
          back_link_(back_link),
          key_(key),
          isHead_(isHead) {}
  };

  template <typename KeyType, typename ValueType>
  class SkipListInnerNode : public SkipListBaseNode {
   public:
    SkipListInnerNode(SkipListBaseNode *next, SkipListBaseNode *down,
                      SkipListBaseNode *back_link, KeyType key, bool isHead)
        : SkipListBaseNode(next, down, back_link, key, isHead) {}

    // set the union value
    void SetValue(ValueType value) {
      PL_ASSERT(this->down_ == NULL);
      this->valueOrRoot_.value = value;
    }

    // set the union root
    void SetRoot(SkipListInnerNode *root) {
      PL_ASSERT(this->down_ != NULL);
      this->valueOrRoot_.root = root;
    }

    ValueType &GetValue() {
      PL_ASSERT(this->down_ == NULL);
      return this->valueOrRoot_.value;
    }

    std::atomic<SkipListInnerNode *> &GetRoot() {
      PL_ASSERT(this->down_ != NULL);
      return this->valueOrRoot_.root;
    }

   private:
    // value when down is null
    // otherwise root
    union valueOrRoot {
      ValueType value;
      std::atomic<SkipListInnerNode *> root;
    } valueOrRoot_;
  };

  // this one provides the abstract interfaces
  class SkipListIterator;
  class ForwardIterator;
  class ReversedIterator;

  /*
   * Insert() - Insert a key-value pair
   *
   * This function returns false if value already exists
   * If CAS fails this function retries until it succeeds
   */
  bool Insert(const KeyType &key, const ValueType &value) {
    LOG_TRACE("Insert called!");
    EpochManager::EpochNode *epoch_node_p = epoch_manager_.JoinEpoch();
    OperationContext ctx{epoch_node_p};
    bool ret = InsertNode(key, value, ctx);
    epoch_manager_.LeaveEpoch(epoch_node_p);
    return ret;
  }

  /*
   * ConditionalInsert() - Insert a key-value pair only if a given
   *                       predicate fails for all values with a key
   *
   * If return true then the value has been inserted
   * If return false then the value is not inserted. The reason could be
   * predicates returning true for one of the values of a given key
   * or because the value is already in the index
   */
  bool ConditionalInsert(const KeyType &key, const ValueType &value,
                         std::function<bool(const void *)> predicate,
                         bool *predicate_satisfied) {
    LOG_TRACE("Cond Insert called!");
    EpochManager::EpochNode *epoch_node_p = epoch_manager_.JoinEpoch();
    OperationContext ctx{epoch_node_p};
    // TODO: Insert key value pair to the skiplist with predicate
    epoch_manager_.LeaveEpoch(epoch_node_p);
    return false;
  }

  /*
   * Delete() - Remove a key-value pair from the tree
   *
   * This function returns false if the key and value pair does not
   * exist. Return true if delete succeeds
   */
  bool Delete(const KeyType &key) {
    LOG_TRACE("Delete called!");
    EpochManager::EpochNode *epoch_node_p = epoch_manager_.JoinEpoch();
    OperationContext ctx{epoch_node_p};
    SkipListBaseNode *node = DeleteNode(key, ctx);
    epoch_manager_.LeaveEpoch(epoch_node_p);
    return node != nullptr;
  }

  /*
   * GetValue() - Fill a value list with values stored
   *
   * This function accepts a value list as argument,
   * and will copy all values into the list
   *
   * The return value is used to indicate whether the value set
   * is empty or not
   */
  void GetValue(const KeyType &search_key, std::vector<ValueType> &value_list) {
    LOG_TRACE("GetValue()");
    EpochManager::EpochNode *epoch_node_p = epoch_manager_.JoinEpoch();
    OperationContext ctx{epoch_node_p};
    // TODO: call contatiner to fillin the value_list
    epoch_manager_.LeaveEpoch(epoch_node_p);
    return;
  }

  // returns a forward iterator from the very beginning
  ForwardIterator ForwardBegin();

  // returns a forward iterator from the key
  ForwardIterator ForwardBegin(KeyType &startsKey);

  ReversedIterator ReverseBegin();

  ReversedIterator ReverseBegin(KeyType &startsKey);

  /*
   * PerformGC() - Interface function for external users to
   *                              force a garbage collection
   */
  void PerformGC() { LOG_TRACE("Perform garbage collection!"); }

  /*
   * NeedGC() - Whether the skiplsit needs garbage collection
   */
  bool NeedGC() {
    LOG_TRACE("Need GC!");
    return true;
  }

  size_t GetMemoryFootprint() {
    LOG_TRACE("Get Memory Footprint!");
    return 0;
  }

 public:
  // Key comparator
  const KeyComparator key_cmp_obj_;

  // Raw key eq checker
  const KeyEqualityChecker key_eq_obj_;

  // Check whether values are equivalent
  const ValueEqualityChecker value_eq_obj_;

  ///////////////////////////////////////////////////////////////////
  // Key Comparison Member Functions
  ///////////////////////////////////////////////////////////////////

  /*
   * KeyCmpLess() - Compare two keys for "less than" relation
   *
   * If key1 < key2 return true
   * If not return false
   *
   * NOTE: In older version of the implementation this might be defined
   * as the comparator to wrapped key type. However wrapped key has
   * been removed from the newest implementation, and this function
   * compares KeyType specified in template argument.
   */
  inline bool KeyCmpLess(const KeyType &key1, const KeyType &key2) const {
    return key_cmp_obj_(key1, key2);
  }

  /*
   * KeyCmpEqual() - Compare a pair of keys for equality
   *
   * This functions compares keys for equality relation
   */
  inline bool KeyCmpEqual(const KeyType &key1, const KeyType &key2) const {
    return key_eq_obj_(key1, key2);
  }

  /*
   * KeyCmpGreaterEqual() - Compare a pair of keys for >= relation
   *
   * It negates result of keyCmpLess()
   */
  inline bool KeyCmpGreaterEqual(const KeyType &key1,
                                 const KeyType &key2) const {
    return !KeyCmpLess(key1, key2);
  }

  /*
   * KeyCmpGreater() - Compare a pair of keys for > relation
   *
   * It flips input for keyCmpLess()
   */
  inline bool KeyCmpGreater(const KeyType &key1, const KeyType &key2) const {
    return KeyCmpLess(key2, key1);
  }

  /*
   * KeyCmpLessEqual() - Compare a pair of keys for <= relation
   */
  inline bool KeyCmpLessEqual(const KeyType &key1, const KeyType &key2) const {
    return !KeyCmpGreater(key1, key2);
  }

  ///////////////////////////////////////////////////////////////////
  // Value Comparison Member
  ///////////////////////////////////////////////////////////////////

  /*
   * ValueCmpEqual() - Compares whether two values are equal
   */
  inline bool ValueCmpEqual(const ValueType &v1, const ValueType &v2) {
    return value_eq_obj_(v1, v2);
  }

  // maintains Epoch
  // has a inside linked list in which every node represents an epoch
  class EpochManager {
   public:
    class EpochNode;

    bool AddGarbageNode(EpochNode *epoch_node, SkipListBaseNode *node);
    /*
     * return the current EpochNode
     * need to add the reference count of current EpochNode
     */
    EpochNode *JoinEpoch();

    /*
     * leaves current EpochNode
     * should maintain atomicity when counting the reference
     */
    void LeaveEpoch(EpochNode *node);

    /*
     * NewEpoch() - start new epoch after the call
     *
     * begins new Epoch that caused by the
     * Need to atomically maintain the epoch list
     */
    void NewEpoch();

    /*
     * ClearEpoch() - Sweep the chain of epoch and free memory
     *
     * The minimum number of epoch we must maintain is 1 which means
     * when current epoch is the head epoch we should stop scanning
     *
     * NOTE: There is no race condition in this function since it is
     * only called by the cleaner thread
     */
    void ClearEpoch();
  };

  /*
   * NodeManager - maintains the SkipList Node pool
   *
   */
  class NodeManager {
   public:
    SkipListBaseNode *GetSkipListNode();
    void ReturnSkipListNode(SkipListBaseNode *node);

    std::atomic<int> NodeNum;
  };

  /*
   * OperationContext - maintains info and context of each thread
   *
   * EpochNode: the epoch node that the thread is in
   */
  class OperationContext {
   public:
    EpochManager::EpochNode *epoch_node_;
    OperationContext(EpochManager::EpochNode *epoch_node)
        : epoch_node_(epoch_node)
    {}
  };
};
}  // namespace index
}  // namespace peloton

#endif