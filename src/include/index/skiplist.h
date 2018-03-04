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
#include <deque>

#include "index/index.h"

namespace peloton {
namespace index {

#define WORD(x) reinterpret_cast<std::uintptr_t>((x))
#define GET_DELETE(addr) ((WORD((addr))) & 1ll)
#define GET_FLAG(addr) ((WORD((addr))) & 2ll)
#define SET_DELETE(addr, bit) ((WORD((addr))) & ~1ll | (bit))
#define SET_FLAG(addr, bit) ((WORD((addr))) & ~2ll | ((bit) << 1)
#define GET_NEXT(node) \
  reinterpret_cast<SkipListBaseNode *>(WORD((node)->next_.load()) & ~3ll)

#define NODE_PAIR std::pair<SkipListBaseNode *, SkipListBaseNode *>
#define NODE_LIST std::vector<SkipListBaseNode *>

#define SKIP_LIST_INITIAL_MAX_LEVEL_ 10

/*
 * SKIPLIST_TEMPLATE_ARGUMENTS - Save some key strokes
 */
#define SKIPLIST_TEMPLATE_ARGUMENTS                                       \
  template <typename KeyType, typename ValueType, typename KeyComparator, \
            typename KeyEqualityChecker, typename ValueEqualityChecker>
template <typename KeyType, typename ValueType, typename KeyComparator,
    typename KeyEqualityChecker, typename ValueEqualityChecker>
class SkipList {
public:
  class NodeManager;
  class EpochManager;
  class OperationContext;
  class SkipListBaseNode;
  class SkipListInnerNode;

private:
  ///////////////////////////////////////////////////////////////////
  // Core components
  ///////////////////////////////////////////////////////////////////
  std::atomic<SkipListBaseNode *> skip_list_head_;
  u_int32_t max_level_;
  EpochManager epoch_manager_;
  NodeManager node_manager_;
  bool duplicate_support_;
  int GC_Interval_;

  /*
   * Get() - Search a key in the skip-list and fill in the node_list
   *
   * The return value is a indicator of the success get
   */
  bool Get(const KeyType &key, NODE_LIST &node_list, OperationContext &ctx) {
    return GetFrom(key, skip_list_head_.load(), node_list, ctx);
  }

  /*
   * GetFrom() - Search a key start from a given node and fill in the node list
   *
   * The return value is a indicator of the success get from
   */

  bool GetFrom(const KeyType &key, const SkipListBaseNode *Node,
               NODE_LIST &node_list, OperationContext &ctx) {
    // TODO: complete the whole function, it's only a toy now
    auto pair = Search(key, ctx);
    auto node = static_cast<SkipListInnerNode *>(pair.second);
    while (node != nullptr && KeyCmpEqual(node->key_, key)) {
      if (GET_DELETE(node->next_.load())) {
        node = GET_NEXT(node);
        continue;
      }
      node_list.push_back(node->GetValue());
    }
    return true;
  }

  /*
   * Search() - Search the first interval that node1.key<key and key<=node2.key
   *
   * the return value is a pair of node1,node2
   * if duplicate is available, the node1 is the last node among all duplicators
   * with dup.key==node1.key as well as the node2 is the first of its
   *duplicators
   *
   * NOTE: the second pointer might be nullptr!!!!!!!!
   */
  NODE_PAIR Search(const KeyType &key, OperationContext &ctx) {
    LOG_INFO("Searchb");
    SkipListBaseNode *headNode = this->skip_list_head_.load();
    while (1) {
      auto sr = SearchFrom(key, headNode, ctx);
      PL_ASSERT(sr != nullptr);
      headNode = sr.first;
      if (headNode->down_.load() == nullptr) {
        return sr;
      } else {
        headNode = headNode->down_.load();
      }
    }
  }

  /*
   * SearchFrom() - Search the first interval that node1.key < key and
   * key <= node2.key
   * from given skip list node
   *
   * The return value is a pair of node1, node2
   * For duplicate enabled skip list, the type of return value is the same as
   * Search()
   * There is no guarantee that the nodes would be succeed after being returned
   *
   * Call this function again in insert and delete if the node pair is not
   * consistent (node1.next != node2)
   */
  NODE_PAIR SearchFrom(const KeyType &key, const SkipListBaseNode *Node,
                       OperationContext &ctx) {
    // LOG_INFO("Search From %p", Node);
    // TODO: physically deletion when search in the list
    if (Node == nullptr) {
      return std::make_pair(nullptr, nullptr);
    }
    SkipListBaseNode *curr_node = const_cast<SkipListBaseNode *>(Node);
    while (curr_node) {
      SkipListBaseNode *tmp_pointer = curr_node->next_.load();
      // LOG_INFO("Search Trace %p", curr_node);
      if (GET_FLAG(tmp_pointer)) {
        HelpFlagged(curr_node, GET_NEXT(curr_node), ctx);
      } else if ((GET_DELETE(tmp_pointer))) {
        curr_node = curr_node->back_link_.load();
      } else if (tmp_pointer == nullptr) {
        return std::make_pair(curr_node, nullptr);
      } else {
        if (KeyCmpGreaterEqual(tmp_pointer->key_, key)) {
          return std::make_pair(curr_node, tmp_pointer);
        } else {
          curr_node = tmp_pointer;
        }
      }
    }
    return std::make_pair(nullptr, nullptr);
  }

  /*
   * SearchWithPath() - Search the skiplist for the key, would store the path of
   *every level
   *
   * @param:
   *  call_stack: used for storing the path
   *  key: the search key
   *  curr_node: the same as SearchFrom, but please send in a SkipListHead
   *  expected_stored_level: from which level the function starts to record the
   *path, default to start recording from
   *  curr_node's level
   *
   * The lowest level starts from 0, the search would start from the curr_node
   * The function defaults to store all nodes in the path from the head to
   *target node
   *
   * returns nothing but will store the path at call_stack
   */
  void SearchWithPath(std::vector<NODE_PAIR> &call_stack, const KeyType &key,
                      SkipListBaseNode *curr_node, OperationContext &ctx,
                      u_int32_t expected_stored_level = 0) {
    expected_stored_level =
        expected_stored_level == 0 ? curr_node->level_ : expected_stored_level;
    u_int32_t level_now = curr_node->level_;
    call_stack.resize(expected_stored_level + 1);
    LOG_INFO("SearchWithPath %d, levelNow: %u", expected_stored_level,
             level_now);
    while (level_now >= 0) {
      if (level_now <= expected_stored_level) {
        call_stack[level_now] = SearchFrom(key, curr_node, ctx);
        curr_node = call_stack[level_now].first->down_.load();
      } else {
        curr_node = SearchFrom(key, curr_node, ctx).first->down_.load();
      }
      // level_now is unsigned
      if (level_now == 0) break;
      level_now--;
    }
  }

  /*
 * AddLevel() - add corresponding level to the SkipList
 *
 * return true if successfully added or the level is already added
 * return false if the level cannot be reached from the highest level now
 */
  bool AddLevel(u_int32_t level) {
    LOG_INFO("AddLevel");
    SkipListBaseNode *head = this->skip_list_head_.load();
    if (head->level_ + 1 < level) {
      return false;
    } else {
      if (head->level_ + 1 == level) {
        SkipListBaseNode *new_head = node_manager_.GetSkipListNode(
            nullptr, head, nullptr, KeyType{}, 1, level);
        if (this->skip_list_head_.compare_exchange_strong(head, new_head)) {
          return true;
        } else {
          node_manager_.ReturnSkipListNode(new_head);
          head = this->skip_list_head_.load();
          return head->level_ == level;
        }
      } else {
        return true;
      }
    }
  }

  /*
   * InserNodeIntoInterval() - this method would try to insert the tower into
   *the interval and retry due to contention
   *
   * It has a call_stack array to accelorate the process
   * NOTE: this method would retry until the world ends (or the root is
   *deleted)!!!
   *
   * Would only return true, or it would retry until succeed or the root is
   *deleted
   * Use this function only the tower can be exactly inserted
   */
  bool InsertTowerIntoInterval(
      const KeyType &key, std::vector<SkipListInnerNode *> &tower,
      std::vector<std::pair<SkipListBaseNode *, SkipListBaseNode *>> &
      call_stack,
      OperationContext &ctx, u_int32_t start_level = 0) {
    LOG_INFO("InsertTower");
    u_int32_t expected_level = tower.size();
    for (u_int32_t i = start_level; i < expected_level; i++) {
      bool insert_flag = false;
      do {
        if (i != 0 && GET_DELETE(tower[i]->GetRoot().load()->next_.load())) {
          // the root has been deleted
          // there is no need to continue
          for (auto j = i; j < expected_level; j++) {
            node_manager_.ReturnSkipListNode(tower[j]);
          }
          return true;
        }
        tower[i]->next_ = call_stack[i].second;
        insert_flag = call_stack[i].first->next_.compare_exchange_strong(
            call_stack[i].second, tower[i]);
        if (insert_flag)
          break;
        else
          call_stack[i] = SearchFrom(key, call_stack[i].first, ctx);
      } while (!insert_flag);
    }
    return true;
  }
  /*
   * InsertNode() - Insert key value tuple to the skip-list
   *
   * The return value is a indicator of success or not
   */
  bool InsertNode(const KeyType &key, const ValueType &value,
                  OperationContext &ctx) {
    LOG_INFO("Insert node");

    u_int32_t expected_level = 0;

    while (expected_level < max_level_) {
      if (rand() & 1) {
        expected_level++;
      } else {
        break;
      }
    }

    SkipListBaseNode *curr_node = this->skip_list_head_.load();

    while (curr_node->level_ < expected_level) {
      AddLevel(curr_node->level_ + 1);
      curr_node = this->skip_list_head_.load();
    }

    // used to store the path
    std::vector<std::pair<SkipListBaseNode *, SkipListBaseNode *>> call_stack;
    std::vector<SkipListInnerNode *> tower(expected_level + 1);
    // build the tower of expected level
    SkipListInnerNode *new_node =
        node_manager_.GetSkipListInnerNode(key, value, 0);
    tower[0] = new_node;
    for (u_int32_t i = 1; i < expected_level + 1; i++) {
      tower[i] =
          node_manager_.GetSkipListInnerNode(key, tower[0], tower[i - 1], i);
    }
    SearchWithPath(call_stack, key, curr_node, ctx, expected_level);
    PL_ASSERT(curr_node != nullptr);
    // if duplicate support, then just try insert
    // else need to verify the next node
    if (this->duplicate_support_) {
      // insert the node from the lowest level
      // redo the search from stack if the insert fails
      return InsertTowerIntoInterval(key, tower, call_stack, ctx);
    } else {
      // unique key
      // need to compare with the second return value's key
      bool insert_flag;
      do {
        // try to insert the key in the lowest level
        // if failed then abort the insert
        if (call_stack[0].second == nullptr ||
            GET_DELETE(call_stack[0].second->next_.load()) ||
            !KeyCmpEqual(call_stack[0].second->key_, key)) {
          tower[0]->next_ = call_stack[0].second;
          insert_flag = call_stack[0].first->next_.compare_exchange_strong(
              call_stack[0].second, tower[0]);
        } else {
          // found duplicate key not deleted
          // abort the insertion
          return false;
        }
        if (insert_flag) break;
        call_stack[0] = SearchFrom(key, call_stack[0].first, ctx);
      } while (!insert_flag);
      // insertion at the lowest level has succeeded
      // those towers should all be inserted into the skiplist successfully
      return InsertTowerIntoInterval(key, tower, call_stack, ctx, 1);
    }
  }

  /*
   * DeleteNode() - Delete certain key from the skip-list and fill in the
   *deleted nodes to del_nodes
   *
   * The return value is the list of node deleted or NULL if failed to delete
   */
  bool DeleteNode(const KeyType &key, NODE_LIST del_nodes,
                  OperationContext &ctx) {
    LOG_INFO("delete called");
    auto pair = Search(key, ctx);
    SkipListBaseNode *prev_node = pair.first;
    SkipListBaseNode *del_node = pair.second;

    // No such key
    while (del_node != nullptr) {
      // Tries to flag the prev node
      auto flag_pair = TryFlag(prev_node, del_node, ctx);
      prev_node = flag_pair.first;
      bool result = flag_pair.second;
      // Attempts to remove the del_node from list
      if (prev_node != nullptr) {
        HelpFlagged(prev_node, del_node, ctx);
      }
      // Node deleted by this process
      if (result) {
        del_nodes.push_back(del_node);
      }

      // Cleanup the superfluous nodes
      std::vector<NODE_PAIR> call_stack;
      SearchWithPath(call_stack, key, skip_list_head_.load(), ctx);
      for (auto node : call_stack) {
        SearchFrom(key, node.first, ctx);
      }

      // Continue searching duplicate key
      auto new_pair = SearchFrom(key, prev_node, ctx);
      prev_node = new_pair.first;
      del_node = new_pair.second;
    }

    return del_nodes.size() > 0;
  }

  /*
   * HelpDeleted() Attempts to physically delete the del_node and unflag
   * prev_node
   */

  void HelpDeleted(UNUSED_ATTRIBUTE SkipListBaseNode *prev_node,
                   UNUSED_ATTRIBUTE SkipListBaseNode *del_node,
                   UNUSED_ATTRIBUTE OperationContext &ctx) {}

  /*
   * HelpFlagged() - Attempts to mark and physically delete del_node
   */
  void HelpFlagged(UNUSED_ATTRIBUTE SkipListBaseNode *prev_node,
                   UNUSED_ATTRIBUTE SkipListBaseNode *del_node,
                   UNUSED_ATTRIBUTE OperationContext &ctx) {}

  /*
   * TryDelete() Attempts to mark the node del node.
   */
  void TryDelete(UNUSED_ATTRIBUTE SkipListBaseNode *del_node,
                 UNUSED_ATTRIBUTE OperationContext &ctx) {}

  /*
   * TryFlag() - Attempts to flag the prev_node, which is the last node known to
   *be the predecessor of target_node
   *
   * The return value is a tuple of deleted node and the success indicator
   */
  std::pair<SkipListBaseNode *, bool> TryFlag(
      UNUSED_ATTRIBUTE SkipListBaseNode *prev_node,
      UNUSED_ATTRIBUTE SkipListBaseNode *target_node,
      UNUSED_ATTRIBUTE OperationContext &ctx) {
    return std::pair<SkipListBaseNode *, bool>{};
  }

public:
  SkipList(bool duplicate, int GC_Interval,
           KeyComparator key_cmp_obj = KeyComparator{},
           KeyEqualityChecker key_eq_obj = KeyEqualityChecker{})
      : duplicate_support_(duplicate),
        GC_Interval_(GC_Interval),

      // Key comparator, equality checker
        key_cmp_obj_{key_cmp_obj},
        key_eq_obj_{key_eq_obj}

  // Value equality checker and hasher
  {
    LOG_INFO("SkipList constructed!");
    this->max_level_ = SKIP_LIST_INITIAL_MAX_LEVEL_;
    this->skip_list_head_ = node_manager_.GetSkipListNode(KeyType{}, 1, 0);
  }

  ~SkipList() {
    // TODO: deconstruct all nodes in the skip list
    LOG_INFO("SkipList deconstructed!");

    return;
  }
  /*
   * struct shouldn't exceed 64 bytes -- cache line
   * possible optimization: add a direct link to the root of the skiplist
   */
  class SkipListBaseNode {
  public:
    std::atomic<SkipListBaseNode *> next_, down_, back_link_;
    KeyType key_;
    bool isHead_;
    u_int32_t level_;

    SkipListBaseNode(SkipListBaseNode *next, SkipListBaseNode *down,
                     SkipListBaseNode *back_link, KeyType key, bool isHead)
        : next_(next),
          down_(down),
          back_link_(back_link),
          key_(key),
          isHead_(isHead) {}

    SkipListBaseNode(SkipListBaseNode *next, SkipListBaseNode *down,
                     SkipListBaseNode *back_link, KeyType key, bool isHead,
                     u_int32_t level)
        : next_(next),
          down_(down),
          back_link_(back_link),
          key_(key),
          isHead_(isHead),
          level_(level) {}

    virtual ~SkipListBaseNode(){};
  };

  class SkipListInnerNode : public SkipListBaseNode {
  public:
    SkipListInnerNode(SkipListBaseNode *next, SkipListBaseNode *down,
                      SkipListBaseNode *back_link, KeyType key, bool isHead)
        : SkipListBaseNode(next, down, back_link, key, isHead) {}
    SkipListInnerNode(SkipListBaseNode *next, SkipListBaseNode *down,
                      SkipListBaseNode *back_link, KeyType key, bool isHead,
                      u_int32_t level)
        : SkipListBaseNode(next, down, back_link, key, isHead, level) {}

    // set the union value
    void SetValue(ValueType value) {
      PL_ASSERT(this->down_ == nullptr);
      this->valueOrRoot_.value = value;
    }

    // set the union root
    void SetRoot(SkipListInnerNode *root) {
      PL_ASSERT(this->down_ != nullptr);
      this->valueOrRoot_.root = root;
    }

    ValueType &GetValue() {
      PL_ASSERT(this->down_ == nullptr);
      return this->valueOrRoot_.value;
    }

    std::atomic<SkipListInnerNode *> &GetRoot() {
      PL_ASSERT(this->down_ != nullptr);
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
  class SkipListIterator {};
  class ForwardIterator {};
  class ReversedIterator {};

  /*
   * Insert() - Insert a key-value pair
   *
   * This function returns false if value already exists
   * If CAS fails this function retries until it succeeds
   */
  bool Insert(const KeyType &key, const ValueType &value) {
    LOG_INFO("Insert called!");
    auto *epoch_node_p = epoch_manager_.JoinEpoch();
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
  bool ConditionalInsert(
      UNUSED_ATTRIBUTE const KeyType &key,
      UNUSED_ATTRIBUTE const ValueType &value,
      UNUSED_ATTRIBUTE std::function<bool(const void *)> predicate,
      UNUSED_ATTRIBUTE bool *predicate_satisfied) {
    LOG_INFO("Cond Insert called!");
    auto *epoch_node_p = epoch_manager_.JoinEpoch();
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
    LOG_INFO("Delete called!");
    auto *epoch_node_p = epoch_manager_.JoinEpoch();
    OperationContext ctx{epoch_node_p};
    std::vector<SkipListBaseNode *> del_nodes;
    bool ret = DeleteNode(key, del_nodes, ctx);
    epoch_manager_.LeaveEpoch(epoch_node_p);
    return ret;
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
  void GetValue(UNUSED_ATTRIBUTE const KeyType &search_key,
                UNUSED_ATTRIBUTE std::vector<ValueType> &value_list) {
    LOG_INFO("GetValue()");
    auto *epoch_node_p = epoch_manager_.JoinEpoch();
    OperationContext ctx{epoch_node_p};
    // TODO: call contatiner to fillin the value_list
    epoch_manager_.LeaveEpoch(epoch_node_p);
    return;
  }

  ForwardIterator ForwardBegin() { return ForwardIterator{}; }

  // returns a forward iterator from the key
  ForwardIterator ForwardBegin(UNUSED_ATTRIBUTE KeyType &startsKey) {
    return ForwardIterator{};
  }

  ReversedIterator ReverseBegin() { return ReversedIterator{}; }

  ReversedIterator ReverseBegin(UNUSED_ATTRIBUTE KeyType &startsKey) {
    return ReversedIterator{};
  };

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
  // const ValueEqualityChecker value_eq_obj_;

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

  // maintains Epoch
  // has a inside linked list in which every node represents an epoch
  class EpochManager {
  public:
    class EpochNode {};

    bool AddGarbageNode(UNUSED_ATTRIBUTE EpochNode *epoch_node,
                        UNUSED_ATTRIBUTE SkipListBaseNode *node) {
      return true;
    }
    /*
     * return the current EpochNode
     * need to add the reference count of current EpochNode
     */

    EpochNode *JoinEpoch() { return nullptr; };

    /*
     * leaves current EpochNode
     * should maintain atomicity when counting the reference
     */

    void LeaveEpoch(UNUSED_ATTRIBUTE EpochNode *node){};

    /*
     * NewEpoch() - start new epoch after the call
     *
     * begins new Epoch that caused by the
     * Need to atomically maintain the epoch list
     */
    void NewEpoch(){};

    /*
     * ClearEpoch() - Sweep the chain of epoch and free memory
     *
     * The minimum number of epoch we must maintain is 1 which means
     * when current epoch is the head epoch we should stop scanning
     *
     * NOTE: There is no race condition in this function since it is
     * only called by the cleaner thread
     */

    void ClearEpoch() {}
  };

  /*
   * NodeManager - maintains the SkipList Node pool
   *
   */
  class NodeManager {
  public:
    /*
     * GetSkipListNode() - getSkipListNode with only key and isHead settled
     */
    SkipListBaseNode *GetSkipListNode(KeyType key, bool isHead,
                                      u_int32_t level) {
      return new SkipListBaseNode(nullptr, nullptr, nullptr, key, isHead,
                                  level);
    }
    /*
     * GetSkipListNode() - get SkipListNode full equiped
     */
    SkipListBaseNode *GetSkipListNode(SkipListBaseNode *next,
                                      SkipListBaseNode *down,
                                      SkipListBaseNode *back_link, KeyType key,
                                      bool isHead, u_int32_t level) {
      return new SkipListBaseNode(next, down, back_link, key, isHead, level);
    }
    /*
     * GetSkipListInnerNode() - get a SkipListInnerNode using key and value
     */
    SkipListInnerNode *GetSkipListInnerNode(KeyType key, ValueType value,
                                            u_int32_t level) {
      auto tmp =
          new SkipListInnerNode(nullptr, nullptr, nullptr, key, false, level);
      tmp->SetValue(value);
      return tmp;
    }
    /*
     * GetSkipListInnerNode() - get a SkipListInnerNode using key and root
     * pointer
     */
    SkipListInnerNode *GetSkipListInnerNode(KeyType key,
                                            SkipListInnerNode *root,
                                            SkipListInnerNode *down,
                                            u_int32_t level) {
      auto tmp =
          new SkipListInnerNode(nullptr, down, nullptr, key, false, level);
      tmp->SetRoot(root);
      return tmp;
    }
    void ReturnSkipListNode(SkipListBaseNode *node) { delete node; }
  };

  /*
   * OperationContext - maintains info and context of each thread
   *
   * EpochNode: the epoch node that the thread is in
   */
  class OperationContext {
  public:
    typename EpochManager::EpochNode *epoch_node_;
    OperationContext(typename EpochManager::EpochNode *epoch_node)
        : epoch_node_(epoch_node) {}
  };
};
}  // namespace index
}  // namespace peloton

#endif
