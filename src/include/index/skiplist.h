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

#pragma once

#include <atomic>
#include <functional>
#include <thread>

namespace peloton {
namespace index {

#define GET_DELETE(addr) ((addr) & 0x1)
#define GET_MARK(addr) ((addr) & 0x2)
#define SET_DELETE(addr, bit) ((addr) & (-1 & ~1) | (bit))
#define SET_MARK(addr, bit) ((addr) & (-1 & ~2) | ((bit) << 1))

/*
 * SKIPLIST_TEMPLATE_ARGUMENTS - Save some key strokes
 */
#define SKIPLIST_TEMPLATE_ARGUMENTS                                       \
  template <typename KeyType, typename ValueType, typename KeyComparator, \
            typename KeyEqualityChecker, typename ValueEqualityChecker>
template <typename KeyType, typename ValueType, typename KeyComparator,
          typename KeyEqualityChecker, typename ValueEqualityChecker>
class SkipList {
  // TODO: Add your declarations here

  class NodeManager;
  class EpochManager;
  class ForwardIterator;
  class ReversedIterator;

 private:
  ///////////////////////////////////////////////////////////////////
  // Core components
  ///////////////////////////////////////////////////////////////////
  SkipListNode *skip_list_head_;
  EpochManager epoch_manager_;
  NodeManager node_manager_;
  bool duplicate_support_;
  int GC_Interval_;

 public:

  SkipList(bool duplicate, int GC_Interval, KeyComparator key_cmp_obj = KeyComparator{},
          KeyEqualityChecker key_eq_obj = KeyEqualityChecker{},
          ValueEqualityChecker value_eq_obj = ValueEqualityChecker{})
          :duplicate_support_(duplicate), GC_Interval_(GC_Interval_){
  }

  ~SkipList(){
    // TO DO:
    // deconstruct all nodes in the skip list
    LOG_TRACE("SkipList deconstructed!");

    return;
  }
  /*
   * struct shouldn't exceed 64 bytes -- cache line
   * possible optimization: add a direct link to the root of the skiplist
   */
  template <typename KeyType, typename ValueType>
  class SkipListBaseNode{
    public:
      SkipListBaseNode *next_, *down_, *back_link_;
      KeyType key_;
      bool isHead_; 
      SkipListHeadNode(SkipListBaseNode *next, SkipListBaseNode *down,
                       SkipListBaseNode *back_link, KeyType key, bool isHead):
      next_(next), down_(down), back_link_(back_link), key_(key), isHead_(isHead){}
  };

  template <typename KeyType, typename ValueType>
  class SkipListInnerNode: public SkipListBaseNode{
    public:
      SkipListInnerNode(SkipListBaseNode *next, SkipListBaseNode *down,
                       SkipListBaseNode *back_link, KeyType key, bool isHead):
      SkipListBaseNode(next,down,back_link,key,isHead){}

      // set the union value
      void SetValue(ValueType value){
        PL_ASSERT(this->down == NULL);
        this->valueOrRoot.value = value;
      }

      // set the union root
      void SetRoot(SkipListInnerNode *root){
        PL_ASSERT(this->down != NULL);
        this->valueOrRoot.root = root;
      }

      // value when down is null
      // otherwise root
      union valueOrRoot{
        ValueType value;
        SkipListInnerNode *root;
      };
  };

  // this one provides the abstract interfaces
  class SkipListIterator;
  class ForwardIterator : protected SkipListIterator;
  class ReversedIterator : protected SkipListIterator;

  bool Insert(const KeyType &key, const ValueType &value);

  bool Delete(const KeyType &key);

  bool ConditionalInsert(const KeyType &key, const ValueType &value,
                         std::function<bool(const void *)> predicate,
                         bool *predicate_satisfied);

  void GetValue(const KeyType &search_key, std::vector<ValueType> &value_list);

  // returns a forward iterator from the very beginning
  ForwardIterator ForwardBegin();

  // returns a forward iterator from the key
  ForwardIterator ForwardBegin(KeyType &startsKey);

  ReversedIterator ReverseBegin();

  ReversedIterator ReverseBegin(KeyType &startsKey);

  void PerformGC();

  bool NeedGC();

  size_t GetMemoryFootprint();

 public:
  // Key comparator
  const KeyComparator key_cmp_obj;

  // Raw key eq checker
  const KeyEqualityChecker key_eq_obj;

  // Check whether values are equivalent
  const ValueEqualityChecker value_eq_obj;

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
    return key_cmp_obj(key1, key2);
  }

  /*
   * KeyCmpEqual() - Compare a pair of keys for equality
   *
   * This functions compares keys for equality relation
   */
  inline bool KeyCmpEqual(const KeyType &key1, const KeyType &key2) const {
    return key_eq_obj(key1, key2);
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
    return value_eq_obj(v1, v2);
  }

  // maintains Epoch
  // has a inside linked list in which every node represents an epoch
  class EpochManager {
   public:
    class EpochNode;

    bool AddGarbageNode(SkipListNode *node);
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
    SkipListNode *GetSkipListNode();
    void ReturnSkipListNode(SkipListNode *node);

    int NodeNum;
  };
};

}  // namespace index
}  // namespace peloton