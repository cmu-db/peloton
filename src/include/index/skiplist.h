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

namespace peloton {
namespace index {

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
private:
  
  SkipListNode skip_list_head;
  class NodeManager;
  class SkipListEpochManager;

  //returns a forward iterator from the very beginning
  ForwardIterator& ForwardBegin();
  //returns a forward iterator from the key
  ForwardIterator& ForwardBegin(KeyType &startsKey);

  ReversedIterator& ReverseBegin();
  ReversedIterator& ReverseBegin(KeyType &startsKey);

public:
  
  class SkipListNode;
  // this one provides the abstract interfaces
  class SkipListIterator;
  class ForwardIterator: protected SkipListIterator;
  class ReversedIterator: protected SkipListIterator;

  bool Insert(const KeyType &key, const ValueType &value);
  
  bool Delete(const KeyType &key);

  bool ConditionalInsert(const KeyType &key, const ValueType &value,
                         std::function<bool(const void *)> predicate,
                         bool *predicate_satisfied);

  ValueType GetSingleValue(const KeyType &key);


  // don't know whether we should take the wrapped values out
  // e.g. take the ValueType val out from <type::Value> values
  // maybe it will cost more time
  void Scan(const std::vector<type::Value> &values,
            const std::vector<oid_t> &key_column_ids,
            const std::vector<ExpressionType> &expr_types,
            ScanDirectionType scan_direction, std::vector<ValueType> &result,
            const ConjunctionScanPredicate *csp_p);

  void ScanLimit(const std::vector<type::Value> &values,
                 const std::vector<oid_t> &key_column_ids,
                 const std::vector<ExpressionType> &expr_types,
                 ScanDirectionType scan_direction,
                 std::vector<ValueType> &result,
                 const ConjunctionScanPredicate *csp_p, uint64_t limit,
                 uint64_t offset);

  void ScanAllKeys(std::vector<ValueType> &result);

  void ScanKey(const storage::Tuple *key, std::vector<ValueType> &result);                        

class SkipListEpochManager{
  public:
    class EpochNode;
    /* 
     * return the current EpochNode
     * need to add the reference count of current EpochNode
     */
    EpochNode* JoinEpoch();

    /* 
     * leaves current EpochNode
     * should maintain atomicity when counting the reference
     */
    void LeaveEpoch(EpochNode* node);

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
  class NodeManager{
    public:
      SkipListNode *GetSkipListNode();
      void ReturnSkipListNode(SkipListNode *node);

      int NodeNum;
  };
};
//maintains Epoch
//has a inside linked list in which every node represents an epoch

}  // namespace index
}  // namespace peloton
