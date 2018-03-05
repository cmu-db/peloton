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
#include <ctime>
#include <cstdlib>

#include "common/logger.h"

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
private: // pre-declare private class names
  class Node;
  class ValueNode;
  // TODO: Add your declarations here
public:
  SkipList(bool start_gc_thread = true,
           bool unique_key = false,
           KeyComparator p_key_cmp_obj = KeyComparator{},
           KeyEqualityChecker p_key_eq_obj = KeyEqualityChecker{},
           ValueEqualityChecker p_value_eq_obj = ValueEqualityChecker{}):
      // Key comparator, equality checker and hasher
      key_cmp_obj{p_key_cmp_obj},
      key_eq_obj{p_key_eq_obj},
      // Value equality checker and hasher
      value_eq_obj{p_value_eq_obj}
  {
    LOG_DEBUG("Skip List Constructor called. ");
    // TODO: initialize head node
    srand(time(nullptr));
    int init_level = get_rand_level();
    LOG_DEBUG("init_level = %d", init_level);
    head = new Node(init_level, KeyType(), nullptr);
    this->unique_key = unique_key;

    if (start_gc_thread) {
      // TODO: garbage collection
      LOG_DEBUG("Garbage Collection");
    }

    LOG_DEBUG("ADDRESS_MASK = %llX", ADDRESS_MASK);
    LOG_DEBUG("Skip List Constructor complete ");
    return;
  }

  ~SkipList(){
    LOG_TRACE("Destructor: Free tree nodes");
    // TODO: free all nodes
    int node_count = 0;
    node_count++;
    LOG_DEBUG("Freed %d tree nodes", node_count);
    return;
  }

public:

  /*
   * Insert - insert a key-value pair into index
   * */
  bool Insert(const KeyType &key, const ValueType &value) {
    ValueNode *val_ptr = new ValueNode(value);
    int node_level = get_rand_level();
    LOG_DEBUG("%s: node_level=%d", __func__, node_level);

    Node **preds = nullptr; // predecessors
    Node **succs = nullptr; // successors
    int level = 0;

    // TODO: node_level > head_level, update head

retry:
    Search(key, preds, succs, level);

    if (KeyCmpEqual(succs[0]->key, key)) {
      // key already exist
      ValueNode *old_val_ptr = succs[0]->val_ptr.load();
      if (old_val_ptr == nullptr) { // current succesor is deleted
        delete_node(succs[0]);
        goto retry;
      }
      if (unique_key) {
        return false;
      } else { // duplicate key allowed
        val_ptr->next = old_val_ptr;
        // install current value into the value linked list
        while (!(succs[0]->val_ptr.compare_exchange_strong(old_val_ptr, val_ptr))) {
          old_val_ptr = succs[0]->val_ptr.load();
        }
        return true;
      }
    }

    // key not exist
    Node *new_node = new Node(node_level, key, val_ptr);
    Node *left, *right, *new_next;
    for (int i=0; i<node_level; i++) {
      new_next = new_node->next[i].load();
      new_node->next[i].compare_exchange_strong(new_next, succs[i]);
    }

    // install new_node at level 0, if fail, just retry
    if (!(preds[0]->next[0].compare_exchange_strong(succs[0], new_node)))
      goto retry;

    for (int i=1; i<node_level; i++) {
      while (true) {
        left = preds[i];
        right = succs[i];
        new_next = new_node->next[i].load();
        // update ptr if the next ptr of new node is deleted or outdated
        if (new_next != right) {
          new_next = get_address(new_next);
          if (!(new_node->next[i].compare_exchange_strong(new_next, right)))
            break;
        }
        // successor may have same key because of
        // concurrent deletion of current node and concurrent insertion of the same key
        if (KeyCmpEqual(right->key, key))
          right = get_address(right->next[i].load());
        if (left->next[i].compare_exchange_strong(right, new_node))
          break;
        // CAS fail
        LOG_DEBUG("%s: CAS fail, search again", __func__);
        Search(key, preds, succs, level);
      }
    }
    return true;
  }

  /*
   * Delete -
   * */
  bool Delete(const KeyType &key, const ValueType &value) {
    Node **preds = nullptr; // predecessors
    Node **succs = nullptr; // successors
    int level = 0;
    Search(key, preds, succs, level);
    if (!KeyCmpEqual(succs[0]->key, key)) {
      // key not exist
      return false;
    }
    ValueNode *old_val_ptr = succs[0]->val_ptr.load();
    // ATTENTION: we have to explicitly declare a variable of nullptr for CAS operation
    // otherwise, compare_exchange_strong CAN NOT compile with nullptr!
    ValueNode *empty_val_ptr = nullptr;
    if (unique_key) {
      // key is unique
      if (old_val_ptr != nullptr && ValueEqualityChecker(old_val_ptr->val, value)) {
        // value match
        while (!(succs[0]->val_ptr.compare_exchange_strong(old_val_ptr, empty_val_ptr))) {
          old_val_ptr = succs[0]->val_ptr.load();
        }
        return true;
      } else { // value not match
        return false;
      }
    } else {
      // key is duplicate
      // CAS operation to delete a target value is complicated and should be handled carefully
      // TODO: delete targe value in value linked list by CAS, need careness!
    }
    // search is intended to just jump all logical deleted nodes
    Search(key, preds, succs, level);
    return true;
  }

  /*
   * Search - get predecessors (p_key) and successors (s_key) for the key
   * all p_key < key
   * all s_key >= key, or s_key not exists
   * */
  void Search(const KeyType &key, Node** &left_list, Node** &right_list, int &level) {
    Node *left = head.load(), *left_next = nullptr, *right = nullptr, *right_next = nullptr;
    level = left->level;
    left_list = new Node*[level];
    right_list = new Node*[level];
retry:
    left = head.load();
    if (level < left->level) {
      delete[](left_list);
      delete[](right_list);
      level = left->level;
      left_list = new Node*[level];
      right_list = new Node*[level];
    }
    for (int i=level-1; i>=0; i--) {
      left_next = left->next[i].load();
      if (left_next != nullptr && is_deleted(left_next)) {
        goto retry;
      }
      // find exist node at this level
      for (right = left_next; ; right = right_next) {
        // skip deleted nodes
        while (true) {
          // right == nullptr means right is at the end of the list
          if (right == nullptr)
            break;
          right_next = right->next[i];
          if (right_next == nullptr || !is_deleted(right_next))
            break;
          right = get_address(right_next);
        }
        // key <= right->key || right == nullptr means right is at the end of the list
        if (right == nullptr || KeyCmpLess(key, right->key))
          break;
        // key > right->key
        left = right;
        left_next = right_next;
      }
      // ensure left and right nodes are adjacent
      if ((left_next != right) && !(left->next[i].compare_exchange_strong(left_next, right)))
        goto retry;
      left_list[i] = left;
      right_list[i] = right;
    }
    return;
  }

private:
  // xingyuj1
  // value node is used to build linked list of ValueType to use CAS in SkipList node
  class ValueNode{
  public:
    ValueType val;
    ValueNode* next;
    ValueNode(ValueType val){ this->val = val; next = nullptr;}
    ValueNode(ValueType val, ValueNode *next) {this->val = val; this->next = next;}
    ~ValueNode();
  };

  // define node in SkipList
  class Node {
  public:
    int level;
    std::atomic<Node *> *next;
    KeyType key;
    std::atomic<ValueNode*> val_ptr; // linked list to support duplicate key

    Node(int level, KeyType key, ValueNode *val_ptr) {
      LOG_DEBUG("Skip List New Node construct");
      this->level = level;
      this->key = key;
      next = new std::atomic<Node *>[level];
      for (int i=0; i<level; i++) {
        Node *tmp = next[i].load();
        while(!next[i].compare_exchange_strong(tmp, nullptr)){
          LOG_DEBUG("%s: next[%d] CAS fail", __func__, i);
        }
      }
      ValueNode* old = this->val_ptr.load();
      while(!this->val_ptr.compare_exchange_strong(old, val_ptr)){
        LOG_DEBUG("%s, val_ptr CAS fail", __func__);
      }
    }

    ~Node(){
      delete[](next);
      ValueNode *t = val_ptr, *p;
      while (t != nullptr) {
        p = t->next;
        delete(t);
        t = p;
      }
      return;
    }
  };

  // head is head of SkipList
  std::atomic<Node*> head;
  // unique key mark whether to use duplicate key
  bool unique_key;
  long long ADDRESS_MASK = ~0x1;
  long long DELETE_MASK = 0x1;

  /*
   * delete_node - mark last bit in address in each level, logical delete node
   * */
  void delete_node(Node *p) {
    Node *next;
    for (int i=p->level; i>=0; i--) {
      do {
        next = p->next[i].load();
        if (is_deleted(next))
          break;
      } while (!(p->next[i].compare_exchange_strong(next, delete_address(next))));
    }
  }

  /*
   * is_deleted - judge whether the next node is deleted from marked bit in pointer
   * */
  bool is_deleted(Node* p) {
    long long t = reinterpret_cast<long long>(p) & DELETE_MASK;
    return (t == DELETE_MASK);
  }

  /*
   * get_address - get address from pointer with a marked bit
   * */
  Node* get_address(Node* p) {
    long long t = reinterpret_cast<long long>(p) & ADDRESS_MASK;
    return reinterpret_cast<Node*>(t);
  }

  /*
   * delete_address - mark the last bit of pointer, as logical delete address
   * */
  Node* delete_address(Node* p) {
    long long t = reinterpret_cast<long long>(p) | DELETE_MASK;
    return reinterpret_cast<Node*>(t);
  }

  /*
   * get_rand_level - return random level for a new SkipList Node
   * */
  int get_rand_level() {
    int level = 0;
    float r = 0;
    do {
      r = static_cast<float>(rand()) / static_cast<float>(RAND_MAX);
      level++;
    } while (r > 0.5);
    return level;
  }

  // Key comparator
  const KeyComparator key_cmp_obj;

  // Raw key eq checker
  const KeyEqualityChecker key_eq_obj;

  // Check whether values are equivalent
  const ValueEqualityChecker value_eq_obj;

  /*
   * KeyCmpLess() - Compare two keys for "less than" relation
   *
   * If key1 < key2 return true
   * If not return false
   *
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
   * ValueCmpEqual() - Compares whether two values are equal
   */
  inline bool ValueCmpEqual(const ValueType &v1, const ValueType &v2) {
    return value_eq_obj(v1, v2);
  }

};

}  // namespace index
}  // namespace peloton
