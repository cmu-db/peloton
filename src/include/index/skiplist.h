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
#include <vector>
#include <functional>

#include "common/logger.h"
#include "index/index.h"

#define MAX_THREAD_COUNT ((int)0x7FFFFFFF)

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
      class Epoch;
      // TODO: Add your declarations here
    public:
      // KeyType-ValueType pair
      using KeyValuePair = std::pair<KeyType, ValueType>;
      using EpochNode = typename Epoch::EpochNode;

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
        // initialize memory footprint
        this->epoch = new Epoch();
        // initialize head node
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
        LOG_DEBUG("SkipList Destructor: Free nodes");
        delete epoch;
        Node *current = head.load(), *next;
        next = current->next[0].load();
	int node_count = 0;
        while (get_node_address(next) != nullptr) {
          if (!is_deleted_node(next)) { // current node is not deleted
	    node_count++;
            delete(current);
	    LOG_WARN("freed 1 node, size is %zu", sizeof(Node));
            current = next;
            next = current->next[0].load();
          } else { // current node is marked as deleted, let GC thread to delete that node
	    LOG_WARN("Node marked as deleted, possibly lost!");
            current = get_node_address(next);
            next = current->next[0].load();
          }
        }

        if (!is_deleted_node(next)) { // current node is not deleted
          delete(current);
	  node_count++;
	  LOG_WARN("freed 1 node, size is %zu", sizeof(Node));
        }

        // TODO: free all nodes
       
        LOG_WARN("Freed %d nodes", node_count);
        return;
      }

    public:

      /*
       * Insert - insert a key-value pair into index
       *
       * assume value is unique under a key
       * */
      bool Insert(const KeyType &key, const ValueType &value) {
        LOG_DEBUG("Insert join epoch");
        EpochNode *current_epoch = epoch->JoinEpoch();

        ValueNode *val_ptr = new ValueNode(value);
        epoch->memory_footprint.fetch_add(sizeof(ValueNode));
	epoch->memory_value.fetch_add(sizeof(ValueNode));
	LOG_WARN("created value node, total size for value node is %zu", (size_t)epoch->memory_value);
        int node_level = get_rand_level();
        Node *new_node = new Node(node_level, key, nullptr);
        epoch->memory_footprint.fetch_add(sizeof(Node));
        LOG_DEBUG("%s: node_level=%d", __func__, node_level);

        Node **preds = nullptr; // predecessors
        Node **succs = nullptr; // successors
        int level = 0;

        Node *old_head = head.load();
        if (node_level > old_head->level) {
          // need a new head with node_level
          LOG_DEBUG("%s: need to install a new head", __func__);
          Node *new_head = new Node(node_level, KeyType(), nullptr);
          epoch->memory_footprint.fetch_add(sizeof(Node));
          for (int i=0; i<old_head->level; i++) {
            new_head->next[i] = old_head->next[i].load();
          }
          while (true) {
            old_head = head.load();
            if (old_head->level >= node_level) {
              // other thread update head before current thread install new_head
              // we do not have to install new_head
              // TODO: GC for new_head, because it won't be installed
              epoch->AddGarbageNode(new_head);
              break;
            }
            if (head.compare_exchange_strong(old_head, new_head)) {
              // TODO: GC for old_head if new_head is installed successfully
              epoch->AddGarbageNode(old_head);
              break;
            }
          }
        }

        retry:
        if (preds != nullptr) {
          delete[](preds);
        }
        if (succs != nullptr) {
          delete[](succs);
        }
        Search(key, preds, succs, level);

        if (succs[0]!= nullptr && KeyCmpEqual(succs[0]->key, key)) {
          // key already exist
          ValueNode *old_val_ptr = succs[0]->val_ptr.load();
          if (old_val_ptr == nullptr) { // current succesor is deleted
            delete_node(succs[0]);
            goto retry;
          }
          if (unique_key) { // insert fail
            epoch->AddGarbageValueNode(val_ptr);
            epoch->AddGarbageNode(new_node);
            // TODO: GC ValueNode
            LOG_DEBUG("Insert leave epoch");
            epoch->LeaveEpoch(current_epoch);
            delete[](preds);
            delete[](succs);
            return false;
          } else { // duplicate key allowed, value should be unique under one key
            while (true) {
              old_val_ptr = succs[0]->val_ptr.load();
              if (old_val_ptr == nullptr) { // current succesor is deleted
                delete_node(succs[0]);
                goto retry;
              }
              // current successor exist
              old_val_ptr = get_tail_value_with_diff(old_val_ptr, val_ptr->val);
              if (old_val_ptr == nullptr) { // there is an exist value in the value list equal to val_ptr->val
                epoch->AddGarbageValueNode(val_ptr);
                epoch->AddGarbageNode(new_node);
                // TODO: GC ValueNode
                LOG_DEBUG("Insert leave epoch");
                epoch->LeaveEpoch(current_epoch);
                delete[](preds);
                delete[](succs);
                return false;
              }
              ValueNode *val_next = old_val_ptr->next.load();
              if (!is_deleted_val_ptr(val_next)) { // tail is not deleted
                if (old_val_ptr->next.compare_exchange_strong(val_next, val_ptr)) {
                  // install current value at the end of value linked list
                  // TODO: GC Node
                  epoch->AddGarbageNode(new_node);
                  LOG_DEBUG("Insert leave epoch");
                  epoch->LeaveEpoch(current_epoch);
                  delete[](preds);
                  delete[](succs);
                  return true;
                }
                // install fail, just run the loop again, retry insertion in the value list
              }
            }
          }
        }

        // key not exist
        new_node->val_ptr = val_ptr;
        Node *left, *right, *new_next;
        for (int i=0; i<node_level; i++) {
//      new_next = new_node->next[i].load();
//      new_node->next[i].compare_exchange_strong(new_next, succs[i]);
          new_node->next[i] = succs[i];
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
              new_next = get_node_address(new_next);
              if (!(new_node->next[i].compare_exchange_strong(new_next, right)))
                break;
            }
            // successor may have same key because of
            // concurrent deletion of current node and concurrent insertion of the same key
            if (right!= nullptr && KeyCmpEqual(right->key, key))
              right = get_node_address(right->next[i].load());
            if (left->next[i].compare_exchange_strong(right, new_node))
              break;
            // CAS fail
            LOG_DEBUG("%s: CAS fail, search again", __func__);

            delete[](preds);
            delete[](succs);
            Search(key, preds, succs, level);
          }
        }
        delete[](preds);
        delete[](succs);
        epoch->LeaveEpoch(current_epoch);
        LOG_DEBUG("Insert leave epoch");
        return true;
      }


      /*
       * ConditionalInsert() - Insert a key-value pair only if a given
       *                       predicate fails for all values with a key
       *
       * If return true then the value has been inserted
       * If return false then the value is not inserted. The reason could be
       * predicates returning true for one of the values of a given key
       * or because the value is already in the index
       *
       * NOTE: We first test the predicate, and then test for duplicated values
       * so predicate test result is always available
       */
      bool ConditionalInsert(const KeyType &key, const ValueType &value,
                             std::function<bool(const void *)> predicate,
                             bool *predicate_satisfied){
        LOG_DEBUG("Cond Insert join epoch");
        EpochNode *current_epoch = epoch->JoinEpoch();
        ValueNode *val_ptr = new ValueNode(value);
        epoch->memory_footprint.fetch_add(sizeof(ValueNode));
        int node_level = get_rand_level();
        Node *new_node = new Node(node_level, key, nullptr);
        epoch->memory_footprint.fetch_add(sizeof(Node));
        LOG_DEBUG("%s: node_level=%d", __func__, node_level);

        Node **preds = nullptr; // predecessors
        Node **succs = nullptr; // successors
        int level = 0;

        Node *old_head = head.load();
        if (node_level > old_head->level) {
          // need a new head with node_level
          LOG_DEBUG("%s: need to install a new head", __func__);
          Node *new_head = new Node(node_level, KeyType(), nullptr);
          epoch->memory_footprint.fetch_add(sizeof(Node));
          for (int i=0; i<old_head->level; i++) {
            new_head->next[i] = old_head->next[i].load();
          }
          while (true) {
            old_head = head.load();
            if (old_head->level >= node_level) {
              // other thread update head before current thread install new_head
              // we do not have to install new_head
              epoch->AddGarbageNode(new_head);
              // TODO: GC for new_head, because it won't be installed
              break;
            }
            if (head.compare_exchange_strong(old_head, new_head)) {
              // TODO: GC for old_head if new_head is installed successfully
              epoch->AddGarbageNode(old_head);
              break;
            }
          }
        }

        retry:
        if (preds != nullptr) {
          delete[](preds);
        }
        if (succs != nullptr) {
          delete[](succs);
        }
        Search(key, preds, succs, level);

        if (succs[0]!= nullptr && KeyCmpEqual(succs[0]->key, key)) {
          // key already exist
          ValueNode *old_val_ptr = succs[0]->val_ptr.load();
          if (old_val_ptr == nullptr) { // current succesor is deleted
            delete_node(succs[0]);
            goto retry;
          }
          if (unique_key) {
            epoch->AddGarbageValueNode(val_ptr);
            epoch->AddGarbageNode(new_node);
            // TODO: GC ValueNode
            LOG_DEBUG("Cond Insert leave epoch");
            epoch->LeaveEpoch(current_epoch);
            delete[](preds);
            delete[](succs);
            return false;
          } else { // duplicate key allowed, value should be unique under one key
            while (true) {
              old_val_ptr = succs[0]->val_ptr.load();
              if (old_val_ptr == nullptr) { // current succesor is deleted
                delete_node(succs[0]);
                goto retry;
              }
              // current successor exist
              old_val_ptr = get_tail_value_without_predicate(old_val_ptr, val_ptr->val, predicate, predicate_satisfied);
              if (old_val_ptr == nullptr) { // there is an exist value in the value list equal to val_ptr->val
                epoch->AddGarbageValueNode(val_ptr);
                epoch->AddGarbageNode(new_node);
                // TODO: GC ValueNode
                LOG_DEBUG("Cond Insert leave epoch");
                epoch->LeaveEpoch(current_epoch);
                delete[](preds);
                delete[](succs);
                return false;
              }
              ValueNode *val_next = old_val_ptr->next.load();
              if (!is_deleted_val_ptr(val_next)) { // tail is not deleted
                if (old_val_ptr->next.compare_exchange_strong(val_next, val_ptr)) {
                  // install current value at the end of value linked list
                  epoch->AddGarbageNode(new_node);
                  LOG_DEBUG("Cond Insert leave epoch");
                  epoch->LeaveEpoch(current_epoch);
                  delete[](preds);
                  delete[](succs);
                  return true;
                }
                // install fail, just run the loop again, retry insertion in the value list
              }
            }
          }
        }

        // key not exist
        new_node->val_ptr = val_ptr;
        Node *left, *right, *new_next;
        for (int i=0; i<node_level; i++) {
          new_node->next[i] = succs[i];
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
              new_next = get_node_address(new_next);
              if (!(new_node->next[i].compare_exchange_strong(new_next, right)))
                break;
            }
            // successor may have same key because of
            // concurrent deletion of current node and concurrent insertion of the same key
            if (right!= nullptr && KeyCmpEqual(right->key, key))
              right = get_node_address(right->next[i].load());
            if (left->next[i].compare_exchange_strong(right, new_node))
              break;
            // CAS fail
            LOG_DEBUG("%s: CAS fail, search again", __func__);
            delete[](preds);
            delete[](succs);
            Search(key, preds, succs, level);
          }
        }
        delete[](preds);
        delete[](succs);
        LOG_DEBUG("Cond Insert leave epoch");
        epoch->LeaveEpoch(current_epoch);
        return true;
      }

      /*
       * Delete - delete key-value pair if the pair exist
       * */
      bool Delete(const KeyType &key, const ValueType &value) {
//    LOG_DEBUG("%s: called", __func__);
        LOG_DEBUG("delete join epoch");
        EpochNode *current_epoch = epoch->JoinEpoch();
        Node **preds = nullptr; // predecessors
        Node **succs = nullptr; // successors
        int level = 0;
        Search(key, preds, succs, level);
        if (succs[0] == nullptr || !KeyCmpEqual(succs[0]->key, key)) { // key not exist
          delete[](preds);
          delete[](succs);
          LOG_DEBUG("delete leave epoch");
          epoch->LeaveEpoch(current_epoch);
          return false;
        }
        ValueNode *old_val_ptr = succs[0]->val_ptr.load();
        // WARNING: we have to explicitly declare a variable of nullptr for CAS operation
        // otherwise, compare_exchange_strong CAN NOT compile with nullptr!
        ValueNode *empty_val_ptr = nullptr;
        if (unique_key) { // key is unique
          if (old_val_ptr != nullptr && !is_deleted_val_ptr(old_val_ptr)
              &&ValueCmpEqual(old_val_ptr->val, value)) {
            // old_val_ptr should exist, and it should not be marked as deleted
            // WARNING: we should guarantee we only use pointer with address WITHOUT a delete bit
            // when we want to use member variables of that pointer
            // value match
            while (!(succs[0]->val_ptr.compare_exchange_strong(old_val_ptr, empty_val_ptr))) {
              old_val_ptr = succs[0]->val_ptr.load();
            }
            // delete Node
            delete_node(succs[0]);
	    epoch->AddGarbageValueNode(old_val_ptr);
          } else { // value not match or the key is already deleted
            delete[](preds);
            delete[](succs);
            LOG_DEBUG("delete leave epoch");
            epoch->LeaveEpoch(current_epoch);
            return false;
          }
        } else { // key is duplicate
          // delete targe value in value linked list by CAS, need careness!
          // There may be duplicate value in the linked list
          bool find_delete = delete_value(old_val_ptr, value);
          if (find_delete) { // find value and logically delete it
            ValueNode* new_val_ptr = traverse_value_list(old_val_ptr);
            if (new_val_ptr != old_val_ptr) {
              // update val_ptr to new_val_ptr
              while (!(succs[0]->val_ptr.compare_exchange_strong(old_val_ptr, new_val_ptr))) {
                old_val_ptr = succs[0]->val_ptr.load();
              }
              if (new_val_ptr == nullptr) { // all values are deleted, the key Node should be deleted too
//            LOG_DEBUG("%s: val all deleted, delete key", __func__);
                delete_node(succs[0]);
              }
            }
          } else { // value is not found
            delete[](preds);
            delete[](succs);
            LOG_DEBUG("delete leave epoch");
            epoch->LeaveEpoch(current_epoch);
            return false;
          }
        }
        delete[](preds);
        delete[](succs);
        // search is intended to just jump all logical deleted nodes
        Search(key, preds, succs, level);
        delete[](preds);
        delete[](succs);
        LOG_DEBUG("delete leave epoch");
        epoch->LeaveEpoch(current_epoch);
        return true;
      }

      /*
       * GetValue - get values with key and store them in value_list
       * */
      void GetValue(const KeyType &key, std::vector<ValueType> &value_list) {
//    LOG_DEBUG("%s: called", __func__);
        LOG_DEBUG("getv join epoch");
        EpochNode *current_epoch = epoch->JoinEpoch();
        Node **preds, **succs;
        int level = 0;
        Search(key, preds, succs, level);
//    LOG_DEBUG("%s: level=%d", __func__, level);
        if (succs[0]!= nullptr && KeyCmpEqual(succs[0]->key, key)) {
//      LOG_DEBUG("%s: find key", __func__);
          ValueNode *val_ptr = succs[0]->val_ptr.load(), *val_next;
          while (val_ptr != nullptr) {
            val_next = val_ptr->next.load();
            if (!is_deleted_val_ptr(val_next)) {
              value_list.push_back(val_ptr->val);
            }
            val_ptr = get_val_address(val_next);
          }
        } else {
          LOG_DEBUG("%s: NOT find key", __func__);
        }
        delete[](preds);
        delete[](succs);
        LOG_DEBUG("getv leave epoch");
        epoch->LeaveEpoch(current_epoch);
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
          if (left_next != nullptr && is_deleted_node(left_next)) {
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
              if (right_next == nullptr || !is_deleted_node(right_next))
                break;
              right = get_node_address(right_next);
            }
            // key <= right->key || right == nullptr means right is at the end of the list
            if (right == nullptr || !KeyCmpLess(right->key, key))
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


      /*
       * class ForwardIterator - Iterator that supports forward iteration of skip list elements
       */
      class ForwardIterator {
      private:
        Node *node_ptr;
        ValueNode *val_ptr;
        SkipList* parent;

      public:
        ForwardIterator(Node *node_ptr, ValueNode *val_ptr, SkipList *parent) {
          LOG_DEBUG("fwd join epoch");
          EpochNode *current_epoch = parent->epoch->JoinEpoch();
          this->node_ptr = node_ptr;
          this->val_ptr = val_ptr;
          this->parent = parent;
          LOG_DEBUG("fwd leave epoch");
          parent->epoch->LeaveEpoch(current_epoch);
        }

        /*
         * IsEnd() - Whether the current iterator caches the last page and
         *           the iterator points to the last element
         */
        bool IsEnd() const {
          return node_ptr == nullptr || val_ptr == nullptr;
        }

        /*
         * operator->() - Returns the value pointer pointed to by this iterator
         *
         * Note that this function returns a contsnat pointer which can be used
         * to access members of the value, but cannot modify
         */
//    inline const KeyValuePair *operator->() { return &KeyValuePair(node_ptr->key, val_ptr->val); }

        inline const KeyType getKey() { return node_ptr->key; }

        inline const ValueType getValue() { return val_ptr->val; }

        ~ForwardIterator() {
          return;
        }

        /*
         * Prefix operator++ - Move the iterator ahead and return the new iterator
         *
         * This operator moves iterator first, and then return the operator itself
         * as value of expression
         *
         * If the iterator is end() iterator then we do nothing
         */
        inline ForwardIterator &operator++() {
          if (IsEnd()) {
            return *this;
          }
          LOG_DEBUG("fwd move ahead join epoch");
          EpochNode *current_epoch = parent->epoch->JoinEpoch();
          MoveAheadByOne();
          LOG_DEBUG("fwd move ahead leave epoch");
          parent->epoch->LeaveEpoch(current_epoch);
          return *this;
        }

        /*
         * Postfix operator++ - Move the iterator ahead, and return the old one
         *
         * For end() iterator we do not do anything but return the same iterator
         */
        inline ForwardIterator operator++(int) {
          if (IsEnd()) {
            return *this;
          }

          // Make a copy of the current one before advancing
          // This will increase ref count temporarily, but it is always consistent
          ForwardIterator temp = *this;

          MoveAheadByOne();

          return temp;
        }

        /*
         * MoveAheadByOne() - Move the iterator ahead by one
         *
         * The caller is responsible for checking whether the iterator has reached
         * its end. If iterator has reached end then assertion fails.
         */
        inline void MoveAheadByOne() {
          // Could not do this on an empty iterator
          PL_ASSERT(node_ptr != nullptr);
          PL_ASSERT(val_ptr != nullptr);

          val_ptr = get_first_exist_value(val_ptr->next.load());
          if (val_ptr != nullptr) // value chain is not exhausted yet
            return;
          // value chain is exhausted, find next exist node's key and exist value of that key
          node_ptr = get_node_address(node_ptr->next[0].load());
          while (node_ptr != nullptr) {
            if (is_deleted_node(node_ptr->next[0].load())) { // node is marked as deleted
              node_ptr = get_node_address(node_ptr->next[0].load());
            } else { // node exist
              val_ptr = get_first_exist_value(node_ptr->val_ptr.load());
              if (val_ptr != nullptr) {
                break;
              } else {
                node_ptr = get_node_address(node_ptr->next[0].load());
              }
            }
          }
          return;
        }
      };  // ForwardIterator

      /*
      * Begin() - Return an iterator pointing the first element in the skip list
      */
      ForwardIterator Begin() {
        LOG_DEBUG("fwd begin join epoch");
        EpochNode *current_epoch = epoch->JoinEpoch();
        Node *node_ptr = get_node_address(head.load()->next[0]);
        while (node_ptr != nullptr && is_deleted_node(node_ptr->next[0].load())) {
          node_ptr = get_node_address(head.load()->next[0]);
        }
        ValueNode *val_ptr = nullptr;
        if (node_ptr != nullptr) {
          val_ptr = get_first_exist_value(node_ptr->val_ptr.load());
        }
        LOG_DEBUG("fwd begin leave epoch");
        epoch->LeaveEpoch(current_epoch);
        return ForwardIterator(node_ptr, val_ptr, this);
      }

      /*
       * Begin() - Return an iterator using a given key
       *
       * The iterator returned will points to a data item whose key is greater than
       * or equal to the given start key. If such key does not exist then it will
       * be the smallest key that is greater than start_key
       */
      ForwardIterator Begin(const KeyType &start_key) {
        LOG_DEBUG("fwd begin join epoch");
        EpochNode *current_epoch = epoch->JoinEpoch();
        Node **preds = nullptr; // predecessors
        Node **succs = nullptr; // successors
        int level = 0;

        Search(start_key, preds, succs, level);

        Node *node_ptr = succs[0];
        ValueNode *val_ptr = nullptr;

        if (node_ptr != nullptr) {
          val_ptr = node_ptr->val_ptr.load();
          val_ptr = get_first_exist_value(val_ptr);
        }
        delete[](preds);
        delete[](succs);
        LOG_DEBUG("fwd begin leave epoch");
        epoch->LeaveEpoch(current_epoch);
        return ForwardIterator(node_ptr, val_ptr, this);
      }

      inline bool KeyCmpLessEqual(const KeyType &key1, const KeyType &key2) const {
        return key_cmp_obj(key1, key2) || key_eq_obj(key1, key2);
      }

      void PerformGC(){
        epoch->CreateEpoch();
        epoch->ClearOldEpoch();
      }

      size_t GetMemoryFootprint(){
        return epoch->memory_footprint;
      }

    private:
      // xingyuj1
      // value node is used to build linked list of ValueType to use CAS in SkipList node
      class ValueNode{
      public:
        ValueType val;
        std::atomic<ValueNode*> next; // support concurrent delete in duplicate key mode
        ValueNode(ValueType val){ this->val = val; next = nullptr;}
        ValueNode(ValueType val, ValueNode *next) {this->val = val; this->next = next;}
        ~ValueNode(){};
      };

      // define node in SkipList
      class Node {
      public:
        int level;
        std::atomic<Node *> *next;
        KeyType key;
        std::atomic<ValueNode*> val_ptr; // linked list to support duplicate key

        Node(int level, KeyType key, ValueNode *val_ptr) {
//      LOG_DEBUG("Skip List New Node construct");
          this->level = level;
          this->key = key;
          next = new std::atomic<Node *>[level];
          for (int i=0; i<level; i++)
            next[i] = nullptr;
          this->val_ptr = val_ptr;
//      Node *empty_node = nullptr;
//      for (int i=0; i<level; i++) {
//        Node *tmp = next[i].load();
//        while(!next[i].compare_exchange_strong(tmp, empty_node)){
//          LOG_DEBUG("%s: next[%d] CAS fail", __func__, i);
//        }
//      }
//      ValueNode* old = this->val_ptr.load();
//      while(!this->val_ptr.compare_exchange_strong(old, val_ptr)){
//        LOG_DEBUG("%s, val_ptr CAS fail", __func__);
//      }
        }

        ~Node(){
          delete[](next);
          ValueNode *t = val_ptr, *p;
	  LOG_WARN("try to delete a node");
          while (t != nullptr) {
            p = t->next.load();
            if (!is_deleted_val_ptr(p)) { // t is not deleted
	      LOG_WARN("deleted value node, size is %zu", sizeof(ValueNode));
              delete(t);
              t = p;
            } else { // t is deleted, pass it
	      LOG_WARN("valuenode marked as deleted, possibly lost!");
              t = get_val_address(p);
            }
          }
          return;
        }
      };

      Epoch *epoch;

      class Epoch{
      public:

        struct GarbageNode{
          const Node *node;
          GarbageNode *next;
        };
        struct GarbageValueNode{
          const ValueNode *node;
          GarbageValueNode *next;
        };

        struct EpochNode{
          std::atomic<int> thread_count;
          std::atomic<size_t> memory_freed;
	  std::atomic<size_t> memory_freed_value;
          std::atomic<GarbageNode *> garbage_list;
          std::atomic<GarbageValueNode *> garbage_value_list;
          EpochNode *next;
        };

        std::atomic<size_t> memory_footprint;
	std::atomic<size_t> memory_value;
        EpochNode *current;
        EpochNode *head;
        std::atomic<bool> exited;

        Epoch() {
	  memory_value = 0;
          memory_footprint = 0;
          current = new EpochNode{};
          head = current;
          current->memory_freed = 0;
          current->thread_count = 0;
          current->garbage_list = nullptr;
          current->garbage_value_list = nullptr;
          current->next = nullptr;
          exited.store(false);
          return;
        }

        ~Epoch() {
          LOG_DEBUG("Epoch Destructor: Free nodes");
          exited.store(true);
          current = nullptr;
          ClearOldEpoch();
          if (head != nullptr) {
            LOG_DEBUG("ERROR: After cleanup there is still epoch left");
            LOG_DEBUG("%s", peloton::GETINFO_THICK_LINE.c_str());
            LOG_DEBUG("DUMP");

            for (EpochNode *epoch_node = head; epoch_node != nullptr;
                 epoch_node = epoch_node->next) {
              LOG_DEBUG("Active thread count: %d",
                        epoch_node->thread_count.load());
              epoch_node->thread_count = 0;
            }

            LOG_DEBUG("RETRY CLEANING...");
            ClearOldEpoch();
          }
          PL_ASSERT(head == nullptr);

          return;
        }

        void CreateEpoch() {
          LOG_DEBUG("Epoch: created new epoch");
          EpochNode *new_epoch = new EpochNode{};
          new_epoch->thread_count = 0;
	  new_epoch->memory_freed = 0;
	  new_epoch->memory_freed_value = 0;
          new_epoch->garbage_list = nullptr;
          new_epoch->garbage_value_list = nullptr;
          new_epoch->next = nullptr;
          current->next = new_epoch;
          current = new_epoch;
          return;
        }

        void AddGarbageNode(const Node* node){
          EpochNode *epoch = current;
          GarbageNode *new_garbage = new GarbageNode();
          new_garbage->node = node;
          new_garbage->next = epoch->garbage_list.load();
          while (!epoch->garbage_list.compare_exchange_strong
                  (new_garbage->next, new_garbage)){
            LOG_TRACE("Add garbage node CAS failed. Retry");
          }
          epoch->memory_freed.fetch_add(sizeof(Node));
	  LOG_WARN("Add garbage node, total garbage is %zu", (size_t)epoch->memory_freed);
          return;
        }

        void AddGarbageValueNode(const ValueNode* vnode){
	  LOG_WARN("add value node for GC");
          EpochNode *epoch = current;
          GarbageValueNode *new_garbage = new GarbageValueNode();
          new_garbage->node = vnode;
          new_garbage->next = epoch->garbage_value_list.load();
          while (!epoch->garbage_value_list.compare_exchange_strong
                  (new_garbage->next, new_garbage)){
            LOG_TRACE("Add garbage value node CAS failed. Retry");
          }
          epoch->memory_freed.fetch_add(sizeof(ValueNode));
	  epoch->memory_freed_value.fetch_add(sizeof(ValueNode));
	  LOG_WARN("Add garbage value node, total garbage is %zu, total value garbage is %zu", (size_t)epoch->memory_freed, (size_t)epoch->memory_freed_value);
          return;
        }

        EpochNode* JoinEpoch(){
          int64_t count = -1;
          EpochNode *epoch = nullptr;
          while (count < 0){
            epoch = current;
            epoch->thread_count.fetch_add(1);
            count = epoch->thread_count;
            if (count < 0){
              epoch->thread_count.fetch_sub(1);
            }
          }
          LOG_DEBUG("join epoch, count = %ld", (long)count);
          return epoch;
        }

        void LeaveEpoch(EpochNode *epoch){
          //LOG_DEBUG("leave epoch");
          epoch->thread_count.fetch_sub(1);
          return;
        }

        size_t ClearOldEpoch(){
          LOG_WARN("Start to clear epoch");
          size_t total_memory_freed = 0;
	  size_t total_memory_freed_value = 0;
          int retry = 0;
	  LOG_WARN("Before gc, total memory is %zu, total memory for valuenode is %zu", (size_t)memory_footprint, (size_t)memory_value);
          while(true){

            if (head == current){
              LOG_WARN("Current epoch is head epoch. Do not clean");
              break;
            }

            int count = head->thread_count.load();
            LOG_WARN("Count of thread is: %d", count);
            PL_ASSERT(count >= 0);
            if (count != 0) {
              if (retry < 100){
                retry++;
                continue;
              }
              else{
                LOG_WARN("Head epoch is not empty. Return");
                break;
              }
            }

            head->thread_count.fetch_sub(MAX_THREAD_COUNT);
            if (head->thread_count > 0) {
              LOG_TRACE("Some thread sneaks in after we have decided to clean. Return");
              head->thread_count.fetch_add(MAX_THREAD_COUNT);
              break;
            }

            const GarbageNode *next_garbage_node = nullptr;
            const GarbageValueNode *next_garbage_value_node = nullptr;

	    for (const GarbageValueNode *current_node = head->garbage_value_list.load(); current_node != nullptr; current_node = next_garbage_value_node) {
              next_garbage_value_node = current_node->next;
              delete current_node->node;
              delete current_node;
            }

            for (const GarbageNode *current_node = head->garbage_list.load(); current_node != nullptr; current_node = next_garbage_node) {
              next_garbage_node = current_node->next;
              delete current_node->node;
              delete current_node;
            }

            total_memory_freed += head->memory_freed;
	    total_memory_freed_value += head->memory_freed_value;
            EpochNode *next_epoch = head->next;
            delete head;
            head = next_epoch;

          }
          LOG_WARN("Finish clearing epoch, memory freed: %zu, memory freed for value nodes: %zu", total_memory_freed, total_memory_freed_value);
          memory_footprint.fetch_sub(total_memory_freed);
	  memory_value.fetch_sub(total_memory_freed_value);
	  LOG_WARN("After gc, total memory is %zu, total memory for valuenode is %zu", (size_t)memory_footprint, (size_t)memory_value);
          LOG_WARN("memory in use: %zu", size_t(memory_footprint));
          return total_memory_freed;
        }

      };


      // head is head of SkipList
      std::atomic<Node*> head;
      // unique key mark whether to use duplicate key
      bool unique_key;
      static const long long ADDRESS_MASK = ~0x1;
      static const long long DELETE_MASK = 0x1;

      /*
       * delete_node - mark last bit in address in each level, logical delete node
       * */
      void delete_node(Node *p) {
        Node *next;
        for (int i=p->level-1; i>=0; i--) {
          while (true) {
            next = p->next[i].load();
            if (is_deleted_node(next))
              break;
            if (p->next[i].compare_exchange_strong(next, delete_node_address(next))) {
              // mark delete bit successfully
              if (i == 0) {
                // TODO: GC this node
                epoch->AddGarbageNode(p);
              }
              break;
            }
          };
        }
      }

      /*
       * is_deleted_node - judge whether the next node is deleted from marked bit in pointer
       * */
      static inline bool is_deleted_node(Node* p) {
        long long t = reinterpret_cast<long long>(p) & DELETE_MASK;
        return (t == DELETE_MASK);
      }

      /*
       * get_node_address - get address from pointer with a marked bit
       * */
      static inline Node* get_node_address(Node* p) {
        long long t = reinterpret_cast<long long>(p) & ADDRESS_MASK;
        return reinterpret_cast<Node*>(t);
      }

      /*
       * delete_node_address - mark the last bit of pointer, as logical delete address
       * */
      inline Node* delete_node_address(Node* p) {
        long long t = reinterpret_cast<long long>(p) | DELETE_MASK;
        return reinterpret_cast<Node*>(t);
      }

      /*
       * get_first_exist_value - return the first exist value node start from val_ptr
       * */
      static inline ValueNode* get_first_exist_value(ValueNode *val_ptr) {
        while (val_ptr != nullptr && is_deleted_val_ptr(val_ptr->next.load())) {
          val_ptr = get_val_address(val_ptr->next.load());
        }
        return val_ptr;
      }

      /*
       * get_tail_value_with_diff - return tail value in the value list, where do not contain "val"
       *                            val_ptr should not be nullptr
       *                            return nullptr if meet existing node with same value with value
       * */
      inline ValueNode* get_tail_value_with_diff(ValueNode *val_ptr, const ValueType& val) {
        ValueNode *val_next = val_ptr->next.load();
        while (get_val_address(val_next) != nullptr) {
          if (!is_deleted_val_ptr(val_next) && ValueCmpEqual(val_ptr->val, val)) {
            return nullptr;
          }
          val_ptr = get_val_address(val_next);
          val_next = val_ptr->next.load();
        }
        if (!is_deleted_val_ptr(val_next) && ValueCmpEqual(val_ptr->val, val)) {
          return nullptr;
        }
        return val_ptr;
      }


      /*
       * get_tail_value_without_predicate - return tail value in the value list, where do not satisfy either
       *                                    predicate function or value equation
       *                                    return nullptr if either of mentioned conditions is satisfied
       * */
      inline ValueNode* get_tail_value_without_predicate(ValueNode *val_ptr, const ValueType& val,
                                                         std::function<bool(const void *)> predicate,
                                                         bool *predicate_satisfied) {
        ValueNode *val_next = val_ptr->next.load();
        while (get_val_address(val_next) != nullptr) {
          if (!is_deleted_val_ptr(val_next)) {
            if (predicate(val_ptr->val)) {
              *predicate_satisfied = true;
              return nullptr;
            }
            if (ValueCmpEqual(val_ptr->val, val)) {
              return nullptr;
            }
          }
          val_ptr = get_val_address(val_next);
          val_next = val_ptr->next.load();
        }
        if (!is_deleted_val_ptr(val_next)) {
          if (predicate(val_ptr->val)) {
            *predicate_satisfied = true;
            return nullptr;
          }
          if (ValueCmpEqual(val_ptr->val, val)) {
            return nullptr;
          }
        }
        return val_ptr;
      }

      /*
       * traverse_value_list - traverse ValueNode linked list, connect exist ValueNodes by changing next pointers
       * return head ValueNode pointer
       * */
      ValueNode* traverse_value_list(ValueNode *val_ptr) {
        // find first exist node or just get nullptr
        ValueNode* head_val_ptr = get_first_exist_value(val_ptr);
        ValueNode *left, *right, *old_next;

        left = head_val_ptr;
        while (left != nullptr) {
          old_next = left->next.load();
          right = get_first_exist_value(old_next);
          if (old_next != right) { // left and right are not adjancent
            // update left->next
            while (!left->next.compare_exchange_strong(old_next, right)) {
              old_next = left->next.load();
            }
          }
          left = right;
        }

        return head_val_ptr;
      }
      /*
       * delete_value - logically delete all ValueNodes whose value equals to value
       *                by marking delete bit in its next pointer
       * return true if delete operation on a value node happens
       * return false if no such delete operation
       * */
      bool delete_value(ValueNode *val_ptr, const ValueType & value) {
        bool find_and_delete_val = false;
        ValueNode *v = get_val_address(val_ptr), *v_next;
        while (v != nullptr) {
          if (ValueCmpEqual(v->val, value)) {
            // find matched value
            while (true) {
              v_next = v->next.load();
              if (is_deleted_val_ptr(v_next)) { // ValueNode is deleted by other thread, skip this node
                break;
              }
              if (v->next.compare_exchange_strong(v_next, delete_val_address(v_next))) {
                // mark delete bit
                find_and_delete_val = true;
                // TODO: GC for ValueNode
                epoch->AddGarbageValueNode(v);
                break;
              }
            }
          }
          // pointer go to next ValueNode
          v = get_val_address(v->next.load());
        }
        return find_and_delete_val;
      }
      /*
       * is_deleted_val_ptr - judge whether the next val_ptr is deleted from marked bit in pointer
       * */
      static inline bool is_deleted_val_ptr(ValueNode* p) {
        long long t = reinterpret_cast<long long>(p) & DELETE_MASK;
        return (t == DELETE_MASK);
      }

      /*
       * get_val_address - get address from pointer with a marked bit
       * */
      static inline ValueNode* get_val_address(ValueNode* p) {
        long long t = reinterpret_cast<long long>(p) & ADDRESS_MASK;
        return reinterpret_cast<ValueNode*>(t);
      }

      /*
       * delete_val_address - mark the last bit of pointer, as logical delete address
       * */
      inline ValueNode* delete_val_address(ValueNode* p) {
        long long t = reinterpret_cast<long long>(p) | DELETE_MASK;
        return reinterpret_cast<ValueNode*>(t);
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
