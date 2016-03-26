//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// BWTree.h
//
// Identification: src/backend/index/BWTree.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once
#include <limits>
#include <vector>
#include <atomic>
#include <algorithm>
#include <cassert>
#include <stack>
#include <unordered_map>
#include <mutex>
#include <string>
#include <iostream>
#include <cassert>
#include "backend/storage/tuple.h"
#include "backend/common/logger.h"
#include "backend/index/index_key.h"

//#define BWTREE_DEBUG
//#define INTERACTIVE_DEBUG

#ifdef INTERACTIVE_DEBUG

#define idb_assert(cond)                                                \
  do {                                                                  \
    if (!(cond)) {                                                      \
      debug_stop_mutex.lock();                                          \
      printf("assert, %-24s, line %d \033[0m", __FUNCTION__, __LINE__); \
      idb.start();                                                      \
      debug_stop_mutex.unlock();                                        \
    }                                                                   \
  } while (0);

#define idb_assert_key(key, cond)                                       \
  do {                                                                  \
    if (!(cond)) {                                                      \
      debug_stop_mutex.lock();                                          \
      printf("assert, %-24s, line %d \033[0m", __FUNCTION__, __LINE__); \
      idb.key_list.push_back(&key);                                     \
      idb.start();                                                      \
      debug_stop_mutex.unlock();                                        \
    }                                                                   \
  } while (0);

#else

#define idb_assert(cond) \
  do {                   \
    assert(cond);        \
  } while (0);

#define idb_assert_key(key, cond) \
  do {                            \
    assert(cond);                 \
  } while (0);

#endif

#ifdef BWTREE_DEBUG

#define bwt_printf(fmt, ...)                              \
  do {                                                    \
    printf("%-24s(): " fmt, __FUNCTION__, ##__VA_ARGS__); \
    fflush(stdout);                                       \
  } while (0);

#define bwt_printf_red(fmt, ...)                                              \
  do {                                                                        \
    printf("\033[1;31m%-24s(): " fmt "\033[0m", __FUNCTION__, ##__VA_ARGS__); \
  } while (0);
#define bwt_printf_redb(fmt, ...)                                             \
  do {                                                                        \
    printf("\033[1;41m%-24s(): " fmt "\033[0m", __FUNCTION__, ##__VA_ARGS__); \
  } while (0);
#define bwt_printf_green(fmt, ...)                                            \
  do {                                                                        \
    printf("\033[1;32m%-24s(): " fmt "\033[0m", __FUNCTION__, ##__VA_ARGS__); \
  } while (0);
#define bwt_printf_greenb(fmt, ...)                                           \
  do {                                                                        \
    printf("\033[1;42m%-24s(): " fmt "\033[0m", __FUNCTION__, ##__VA_ARGS__); \
  } while (0);
#define bwt_printf_blue(fmt, ...)                                             \
  do {                                                                        \
    printf("\033[1;34m%-24s(): " fmt "\033[0m", __FUNCTION__, ##__VA_ARGS__); \
  } while (0);
#define bwt_printf_blueb(fmt, ...)                                            \
  do {                                                                        \
    printf("\033[1;44m%-24s(): " fmt "\033[0m", __FUNCTION__, ##__VA_ARGS__); \
  } while (0);
#else

#define bwt_printf(fmt, ...)   \
  do {                         \
    dummy(fmt, ##__VA_ARGS__); \
  } while (0);

#define bwt_printf_red(fmt, ...) \
  do {                           \
  } while (0);
#define bwt_printf_redb(fmt, ...) \
  do {                            \
  } while (0);
#define bwt_printf_green(fmt, ...) \
  do {                             \
  } while (0);
#define bwt_printf_greenb(fmt, ...) \
  do {                              \
  } while (0);
#define bwt_printf_blue(fmt, ...) \
  do {                            \
  } while (0);
#define bwt_printf_blueb(fmt, ...) \
  do {                             \
  } while (0);

static void dummy(const char*, ...) {}

#endif

namespace peloton {
namespace index {

#ifdef INTERACTIVE_DEBUG
// In multi-threaded testing, if we want to halt all threads when an error
// happens
// then we lock this mutex
// Since every other thread will try to lock this at the beginning of
// findLeafPage() which is called for every operation, they will all stop
static std::mutex debug_stop_mutex;
#endif

template <typename KeyType, typename ValueType, typename KeyComparator>
struct LessFnT;
/*
 * struct LessFn - Comparator for <key, value> tuples
 *
 * This is required by std::sort in order to consolidate pages
 * This function object compares tuples by its key (less than relation)
 */
template <typename KeyType, typename Value, class KeyComparator>
struct LessFn {
  LessFn(const KeyComparator& comp) : m_key_less(comp) {}

  bool operator()(const std::pair<KeyType, Value>& l,
                  const std::pair<KeyType, Value>& r) const {
    return m_key_less(std::get<0>(l), std::get<0>(r));
  }

  const KeyComparator& m_key_less;
};

template <typename KeyType, typename ValueType, typename KeyComparator>
class BWTree {
  class InteractiveDebugger;
  friend InteractiveDebugger;

 private:
  using PID = uint64_t;

  constexpr static PID NONE_PID = std::numeric_limits<PID>::max();

  //////////////////////////////////////////////////////////////////////////////
  /// Performance configuration constants
  constexpr static unsigned int max_table_size = 1 << 24;
  // Threshold of delta chain length on an inner node to trigger a consolidate
  constexpr static unsigned int DELTA_CHAIN_INNER_THRESHOLD = 2;
  // Threshold of delta chain length on a leaf node to trigger a consolidate
  constexpr static unsigned int DELTA_CHAIN_LEAF_THRESHOLD = 8;
  // Node sizes for triggering splits and merges on inner nodes
  constexpr static unsigned int INNER_NODE_SIZE_MIN = 4;
  constexpr static unsigned int INNER_NODE_SIZE_MAX = 16;
  // Node sizes for triggering splits and merges on leaf nodes
  constexpr static unsigned int LEAF_NODE_SIZE_MIN = 10;
  constexpr static unsigned int LEAF_NODE_SIZE_MAX = 31;
  // Debug constant: The maximum number of iterations we could do
  // It prevents dead loop hopefully
  constexpr static int ITER_MAX = 99999;

  // Enumeration of the types of nodes required in updating both the values
  // and the index in the BW Tree. Currently only adding node types for
  // supporting splits.

  enum PageType {
    leaf,
    inner,
    // Page type
    deltaInsert,
    deltaDelete,
    // Inner type & page type
    deltaSplit,
    deltaIndexTermInsert,
    deltaIndexTermDelete,
    deltaRemove,
    deltaMerge,
  };

  enum InstallDeltaResult {
    install_success,
    install_try_again,
    install_node_invalid,
    install_need_consolidate,
    install_is_remove,
  };

  /*
   * class BWNode - Generic node class; inherited by leaf, inner
   * and delta node
   */
  class BWNode {
   public:
    BWNode(PageType _type) : type(_type) {}
    virtual ~BWNode() {}

    PageType type;
  };  // class BWNode

  /*
   * class BWDeltaNode - Delta page
   */
  class BWDeltaNode : public BWNode {
   public:
    BWDeltaNode(PageType _type, BWNode* _child_node) : BWNode(_type) {
      child_node = _child_node;
    }

    BWNode* child_node;
  };  // class BWDeltaNode

  /*
   * class BWDeltaInsertNode - Key insert delta
   */
  class BWDeltaInsertNode : public BWDeltaNode {
   public:
    BWDeltaInsertNode(BWNode* _child_node,
                      std::pair<KeyType, ValueType> _ins_record)
        : BWDeltaNode(PageType::deltaInsert, _child_node) {
      ins_record = _ins_record;
    }

    std::pair<KeyType, ValueType> ins_record;
  };  // class BWDeltaInsertNode

  /*
   * class BWDeltaDeleteNode - Key delete delta
   */
  class BWDeltaDeleteNode : public BWDeltaNode {
   public:
    BWDeltaDeleteNode(BWNode* _child_node,
                      std::pair<KeyType, ValueType> _del_record)
        : BWDeltaNode(PageType::deltaDelete, _child_node) {
      del_record = _del_record;
    }

    std::pair<KeyType, ValueType> del_record;
  };  // class BWDeltaDeleteNode

  /*
   * class BWDeltaSplitNode - Leaf and inner split node
   */
  class BWDeltaSplitNode : public BWDeltaNode {
   public:
    BWDeltaSplitNode(BWNode* _child_node, KeyType _separator_key,
                     PID _split_sibling, KeyType _next_separator_key)
        : BWDeltaNode(PageType::deltaSplit, _child_node),
          separator_key(_separator_key),
          next_separator_key(_next_separator_key),
          split_sibling(_split_sibling) {}

    KeyType separator_key;
    KeyType next_separator_key;
    PID split_sibling;
  };  // class BWDeltaSplitNode

  /*
   * class BWDeltaIndexTermInsertNode - Index separator add
   */
  class BWDeltaIndexTermInsertNode : public BWDeltaNode {
   public:
    BWDeltaIndexTermInsertNode(BWNode* _child_node,
                               KeyType new_split_separator_key,
                               PID new_split_sibling,
                               KeyType next_separator_key)
        : BWDeltaNode(PageType::deltaIndexTermInsert, _child_node),
          new_split_separator_key(new_split_separator_key),
          new_split_sibling(new_split_sibling),
          next_separator_key(next_separator_key) {}

    KeyType new_split_separator_key;
    PID new_split_sibling;
    KeyType next_separator_key;
  };  // class BWDeltaIndexTermInsertNode

  /*
   * BWDeltaIndexTermDeleteNode - Remove separator in inner page
   */
  class BWDeltaIndexTermDeleteNode : public BWDeltaNode {
   public:
    BWDeltaIndexTermDeleteNode(BWNode* _child_node, PID node_to_merge_into,
                               PID node_to_remove, KeyType merge_node_low_key,
                               KeyType remove_node_low_key,
                               KeyType next_separator_key)
        : BWDeltaNode(PageType::deltaIndexTermDelete, _child_node),
          node_to_merge_into(node_to_merge_into),
          node_to_remove(node_to_remove),
          merge_node_low_key(merge_node_low_key),
          remove_node_low_key(remove_node_low_key),
          next_separator_key(next_separator_key) {}

    PID node_to_merge_into;
    PID node_to_remove;
    KeyType merge_node_low_key;
    KeyType remove_node_low_key;
    KeyType next_separator_key;
  };  // class BWDeltaIndexTermDeleteNode

  /*
   * class BWDeltaRemoveNode - Delete and free page
   *
   * NOTE: This is not delete key node
   */
  class BWDeltaRemoveNode : public BWDeltaNode {
   public:
    BWDeltaRemoveNode(BWNode* _child_node)
        : BWDeltaNode(PageType::deltaRemove, _child_node) {}
  };  // class BWDeltaRemoveNode

  /*
   * class BWDeltaMergeNode - Merge two pages into one
   */
  class BWDeltaMergeNode : public BWDeltaNode {
   public:
    BWDeltaMergeNode(BWNode* _child_node, PID node_to_remove,
                     KeyType separator_key, BWNode* merge_node,
                     KeyType next_separator_key)
        : BWDeltaNode(PageType::deltaMerge, _child_node),
          node_to_remove(node_to_remove),
          separator_key(separator_key),
          merge_node(merge_node),
          next_separator_key(next_separator_key) {}

    PID node_to_remove;
    KeyType separator_key;
    BWNode* merge_node;
    KeyType next_separator_key;
  };  // class BWDeltaMergeNode

  /*
   * class BWInnerNode - Inner node that contains separator
   *
   * Contains guide post keys for pointing to the right PID when search
   * for a key in the index
   *
   * Elastic container to allow for separation of consolidation, splitting
   * and merging
   */
  class BWInnerNode : public BWNode {
   public:
    BWInnerNode(KeyType _lower_bound, KeyType _upper_bound)
        : BWNode(PageType::inner),
          lower_bound(_lower_bound),
          upper_bound(_upper_bound) {}

    const KeyType lower_bound;
    const KeyType upper_bound;
    std::vector<std::pair<KeyType, PID>> separators;
  };  // class BWInnerNode

  /*
   * class BWLeafNode - Leaf node that actually stores data
   *
   * Lowest level nodes in the tree which contain the payload/value
   * corresponding to the keys
   */
  class BWLeafNode : public BWNode {
   public:
    BWLeafNode(KeyType _lower_bound, KeyType _upper_bound, PID _next)
        : BWNode(PageType::leaf),
          lower_bound(_lower_bound),
          upper_bound(_upper_bound),
          next(_next) {}

    const KeyType lower_bound;
    const KeyType upper_bound;
    PID next;

    // Elastic container to allow for separation of consolidation, splitting
    // and merging
    std::vector<std::pair<KeyType, std::vector<ValueType>>> data;
  };  // class BWLeafNode

  /// //////////////////////////////////////////////////////////////
  /// ValueType Comparator (template type oblivious)
  /// //////////////////////////////////////////////////////////////

  /*
   * class ValueComparator - Compare struct ItemPointer which is always
   *                         used as ValueType
   *
   * If not then compilation error
   */
  class ValueComparator {
    // ItemPointerData -- BlockIdData -- uint16 lo
    //                 |             |-- uint16 hi
    //                 |
    //                 |- OffsetNumber - typedefed as uint16
   public:
    /*
     * operator() - Checks for equality
     *
     * Return True if two data pointers are the same. False otherwise
     * Since we do not enforce any order for data it is sufficient for
     * us just to compare equality
     */
    bool operator()(const ItemPointer& a, const ItemPointer& b) const {
      return (a.block == b.block) && (a.offset == b.offset);
    }
  };

  /*
   * class ConsistencyChecker - Checks for tree structural integrity
   *
   * NOTE: Only prototypes are defined. We need to come up with more
   * checks
   */
  class ConsistencyChecker {
   public:
    ConsistencyChecker() {}
    void printTreeStructure();
    bool checkInnerNode(BWNode* node_p);
    bool checkLeafNode(BWNode* node_p);
    bool checkSeparator(BWInnerNode* inner_node_p);
    bool checkInnerNodeBound(BWNode* node_p);
    bool checkLeafNodeBound(BWNode* node_p);
  };

  /*
   * class EpochRecord - We keep such a record for each epoch
   */
  struct EpochRecord {
    uint64_t thread_count;
    std::vector<BWNode*> node_list;

    EpochRecord(EpochRecord&& et) {
      node_list = std::move(et.node_list);
      thread_count = et.thread_count;

      return;
    }

    EpochRecord& operator=(EpochRecord&& et) {
      node_list = std::move(et.node_list);
      thread_count = et.thread_count;

      return *this;
    }

    EpochRecord() : thread_count(1) {}
  };

  /*
   * EpochManager - Manages epoch and garbage collection
   *
   * NOTE: We implement this using std::mutex to handle std::vector
   * and std::unordered_map
   */
  class EpochManager {
   private:
    using bw_epoch_t = uint64_t;
    // This could be handled with CAS
    std::atomic<bw_epoch_t> current_epoch;
    std::mutex lock;

    // It is a counter that records how many joins has been called;
    // If this reaches a threshold then we just start next epoch
    // synchronously inside some thread's join() procedure
    uint64_t join_counter;

    // We allow at most 1000 join threads inside one epoch
    // This could be tuned
    static const uint64_t join_threshold = 1000;

    // This structure must be handled inside critical section
    // Also we rely on the fact that when we scan the map, epochs are
    // scanned in an increasing order which facilitates our job
    std::map<bw_epoch_t, EpochRecord> garbage_list;

   public:
    EpochManager();

    // Called by the thread to announce their existence
    bw_epoch_t joinEpoch();
    void leaveEpoch(bw_epoch_t e);
    void advanceEpoch();

    void addGarbageNode(BWNode* node_p);

    void sweepAndClean();
  };

  /*
   * InteractiveDebugger - Allows us to explore the tree interactively
   */
  class InteractiveDebugger {
   public:
    BWTree* tree;

    PID current_pid;
    // This always points to the current top node of
    // delta chain which is exactly the node pointer
    // for current_pid
    BWNode* current_pid_node_p;

    BWNode* current_node_p;
    PageType current_type;

    KeyComparator* m_key_less_p;

    struct IDBKeyCmp {
      static KeyComparator* m_key_less_p;

      bool operator()(const KeyType& a, const KeyType& b) const {
        return (*IDBKeyCmp::m_key_less_p)(a, b);
      }
    };

    struct IDBValueCmp {
      bool operator()(const ValueType& a, const ValueType& b) const {
        if (a.block < b.block) {
          return true;
        }

        if (a.block > b.block) {
          return false;
        }

        // a.block == b.block if we get here...
        if (a.offset < b.offset) {
          return true;
        }

        if (a.offset >= b.offset) {
          return false;
        }

        assert(false);
        return false;
      }
    };

    uint64_t next_key_id;
    uint64_t next_value_id;
    std::map<KeyType, uint64_t, IDBKeyCmp> key_map;
    std::map<ValueType, uint64_t, IDBValueCmp> value_map;

    std::stack<BWNode*> node_stack;
    std::stack<BWNode*> pid_node_stack;
    std::stack<PID> pid_stack;
    std::stack<bool> need_switch_stack;

    // Used as a buffer to hold keys
    std::vector<const KeyType*> key_list;
    // Also used as a buffer to hold PID
    std::vector<PID> pid_list;

    /*
     * getKeyID() - Return a unique ID for each unique key
     */
    std::string getKeyID(const KeyType& key) {
      auto it = key_map.find(key);
      if (it == key_map.end()) {
        uint64_t id = next_key_id++;
        key_map[key] = id;

        return std::string("key-") + std::to_string(id);
      }

      // 0 is -inf 1 is +inf
      if (key_map[key] == 0) {
        return "-Inf";
      } else if (key_map[key] == key_map.size() - 1) {
        return "+Inf";
      } else {
        return std::string("key-") + std::to_string(key_map[key]);
      }

      assert(false);
      return "";
    }

    /*
     * getValueID() - Get a unique ID for each unique value
     */
    std::string getValueID(const ValueType& value) {
      auto it = value_map.find(value);
      if (it == value_map.end()) {
        uint64_t id = next_value_id++;
        value_map[value] = id;

        return std::string("val-") + std::to_string(id);
      }

      return std::string("val-") + std::to_string(value_map[value]);
    }

    void printPrompt() {
      std::cout << "[(" << pageTypeToString(current_type)
                << ") PID=" << current_pid << "]>> ";

      return;
    }

    void prepareNode(BWNode* node_p, bool need_switch = false) {
      // Node pointer must be valid
      assert(node_p != nullptr);

      node_stack.push(current_node_p);

      current_node_p = node_p;
      current_type = node_p->type;

      need_switch_stack.push(need_switch);

      return;
    }

    /*
     * prepareNodeByPID() - Returns false if the PID is invalid
     */
    bool prepareNodeByPID(PID pid, bool init_flag = false) {
      if (pid == NONE_PID) {
        return false;
      }

      if (init_flag == true) {
        current_pid = pid;
        current_node_p = tree->mapping_table[pid].load();
        current_pid_node_p = current_node_p;
        current_type = current_node_p->type;

        return true;
      }

      // Save the root of delta chian and the corresponding PID
      // There is some sort of interrelationship between these two
      // We always need to check for consistency
      pid_stack.push(current_pid);
      pid_node_stack.push(current_pid_node_p);

      // Also need to check for each element's identity somewhere else
      assert(pid_stack.size() == pid_node_stack.size());

      current_pid = pid;
      // It calls push for node_stack and renew the type
      // We also need to switch in this case
      prepareNode(tree->mapping_table[pid].load(), true);

      // We only change this node when PID changes
      current_pid_node_p = current_node_p;

      return true;
    }

    std::string pageTypeToString(PageType type) {
      switch (type) {
        case leaf:
          return "Leaf";
        case inner:
          return "Inner";
        case deltaInsert:
          return "Delta Insert";
        case deltaDelete:
          return "Delta Delete";
        case deltaSplit:
          return "Delta Split";
        case deltaIndexTermInsert:
          return "Index Insert";
        case deltaIndexTermDelete:
          return "Index Delete";
        case deltaRemove:
          return "Remove";
        case deltaMerge:
          return "Merge";
        default:
          return "Unknown Type (Error!)";
      }

      assert(false);
      return "";
    }

    void processPrint() {
      std::string s;
      std::cin >> s;

      if (s == "") {
        std::cout << "Nothing to print!" << std::endl;
        return;
      } else if (s == "node-pointer") {
        std::cout << current_node_p << std::endl;
      } else if (s == "type") {
        std::cout << current_type << " (" << pageTypeToString(current_type)
                  << ")" << std::endl;
      } else {
        std::cout << "Unkown print argument: " << s << std::endl;
      }

      return;
    }

    /*
     * processGotoChild() - Go to the child node of a delta node
     *
     * This does not work for inner and leaf nodes
     */
    void processGotoChild() {
      BWDeltaNode* delta_node_p = nullptr;

      switch (current_node_p->type) {
        case deltaDelete:
        case deltaIndexTermDelete:
        case deltaIndexTermInsert:
        case deltaInsert:
        case deltaMerge:
        case deltaRemove:
        case deltaSplit:
          delta_node_p = (static_cast<BWDeltaNode*>(current_node_p));
          // Make this current node
          prepareNode(static_cast<BWNode*>(delta_node_p->child_node));
          break;
        case leaf:
        case inner:
          std::cout << "Type (" << pageTypeToString(current_type)
                    << ") does not have child node" << std::endl;
          break;
      }

      return;
    }

    /*
     * processGotoSplitSibling() - Go to split sibling of a split delta node
     */
    void processGotoSplitSibling() {
      if (current_type != deltaSplit) {
        std::cout << "Type (" << pageTypeToString(current_type)
                  << ") does not have split sibling" << std::endl;
        return;
      }

      BWDeltaSplitNode* split_node_p =
          static_cast<BWDeltaSplitNode*>(current_node_p);

      prepareNodeByPID(split_node_p->split_sibling);

      return;
    }

    /*
     * processGotoSibling() - Goto sibling node (only for leaf nodes)
     */
    void processGotoSibling() {
      if (current_type != leaf) {
        std::cout << "Type (" << pageTypeToString(current_type)
                  << ") does not have sibling node" << std::endl;
        return;
      }

      BWLeafNode* leaf_node_p = static_cast<BWLeafNode*>(current_node_p);
      prepareNodeByPID(leaf_node_p->next);

      return;
    }

    void processGotoMergeSibling() {
      if (current_type != deltaMerge) {
        std::cout << "Type (" << pageTypeToString(current_type)
                  << ") does not have merge sibling" << std::endl;
        return;
      }

      BWDeltaMergeNode* merge_node_p =
          static_cast<BWDeltaMergeNode*>(current_node_p);

      // Physical pointer in merge delta (PID in split delta)
      prepareNode(merge_node_p->merge_node);

      return;
    }

    void processPrintSep() {
      if (current_type != inner) {
        std::cout << "Type (" << pageTypeToString(current_type)
                  << ") does not have separator array" << std::endl;
        return;
      }

      BWInnerNode* inner_node_p = static_cast<BWInnerNode*>(current_node_p);

      std::cout << "Number of separators: " << inner_node_p->separators.size()
                << std::endl;
      for (auto it = inner_node_p->separators.begin();
           it != inner_node_p->separators.end(); it++) {
        std::cout << "[" << getKeyID(it->first) << ", " << it->second << "]"
                  << ", ";
      }

      std::cout << std::endl;

      return;
    }

    void processPrintBound() {
      if (current_type == inner) {
        BWInnerNode* inner_node_p = static_cast<BWInnerNode*>(current_node_p);
        std::cout << "Lower, Upper: " << getKeyID(inner_node_p->lower_bound)
                  << ", " << getKeyID(inner_node_p->upper_bound) << std::endl;
      } else if (current_type == leaf) {
        BWLeafNode* leaf_node_p = static_cast<BWLeafNode*>(current_node_p);
        std::cout << "Lower, Upper: " << getKeyID(leaf_node_p->lower_bound)
                  << ", " << getKeyID(leaf_node_p->upper_bound) << std::endl;
      } else {
        std::cout << "Type (" << pageTypeToString(current_type)
                  << ") does not have bound key" << std::endl;
      }

      return;
    }

    void processPrintLeaf() {
      if (current_type != leaf) {
        std::cout << "Type (" << pageTypeToString(current_type)
                  << ") does not have leaf array" << std::endl;
        return;
      }

      BWLeafNode* leaf_node_p = static_cast<BWLeafNode*>(current_node_p);
      std::cout << "Node size: " << leaf_node_p->data.size() << std::endl;
      for (auto it = leaf_node_p->data.begin(); it != leaf_node_p->data.end();
           it++) {
        std::cout << getKeyID(it->first) << ": [";
        for (auto it2 = it->second.begin(); it2 != it->second.end(); it2++) {
          std::cout << getValueID(*it2) << ", ";
        }

        std::cout << "], " << std::endl;
      }

      return;
    }

    void processGotoSep() {
      if (current_type != inner) {
        std::cout << "Type (" << pageTypeToString(current_type)
                  << ") does not have separator array" << std::endl;
      }

      int index;
      std::cin >> index;

      BWInnerNode* inner_node_p = static_cast<BWInnerNode*>(current_node_p);
      if (index < 0 || index >= inner_node_p->separators.size()) {
        std::cout << "Index " << index << " is not valid" << std::endl;
        return;
      }

      PID next_pid = inner_node_p->separators[index].second;
      std::cout << "Going to PID: " << next_pid << std::endl;
      prepareNodeByPID(next_pid);

      return;
    }

    void processPrintRecord() {
      BWDeltaSplitNode* split_node_p = nullptr;
      BWDeltaMergeNode* merge_node_p = nullptr;

      BWDeltaInsertNode* insert_node_p = nullptr;
      BWDeltaDeleteNode* delete_node_p = nullptr;

      BWDeltaIndexTermInsertNode* index_insert_node_p = nullptr;
      BWDeltaIndexTermDeleteNode* index_delete_node_p = nullptr;

      std::cout << "Node type: ";
      std::cout << current_type << " (" << pageTypeToString(current_type) << ")"
                << std::endl;

      switch (current_type) {
        case leaf:
        case inner:
        case deltaRemove:
          std::cout << "Type (" << pageTypeToString(current_type)
                    << ") does not have record" << std::endl;
          break;
        case deltaSplit:
          split_node_p = static_cast<BWDeltaSplitNode*>(current_node_p);
          std::cout << "Separator key: "
                    << getKeyID(split_node_p->separator_key) << std::endl;

          std::cout << "Next Sep key: "
                    << getKeyID(split_node_p->next_separator_key) << std::endl;

          std::cout << "Sep sibling PID: " << split_node_p->split_sibling
                    << std::endl;
          break;
        case deltaMerge:
          merge_node_p = static_cast<BWDeltaMergeNode*>(current_node_p);
          std::cout << "Separator key: "
                    << getKeyID(merge_node_p->separator_key) << std::endl;

          std::cout << "Next Sep key: "
                    << getKeyID(merge_node_p->next_separator_key) << std::endl;

          std::cout << "Removed node PID: " << merge_node_p->node_to_remove
                    << std::endl;
          break;
        case deltaInsert:
          insert_node_p = static_cast<BWDeltaInsertNode*>(current_node_p);

          std::cout << "key, value = ["
                    << getKeyID(insert_node_p->ins_record.first) << ", "
                    << getValueID(insert_node_p->ins_record.second) << "]"
                    << std::endl;

          break;

        case deltaDelete:
          delete_node_p = static_cast<BWDeltaDeleteNode*>(current_node_p);

          std::cout << "key, value = ["
                    << getKeyID(delete_node_p->del_record.first) << ", "
                    << getValueID(delete_node_p->del_record.second) << "]"
                    << std::endl;

          break;
        case deltaIndexTermInsert:
          index_insert_node_p =
              static_cast<BWDeltaIndexTermInsertNode*>(current_node_p);

          std::cout << "New split sep: "
                    << getKeyID(index_insert_node_p->new_split_separator_key)
                    << std::endl;
          std::cout << "Next split sep: "
                    << getKeyID(index_insert_node_p->next_separator_key)
                    << std::endl;
          std::cout << "New child PID: "
                    << index_insert_node_p->new_split_sibling << std::endl;

          break;
        case deltaIndexTermDelete:
          index_delete_node_p =
              static_cast<BWDeltaIndexTermDeleteNode*>(current_node_p);

          std::cout << "Merge node low: "
                    << getKeyID(index_delete_node_p->merge_node_low_key)
                    << std::endl;
          std::cout << "Rm node low: "
                    << getKeyID(index_delete_node_p->remove_node_low_key)
                    << std::endl;
          std::cout << "Next sep: "
                    << getKeyID(index_delete_node_p->next_separator_key)
                    << std::endl;
          std::cout << "Merge node PID: "
                    << index_delete_node_p->node_to_merge_into << std::endl;
          std::cout << "Rm node PID: " << index_delete_node_p->node_to_remove
                    << std::endl;

          break;
        default:
          std::cout << "Unknown node type for record: "
                    << pageTypeToString(current_type) << std::endl;
          assert(false);
          break;
      }

      return;
    }

    void processBack() {
      assert(pid_stack.size() == pid_node_stack.size());

      if (node_stack.size() == 0) {
        std::cout << "Already at root. Cannot go back" << std::endl;
        return;
      }

      // We know we are on top of a PID delta chain
      if (need_switch_stack.top() == true) {
        std::cout << "Return to previous PID: " << pid_stack.top() << std::endl;

        current_pid_node_p = pid_node_stack.top();
        pid_node_stack.pop();

        current_pid = pid_stack.top();
        pid_stack.pop();
      }

      need_switch_stack.pop();

      current_node_p = node_stack.top();
      node_stack.pop();

      current_type = current_node_p->type;

      return;
    }

    void initKeyMap() {
      // std::cout << "Negative inf: " << getKeyID(KeyType::NEG_INF_KEY)
      //           << std::endl;
      // std::cout << "Positive inf: " << getKeyID(KeyType::POS_INF_KEY)
      //           << std::endl;
      getKeyID(KeyType::NEG_INF_KEY);
      getKeyID(KeyType::POS_INF_KEY);

      return;
    }

    /*
     * sortKeyMap() - Sort all keys so that key ID reflects key order
     */
    void sortKeyMap() {
      uint64_t counter = 0;
      for (auto it = key_map.begin(); it != key_map.end(); it++) {
        // -inf = 0; + inf = key_map.size() - 1
        it->second = counter++;
      }

      return;
    }

    void start() {
      // We could not start with empty root node
      assert(prepareNodeByPID(tree->m_root.load(), true) == true);
      sortKeyMap();

      std::cout << "********* Interactive Debugger *********\n";

      while (1) {
        printPrompt();

        std::string opcode;
        std::cin >> opcode;

        // If read EOF then resume BWTree execution
        if (!std::cin) {
          return;
        }

        // Ctrl-D (EOF)  will resume execution
        if (opcode == "exit") {
          exit(0);
        } else if (opcode == "continue") {
          break;
        } else if (opcode == "print") {
          processPrint();
        } else if (opcode == "print-sep") {
          // print sep-PID pairs for inner node
          processPrintSep();
        } else if (opcode == "print-leaf") {
          // print key-value_list pairs for leaf node
          processPrintLeaf();
        } else if (opcode == "print-bound") {
          // print lower and upper bound for leaf and inenr node
          processPrintBound();
        } else if (opcode == "type") {
          std::cout << current_type << " (" << pageTypeToString(current_type)
                    << ")" << std::endl;
        } else if (opcode == "goto-child") {
          // Goto child node pointed to by a physical pointer
          // in delta nodes
          processGotoChild();
        } else if (opcode == "goto-split-sibling") {
          processGotoSplitSibling();
        } else if (opcode == "goto-sibling") {
          processGotoSibling();
        } else if (opcode == "goto-merge-sibling") {
          processGotoMergeSibling();
        } else if (opcode == "goto-sep") {
          processGotoSep();
        } else if (opcode == "print-record") {
          // For delta nodes, print the content
          processPrintRecord();
        } else if (opcode == "back") {
          // Go back to the previous position
          // (switch between PID and PID root and current node
          // is done using stacks)
          processBack();
        } else if (opcode == "goto-pid") {
          // Goto the first page pointed to by a PID
          uint64_t target_pid;
          std::cin >> target_pid;
          prepareNodeByPID(target_pid);
        } else if (opcode == "get-key-id") {
          // We push the target key of findLeafPage() into key_list
          // when we hit the assertion, and then invoke the debugger
          // then we could use its index to see the relative position of the key
          int key_index;
          std::cin >> key_index;

          if (key_index < 0 || key_index >= key_list.size()) {
            std::cout << "Key index " << key_index << " invalid!" << std::endl;
          } else {
            std::cout << getKeyID(*key_list[key_index]) << std::endl;
          }
        } else if (opcode == "get-pid") {
          int pid_index;
          std::cin >> pid_index;

          if (pid_index < 0 || pid_index >= pid_list.size()) {
            std::cout << "PID index " << pid_index << " invalid!" << std::endl;
          } else {
            std::cout << "pid_list[" << pid_index
                      << "] = " << pid_list[pid_index] << std::endl;
          }
        } else {
          std::cout << "Unknown command: " << opcode << std::endl;
        }
      }

      return;
    }

    InteractiveDebugger(BWTree* _tree, KeyComparator _m_key_less)
        : tree(_tree),
          current_pid(0),
          current_node_p(nullptr),
          next_key_id(0),
          next_value_id(0) {
      m_key_less_p = new KeyComparator(_m_key_less);
      IDBKeyCmp::m_key_less_p = m_key_less_p;

      initKeyMap();

      return;
    }

    ~InteractiveDebugger() { delete m_key_less_p; }
  };

  /// //////////////////////////////////////////////////////////////
  /// Method decarations & definitions
  /// //////////////////////////////////////////////////////////////
 public:
  // TODO: pass a settings structure as we go along instead of
  // passing in individual parameter values
  BWTree(KeyComparator _m_key_less, bool _m_unique_keys);
  ~BWTree();

  bool insert(const KeyType& key, const ValueType& value);
  bool exists(const KeyType& key);
  bool erase(const KeyType& key, const ValueType& value);

  size_t getMemoryFootprint() { return m_foot_print; }

  std::vector<ValueType> find(const KeyType& key);
  // Scan all values
  void getAllValues(std::vector<ValueType>& result);
  void getAllKeyValues(
      std::vector<std::pair<KeyType, std::vector<ValueType>>>& result);

 private:
  std::vector<ValueType> collectPageContentsByKey(BWNode* node_p,
                                                  const KeyType& key,
                                                  PID& next_page,
                                                  KeyType& high_key);
  std::vector<std::pair<KeyType, std::vector<ValueType>>>
  collectAllPageContents(BWNode* node_p, PID& next_page, KeyType& high_key);

  /*
   * isValueEqual() - Compare two values and see if they are equal
   */
  inline bool isValueEqual(const ValueType& a, const ValueType& b) const {
    return m_val_equal(a, b);
  }

  /*
   * isTupleEqual() - Whether two tuples are equal
   *
   * It calls key comparator and value comparator respectively
   * We need this function to determine deletion in a duplicated key
   * environment
   */
  inline bool isTupleEqual(const std::pair<KeyType, ValueType>& a,
                           const std::pair<KeyType, ValueType>& b) const {
    return key_equal(a.first, b.first) && m_val_equal(a.second, b.second);
  }

  /// True if a < b ? "constructed" from m_key_less()
  inline bool key_less(const KeyType& a, const KeyType& b) const {
    return m_key_less(a, b);
  }

  /// True if a <= b ? constructed from key_less()
  inline bool key_lessequal(const KeyType& a, const KeyType& b) const {
    return !m_key_less(b, a);
  }

  /// True if a > b ? constructed from key_less()
  inline bool key_greater(const KeyType& a, const KeyType& b) const {
    return m_key_less(b, a);
  }

  /// True if a >= b ? constructed from key_less()
  inline bool key_greaterequal(const KeyType& a, const KeyType& b) const {
    return !m_key_less(a, b);
  }

  /// True if a == b ? constructed from key_less(). This requires the <
  /// relation to be a total order, otherwise the B+ tree cannot be sorted.
  inline bool key_equal(const KeyType& a, const KeyType& b) const {
    return !m_key_less(a, b) && !m_key_less(b, a);
  }

  struct FindLeafResult {
    PID pid;
    BWNode* node;
    std::vector<PID> parent_pids;
    std::vector<BWNode*> parent_nodes;

    void push(PID n_pid, BWNode* n_node) {
      parent_pids.push_back(pid);
      parent_nodes.push_back(node);
      pid = n_pid;
      node = n_node;
    }

    void pop() {
      pid = parent_pids.back();
      node = parent_nodes.back();
      parent_pids.pop_back();
      parent_nodes.pop_back();
    }
  };

  // Input invariants:
  // 1. All insert records should end up in the final output. That means that
  //    any intersection with the delete records has already been done
  // 2. The insert records should be sorted on key type
  // 3. Data is sorted on key tyep
  void consolidateModificationsLeaf(
      std::vector<std::pair<KeyType, ValueType>>& insert_records,
      std::set<std::pair<KeyType, ValueType>,
               LessFn<KeyType, ValueType, KeyComparator>>& delete_records,
      typename std::vector<std::pair<KeyType, std::vector<ValueType>>>::iterator
          data_start,
      typename std::vector<std::pair<KeyType, std::vector<ValueType>>>::iterator
          data_end,
      LessFn<KeyType, ValueType, KeyComparator>& less_fn,
      std::vector<std::pair<KeyType, std::vector<ValueType>>>& output_data);

  void consolidateModificationsInner(
      std::vector<std::pair<KeyType, PID>>& insert_records,
      std::set<std::pair<KeyType, PID>, LessFn<KeyType, PID, KeyComparator>>&
          delete_records,
      typename std::vector<std::pair<KeyType, PID>>::iterator data_start,
      typename std::vector<std::pair<KeyType, PID>>::iterator data_end,
      LessFn<KeyType, PID, KeyComparator>& less_fn,
      std::vector<std::pair<KeyType, PID>>& output_data);

  void traverseAndConsolidateLeaf(
      FindLeafResult& leaf_info, BWNode* node,
      std::vector<BWNode*>& garbage_nodes,
      std::vector<std::pair<KeyType, std::vector<ValueType>>>& data,
      PID& sibling, bool& has_merge, BWNode*& merge_node, KeyType& lower_bound,
      KeyType& upper_bound);

  bool consolidateLeafNode(FindLeafResult& leaf_info);

  void traverseAndConsolidateInner(
      FindLeafResult& leaf_info, BWNode* node,
      std::vector<BWNode*>& garbage_nodes,
      std::vector<std::pair<KeyType, PID>>& separators, bool& has_merge,
      BWNode*& merge_node, KeyType& lower_bound, KeyType& upper_bound);

  bool consolidateInnerNode(FindLeafResult& leaf_info);

  std::pair<KeyType, KeyType> findBounds(const BWNode* node);

  bool isLeaf(BWNode* node);

  bool performConsolidation(FindLeafResult& leaf_info);

  bool fixSMO(FindLeafResult& info, bool& isSMO);

  void fixSMOForInstall(FindLeafResult& info);

  FindLeafResult findLeafPage(const KeyType& key);

  FindLeafResult findPID(PID pid);

  BWNode* spinOnSMOByKey(KeyType& key);

  // Atomically install a page into mapping table
  // NOTE: There are times that new pages are not installed into
  // mapping table but instead they directly replace other page
  // with the same PID
  PID installPage(BWNode* new_node_p);

  PID findNext(BWNode* node);
  // This only applies to leaf node - For intermediate nodes
  // the insertion of sep/child pair must be done using different
  // insertion method
  void installDeltaInsert(const FindLeafResult& leaf_info, const KeyType& key,
                          const ValueType& value);

  void installDeltaDelete(const FindLeafResult& leaf_info, const KeyType& key,
                          const ValueType& value);

  void installIndexTermDeltaInsert(FindLeafResult& leaf_info);

  void installIndexTermDeltaDelete(FindLeafResult& leaf_info);

  void installDeltaMerge(FindLeafResult& leaf_info);

  void deleteDeltaChain(BWNode* node);

  void addGarbageNodes(std::vector<BWNode*>& garbage);

  /// //////////////////////////////////////////////////////////////
  /// Data member definition
  /// //////////////////////////////////////////////////////////////

  // Note that this cannot be resized nor moved. So it is effectively
  // like declaring a static array
  // TODO: Maybe replace with a static array
  // NOTE: This is updated together with next_pid atomically
  std::atomic<size_t> current_mapping_table_size;
  // Next available PID to allocate for any node
  // This variable is made atomic to facilitate our atomic mapping table
  // implementation
  std::atomic<PID> next_pid;
  std::vector<std::atomic<BWNode*>> mapping_table{max_table_size};

  std::atomic<size_t> m_foot_print;

  // Not efficient but just for correctness
  std::mutex m_garbage_mutex;
  std::vector<BWNode*> m_garbage_nodes;

  std::atomic<PID> m_root;
  const KeyComparator m_key_less;
  const bool m_unique_keys;
  const ValueComparator m_val_equal;

  // TODO: UNFINISHED!!!
  const ConsistencyChecker checker;

  // TODO: Add a global garbage vector per epoch using a lock
  EpochManager epoch_mgr;
  InteractiveDebugger idb;

  // Leftmost leaf page
  // NOTE: We assume the leftmost lead page will always be there
  // For split it remains to be the leftmost page
  // For merge and remove we need to make sure the last remianing page
  // shall not be removed
  // Using this pointer we could do sequential search more efficiently
  PID first_leaf;
};

}  // End index namespace
}  // End peloton namespace

/// //////////////////////////////////////////////////////////
/// Implementations
/// //////////////////////////////////////////////////////////

#include <set>

namespace peloton {
namespace index {

/*
 * Constructor - Construct a new tree with an single element
 *               intermediate node and empty leaf node
 *
 * NOTE: WE have a corner case here: initially the leaf node
 * is empty, so any leaf page traversal needs to be able to handle
 * empty lead node (i.e. calling data.back() causes undefined behaviour)
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
BWTree<KeyType, ValueType, KeyComparator>::BWTree(KeyComparator _m_key_less,
                                                  bool _m_unique_keys)
    : current_mapping_table_size(0),
      next_pid(0),
      m_foot_print(0),
      m_key_less(_m_key_less),
      m_unique_keys(_m_unique_keys),
      m_val_equal(),
      checker(),
      epoch_mgr(),
      idb(this, _m_key_less) {
  // Initialize an empty tree
  KeyType low_key = KeyType::NEG_INF_KEY;
  KeyType high_key = KeyType::POS_INF_KEY;

  BWLeafNode* initial_leaf = new BWLeafNode(low_key, high_key, NONE_PID);
  PID leaf_pid = installPage(initial_leaf);

  BWInnerNode* initial_inner = new BWInnerNode(low_key, high_key);
  initial_inner->separators.emplace_back(low_key, leaf_pid);
  PID inner_pid = installPage(initial_inner);

  m_root = inner_pid;
  first_leaf = leaf_pid;

  bwt_printf("Init: Initializer returns. Leaf = %lu, inner = %lu\n", leaf_pid,
             inner_pid);
}

/*
 * Destructor - Free up all pages
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
BWTree<KeyType, ValueType, KeyComparator>::~BWTree() {
  // TODO: finish this
  for (std::atomic<BWNode*>& atm_node : mapping_table) {
    BWNode* node = atm_node.load();
    // If this is a remove node, we need to check if the merge was installed
    // because otherwise we won't cleanup the data under the remove node
    deleteDeltaChain(node);
  }
  for (BWNode* node : m_garbage_nodes) {
    delete node;
  }
}

/*
 * spinOnSMOByKey() - This method keeps finding page if it sees a SMO on
 * the top of a leaf delta chain
 *
 * Since findLeafPage() will do all of the consolidation work, we just
 * need to reinvoke routine
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
typename BWTree<KeyType, ValueType, KeyComparator>::BWNode*
BWTree<KeyType, ValueType, KeyComparator>::spinOnSMOByKey(KeyType& key) {
  int counter = 0;
  BWNode* node_p = nullptr;

  // Find the first page where the key lies in
  FindLeafResult r = findLeafPage(key);

  // The returned page is guaranteed not to be SMO
  /// Even if some other operation adds SMO on top of that
  /// we could only see the physical pointer
  return r.node;
}

/*
 * getAllValues() - Get all values in the tree, using key order
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
void BWTree<KeyType, ValueType, KeyComparator>::getAllValues(
    std::vector<ValueType>& result) {
  bwt_printf("Get All Values!\n");

  // This will find the left most leaf page in the tree
  FindLeafResult r = findLeafPage(KeyType::NEG_INF_KEY);
  BWNode* curr_node = r.node;
  PID start_page = r.pid;

  while (curr_node != nullptr) {
    PID next_page;
    KeyType high_key;
    std::vector<std::pair<KeyType, std::vector<ValueType>>> curr_page_values =
        collectAllPageContents(curr_node, next_page, high_key);

    for (auto key_it = curr_page_values.begin();
         key_it != curr_page_values.end(); key_it++) {
      result.insert(result.end(), key_it->second.begin(), key_it->second.end());
    }

    // There is nothing more to check
    if (next_page == NONE_PID) {
      curr_node = nullptr;
    } else {
      curr_node = mapping_table[next_page];
    }
  }
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void BWTree<KeyType, ValueType, KeyComparator>::getAllKeyValues(
    std::vector<std::pair<KeyType, std::vector<ValueType>>>& result) {
  bwt_printf("Get Predicate Values!\n");

  // This will find the left most leaf page in the tree
  FindLeafResult r = findLeafPage(KeyType::NEG_INF_KEY);
  BWNode* curr_node = r.node;
  PID start_page = r.pid;

  while (curr_node != nullptr) {
    PID next_page;
    KeyType high_key;
    std::vector<std::pair<KeyType, std::vector<ValueType>>> curr_page_values =
        collectAllPageContents(curr_node, next_page, high_key);

    result.insert(result.end(), curr_page_values.begin(),
                  curr_page_values.end());

    // There is nothing more to check
    if (next_page == NONE_PID) {
      curr_node = nullptr;
    } else {
      curr_node = mapping_table[next_page];
    }
  }

  return;
}

/*
 * exists() - Return true if a tuple with the given key exists in the tree
 *
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
bool BWTree<KeyType, ValueType, KeyComparator>::exists(const KeyType& key) {
  bwt_printf("key = %d\n", key);

  std::vector<ValueType> values = find(key);
  return values.size() > 0;
}

/*
 * collectPageContentsByKey() - Collect items in a page with a given key
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
std::vector<ValueType>
BWTree<KeyType, ValueType, KeyComparator>::collectPageContentsByKey(
    BWNode* node_p, const KeyType& key, PID& next_page, KeyType& high_key) {
  std::vector<ValueType> values;

  std::vector<std::pair<KeyType, std::vector<ValueType>>> all_records =
      collectAllPageContents(node_p, next_page, high_key);

  // Filter tuple values by key
  for (auto it = all_records.begin(); it != all_records.end(); ++it) {
    if (key_equal(it->first, key)) {
      values.insert(values.begin(), it->second.begin(), it->second.end());
      break;
    }
  }

  return values;
}

/*
 * collectAllPageContents() - Collect items on a given logical page (PID)
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
std::vector<std::pair<KeyType, std::vector<ValueType>>>
BWTree<KeyType, ValueType, KeyComparator>::collectAllPageContents(
    BWNode* node_p, PID& next_page, KeyType& high_key) {
  using LessFnT = LessFn<KeyType, ValueType, KeyComparator>;

  LessFnT less_fn(m_key_less);
  BWNode* curr_node = node_p;

  std::vector<std::pair<KeyType, std::vector<ValueType>>> all_records;
  std::set<std::pair<KeyType, ValueType>, LessFnT> delete_records(less_fn);
  std::vector<std::pair<KeyType, ValueType>> insert_records;

  while (curr_node != nullptr) {
    switch (curr_node->type) {
      case PageType::deltaRemove: {
        // TODO: Reason if this is the right thing to do
        BWDeltaRemoveNode* remove_node =
            static_cast<BWDeltaRemoveNode*>(curr_node);
        curr_node = remove_node->child_node;
        break;
      }
      case PageType::deltaSplit: {
        BWDeltaSplitNode* split_node =
            static_cast<BWDeltaSplitNode*>(curr_node);
        // The side pointer to the next page is not logically a part of the
        // page and is ignored since the sibling pointer will point to it. Any
        // client doing a scan or find will look in the sibiling to get its
        // contents if necessary
        curr_node = split_node->child_node;
        break;
      }
      case PageType::deltaMerge: {
        BWDeltaMergeNode* merge_delta =
            static_cast<BWDeltaMergeNode*>(curr_node);
        // Both the merge node and child node are considered part of the same
        // logical page so the contents of both the physical pages has to be
        // collected

        // Note: The next PID of the left node in the merge should point to no
        // other node. The sibling information is part of the merge node.
        PID left_next;
        KeyType left_high_key;
        std::vector<std::pair<KeyType, std::vector<ValueType>>> child_contents =
            collectAllPageContents(merge_delta->child_node, left_next,
                                   left_high_key);
        // assert(left_next == NONE_PID);
        // Add all the collected values to the vector of records to be inserted
        for (auto it = child_contents.begin(); it != child_contents.end();
             ++it) {
          for (auto val_it = it->second.begin(); val_it != it->second.end();
               val_it++) {
            insert_records.push_back(std::make_pair(it->first, *val_it));
          }
        }
        curr_node = merge_delta->merge_node;
        break;
      }
      case PageType::deltaInsert: {
        BWDeltaInsertNode* node = static_cast<BWDeltaInsertNode*>(curr_node);

        // If the tuple is already in the delete list ignore it
        auto bounds = std::equal_range(
            delete_records.begin(), delete_records.end(),
            std::make_pair(node->ins_record.first, ValueType{}), less_fn);

        bool found = false;
        for (auto del_it = bounds.first; del_it != bounds.second; ++del_it) {
          if (m_val_equal(del_it->second, node->ins_record.second)) {
            found = true;
            break;
          }
        }

        if (!found) {
          insert_records.push_back(node->ins_record);
        }

        curr_node = node->child_node;
        break;
      }
      case PageType::deltaDelete: {
        BWDeltaDeleteNode* node = static_cast<BWDeltaDeleteNode*>(curr_node);
        delete_records.insert(node->del_record);
        curr_node = node->child_node;
        break;
      }
      case PageType::leaf: {
        BWLeafNode* node = static_cast<BWLeafNode*>(curr_node);

        for (auto it = node->data.begin(); it != node->data.end(); ++it) {
          // If the tuple is already in the delete list ignore it
          std::vector<ValueType> values;
          auto bounds =
              std::equal_range(delete_records.begin(), delete_records.end(),
                               std::make_pair(it->first, ValueType{}), less_fn);
          for (auto val_it = it->second.begin(); val_it < it->second.end();
               ++val_it) {
            bool found = false;
            for (auto del_it = bounds.first; del_it != bounds.second;
                 ++del_it) {
              if (m_val_equal(del_it->second, *val_it)) {
                found = true;
                break;
              }
            }
            if (!found) {
              values.push_back(*val_it);
            }
          }

          if (values.size() > 0) {
            all_records.push_back(std::make_pair(it->first, values));
          }
        }

        // Add all the insert records
        for (auto ins_it = insert_records.begin();
             ins_it != insert_records.end(); ins_it++) {
          bool found = false;
          for (auto out_it = all_records.begin(); out_it != all_records.end();
               out_it++) {
            if (key_equal(out_it->first, ins_it->first)) {
              out_it->second.push_back(ins_it->second);
              found = true;
              break;
            }
          }
          if (!found) {
            std::vector<ValueType> values{ins_it->second};
            all_records.push_back(std::make_pair(ins_it->first, values));
          }
        }

        curr_node = nullptr;
        next_page = node->next;
        high_key = node->upper_bound;
        break;
      }
      case PageType::deltaIndexTermInsert:
      case PageType::deltaIndexTermDelete:
      case PageType::inner: {
        // Could our PID have been replaced with an index node via a remove
        // and then subsequent split?
        // This should not happen currently because we do not reuse pid values
        // also the garbage collection should ensure that if some thread is
        // is reading the page it should not have been reused
        bwt_printf("The PID points to an index node\n");
        idb_assert(false);
        break;
      }
      default:
        assert(false);
    }
  }
  return all_records;
}

/*
 * insert() - Insert a key-value pair into B-Tree
 *
 * NOTE: Natural duplicated key support - we do not check for
 * duplicated key/val pair since they are allowed to be there
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
bool BWTree<KeyType, ValueType, KeyComparator>::insert(const KeyType& key,
                                                       const ValueType& value) {
  m_foot_print += 4;
  if (m_unique_keys) {
    std::vector<ValueType> values = find(key);
    // There can only be one value corresponding to a key
    assert(values.size() <= 1);
    if (values.size() > 0) return false;
  }

  // First reach the leaf page where the key should be inserted
  FindLeafResult r = findLeafPage(key);

  // Then install an insertion record
  installDeltaInsert(r, key, value);

  return true;
}

/*
 * erase() - Delete a key-value pair from the tree
 *
 * Since there could be duplicated keys, we need to
 * specify the data item locate the record for deletion
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
bool BWTree<KeyType, ValueType, KeyComparator>::erase(const KeyType& key,
                                                      const ValueType& value) {
  m_foot_print -= 4;
  std::vector<ValueType> values = find(key);
  if (values.size() == 0) return false;

  // First reach the leaf page where the key should be inserted
  FindLeafResult r = findLeafPage(key);

  // Then install an insertion record
  installDeltaDelete(r, key, value);

  return true;
}

template <typename KeyType, typename ValueType, class KeyComparator>
std::vector<ValueType> BWTree<KeyType, ValueType, KeyComparator>::find(
    const KeyType& key) {
  // Find the leaf page the key maps into
  FindLeafResult r = findLeafPage(key);
  PID next_page;
  KeyType high_key;
  std::vector<ValueType> values =
      collectPageContentsByKey(r.node, key, next_page, high_key);

  // Check that the high_key of the leaf page is less than that of the
  // key being searched otherwise something has gone wrong
  idb_assert_key(key, key_less(key, high_key));

  return values;
}

template <typename KeyType, typename ValueType, class KeyComparator>
void BWTree<KeyType, ValueType, KeyComparator>::consolidateModificationsLeaf(
    std::vector<std::pair<KeyType, ValueType>>& insert_records,
    std::set<std::pair<KeyType, ValueType>,
             LessFn<KeyType, ValueType, KeyComparator>>& delete_records,
    typename std::vector<std::pair<KeyType, std::vector<ValueType>>>::iterator
        data_start,
    typename std::vector<std::pair<KeyType, std::vector<ValueType>>>::iterator
        data_end,
    LessFn<KeyType, ValueType, KeyComparator>& less_fn,
    std::vector<std::pair<KeyType, std::vector<ValueType>>>& output_data) {
  using sortFnT = LessFn<KeyType, std::vector<ValueType>, KeyComparator>;
  sortFnT sort_fn(m_key_less);

  for (auto it = data_start; it != data_end; ++it) {
    std::vector<ValueType> values;
    auto bounds =
        std::equal_range(delete_records.begin(), delete_records.end(),
                         std::make_pair(it->first, ValueType{}), less_fn);

    for (auto val_it = it->second.begin(); val_it != it->second.end();
         val_it++) {
      bool found = false;
      for (auto del_it = bounds.first; del_it != bounds.second; ++del_it) {
        if (m_val_equal(del_it->second, *val_it)) {
          found = true;
          break;
        }
      }
      if (!found) {
        values.push_back(*val_it);
      }
    }
    if (values.size() > 0) {
      output_data.push_back(std::make_pair(it->first, values));
    }
  }

  // Add insert elements
  // TODO: Naive way of doing the search can be optimized
  for (auto ins_it = insert_records.begin(); ins_it != insert_records.end();
       ins_it++) {
    bool found = false;
    for (auto out_it = output_data.begin(); out_it != output_data.end();
         out_it++) {
      if (key_equal(out_it->first, ins_it->first)) {
        out_it->second.push_back(ins_it->second);
        found = true;
        break;
      }
    }
    if (!found) {
      std::vector<ValueType> values{ins_it->second};
      output_data.push_back(std::make_pair(ins_it->first, values));
    }
  }

  // Sort the output
  std::sort(output_data.begin(), output_data.end(), sort_fn);
}

template <typename KeyType, typename ValueType, class KeyComparator>
void BWTree<KeyType, ValueType, KeyComparator>::consolidateModificationsInner(
    std::vector<std::pair<KeyType, PID>>& insert_records,
    std::set<std::pair<KeyType, PID>, LessFn<KeyType, PID, KeyComparator>>&
        delete_records,
    typename std::vector<std::pair<KeyType, PID>>::iterator data_start,
    typename std::vector<std::pair<KeyType, PID>>::iterator data_end,
    LessFn<KeyType, PID, KeyComparator>& less_fn,
    std::vector<std::pair<KeyType, PID>>& output_data) {
  // Perform set difference
  size_t begin_size = output_data.size();
  for (auto it = data_start; it != data_end; ++it) {
    auto del_it = delete_records.find(*it);
    if (del_it == delete_records.end()) {
      output_data.push_back(*it);
    }
  }
  // Add insert elements
  size_t middle_size = output_data.size();
  output_data.insert(output_data.end(), insert_records.begin(),
                     insert_records.end());
  auto begin_it = output_data.begin() + begin_size;
  auto middle_it = output_data.begin() + middle_size;
  // Perform merge
  std::inplace_merge(begin_it, middle_it, output_data.end(), less_fn);
  auto it = std::unique(
      output_data.begin(), output_data.end(),
      [=](const std::pair<KeyType, PID>& l, const std::pair<KeyType, PID>& r)
          -> bool {
            return (key_equal(l.first, r.first) && l.second == r.second);
          });
  output_data.erase(it, output_data.end());
}

template <typename KeyType, typename ValueType, class KeyComparator>
void BWTree<KeyType, ValueType, KeyComparator>::traverseAndConsolidateLeaf(
    FindLeafResult& leaf_info, BWNode* original_node,
    std::vector<BWNode*>& garbage_nodes,
    std::vector<std::pair<KeyType, std::vector<ValueType>>>& data, PID& sibling,
    bool& has_merge, BWNode*& merge_node, KeyType& lower_bound,
    KeyType& upper_bound) {
  using LessFnT = LessFn<KeyType, ValueType, KeyComparator>;

  LessFnT less_fn(m_key_less);
  std::vector<std::pair<KeyType, ValueType>> insert_records;
  std::set<std::pair<KeyType, ValueType>, LessFnT> delete_records(less_fn);

  bool has_split = false;
  KeyType split_separator_key = KeyType();
  PID new_sibling;

  has_merge = false;
  KeyType merge_separator_key = KeyType();
  merge_node = nullptr;

  BWNode* node = original_node;
  while (node->type != leaf) {
    switch (node->type) {
      case deltaInsert: {
        BWDeltaInsertNode* insert_node = static_cast<BWDeltaInsertNode*>(node);
        // First check if we pass the split
        if (!has_split ||
            key_less(insert_node->ins_record.first, split_separator_key)) {
          // If we have a delete for this record, don't add
          auto bounds = std::equal_range(
              delete_records.begin(), delete_records.end(),
              std::make_pair(insert_node->ins_record.first, ValueType{}),
              less_fn);

          bool found = false;
          for (auto del_it = bounds.first; del_it != bounds.second; ++del_it) {
            if (m_val_equal(del_it->second, insert_node->ins_record.second)) {
              found = true;
              break;
            }
          }

          if (!found) {
            insert_records.push_back(insert_node->ins_record);
          }
        }
        break;
      }
      case deltaDelete: {
        BWDeltaDeleteNode* delete_node = static_cast<BWDeltaDeleteNode*>(node);
        // Don't need to check if we pass the split because extra deletes
        // won't cause an issue
        delete_records.insert(delete_node->del_record);
        break;
      }
      case deltaSplit: {
        // Our invariant is that split nodes always force a consolidate, so
        // should be at the top
        assert(node == original_node);  // Ensure the split is at the top
        assert(!has_split);             // Ensure this is the only split
        BWDeltaSplitNode* split_node = static_cast<BWDeltaSplitNode*>(node);
        has_split = true;
        split_separator_key = split_node->separator_key;
        new_sibling = split_node->split_sibling;

        // Try to install IndexTermDeltaInsert
        installIndexTermDeltaInsert(leaf_info);
        break;
      }
      case deltaMerge: {
        // Same as split, invariant is that merge nodes always force a
        // consolidate, so should be at the top
        assert(node == original_node);
        BWDeltaMergeNode* merge_delta = static_cast<BWDeltaMergeNode*>(node);
        has_merge = true;
        merge_separator_key = merge_delta->separator_key;
        merge_node = merge_delta->merge_node;

        // Try to install IndexTermDeltaDelete
        if (!leaf_info.parent_pids.empty()) {
          installIndexTermDeltaDelete(leaf_info);
        }
        break;
      }
      default:
        assert(false);
    }

    garbage_nodes.push_back(node);
    node = static_cast<BWDeltaNode*>(node)->child_node;
    assert(node != nullptr);
  }
  garbage_nodes.push_back(node);
  std::sort(insert_records.begin(), insert_records.end(), less_fn);

  // node is a leaf node
  BWLeafNode* leaf_node = static_cast<BWLeafNode*>(node);

  lower_bound = leaf_node->lower_bound;

  if (has_split) {
    // Change sibling pointer if we did a split
    sibling = new_sibling;
    upper_bound = split_separator_key;
  } else {
    sibling = leaf_node->next;
    upper_bound = leaf_node->upper_bound;
  }

  consolidateModificationsLeaf(insert_records, delete_records,
                               leaf_node->data.begin(), leaf_node->data.end(),
                               less_fn, data);
}

template <typename KeyType, typename ValueType, class KeyComparator>
bool BWTree<KeyType, ValueType, KeyComparator>::consolidateLeafNode(
    FindLeafResult& leaf_info) {
  // Keep track of nodes so we can garbage collect later
  BWNode* original_node = leaf_info.node;

  std::vector<BWNode*> garbage_nodes;
  std::vector<std::pair<KeyType, std::vector<ValueType>>> data;
  PID sibling;
  bool has_merge = false;
  BWNode* merge_node = nullptr;
  KeyType lower_bound = KeyType::POS_INF_KEY;
  KeyType upper_bound = KeyType::NEG_INF_KEY;
  traverseAndConsolidateLeaf(leaf_info, original_node, garbage_nodes, data,
                             sibling, has_merge, merge_node, lower_bound,
                             upper_bound);
  if (has_merge) {
    BWNode* dummy_node;
    KeyType dummy_bound;
    traverseAndConsolidateLeaf(leaf_info, merge_node, garbage_nodes, data,
                               sibling, has_merge, dummy_node, dummy_bound,
                               upper_bound);
    assert(!has_merge);
  }
  assert(!key_equal(lower_bound, KeyType::POS_INF_KEY));
  assert(!key_equal(upper_bound, KeyType::NEG_INF_KEY));
  assert(key_lessequal(lower_bound, upper_bound));

  // Check size and insert split if needed
  BWNode* swap_node = nullptr;

  size_t data_size = data.size();
  bwt_printf("Consolidated data size: %lu\n", data_size);

  if (data_size > 0) {
    assert(key_lessequal(lower_bound, upper_bound));
    assert(key_lessequal(lower_bound, data.begin()->first));
    idb_assert(
        key_greater(upper_bound, (data.begin() + (data_size - 1))->first));
  }

  if (LEAF_NODE_SIZE_MAX < data_size) {
    bwt_printf("Data size greater than threshold, splitting...\n");
    // Find separator key by grabbing middle element
    // auto middle_it = data.begin() + data_size / 2;
    using LessFnT = LessFn<KeyType, std::vector<ValueType>, KeyComparator>;
    LessFnT less_fn(m_key_less);
    auto middle_it = std::lower_bound(data.begin(), data.end(),
                                      *(data.begin() + data_size / 2), less_fn);
    KeyType separator_key = middle_it->first;
    // Place second half in other node
    BWLeafNode* upper_leaf_node =
        new BWLeafNode(separator_key, upper_bound, sibling);
    assert(upper_leaf_node != nullptr);
    upper_leaf_node->data.insert(upper_leaf_node->data.end(), middle_it,
                                 data.end());
    // Install second node
    PID new_split_pid = installPage(upper_leaf_node);
    // Place first half in one node
    BWLeafNode* lower_leaf_node =
        new BWLeafNode(lower_bound, separator_key, new_split_pid);
    assert(lower_leaf_node != nullptr);
    lower_leaf_node->data.insert(lower_leaf_node->data.end(), data.begin(),
                                 middle_it);
    // Create split record
    idb_assert(key_lessequal(separator_key, upper_bound));
    BWDeltaSplitNode* split_node = new BWDeltaSplitNode(
        lower_leaf_node, separator_key, new_split_pid, upper_bound);
    assert(split_node != nullptr);
    swap_node = split_node;
  } else {
    BWLeafNode* consolidated_node =
        new BWLeafNode(lower_bound, upper_bound, sibling);
    assert(consolidated_node != nullptr);
    consolidated_node->data.swap(data);

    if (data_size < LEAF_NODE_SIZE_MIN &&
        // Left child of the parent should never be deleted
        !key_equal(findBounds(leaf_info.parent_nodes.back()).first,
                   lower_bound)) {
      bwt_printf("Data size less than threshold, placing remove node...\n");
      // Install a remove delta on top of the node
      BWDeltaRemoveNode* remove_node = new BWDeltaRemoveNode(consolidated_node);
      assert(remove_node != nullptr);
      swap_node = remove_node;
    } else {
      swap_node = consolidated_node;
    }
  }

  bool result = mapping_table[leaf_info.pid].compare_exchange_strong(
      original_node, swap_node);
  if (result) {
    // Succeeded, request garbage collection of processed nodes
    leaf_info.node = swap_node;
    addGarbageNodes(garbage_nodes);
  } else {
    // Failed, cleanup
    leaf_info.node = mapping_table[leaf_info.pid].load();
    if (swap_node->type == deltaRemove) {
      BWDeltaRemoveNode* node = static_cast<BWDeltaRemoveNode*>(swap_node);
      swap_node = node->child_node;
      delete node;
    }
    deleteDeltaChain(swap_node);
  }
  return result;
}

template <typename KeyType, typename ValueType, class KeyComparator>
void BWTree<KeyType, ValueType, KeyComparator>::traverseAndConsolidateInner(
    FindLeafResult& leaf_info, BWNode* original_node,
    std::vector<BWNode*>& garbage_nodes,
    std::vector<std::pair<KeyType, PID>>& separators, bool& has_merge,
    BWNode*& merge_node, KeyType& lower_bound, KeyType& upper_bound) {
  using LessFnT = LessFn<KeyType, PID, KeyComparator>;

  LessFnT less_fn(m_key_less);
  std::set<PID> insert_pids;
  std::vector<std::pair<KeyType, PID>> insert_separators;
  std::set<std::pair<KeyType, PID>, LessFnT> delete_separators(less_fn);

  // Split variables
  bool has_split = false;
  KeyType split_separator_key = KeyType();

  // Merge variables
  has_merge = false;
  KeyType merge_separator_key = KeyType();
  merge_node = nullptr;

  // Keep track of nodes so we can garbage collect later
  // NOTE(apoms): This logic is almost identical to the leaf node traversal
  // but its more efficient because it was my second time writing it
  BWNode* node = original_node;
  while (node->type != inner) {
    switch (node->type) {
      case deltaIndexTermInsert: {
        BWDeltaIndexTermInsertNode* insert_node =
            static_cast<BWDeltaIndexTermInsertNode*>(node);
        if (!has_split || key_less(insert_node->new_split_separator_key,
                                   split_separator_key)) {
          std::pair<KeyType, PID> ins_separator(
              insert_node->new_split_separator_key,
              insert_node->new_split_sibling);
          // If we have a delete for this record, don't add
          auto it = delete_separators.find(ins_separator);
          if (it == delete_separators.end()) {
            // Check for duplicates
            auto dup_it = insert_pids.find(ins_separator.second);
            if (dup_it == insert_pids.end()) {
              insert_pids.insert(ins_separator.second);
              insert_separators.push_back(ins_separator);
            }
          }
        }
        break;
      }
      case deltaIndexTermDelete: {
        BWDeltaIndexTermDeleteNode* delete_node =
            static_cast<BWDeltaIndexTermDeleteNode*>(node);
        delete_separators.insert(
            {delete_node->remove_node_low_key, delete_node->node_to_remove});
        break;
      }
      case deltaSplit: {
        // Our invariant is that split nodes always force a consolidate, so
        // should be at the top
        assert(node == original_node);  // Ensure the split is at the top
        assert(!has_split);             // Ensure this is the only split
        BWDeltaSplitNode* split_node = static_cast<BWDeltaSplitNode*>(node);
        has_split = true;
        split_separator_key = split_node->separator_key;

        // Try to install IndexTermDeltaInsert
        bwt_printf(
            "Before install index insert in consolidateInner, "
            "pid %lu, sibling %lu, parents %lu\n",
            leaf_info.pid, split_node->split_sibling,
            leaf_info.parent_pids.size());
        // idb_assert(leaf_info.pid != m_root.load());
        installIndexTermDeltaInsert(leaf_info);
        break;
      }
      case deltaMerge: {
        // Same as split, invariant is that merge nodes always force a
        // consolidate, so should be at the top
        assert(node == original_node);
        BWDeltaMergeNode* merge_delta = static_cast<BWDeltaMergeNode*>(node);
        has_merge = true;
        merge_separator_key = merge_delta->separator_key;
        merge_node = merge_delta->merge_node;

        idb_assert(!leaf_info.parent_pids.empty());
        installIndexTermDeltaDelete(leaf_info);
        break;
      }
      default:
        assert(false);
    }

    garbage_nodes.push_back(node);
    node = static_cast<BWDeltaNode*>(node)->child_node;
    assert(node != nullptr);
  }
  garbage_nodes.push_back(node);
  std::sort(insert_separators.begin(), insert_separators.end(), less_fn);

  BWInnerNode* inner_node = static_cast<BWInnerNode*>(node);

  lower_bound = inner_node->lower_bound;

  if (has_split) {
    upper_bound = split_separator_key;
  } else {
    upper_bound = inner_node->upper_bound;
  }

  consolidateModificationsInner(
      insert_separators, delete_separators, inner_node->separators.begin(),
      inner_node->separators.end(), less_fn, separators);
}

template <typename KeyType, typename ValueType, class KeyComparator>
bool BWTree<KeyType, ValueType, KeyComparator>::consolidateInnerNode(
    FindLeafResult& leaf_info) {
  BWNode* original_node = leaf_info.node;

  // Keep track of nodes so we can garbage collect later
  std::vector<BWNode*> garbage_nodes;
  std::vector<std::pair<KeyType, PID>> separators;
  bool has_merge = false;
  BWNode* merge_node = nullptr;
  KeyType lower_bound = KeyType::POS_INF_KEY;
  KeyType upper_bound = KeyType::NEG_INF_KEY;
  traverseAndConsolidateInner(leaf_info, original_node, garbage_nodes,
                              separators, has_merge, merge_node, lower_bound,
                              upper_bound);
  if (has_merge) {
    BWNode* dummy_node;
    KeyType dummy_bound;
    traverseAndConsolidateInner(leaf_info, merge_node, garbage_nodes,
                                separators, has_merge, dummy_node, dummy_bound,
                                upper_bound);
    assert(!has_merge);
  }
  assert(!key_equal(lower_bound, KeyType::POS_INF_KEY));
  assert(!key_equal(upper_bound, KeyType::NEG_INF_KEY));
  // Check size and insert split if needed
  BWNode* swap_node = nullptr;

  bool did_split = false;
  size_t data_size = separators.size();
  bwt_printf("Consolidated data size: %lu\n", data_size);
  if (data_size > 0) {
    assert(key_lessequal(lower_bound, upper_bound));
    idb_assert(key_lessequal(lower_bound, separators.begin()->first));
    idb_assert(key_greater(upper_bound,
                           (separators.begin() + (data_size - 1))->first));
  }

  bool result;
  if (leaf_info.parent_nodes.empty() && data_size == 1) {
    // Root node with one child
    // Attempt to swap child PID with root
    bwt_printf("Attempting to replace root with child...\n");
    PID leaf_pid = leaf_info.pid;
    PID child_pid = separators[0].second;
    idb_assert(false);
    result = m_root.compare_exchange_strong(leaf_pid, child_pid);
    idb_assert(false);
    if (result) {
      bwt_printf("Replaced root with prev root child successfully\n");
      addGarbageNodes(garbage_nodes);
    } else {
      bwt_printf("Failed to replace root with prev root child\n");
    }
  } else {
    if (INNER_NODE_SIZE_MAX < data_size) {
      bwt_printf("Data size greater than threshold, splitting...\n");
      // Find separator key by grabbing middle element
      // auto middle_it = separators.begin() + (data_size / 2);
      using LessFnT = LessFn<KeyType, PID, KeyComparator>;
      LessFnT less_fn(m_key_less);
      auto middle_it =
          std::lower_bound(separators.begin(), separators.end(),
                           *(separators.begin() + data_size / 2), less_fn);
      KeyType separator_key = middle_it->first;
      // Place first half in one node
      BWInnerNode* lower_inner_node =
          new BWInnerNode(lower_bound, separator_key);
      assert(lower_inner_node != nullptr);
      lower_inner_node->separators.insert(lower_inner_node->separators.end(),
                                          separators.begin(), middle_it);
      // Place second half in other node
      BWInnerNode* upper_inner_node =
          new BWInnerNode(separator_key, upper_bound);
      assert(upper_inner_node != nullptr);
      upper_inner_node->separators.insert(upper_inner_node->separators.end(),
                                          middle_it, separators.end());
      // Install second node
      PID new_split_pid = installPage(upper_inner_node);
      // Create split record
      idb_assert(key_lessequal(separator_key, upper_bound));
      BWDeltaSplitNode* split_node = new BWDeltaSplitNode(
          lower_inner_node, separator_key, new_split_pid, upper_bound);
      assert(split_node != nullptr);
      swap_node = split_node;
      did_split = true;
    } else {
      BWInnerNode* consolidated_node =
          new BWInnerNode(lower_bound, upper_bound);
      consolidated_node->separators.swap(separators);
      assert(consolidated_node != nullptr);

      if (data_size < INNER_NODE_SIZE_MIN &&
          // Never remove root
          !leaf_info.parent_nodes.empty() &&
          // Left child of the parent should never be deleted
          !key_equal(findBounds(leaf_info.parent_nodes.back()).first,
                     lower_bound)) {
        bwt_printf("Data size less than threshold, placing remove node...\n");
        // Install a remove delta on top of the node
        BWDeltaRemoveNode* remove_node =
            new BWDeltaRemoveNode(consolidated_node);
        assert(remove_node != nullptr);
        swap_node = remove_node;
      } else {
        swap_node = consolidated_node;
      }
    }

    // printf("before cex inner\n");
    result = mapping_table[leaf_info.pid].compare_exchange_strong(original_node,
                                                                  swap_node);
    // printf("after cex inner\n");
    // idb_assert(false);

    if (result) {
      // Succeeded, request garbage collection of processed nodes
      leaf_info.node = swap_node;
      addGarbageNodes(garbage_nodes);
    } else {
      leaf_info.node = mapping_table[leaf_info.pid].load();

      if (did_split) {
      }
      // Failed, cleanup
      if (swap_node->type == deltaRemove) {
        BWDeltaRemoveNode* node = static_cast<BWDeltaRemoveNode*>(swap_node);
        swap_node = node->child_node;
        delete node;
      }
      deleteDeltaChain(swap_node);
    }
  }
  return result;
}

template <typename KeyType, typename ValueType, class KeyComparator>
std::pair<KeyType, KeyType>
BWTree<KeyType, ValueType, KeyComparator>::findBounds(const BWNode* node) {
  const BWNode* current_node = node;
  KeyType lower_bound;
  KeyType upper_bound;
  bool upper_set = false;
  while (current_node != nullptr) {
    switch (current_node->type) {
      case PageType::leaf: {
        const BWLeafNode* leaf = static_cast<const BWLeafNode*>(current_node);
        lower_bound = leaf->lower_bound;
        if (!upper_set) {
          upper_bound = leaf->upper_bound;
        }
        current_node = nullptr;
        break;
      }
      case PageType::inner: {
        const BWInnerNode* inner =
            static_cast<const BWInnerNode*>(current_node);
        lower_bound = inner->lower_bound;
        if (!upper_set) {
          upper_bound = inner->upper_bound;
        }
        current_node = nullptr;
        break;
      }
      case PageType::deltaMerge: {
        const BWDeltaMergeNode* merge_node =
            static_cast<const BWDeltaMergeNode*>(current_node);
        upper_bound = merge_node->next_separator_key;
        upper_set = true;
        current_node = merge_node->child_node;
        break;
      }
      case PageType::deltaSplit: {
        const BWDeltaSplitNode* split_node =
            static_cast<const BWDeltaSplitNode*>(current_node);
        upper_bound = split_node->separator_key;
        upper_set = true;
        current_node = split_node->child_node;
        break;
      }
      default: {
        const BWDeltaNode* delta =
            static_cast<const BWDeltaNode*>(current_node);
        current_node = delta->child_node;
        break;
      }
    }
  }
  return {lower_bound, upper_bound};
}

template <typename KeyType, typename ValueType, class KeyComparator>
bool BWTree<KeyType, ValueType, KeyComparator>::isLeaf(BWNode* node) {
  BWNode* current_node = node;
  bool is_leaf = false;
  while (current_node != nullptr) {
    switch (current_node->type) {
      case PageType::deltaInsert:
      case PageType::deltaDelete:
      case PageType::leaf:
        is_leaf = true;
        current_node = nullptr;
        break;
      case PageType::deltaIndexTermInsert:
      case PageType::deltaIndexTermDelete:
      case PageType::inner:
        is_leaf = false;
        current_node = nullptr;
        break;
      default:
        BWDeltaNode* delta = static_cast<BWDeltaNode*>(current_node);
        current_node = delta->child_node;
        break;
    }
  }
  return is_leaf;
}

template <typename KeyType, typename ValueType, class KeyComparator>
bool BWTree<KeyType, ValueType, KeyComparator>::performConsolidation(
    FindLeafResult& leaf_info) {
  // Figure out if this is a leaf or inner node
  bool is_leaf = isLeaf(leaf_info.node);
  if (is_leaf) {
    return consolidateLeafNode(leaf_info);
  } else {
    return consolidateInnerNode(leaf_info);
  }
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool BWTree<KeyType, ValueType, KeyComparator>::fixSMO(FindLeafResult& owner,
                                                       bool& isSMO) {
  isSMO = true;
  switch (owner.node->type) {
    case PageType::deltaSplit: {
      bwt_printf("Before install index insert in fixSMO\n");
      installIndexTermDeltaInsert(owner);
      break;
    }
    case PageType::deltaMerge: {
      installIndexTermDeltaDelete(owner);
      break;
    }
    case PageType::deltaRemove: {
      installDeltaMerge(owner);
      return true;
      break;
    }
    default:
      isSMO = false;
      return false;
  }
  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void BWTree<KeyType, ValueType, KeyComparator>::fixSMOForInstall(
    FindLeafResult& leaf_info) {
  FindLeafResult info = leaf_info;
  bool isSMO;
  bwt_printf("Entering fixSMOForInstall with pid %lu\n", leaf_info.pid);
  while (true) {
    bwt_printf("first fixSMO\n");
    while (fixSMO(info, isSMO)) {
      bwt_printf("Looping on fixSMO\n");
      info.node = mapping_table[info.pid].load();
    }
    if (isSMO) {
      bwt_printf("Consolidating after fixSMO loop\n");
      performConsolidation(info);
    } else {
      break;
    }
    info.node = mapping_table[info.pid].load();
  }
  bwt_printf("Exiting fixSMOForInstall with pid %lu to pid %lu\n",
             leaf_info.pid, info.pid);
  leaf_info = info;
}

template <typename KeyType, typename ValueType, class KeyComparator>
typename BWTree<KeyType, ValueType, KeyComparator>::FindLeafResult
BWTree<KeyType, ValueType, KeyComparator>::findPID(PID pid) {
  BWNode* node = mapping_table[pid].load();
  auto bounds = findBounds(node);
  FindLeafResult result = findLeafPage(bounds.first);
  while (!result.parent_pids.empty()) {
    if (result.pid == pid) {
      break;
    }
    result.pop();
  }
  idb_assert(result.pid == pid);
  return result;
}

// Returns the first page where the key can reside
// For insert and delete this means the page on which delta record can be
// added
// For search means the first page the cursor needs to be constructed on
template <typename KeyType, typename ValueType, class KeyComparator>
typename BWTree<KeyType, ValueType, KeyComparator>::FindLeafResult
BWTree<KeyType, ValueType, KeyComparator>::findLeafPage(const KeyType& key) {
  // Root should always have a valid pid
  assert(m_root != NONE_PID);

// Every user method must go through this function, so let's register key
// here and then sort it
#ifdef INTERACTIVE_DEBUG
  idb.getKeyID(key);
#endif

#ifdef INTERACTIVE_DEBUG
  // If we entered interactive debugging then it would block on
  // lock() operation. If not if will then be running in parallel
  debug_stop_mutex.lock();
  debug_stop_mutex.unlock();
#endif

  bwt_printf("********* Start finding the leaf page *********\n");

  FindLeafResult leaf_info;
  PID& curr_pid = leaf_info.pid;
  BWNode*& curr_pid_root_node = leaf_info.node;

  curr_pid = m_root.load();
  curr_pid_root_node = mapping_table[curr_pid].load();
  BWNode* curr_node = curr_pid_root_node;

  assert(key_equal(findBounds(curr_node).first, KeyType::NEG_INF_KEY));
  if (curr_node->type != PageType::deltaSplit) {
    assert(key_equal(findBounds(curr_node).second, KeyType::POS_INF_KEY));
  }

  PID parent_pid = NONE_PID;
  BWNode* parent_pid_root_node = nullptr;
  int chain_length = 0;  // Length of delta chain, including current node

  // Trigger consolidation

  // Trigger structure modifying operations
  // split, remove, merge
  bool still_searching = true;
  while (still_searching) {
    assert(curr_node != nullptr);
    chain_length += 1;

    if (DELTA_CHAIN_INNER_THRESHOLD < chain_length) {
      bwt_printf(
          "Delta chain greater than threshold, performing "
          "consolidation...\n");
      performConsolidation(leaf_info);
      // Reset to top of chain
      curr_pid_root_node = mapping_table[curr_pid].load();
      curr_node = curr_pid_root_node;
      chain_length = 0;
      continue;
    }

    // Set by any delta node which wishes to traverse to a child
    bool request_traverse_child = false;
    bool request_traverse_split = false;
    // Set when posting to update index fails due to change in parent
    bool request_restart_top = false;
    PID child_pid = NONE_PID;

    switch (curr_node->type) {
      ////////////////////////////////////////////////////////////////////////////
      /// Index Term Insert
      case PageType::deltaIndexTermInsert: {
        bwt_printf("Traversing index term insert node\n");
        BWDeltaIndexTermInsertNode* index_insert_delta =
            static_cast<BWDeltaIndexTermInsertNode*>(curr_node);

        bwt_printf(
            "Index term insert node geq %d, le %d\n",
            key_greaterequal(key, index_insert_delta->new_split_separator_key),
            key_less(key, index_insert_delta->next_separator_key));
        if (key_greaterequal(key,
                             index_insert_delta->new_split_separator_key) &&
            key_less(key, index_insert_delta->next_separator_key)) {
          // Shortcut to child page
          request_traverse_child = true;
          child_pid = index_insert_delta->new_split_sibling;
        } else {
          // Keep going down chain
          curr_node = index_insert_delta->child_node;
        }
        break;
      }
      ////////////////////////////////////////////////////////////////////////////
      /// Index Term Delete
      case PageType::deltaIndexTermDelete: {
        bwt_printf("Traversing index term delete node\n");
        BWDeltaIndexTermDeleteNode* index_delete_delta =
            static_cast<BWDeltaIndexTermDeleteNode*>(curr_node);
        if (key_greaterequal(key, index_delete_delta->merge_node_low_key) &&
            key_less(key, index_delete_delta->next_separator_key)) {
          // Shortcut to child page
          request_traverse_child = true;
          child_pid = index_delete_delta->node_to_merge_into;
        } else {
          // Keep going down chain
          curr_node = index_delete_delta->child_node;
        }
        break;
      }
      ////////////////////////////////////////////////////////////////////////////
      /// Inner
      case PageType::inner: {
        bwt_printf("Traversing inner node\n");
        BWInnerNode* inner_node = static_cast<BWInnerNode*>(curr_node);
        // The consolidate has to ensure that it does not leave empty
        // inner nodes
        assert(inner_node->separators.size() > 0);

        // TODO Change this to binary search
        PID next_pid = inner_node->separators.back().second;
        for (int i = 1; i < inner_node->separators.size(); i++) {
          bwt_printf("Inside for loop, i = %d, pid %lu\n", i,
                     inner_node->separators[i - 1].second);
          if (key_less(key, inner_node->separators[i].first)) {
            next_pid = inner_node->separators[i - 1].second;
            break;
          }
        }

        request_traverse_child = true;
        child_pid = next_pid;
        break;
      }
      ////////////////////////////////////////////////////////////////////////////
      /// Leaf Insert
      case PageType::deltaInsert: {
        bwt_printf("Traversing insert node\n");
        BWDeltaInsertNode* insert_node =
            static_cast<BWDeltaInsertNode*>(curr_node);
        curr_node = insert_node->child_node;
        assert(curr_node != nullptr);
        break;
      }
      ////////////////////////////////////////////////////////////////////////////
      /// Leaf Delete
      case PageType::deltaDelete: {
        bwt_printf("Traversing delete node\n");
        BWDeltaDeleteNode* delete_node =
            static_cast<BWDeltaDeleteNode*>(curr_node);
        curr_node = delete_node->child_node;
        assert(curr_node != nullptr);
        break;
      }
      ////////////////////////////////////////////////////////////////////////////
      /// Leaf
      case PageType::leaf: {
        bwt_printf("Traversing leaf node\n");

        BWLeafNode* leaf_node = static_cast<BWLeafNode*>(curr_node);

        bwt_printf("leaf_node_size = %lu\n", leaf_node->data.size());

        KeyType lower_bound;
        KeyType upper_bound;
        std::tie(lower_bound, upper_bound) = findBounds(curr_node);
        bool geq = key_greaterequal(key, lower_bound);
        (void)geq;
        idb_assert(geq);
        bool le = key_less(key, upper_bound);
        if (!le) {
          // Traverse to sibling
          request_traverse_split = true;
          child_pid = leaf_node->next;
        } else {
// Check that we have not ended up on the wrong page for the key
#ifdef BWTREE_DEBUG
          bwt_printf("key_greaterequal = %d\n", geq);
          bwt_printf("key_le = %d\n", le);
          bwt_printf("(lower == upper) = %d\n",
                     key_equal(lower_bound, upper_bound));
#endif
#ifdef INTERACTIVE_DEBUG
          // We cannot do this in multi-threaded environment since
          // vector is not thread-safe
          // idb.pid_list.push_back(curr_pid);
          idb_assert_key(key,
                         geq && (key_equal(lower_bound, upper_bound) || le));
// idb.pid_list.pop_back();
#endif

          still_searching = false;
        }
        break;
      }
      ////////////////////////////////////////////////////////////////////////////
      /// Split
      case PageType::deltaSplit: {
        bwt_printf("Traversing split node\n");
        // Our invariant is that there should be no delta chains on top of a
        // split node
        idb_assert(chain_length == 1);
        BWDeltaSplitNode* split_delta =
            static_cast<BWDeltaSplitNode*>(curr_node);

        // Install an IndexTermDeltaInsert and retry till it succeds. Thread
        // cannot proceed until this succeeds
        // 1) This might install multiple updates which say the same thing.
        // The
        //    consolidate must be able to handle the duplicates
        // 2) The parent_pid might not be correct because the parent might
        // have
        //    merged into some other node. This needs to be detected by the
        //    installIndexTermDeltaInsert and return the
        //    install_node_invalid
        //    which triggers a search from the root.

        // Must handle the case where the parent_pid is NONE_PID
        // Attempt to create a new inner node
        if (parent_pid == NONE_PID) {
          // Create new root inner ndoe
          BWInnerNode* new_root =
              new BWInnerNode(KeyType::NEG_INF_KEY, KeyType::POS_INF_KEY);
          assert(new_root != nullptr);
          // Add current root as child and new split sibling
          new_root->separators.emplace_back(KeyType::NEG_INF_KEY, curr_pid);
          new_root->separators.emplace_back(split_delta->separator_key,
                                            split_delta->split_sibling);
          // Install the new root
          PID new_root_pid = installPage(new_root);
          // Try to install the root
          bwt_printf("Before cex root in findLeaf\n");
          bool result = (curr_node == mapping_table[curr_pid].load());
          // idb.pid_list.clear();
          // idb.pid_list.push_back(curr_pid);
          // idb.pid_list.push_back(new_root_pid);
          // idb.pid_list.push_back(m_root.load());
          // idb_assert(0 == 1);
          result = m_root.compare_exchange_strong(curr_pid, new_root_pid);
          // idb.pid_list.clear();
          // idb.pid_list.push_back(curr_pid);
          // idb.pid_list.push_back(new_root_pid);
          // idb.pid_list.push_back(m_root.load());
          // idb_assert(0 == 2);

          if (result) {
            bwt_printf("Replaced new root successfully with PID %lu.\n",
                       new_root_pid);

            parent_pid = new_root_pid;
            parent_pid_root_node = new_root;
            leaf_info.parent_pids.clear();
            leaf_info.parent_nodes.clear();
            leaf_info.parent_pids.push_back(parent_pid);
            leaf_info.parent_nodes.push_back(parent_pid_root_node);
            // idb_assert(false);
          } else {
            // Should cleanup the root page but for now this will be cleaned
            // up in the destructor
            bwt_printf("Compare exchange with root failed, restarting...");
          }
          request_restart_top = true;
        }
        if (!request_restart_top) {
          if (key_greaterequal(key, split_delta->separator_key)) {
            // idb_assert_key(key, key_less(key,
            // split_delta->next_separator_key));
            // Add index term delta insert
            bwt_printf("Before install index insert in findLeaf\n");
            installIndexTermDeltaInsert(leaf_info);
            bwt_printf("Split index parent install insert success\n");
            request_traverse_split = true;
            child_pid = split_delta->split_sibling;
          } else {
            curr_node = split_delta->child_node;
          }
        }
        break;
      }
      ////////////////////////////////////////////////////////////////////////////
      /// Remove
      case PageType::deltaRemove: {
        bwt_printf("Traversing remove node\n");
        // Note: should not trigger a remove on the left most leaf even if
        // the
        // number of tuples is below a threshold
        idb_assert(chain_length == 1);
        installDeltaMerge(leaf_info);

        request_traverse_split = true;
        child_pid = leaf_info.pid;
        break;
      }
      ////////////////////////////////////////////////////////////////////////////
      /// Merge
      case PageType::deltaMerge: {
        bwt_printf("Traversing merge node\n");
        // Our invariant is that there should be no delta chains on top of a
        // merge node
        idb_assert(chain_length == 1);
        BWDeltaMergeNode* merge_delta =
            static_cast<BWDeltaMergeNode*>(curr_node);

        // Install an IndexTermDeltaDelete and retry till it succeds. Thread
        // cannot proceed until this succeeds
        // 1) This might install multiple updates which say the same thing.
        // The
        //    consolidate must be able to handle the duplicates
        // 2) The parent_pid might not be correct because the parent might
        // have
        //    merged into some other node. This needs to be detected by the
        //    installIndexTermDeltaDelete and return the
        //    install_node_invalid
        //    which triggers a search from the root.

        installIndexTermDeltaDelete(leaf_info);

        if (key_greaterequal(key, merge_delta->separator_key)) {
          curr_node = merge_delta->merge_node;
        } else {
          curr_node = merge_delta->child_node;
        }
        break;
      }
      default:
        assert(false);
    }

    if (request_traverse_split) {
      bwt_printf("Request to traverse to split or remove PID %lu\n", child_pid);
      curr_pid = child_pid;
      curr_pid_root_node = mapping_table[curr_pid].load();
      curr_node = curr_pid_root_node;
      chain_length = 0;
    }

    if (request_traverse_child) {
      bwt_printf("Request to traverse to child PID %lu from parent %lu\n",
                 child_pid, curr_pid);
      parent_pid = curr_pid;
      parent_pid_root_node = curr_pid_root_node;
      leaf_info.parent_pids.push_back(parent_pid);
      leaf_info.parent_nodes.push_back(parent_pid_root_node);
      curr_pid = child_pid;
      curr_pid_root_node = mapping_table[curr_pid].load();
      curr_node = curr_pid_root_node;
      chain_length = 0;
    }

    if (request_restart_top) {
      bwt_printf("Request to restart from top %lu\n", curr_pid);
      parent_pid = NONE_PID;
      parent_pid_root_node = nullptr;
      leaf_info.parent_pids.clear();
      leaf_info.parent_nodes.clear();
      curr_pid = m_root.load();
      curr_pid_root_node = mapping_table[curr_pid].load();
      curr_node = curr_pid_root_node;
      chain_length = 0;
    }

  }  // while(1)
  bwt_printf("Finished findLeafPage with PID %lu\n", curr_pid);

  return leaf_info;
}

// This function will assign a page ID for a given page, and put that page
// into the mapping table
template <typename KeyType, typename ValueType, class KeyComparator>
typename BWTree<KeyType, ValueType, KeyComparator>::PID
BWTree<KeyType, ValueType, KeyComparator>::installPage(BWNode* new_node_p) {
  // Let's assume first this will not happen; If it happens
  // then we change this to a DEBUG output
  // Need to use a variable length data structure
  assert(next_pid < max_table_size);

  // Though it is operating on std::atomic<PID>, the ++ operation
  // will be reflected to the underlying storage
  // Also threads will be serialized here to get their own PID
  // Once a PID is assigned, different pages on different slots will
  // interfere with each other
  PID assigned_pid = next_pid++;
  mapping_table[assigned_pid] = new_node_p;

  current_mapping_table_size++;

  return assigned_pid;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
typename BWTree<KeyType, ValueType, KeyComparator>::PID
BWTree<KeyType, ValueType, KeyComparator>::findNext(BWNode* node) {
  PID next = NONE_PID;
  while (node != nullptr) {
    switch (node->type) {
      case leaf: {
        next = static_cast<BWLeafNode*>(node)->next;
        node = nullptr;
        break;
      }
      default: { node = static_cast<BWDeltaNode*>(node)->child_node; }
    }
  }
  assert(next != NONE_PID);
  return next;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void BWTree<KeyType, ValueType, KeyComparator>::installDeltaInsert(
    const FindLeafResult& leaf_info, const KeyType& key,
    const ValueType& value) {
  auto ins_record = std::pair<KeyType, ValueType>(key, value);

  FindLeafResult info = leaf_info;
  while (true) {
    fixSMOForInstall(info);

    // Leaf might have split, check if we are still in bounds
    if (key_greaterequal(key, findBounds(info.node).second)) {
      info.pid = findNext(info.node);
      idb_assert(info.pid != NONE_PID);
      info.node = mapping_table[info.pid].load();
      continue;
    }

    BWNode* old_leaf_p = info.node;

    BWNode* new_leaf_p = (BWNode*)new BWDeltaInsertNode(old_leaf_p, ins_record);
    assert(new_leaf_p != nullptr);

    // Either the page has been consolidated, in which case we try again,
    // or the page has been appended another record, in which case we also
    // try agaijn
    if (mapping_table[info.pid].compare_exchange_strong(old_leaf_p,
                                                        new_leaf_p)) {
      return;
    } else {
      delete new_leaf_p;
      info.node = mapping_table[info.pid].load();
    }
  }
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void BWTree<KeyType, ValueType, KeyComparator>::installDeltaDelete(
    const FindLeafResult& leaf_info, const KeyType& key,
    const ValueType& value) {
  auto delete_record = std::pair<KeyType, ValueType>(key, value);

  FindLeafResult info = leaf_info;
  while (true) {
    fixSMOForInstall(info);

    // Leaf might have split, check if we are still in bounds
    if (key_greaterequal(key, findBounds(info.node).second)) {
      info.pid = findNext(info.node);
      idb_assert(info.pid != NONE_PID);
      info.node = mapping_table[info.pid].load();
      continue;
    }

    BWNode* old_leaf_p = info.node;

    BWNode* new_leaf_p =
        (BWNode*)new BWDeltaDeleteNode(old_leaf_p, delete_record);
    assert(new_leaf_p != nullptr);

    // Either the page has been consolidated, in which case we try again,
    // or the page has been appended another record, in which case we also
    // try agaijn
    if (mapping_table[info.pid].compare_exchange_strong(old_leaf_p,
                                                        new_leaf_p)) {
      return;
    } else {
      delete new_leaf_p;
      info.node = mapping_table[info.pid].load();
    }
  }
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void BWTree<KeyType, ValueType, KeyComparator>::installIndexTermDeltaInsert(
    FindLeafResult& leaf_info) {
  BWDeltaSplitNode* split_node =
      dynamic_cast<BWDeltaSplitNode*>(leaf_info.node);
  assert(split_node != nullptr);

  if (leaf_info.parent_pids.empty()) {
    // Create new root inner ndoe
    BWInnerNode* new_root =
        new BWInnerNode(KeyType::NEG_INF_KEY, KeyType::POS_INF_KEY);
    assert(new_root != nullptr);
    // Add current root as child and new split sibling
    new_root->separators.emplace_back(KeyType::NEG_INF_KEY, leaf_info.pid);
    new_root->separators.emplace_back(split_node->separator_key,
                                      split_node->split_sibling);
    // Install the new root
    PID new_root_pid = installPage(new_root);
    // Try to install the root
    // idb.pid_list.clear();
    // idb.pid_list.push_back(leaf_info.pid);
    // idb.pid_list.push_back(new_root_pid);
    // idb.pid_list.push_back(m_root.load());
    // idb_assert(0 == 1);
    bool result = m_root.compare_exchange_strong(leaf_info.pid, new_root_pid);
    // idb.pid_list.clear();
    // idb.pid_list.push_back(leaf_info.pid);
    // idb.pid_list.push_back(new_root_pid);
    // idb.pid_list.push_back(m_root.load());
    // idb_assert(0 == 2);
    if (result) {
      bwt_printf("Replaced new root successfully with PID %lu.\n",
                 new_root_pid);

      leaf_info.parent_pids.clear();
      leaf_info.parent_nodes.clear();
      leaf_info.parent_pids.push_back(new_root_pid);
      leaf_info.parent_nodes.push_back(new_root);
      // idb_assert(false);
    } else {
      // Should cleanup the root page but for now this will be cleaned
      // up in the destructor
      bwt_printf("Compare exchange with root failed, someone else did it...\n");
      leaf_info = findPID(leaf_info.pid);
    }
    return;
  }
  assert(!leaf_info.parent_pids.empty());
  FindLeafResult parent_info = leaf_info;
  parent_info.pop();

  while (true) {
    fixSMOForInstall(parent_info);

    PID install_pid = parent_info.pid;
    BWNode* old_inner_p = parent_info.node;

    KeyType new_separator_key = split_node->separator_key;
    PID split_sibling = split_node->split_sibling;
    KeyType next_separator_key = split_node->next_separator_key;

    // Check if already installed
    BWNode* c_node = old_inner_p;
    while (c_node != nullptr) {
      switch (c_node->type) {
        case deltaIndexTermInsert: {
          BWDeltaIndexTermInsertNode* insert_node =
              static_cast<BWDeltaIndexTermInsertNode*>(c_node);
          if (insert_node->new_split_sibling == split_sibling) {
            return;
          }
          c_node = insert_node->child_node;
          break;
        }
        case inner: {
          BWInnerNode* inner_node = static_cast<BWInnerNode*>(c_node);
          for (int i = 0; i < inner_node->separators.size(); i++) {
            bwt_printf("Inside for loop, i = %d\n", i);
            if (inner_node->separators[i].second == split_sibling) {
              return;
            }
          }
          c_node = nullptr;
          break;
        }
        default: {
          BWDeltaNode* delta = static_cast<BWDeltaNode*>(c_node);
          c_node = delta->child_node;
        }
      }
    }
    auto nb = findBounds(old_inner_p);

    if (key_greaterequal(new_separator_key, nb.second)) {
      // This means that the split sibling was already cut off into
      // another adjacent sibling of the parent inner node
      return;
    }

    // auto b = findBounds(split_node);
    // idb_assert(key_equal(b.second, new_separator_key));
    // auto b2 = findBounds(mapping_table[split_sibling].load());
    // idb_assert(key_equal(b2.first, new_separator_key));
    // idb_assert(key_equal(b2.second, next_separator_key));

    idb_assert(key_lessequal(new_separator_key, next_separator_key));
    BWNode* new_inner_p = (BWNode*)new BWDeltaIndexTermInsertNode(
        old_inner_p, new_separator_key, split_sibling, next_separator_key);
    assert(new_inner_p != nullptr);

    if (mapping_table[install_pid].compare_exchange_strong(old_inner_p,
                                                           new_inner_p)) {
      parent_info.node = new_inner_p;
      parent_info.push(leaf_info.pid, leaf_info.node);
      leaf_info = parent_info;
      return;
    } else {
      parent_info.node = mapping_table[parent_info.pid].load();
      delete new_inner_p;
    }
  }
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void BWTree<KeyType, ValueType, KeyComparator>::installIndexTermDeltaDelete(
    FindLeafResult& leaf_info) {
  BWDeltaMergeNode* merge_node = static_cast<BWDeltaMergeNode*>(leaf_info.node);
  PID merge_pid = leaf_info.pid;

  FindLeafResult parent_info = leaf_info;
  parent_info.pop();

  while (true) {
    fixSMOForInstall(parent_info);

    PID install_pid = parent_info.pid;
    BWNode* old_inner_p = parent_info.node;

    // Check if already installed
    BWNode* c_node = old_inner_p;
    while (c_node != nullptr) {
      switch (c_node->type) {
        case deltaIndexTermDelete: {
          BWDeltaIndexTermDeleteNode* delete_node =
              static_cast<BWDeltaIndexTermDeleteNode*>(c_node);
          if (delete_node->node_to_remove == merge_node->node_to_remove) {
            return;
          }
          c_node = delete_node->child_node;
          break;
        }
        case inner: {
          BWInnerNode* inner_node = static_cast<BWInnerNode*>(c_node);
          bool found = false;
          for (int i = 0; i < inner_node->separators.size(); i++) {
            bwt_printf("Inside for loop, i = %d\n", i);
            if (inner_node->separators[i].second ==
                merge_node->node_to_remove) {
              found = true;
              break;
            }
          }
          if (!found) {
            return;
          }
          c_node = nullptr;
          break;
        }
        default: {
          BWDeltaNode* delta = static_cast<BWDeltaNode*>(c_node);
          c_node = delta->child_node;
        }
      }
    }

    idb_assert(merge_pid != merge_node->node_to_remove);
    BWNode* new_inner_p = (BWNode*)new BWDeltaIndexTermDeleteNode(
        old_inner_p, merge_pid, merge_node->node_to_remove,
        findBounds(merge_node).first, merge_node->separator_key,
        merge_node->next_separator_key);
    assert(new_inner_p != nullptr);

    if (mapping_table[install_pid].compare_exchange_strong(old_inner_p,
                                                           new_inner_p)) {
      parent_info.node = new_inner_p;
      parent_info.push(leaf_info.pid, leaf_info.node);
      leaf_info = parent_info;
      return;
    } else {
      parent_info.node = mapping_table[parent_info.pid].load();
      delete new_inner_p;
    }
  }
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void BWTree<KeyType, ValueType, KeyComparator>::installDeltaMerge(
    FindLeafResult& leaf_info) {
  BWDeltaNode* remove_node = dynamic_cast<BWDeltaNode*>(leaf_info.node);
  PID remove_pid = leaf_info.pid;
  KeyType remove_lower_bound;
  KeyType remove_upper_bound;
  std::tie(remove_lower_bound, remove_upper_bound) = findBounds(remove_node);

  FindLeafResult parent_info = leaf_info;
  parent_info.pop();

  while (true) {
    // Handles remove on the parent
    fixSMOForInstall(parent_info);

    BWNode* c_node = parent_info.node;
    PID s_pid = NONE_PID;
    while (c_node != nullptr) {
      switch (c_node->type) {
        case PageType::deltaMerge: {
          BWDeltaMergeNode* merge_delta =
              static_cast<BWDeltaMergeNode*>(c_node);
          if (key_greater(remove_lower_bound, merge_delta->separator_key)) {
            c_node = merge_delta->merge_node;
          } else {
            c_node = merge_delta->child_node;
          }
          break;
        }
        case PageType::deltaSplit: {
          BWDeltaSplitNode* split_delta =
              static_cast<BWDeltaSplitNode*>(c_node);
          // Should be to the left, so never need to go right
          assert(!key_greater(remove_lower_bound, split_delta->separator_key));
          c_node = split_delta->child_node;
          break;
        }
        case PageType::deltaIndexTermInsert: {
          BWDeltaIndexTermInsertNode* insert_delta =
              static_cast<BWDeltaIndexTermInsertNode*>(c_node);
          if (key_equal(remove_lower_bound, insert_delta->next_separator_key)) {
            s_pid = insert_delta->new_split_sibling;
            c_node = nullptr;
          } else {
            c_node = insert_delta->child_node;
          }
          break;
        }
        case PageType::deltaIndexTermDelete: {
          BWDeltaIndexTermDeleteNode* delete_delta =
              static_cast<BWDeltaIndexTermDeleteNode*>(c_node);
          if (key_equal(remove_lower_bound,
                        delete_delta->remove_node_low_key)) {
            // Delete is already installed, means merge was installed
            parent_info.push(
                delete_delta->node_to_merge_into,
                mapping_table[delete_delta->node_to_merge_into].load());
            leaf_info = parent_info;
            return;
          } else if (key_equal(remove_lower_bound,
                               delete_delta->next_separator_key)) {
            // Merge into node that was just merged into
            s_pid = delete_delta->node_to_merge_into;
            c_node = nullptr;
          } else {
            c_node = delete_delta->child_node;
          }
          break;
        }
        case PageType::inner: {
          BWInnerNode* inner_node = static_cast<BWInnerNode*>(c_node);

          s_pid = inner_node->separators.back().second;
          for (int i = 1; i < inner_node->separators.size(); i++) {
            bwt_printf("Inside for loop, i = %d\n", i);
            if (key_lessequal(remove_lower_bound,
                              inner_node->separators[i].first)) {
              s_pid = inner_node->separators[i - 1].second;
              break;
            }
          }
          c_node = nullptr;
          break;
        }
        default:
          idb_assert(false);
          printf("type %d, remove on %lu with parent pid %lu\n", c_node->type,
                 remove_pid, parent_info.pid);
          idb_assert(false);
      }
    }
    bwt_printf("s_pid %lu\n", s_pid);
    idb_assert(s_pid != NONE_PID);
    idb_assert(s_pid != leaf_info.pid);
    BWNode* sibling_node = mapping_table[s_pid].load();

    parent_info.push(s_pid, sibling_node);

    // Don't install if already installed
    if (BWDeltaMergeNode* merge_node =
            dynamic_cast<BWDeltaMergeNode*>(sibling_node)) {
      if (merge_node->node_to_remove == remove_pid) {
        leaf_info = parent_info;
        return;
      }
    }

    fixSMOForInstall(parent_info);

    bwt_printf("s_pid after install %lu\n", parent_info.pid);
    idb_assert(parent_info.pid != NONE_PID);
    idb_assert(parent_info.pid != remove_pid);

    if (key_greater(findBounds(parent_info.node).second, remove_lower_bound)) {
      // Already merged
      leaf_info = parent_info;
      return;
    }

    BWNode* new_p = (BWNode*)new BWDeltaMergeNode(
        parent_info.node, remove_pid, remove_lower_bound,
        remove_node->child_node, remove_upper_bound);
    assert(new_p != nullptr);

    if (mapping_table[parent_info.pid].compare_exchange_strong(parent_info.node,
                                                               new_p)) {
      parent_info.node = new_p;
      leaf_info = parent_info;
      return;
    } else {
      bwt_printf("Failed to install merge... retrying...\n");
      delete new_p;
      parent_info.pop();
      parent_info = findPID(parent_info.pid);
    }
  }
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void BWTree<KeyType, ValueType, KeyComparator>::deleteDeltaChain(BWNode* node) {
  while (node != nullptr) {
    switch (node->type) {
      case PageType::deltaInsert:
        bwt_printf("Freeing insert node\n");
        break;
      case PageType::deltaDelete:
        bwt_printf("Freeing delete node\n");
        break;
      case PageType::deltaIndexTermInsert:
        bwt_printf("Freeing index insert node\n");
        break;
      case PageType::deltaIndexTermDelete:
        bwt_printf("Freeing index delete node\n");
        break;
      case PageType::deltaSplit:
        bwt_printf("Freeing split node\n");
        break;
      case PageType::deltaRemove:
        bwt_printf("Freeing remove node\n");
        break;
      case PageType::deltaMerge:
        bwt_printf("Freeing merge node\n");
        break;
      case PageType::inner:
        bwt_printf("Freeing inner node\n");
        break;
      case PageType::leaf:
        bwt_printf("Freeing leaf node\n");
        break;
      default:
        assert(false);
    }

    switch (node->type) {
      case PageType::deltaInsert:
      case PageType::deltaDelete:
      case PageType::deltaIndexTermInsert:
      case PageType::deltaIndexTermDelete:
      case PageType::deltaSplit: {
        BWDeltaNode* delta = static_cast<BWDeltaNode*>(node);
        node = delta->child_node;
        delete delta;
        break;
      }
      case PageType::deltaRemove: {
        delete node;
        node = nullptr;
        break;
      }
      case PageType::deltaMerge: {
        BWDeltaMergeNode* merge_node = static_cast<BWDeltaMergeNode*>(node);
        deleteDeltaChain(merge_node->merge_node);
        node = merge_node->child_node;
        delete merge_node;
        break;
      }
      case PageType::inner:
      case PageType::leaf: {
        delete node;
        node = nullptr;
        break;
      }
      default:
        assert(false);
    }
  }
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void BWTree<KeyType, ValueType, KeyComparator>::addGarbageNodes(
    std::vector<BWNode*>& garbage) {
  while (!m_garbage_mutex.try_lock())
    ;
  m_garbage_nodes.insert(m_garbage_nodes.end(), garbage.begin(), garbage.end());
  m_garbage_mutex.unlock();
}

/////////////////////////////////////////////////////////////////////
// Epoch Manager Member Functions
/////////////////////////////////////////////////////////////////////

/*
 * EpochManager Constructor - Initialize current epoch to 0
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
BWTree<KeyType, ValueType, KeyComparator>::BWTree::EpochManager::EpochManager()
    : current_epoch(0), join_counter(0) {
  return;
}

/*
 * advanceEpoch() - Advance to a new epoch. All older epoch garbages are now
 * pending to be collected once all older epoches has cleared
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
void BWTree<KeyType, ValueType,
            KeyComparator>::BWTree::EpochManager::advanceEpoch() {
  bool retry = false;
  do {
    bw_epoch_t old_epoch = current_epoch.load();
    bw_epoch_t new_epoch = old_epoch++;
    retry = current_epoch.compare_exchange_strong(old_epoch, new_epoch);
  } while (retry == false);

  return;
}

/*
 * EpochManager::joinEpoch() - A thread joins the current epoch
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
typename BWTree<KeyType, ValueType, KeyComparator>::EpochManager::bw_epoch_t
BWTree<KeyType, ValueType, KeyComparator>::EpochManager::joinEpoch() {
  lock.lock();
  // Critical section begin

  bw_epoch_t e = current_epoch.load();
  auto it = garbage_list.find(e);

  // This is the first thread that joins this epoch
  // Create a new record
  if (it == garbage_list.end()) {
    garbage_list[e] = EpochRecord{};
  } else {
    // Otherwise just increase its count
    EpochRecord er = std::move(it->second);
    er.thread_count++;
    garbage_list[e] = std::move(er);
  }

  // This does not need be done atomically
  join_counter++;
  if (join_counter > EpochManager::join_threshold) {
    advanceEpoch();
    join_counter = 0;
  }

  // Critical section end
  lock.unlock();
  return e;
}

/*
 * EpochManager::leaveEpoch() - Leave an epoch that a thread was in
 *
 * This function decreases the corrsponding epoch counter by 1, and if it
 * goes to 0, just remove all references inside that epoch since now we
 * know nobody could be referencing to the nodes
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
void BWTree<KeyType, ValueType, KeyComparator>::EpochManager::leaveEpoch(
    bw_epoch_t e) {
  lock.lock();

  auto it = garbage_list.find(e);
  assert(it != garbage_list.end());

  EpochRecord er = std::move(it->second);
  assert(er.thread_count > 0);
  er.thread_count--;

  bool need_clean = (er.thread_count == 0) && (current_epoch.load() != e);

  garbage_list[e] = std::move(er);

  if (need_clean == true) {
    sweepAndClean();
  }

  lock.unlock();
  return;
}

/*
 * EpochManager::sweepAndClean() - Cleans oldest epochs whose ref count is 0
 *
 * We never free memory for the current epoch (there might be a little bit
 *delay)
 * We stops scanning the list of epoches once an epoch whose
 * ref count != not 0 is seen
 *
 * NOTE: This function must be called under critical section
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
void BWTree<KeyType, ValueType, KeyComparator>::EpochManager::sweepAndClean() {
  // This could be a little bit late compared the real time epoch
  // but it is OK since we could recycle what we have missed in the next run
  bw_epoch_t e = current_epoch.load();

  for (auto it = garbage_list.begin(); it != garbage_list.end(); it++) {
    assert(it->first <= e);

    if (it->first == e) {
      break;
    } else if (it->second.thread_count > 0) {
      // We stop when some epoch still has ongoing threads
      break;
    }

    std::vector<BWNode*> node_list = std::move(it->second);

    for (auto it2 = node_list.begin(); it2 != node_list.end(); it2++) {
      // TODO: Maybe need to declare destructor as virtual?
      delete it2;
    }

    garbage_list.erase(it->first);
  }

  return;
}

/*
 * EpochManager::addGarbageNode() - Adds a garbage node into the current
 *epoch
 *
 * NOTE: We do not add it to the thread's join epoch since remove actually
 *happens
 * after that, therefore other threads could observe the node after the join
 *thread
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
void BWTree<KeyType, ValueType, KeyComparator>::EpochManager::addGarbageNode(
    BWNode* node_p) {
  lock.lock();
  // Critical section begins

  // This might be a little bit late but it is OK since when the node is
  // unlinked
  // we are sure that the real e is smaller than or equal to this e
  // So all threads after this e could not see the unlinked node still holds
  bw_epoch_t e = current_epoch.load();

  auto it = garbage_list.find(e);
  assert(it != garbage_list.end());

  EpochRecord er = std::move(it->second);
  er.node_list.push_back(node_p);
  garbage_list[e] = std::move(er);

  // Critical section ends
  lock.unlock();
  return;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
KeyComparator*
    BWTree<KeyType, ValueType,
           KeyComparator>::InteractiveDebugger::IDBKeyCmp::m_key_less_p =
        nullptr;

}  // End index namespace
}  // End peloton namespace
