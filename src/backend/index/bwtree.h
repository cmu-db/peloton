//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// bwTree.h
//
// Identification: src/backend/index/bwtree.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
//
// NOTE: If you encounter any bug, assertion failure, segment fault or
// other anomalies, please contact:
//
// Ziqi Wang
// ziqiw a.t. andrew.cmu.edu
//
// in order to get a quick response and diagnosis
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>
#include <atomic>
#include <algorithm>
#include <cassert>
#include <unordered_map>
#include <mutex>
#include <string>
#include <iostream>
#include <set>
#include <unordered_set>
#include <map>
#include <stack>
#include <thread>
#include <chrono>

/*
 * BWTREE_PELOTON - Specifies whether Peloton-specific features are
 *                  Compiled or not
 *                  We strive to make BwTree a standalone and independent
 *                  module that can be plugged-and-played in any situation
 */
#define BWTREE_PELOTON

// Used for debugging
#include <mutex>

#ifdef BWTREE_PELOTON
// This contains IndexMetadata definition
// It uses peloton::index namespace
#include "backend/index/index.h"
#endif

/*
 * BWTREE_DEBUG - This flag enables assertions that check for
 *                structural consistency
 *                DO NOT RECOMMEND REMOVING
 */
#define BWTREE_DEBUG

/*
 * INTERACTIVE_DEBUG - This flag enables interactive debugger
 *                     which serializes CAS operation using a
 *                     lock, halting all threads before starting
 *                     the interactive debugger when assertion fails
 *                     RECOMMEND DEACTIVATING WHEN RELASING
 */
//#define INTERACTIVE_DEBUG

/*
 * ALL_PUBLIC - This flag makes all private members become public
 *              to simplify debugging
 */
#define ALL_PUBLIC

/*
 * BWTREE_TEMPLATE_ARGUMENTS - Save some key strokes
 */
#define BWTREE_TEMPLATE_ARGUMENTS template <typename KeyType, \
                                            typename ValueType, \
                                            typename KeyComparator, \
                                            typename KeyEqualityChecker, \
                                            typename ValueEqualityChecker, \
                                            typename ValueHashFunc>

#ifdef BWTREE_PELOTON
namespace peloton {
namespace index {
#endif

#ifdef INTERACTIVE_DEBUG

#define idb_assert(cond)                                                \
  do {                                                                  \
    if (!(cond)) {                                                      \
      debug_stop_mutex.lock();                                          \
      printf("assert, %-24s, line %d\n", __FUNCTION__, __LINE__); \
      idb.Start();                                                      \
      debug_stop_mutex.unlock();                                        \
    }                                                                   \
  } while (0);

#define idb_assert_key(node_id, key, context_p, cond)                   \
  do {                                                                  \
    if (!(cond)) {                                                      \
      debug_stop_mutex.lock();                                          \
      printf("assert, %-24s, line %d\n", __FUNCTION__, __LINE__); \
      idb.key_list.push_back(key);                                      \
      idb.node_id_list.push_back(node_id);                              \
      idb.context_p = context_p;                                        \
      idb.Start();                                                      \
      debug_stop_mutex.unlock();                                        \
    }                                                                   \
  } while (0);

#else

#define idb_assert(cond) \
  do {                   \
    assert(cond);        \
  } while (0);

#define idb_assert_key(key, id, context_p, cond) \
  do {                                           \
    assert(cond);                                \
  } while (0);

#endif

#ifdef BWTREE_DEBUG

#define bwt_printf(fmt, ...)                              \
  do {                                                    \
    if(print_flag == false) break;                        \
    printf("%-24s(%8lX): " fmt, __FUNCTION__, std::hash<std::thread::id>()(std::this_thread::get_id()), ##__VA_ARGS__); \
    fflush(stdout);                                       \
  } while (0);

#else

#define bwt_printf(fmt, ...)   \
  do {                         \
    dummy(fmt, ##__VA_ARGS__); \
  } while (0);

static void dummy(const char*, ...) {}

#endif

#ifdef INTERACTIVE_DEBUG
// In multi-threaded testing, if we want to halt all threads when an error
// happens
// then we lock this mutex
// Since every other thread will try to lock this before SMO
// it guarantees no SMO would happen before everybody stops
static std::mutex debug_stop_mutex;
#endif

using NodeID = uint64_t;

extern NodeID INVALID_NODE_ID;
extern bool print_flag;

/*
 * class BwTree - Lock-free BwTree index implementation
 *
 * Template Arguments:
 *
 * template <typename RawKeyType,
 *          typename ValueType,
 *          typename KeyComparator = std::less<RawKeyType>,
 *          typename KeyEqualityChecker = std::equal_to<RawKeyType>,
 *          typename ValueEqualityChecker = std::equal_to<ValueType>,
 *          typename ValueHashFunc = std::hash<ValueType>>
 *
 * Explanation:
 *
 *  - RawKeyType: Key type of the map
 *                *** DO NOT CONFUSE THIS WITH WRAPPED KEY TYPE
 *
 *  - ValueType: Value type of the map. Note that it is possible
 *               that a single key is mapped to multiple values
 *
 *  - KeyComparator: "less than" relation comparator for RawKeyType
 *                   Returns true if "less than" relation holds
 *                   *** NOTE: THIS OBJECT DO NOT NEED TO HAVE A DEFAULT
 *                   CONSTRUCTOR. THIS MODIFICATION WAS MADE TO FIT
 *                   INTO Peloton's ARCHITECTURE
 *                   Please refer to main.cpp, class KeyComparator for more
 *                   information on how to define a proper key comparator
 *
 *  - KeyEqualityChecker: Equality checker for RawKeyType
 *                        Returns true if two keys are equal
 *
 *  - ValueEqualityChecker: Equality checker for value type
 *                          Returns true for ValueTypes that are equal
 *
 *  - ValueHashFunc: Hashes ValueType into a 32 bit integer
 *                   This is used in unordered_map
 *
 * If not specified, then by default all arguments except the first two will
 * be set as the standard operator in C++ (i.e. the operator for primitive types
 * AND/OR overloaded operators for derived types)
 */
template <typename RawKeyType,
          typename ValueType,
          typename KeyComparator = std::less<RawKeyType>,
          typename KeyEqualityChecker = std::equal_to<RawKeyType>,
          typename ValueEqualityChecker = std::equal_to<ValueType>,
          typename ValueHashFunc = std::hash<ValueType>>
class BwTree {
 /*
  * Private & Public declaration (no definition)
  */
#ifndef ALL_PUBLIC
 private:
#else
 public:
#endif
  class InteractiveDebugger;
  friend InteractiveDebugger;

  // This does not have to be the friend class of BwTree
  class EpochManager;

 public:
  class BaseNode;
  class KeyType;
  class WrappedKeyComparator;
  class BaseLogicalNode;
  class NodeSnapshot;

 /*
  * private: Basic type definition
  */
#ifndef ALL_PUBLIC
 private:
#else
 public:
#endif

  // This is used to hold values in a set
  using ValueSet = std::unordered_set<ValueType,
                                      ValueHashFunc,
                                      ValueEqualityChecker>;

  // This is used to hold mapping from key to a set of values
  // NOTE: Wherever this is used, we need also to pass in the
  // wrapped key comparator object as constructor argument
  using KeyValueSet = std::map<KeyType, ValueSet, WrappedKeyComparator>;

  // This is used to hold key and NodeID ordered mapping relation
  // NOTE: For constructing WrappedKeyComparator please refer to KeyValueSet
  using KeyNodeIDMap = std::map<KeyType, NodeID, WrappedKeyComparator>;

  using EpochNode = typename EpochManager::EpochNode;

  // The maximum number of nodes we could map in this index
  constexpr static NodeID MAPPING_TABLE_SIZE = 1 << 24;

  // If the length of delta chain exceeds this then we consolidate the node
  constexpr static int DELTA_CHAIN_LENGTH_THRESHOLD = 8;

  // If node size goes above this then we split it
  constexpr static size_t INNER_NODE_SIZE_UPPER_THRESHOLD = 16;
  constexpr static size_t LEAF_NODE_SIZE_UPPER_THRESHOLD = 16;

  constexpr static size_t INNER_NODE_SIZE_LOWER_THRESHOLD = 7;
  constexpr static size_t LEAF_NODE_SIZE_LOWER_THRESHOLD = 7;

  /*
   * enum class NodeType - Bw-Tree node type
   */
  enum class NodeType {
    // Data page type
    LeafType = 0,
    InnerType,

    // Only valid for leaf
    LeafInsertType,
    LeafSplitType,
    LeafDeleteType,
    LeafUpdateType,
    LeafRemoveType,
    LeafMergeType,
    LeafAbortType, // Unconditional abort (this type is not used)

    // Only valid for inner
    InnerInsertType,
    InnerSplitType,
    InnerDeleteType,
    InnerRemoveType,
    InnerMergeType,
    InnerAbortType, // Unconditional abort
  };

  /*
   * enum class ExtendedKeyValue - We wrap the raw key with an extra
   * of indirection in order to identify +/-Inf
   */
  enum class ExtendedKeyValue {
    RawKey,
    PosInf,
    NegInf,
  };

  /*
   * struct KeyType - Wrapper class for RawKeyType which supports +/-Inf
   * for arbitrary key types
   *
   * NOTE: Comparison between +Inf and +Inf, comparison between
   * -Inf and -Inf, and equality check between them are also defined
   * because they will be needed in corner cases
   */
  class KeyType {
   public:
    // If type is +/-Inf then we use this key to compare
    RawKeyType key;

    // Denote whether the key value is +/-Inf or not
    ExtendedKeyValue type;

    /*
     * Constructor  - Use RawKeyType object to initialize
     */
    KeyType(const RawKeyType &p_key) :
      key{p_key},
      type{ExtendedKeyValue::RawKey} // DO NOT FORGET THIS!
    {}

    /*
     * Constructor - Use value type only (must not be raw value type)
     *
     * This function assumes RawKeyType being default constructible
     */
    KeyType(ExtendedKeyValue p_type) :
      key{},
      type{p_type} {
      // If it is raw type then we are having an empty key
      assert(p_type != ExtendedKeyValue::RawKey);
      return;
    }

    /*
     * Copy Constructoe - copy construct a KeyType with exactly the same
     *                    raw key and key type
     */
    KeyType(const KeyType &p_key) :
      key{p_key.key},
      type{p_key.type}
    {}

    /*
     * IsNegInf() - Whether the key value is -Inf
     */
    inline bool IsNegInf() const {
      return type == ExtendedKeyValue::NegInf;
    }

    /*
     * IsPosInf() - Whether the key value is +Inf
     */
    inline bool IsPosInf() const {
      return type == ExtendedKeyValue::PosInf;
    }
  };

  /*
   * class WrappedKeyComparator - Compares wrapped key, using raw key
   *                              comparator in template argument
   */
  class WrappedKeyComparator {
   public:
    // A pointer to the "less than" comparator object that might be
    // context sensitive
    const KeyComparator *key_cmp_obj_p;

    /*
     * Constructor - Takes a pointer to raw key comparator object
     */
    WrappedKeyComparator(const KeyComparator *p_key_cmp_obj_p) :
      key_cmp_obj_p{p_key_cmp_obj_p}
    {}

    /*
     * Default Constructor - Deleted
     *
     * This is defined to emphasize the face that a key comparator
     * is always needed to construct this object
     */
    WrappedKeyComparator() = delete;

    bool operator()(const KeyType &key1, const KeyType &key2) const {
      // As long as the second operand is not -Inf then
      // we return true
      if(key1.IsNegInf()) {
        if(key2.IsNegInf()) {
          return false;
        }

        return true;
      }

      // We already know key1 would not be negInf
      if(key2.IsNegInf()) {
        return false;
      }

      // As long as second operand is not
      if(key2.IsPosInf()) {
        if(key1.IsPosInf()) {
          return false;
        }

        return true;
      }

      // We already know key2.type is not posInf
      if(key1.IsPosInf()) {
        return false;
      }

      // Then we need to compare real key
      // NOTE: We use the comparator object passed as constructor argument
      // This might not be necessary if the comparator is trivially
      // constructible, but in the case of Peloton, tuple key comparison
      // requires knowledge of the tuple schema which cannot be achieved
      // without a comparator object that is not trivially constructible
      return (*key_cmp_obj_p)(key1.key, key2.key);
    }
  };

  /*
   * RawKeyCmpLess() - Compare raw key for < relation
   *
   * Directly uses the comparison object
   */
  inline bool RawKeyCmpLess(const RawKeyType &key1, const RawKeyType &key2) {
    return key_cmp_obj(key1, key2);
  }

  /*
   * RawKeyCmpEqual() - Compare raw key for == relation
   *
   * We use the fast comparison object rather than traditional < && >
   * approach to avoid performance penalty
   */
  inline bool RawKeyCmpEqual(const RawKeyType &key1, const RawKeyType &key2) {
    return key_eq_obj(key1, key2);
  }

  /*
   * RawKeyCmpNotEqual() - Compare raw key for != relation
   *
   * It negates result of RawKeyCmpEqual()
   */
  inline bool RawKeyCmpNotEqual(const RawKeyType &key1, const RawKeyType &key2) {
    return !RawKeyCmpEqual(key1, key2);
  }

  /*
   * RawKeyCmpGreaterEqual() - Compare raw key for >= relation
   *
   * It negates result of RawKeyCmpLess()
   */
  inline bool RawKeyCmpGreaterEqual(const RawKeyType &key1, const RawKeyType &key2) {
    return !RawKeyCmpLess(key1, key2);
  }

  /*
   * RawKeyCmpGreater() - Compare raw key for > relation
   *
   * It inverts input of RawKeyCmpLess()
   */
  inline bool RawKeyCmpGreater(const RawKeyType &key1, const RawKeyType &key2) {
    return RawKeyCmpLess(key2, key1);
  }

  /*
   * RawKeyCmpLessEqual() - Cmpare raw key for <= relation
   *
   * It negates result of RawKeyCmpGreater()
   */
  bool RawKeyCmpLessEqual(const RawKeyType &key1, const RawKeyType &key2) {
    return !RawKeyCmpGreater(key1, key2);
  }

  /*
   * KeyCmpLess() - Compare two keys for "less than" relation
   *
   * If key1 < key2 return true
   * If key1 >= key2 return false
   *
   * NOTE: All comparisons are defined, since +/- Inf themselves
   * could be used as separator/high key
   */
  bool KeyCmpLess(const KeyType &key1, const KeyType &key2) const {
    // The wrapped key comparator object is defined as a object member
    // since we need it to be passed in std::map as the comparator
    return wrapped_key_cmp_obj(key1, key2);
  }

  /*
   * KeyCmpEqual() - Compare a pair of keys for equality
   *
   * If both of the keys are +/-Inf then assertion fails
   * If one of them are +/-Inf, then always return false
   * Otherwise use key_eq_obj to compare (fast comparison)
   *
   * NOTE: This property does not affect <= and >= since these
   * two are implemented using > and < respectively
   */
  bool KeyCmpEqual(const KeyType &key1, const KeyType &key2) const {
    // This is special treatment since we are using -Inf
    // as the low key for parent node, so there would be
    // comparison related with it in order to decide
    // whether the child is a left most one
    if(key1.IsNegInf() && key2.IsNegInf()) {
      return true;
    } else if(key1.IsPosInf() && key2.IsPosInf()) {
      return true;
    } else if(key1.IsNegInf() && key2.IsPosInf()) {
      return false;
    } else if(key1.IsPosInf() && key2.IsNegInf()) {
      return false;
    }

    return key_eq_obj(key1.key, key2.key);
  }

  /*
   * KeyCmpNotEqual() - Comapre a pair of keys for inequality
   *
   * It negates result of keyCmpEqual()
   */
  inline bool KeyCmpNotEqual(const KeyType &key1, const KeyType &key2) const {
    return !KeyCmpEqual(key1, key2);
  }

  /*
   * KeyCmpGreaterEqual() - Compare a pair of keys for >= relation
   *
   * It negates result of keyCmpLess()
   */
  inline bool KeyCmpGreaterEqual(const KeyType &key1, const KeyType &key2) const {
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

  /*
   * ValueCmpEqual() - Compare value for equality relation
   *
   * It directly wraps value comparatpr object
   */
  inline bool ValueCmpEuqal(const ValueType &val1, const ValueType &val2) const {
    return value_eq_obj(val1, val2);
  }

  /*
   * enum class OpState - Current state of the state machine
   *
   * Init - We need to load root ID and start the traverse
   *        After loading root ID should switch to Inner state
   *        since we know there must be an inner node
   * Inner - We are currently on an inner node, and want to go one
   *         level down
   * Abort - We just saw abort flag, and will reset everything back to Init
   */
  enum class OpState {
    Init,
    Inner,
    Leaf,
    Abort
  };

  /*
   * struct Context - Stores per thread context data that is used during
   *                  tree traversal
   *
   * NOTE: For each thread there could be only 1 instance of this object
   * so we forbid copy construction and assignment and move
   */
  struct Context {
    // This is cannot be changed once initialized
    const KeyType search_key;
    OpState current_state;

    // Whether to abort current traversal, and start a new one
    // after seeing this flag, all function should return without
    // any further action, and let the main driver to perform abort
    // and restart
    // NOTE: Only the state machine driver could abort
    // and other functions just return on seeing this flag
    bool abort_flag;
    std::vector<NodeSnapshot> path_list;

    // How many times we have aborted
    size_t abort_counter;
    // Current level (root = 0)
    int current_level;

    /*
     * Constructor - Initialize a context object into initial state
     */
    Context(const KeyType p_search_key) :
      search_key{p_search_key},
      current_state{OpState::Init},
      abort_flag{false},
      path_list{},
      abort_counter{0},
      current_level{0}
    {}

    /*
     * Destructor - Cleanup
     */
    ~Context() {
      path_list.clear();

      return;
    }

    /*
     * Copy constructor - deleted
     * Assignment operator - deleted
     * Move constructor - deleted
     * Move assignment - deleted
     */
    Context(const Context &p_context) = delete;
    Context &operator=(const Context &p_context) = delete;
    Context(Context &&p_context) = delete;
    Context &operator=(Context &&p_context) = delete;
  };

  /*
   * struct DataItem - Actual data stored inside leaf of bw-tree
   *
   * We choose to define our own data container rather than using
   * std::pair because we anticipate to add more control structure
   * inside this class
   */
  struct DataItem {
    KeyType key;

    // Each key could be mapped to multiple values
    // These values could be repeated, be unique by value
    // or be unique by key. No matter which rule it obeys
    // we always use a vector to hold them and transfer the
    // logic to identify situation to data manipulation routines
    std::vector<ValueType> value_list;

    /*
     * Constructor - Construction from a list of data values
     *
     * This one is mainly used for debugging purposes
     */
    DataItem(const KeyType p_key,
             const std::vector<ValueType> &p_value_list) :
      key{p_key},
      value_list{p_value_list}
    {}

    /*
     * Constructor - Use a value set to construct
     *
     * It bulk loads the value vector with an unordered set's begin()
     * and end() iterator
     */
    DataItem(const KeyType &p_key, const ValueSet &p_value_set) :
      key{p_key},
      value_list{p_value_set.begin(), p_value_set.end()}
    {}

    /*
     * Copy Constructor - Copy construct key and value vector
     *
     * We must declare this function since we defined a move constructor
     * and move assignment, copy constructor is deleted by default
     */
    DataItem(const DataItem &di) :
      key{di.key},
      value_list{di.value_list}
    {}

    /*
     * Move Constructor - Move value_list
     */
    DataItem(DataItem &&di) :
      key{di.key},
      value_list{std::move(di.value_list)}
    {}

    /*
     * Move Assignment - DO NOT ALLOW THIS
     */
    DataItem &operator=(DataItem &&di) = delete;
  };

  /*
   * class DataItemComparator - Data item comparator function object
   *
   * NOTE: Since we could not instanciate object comparator so in order
   * to construct this object we need to pass in the object
   *
   * TODO: This is not used currently. And the implementation is problematic
   */
  class DataItemComparator {
   public:
    KeyComparator &key_cmp_obj;

    DataItemComparator(const KeyComparator &p_key_cmp_obj) :
      key_cmp_obj{p_key_cmp_obj} {}

    bool operator()(const DataItem &d1, const DataItem &d2) const {
      return key_cmp_obj(d1.key, d2.key);
    }
  };

  /*
   * struct SepItem() - Separator item for inner nodes
   *
   * We choose not to use std::pair bacause we probably need to
   * add information into this structure
   */
  struct SepItem {
    KeyType key;
    NodeID node;

    SepItem(const KeyType &p_key, NodeID p_node) :
      key{p_key},
      node{p_node}
    {}

    SepItem(const SepItem &si) :
      key{si.key},
      node{si.node}
    {}
  };

  /*
   * class BWNode - Generic node class; inherited by leaf, inner
   *                and delta node
   */
  class BaseNode {
   public:
    const NodeType type;

    /*
     * Constructor - Initialize type
     */
    BaseNode(NodeType p_type) : type{p_type} {}

    /*
     * Destructor - This must be virtual in order to properly destroy
     * the object only given a base type key
     */
    ~BaseNode() {}

    /*
     * GetType() - Return the type of node
     *
     * This method does not allow overridding
     */
    inline NodeType GetType() const {
      return type;
    }

    /*
     * IsLeafRemoveNode() - Return true if it is
     *
     * This function is specially defined since we want to test
     * for remove node as a special case
     */
    inline bool IsLeafRemoveNode() const {
      return type == NodeType::LeafRemoveType;
    }

    /*
     * IsInnerRemoveNode() - Return true if it is
     *
     * Same reason as above
     */
    inline bool IsInnerRemoveNode() const {
      return type == NodeType::InnerRemoveType;
    }

    /*
     * IsDeltaNode() - Return whether a node is delta node
     *
     * All nodes that are neither inner nor leaf type are of
     * delta node type
     */
    inline bool IsDeltaNode() const {
      if(type == NodeType::InnerType || \
         type == NodeType::LeafType) {
        return false;
      } else {
        return true;
      }
    }

    /*
     * IsOnLeafDeltaChain() - Return whether the node is part of
     *                        leaf delta chain
     *
     * This is true even for NodeType::LeafType
     *
     * NOTE: WHEN ADDING NEW NODE TYPES PLEASE UPDATE THIS LIST
     *
     * Note 2: Avoid calling this in multiple places. Currently we only
     * call this in TakeNodeSnapshot() or in the debugger
     */
    inline bool IsOnLeafDeltaChain() const {
      return (type == NodeType::LeafType || \
              type == NodeType::LeafInsertType || \
              type == NodeType::LeafDeleteType || \
              type == NodeType::LeafSplitType || \
              type == NodeType::LeafMergeType || \
              type == NodeType::LeafRemoveType || \
              type == NodeType::LeafUpdateType || \
              // This is necessary, since we need to first load
              // node ID and then judge its type
              type == NodeType::LeafAbortType);
    }

    /*
     * CastTo() - Get a derived node type
     *
     * NOTE: Template argument is Node type, not pointer type
     */
    template<typename TargetType>
    TargetType *CastTo() {
      return static_cast<TargetType *>(this);
    }
  };

  /*
   * class DeltaNode - Common element in a delta node
   *
   * Common elements include depth of the node and pointer to
   * children node
   */
  class DeltaNode : public BaseNode {
   public:
    const int depth;

    const BaseNode *child_node_p;

    /*
     * Constructor
     */
    DeltaNode(NodeType p_type,
              int p_depth,
              const BaseNode *p_child_node_p) :
      BaseNode{p_type},
      depth{p_depth},
      child_node_p{p_child_node_p}
    {}
  };

  /*
   * class LeafNode - Leaf node that holds data
   *
   * There are 5 types of delta nodes that could be appended
   * to a leaf node. 3 of them are SMOs, and 2 of them are data operation
   */
  class LeafNode : public BaseNode {
   public:
    // We always hold data within a vector of vectors
    std::vector<DataItem> data_list;
    const KeyType lbound;
    const KeyType ubound;

    // If this is INVALID_NODE_ID then we know it is the
    // last node in the leaf chain
    const NodeID next_node_id;

    /*
     * Constructor - Initialize bounds and next node ID
     */
    LeafNode(const KeyType &p_lbound,
             const KeyType &p_ubound,
             NodeID p_next_node_id) :
      BaseNode{NodeType::LeafType},
      lbound{p_lbound},
      ubound{p_ubound},
      next_node_id{p_next_node_id}
      {}

    /*
     * GetSplitSibling() - Split the node into two halves
     *
     * We always split on the middle of the node; If the number
     * of separators are not even, then the sibling will have
     * one more key
     *
     * NOTE: This function allocates memory, and if it is not used
     * e.g. by a CAS failure, then caller needs to delete it
     *
     * NOTE 2: We could get the split key by looking into this node's
     * low key
     */
    LeafNode *GetSplitSibling() const {
      size_t node_size = data_list.size();
      size_t split_key_index = node_size / 2;

      assert(node_size >= 2);

      const KeyType &split_key = data_list[split_key_index].key;

      LeafNode *leaf_node_p = \
        new LeafNode{split_key, ubound, next_node_id};

      // Copy data item into the new node
      for(int i = split_key_index;i < (int)node_size;i++) {
        leaf_node_p->data_list.push_back(data_list[i]);
      }

      return leaf_node_p;
    }
  };

  /*
   * class LeafInsertNode - Insert record into a leaf node
   */
  class LeafInsertNode : public DeltaNode {
   public:
    const KeyType insert_key;
    const ValueType value;

    /*
     * Constructor
     */
    LeafInsertNode(const KeyType &p_insert_key,
                   const ValueType &p_value,
                   int p_depth,
                   const BaseNode *p_child_node_p) :
    DeltaNode{NodeType::LeafInsertType, p_depth, p_child_node_p},
    insert_key{p_insert_key},
    value{p_value}
    {}
  };

  /*
   * class LeafDeleteNode - Delete record from a leaf node
   *
   * In multi-value mode, it takes a value to identify which value
   * to delete. In single value mode, value is redundant but what we
   * could use for sanity check
   */
  class LeafDeleteNode : public DeltaNode {
   public:
    KeyType delete_key;
    ValueType value;

    /*
     * Constructor
     */
    LeafDeleteNode(const KeyType &p_delete_key,
                   const ValueType &p_value,
                   int p_depth,
                   const BaseNode *p_child_node_p) :
      DeltaNode{NodeType::LeafDeleteType, p_depth, p_child_node_p},
      delete_key{p_delete_key},
      value{p_value}
    {}
  };

  /*
   * class LeafUpdateNode - Update a key atomically
   *
   * Without using this node there is no guarantee that an insert
   * after delete is atomic
   */
  class LeafUpdateNode : public DeltaNode {
   public:
    KeyType update_key;
    ValueType old_value;
    ValueType new_value;

    /*
     * Constructor
     */
    LeafUpdateNode(const KeyType &p_update_key,
                   const ValueType &p_old_value,
                   const ValueType &p_new_value,
                   int p_depth,
                   const BaseNode *p_child_node_p) :
      DeltaNode{NodeType::LeafUpdateType, p_depth, p_child_node_p},
      update_key{p_update_key},
      old_value{p_old_value},
      new_value{p_new_value}
    {}
  };

  /*
   * class LeafSplitNode - Split node for leaf
   *
   * It includes a separator key to direct search to a correct direction
   * and a physical pointer to find the current next node in delta chain
   */
  class LeafSplitNode : public DeltaNode {
   public:
    KeyType split_key;
    NodeID split_sibling;

    /*
     * Constructor
     */
    LeafSplitNode(const KeyType &p_split_key,
                  NodeID p_split_sibling,
                  int p_depth,
                  const BaseNode *p_child_node_p) :
      DeltaNode{NodeType::LeafSplitType, p_depth, p_child_node_p},
      split_key{p_split_key},
      split_sibling{p_split_sibling}
    {}
  };

  /*
   * class LeafRemoveNode - Remove all physical children and redirect
   *                        all access to its logical left sibling
   *
   * It does not contain data and acts as merely a redirection flag
   */
  class LeafRemoveNode : public DeltaNode {
   public:

    /*
     * Constructor
     */
    LeafRemoveNode(int p_depth,
                   const BaseNode *p_child_node_p) :
      DeltaNode{NodeType::LeafRemoveType, p_depth, p_child_node_p}
    {}
  };

  /*
   * class LeafMergeNode - Merge two delta chian structure into one node
   *
   * This structure uses two physical pointers to indicate that the right
   * half has become part of the current node and there is no other way
   * to access it
   */
  class LeafMergeNode : public DeltaNode {
   public:
    KeyType merge_key;

    // For merge nodes we use actual physical pointer
    // to indicate that the right half is already part
    // of the logical node
    const BaseNode *right_merge_p;

    /*
     * Constructor
     */
    LeafMergeNode(const KeyType &p_merge_key,
                  const BaseNode *p_right_merge_p,
                  int p_depth,
                  const BaseNode *p_child_node_p) :
      DeltaNode{NodeType::LeafMergeType, p_depth, p_child_node_p},
      merge_key{p_merge_key},
      right_merge_p{p_right_merge_p}
    {}
  };

  /*
   * class LeafAbortNode - All operations must abort unconditionally
   *
   * This node is used to block all further updates when we want to
   * perform split on the parent node of the target node
   *
   * It needs to be removed by the same thread that posted this node.
   * And if the node has changed, then
   */
  class LeafAbortNode : public DeltaNode {
   public:

    /*
     * Constructor
     *
     * It does not have to take a depth as argument, because
     * no thread is allowed to post on top of that
     */
    LeafAbortNode(const BaseNode *p_child_node_p) :
      DeltaNode{NodeType::LeafAbortType, -1, p_child_node_p}
    {}
  };

  /*
   * class InnerNode - Inner node that holds separators
   */
  class InnerNode : public BaseNode {
   public:
    std::vector<SepItem> sep_list;
    KeyType lbound;
    KeyType ubound;

    // Used even in inner node to prevent early consolidation
    NodeID next_node_id;

    /*
     * Constructor
     */
    InnerNode(const KeyType &p_lbound,
              const KeyType &p_ubound,
              NodeID p_next_node_id) :
      BaseNode{NodeType::InnerType},
      lbound{p_lbound},
      ubound{p_ubound},
      next_node_id{p_next_node_id}
    {}

    /*
     * GetSplitSibling() - Split the node into two halves
     *
     * This functions is similar to that in LeafNode
     */
    InnerNode *GetSplitSibling() const {
      size_t node_size = sep_list.size();
      size_t split_key_index = node_size / 2;

      // We enforce this to avoid empty nodes
      assert(node_size >= 2);

      const KeyType &split_key = sep_list[split_key_index].key;

      InnerNode *inner_node_p = \
        new InnerNode{split_key, ubound, next_node_id};

      // Copy data item into the new node
      for(int i = split_key_index;i < (int)node_size;i++) {
        inner_node_p->sep_list.push_back(sep_list[i]);
      }

      return inner_node_p;
    }
  };

  /*
   * class InnerInsertNode - Insert node for inner nodes
   *
   * It has two keys in order to make decisions upon seeing this
   * node when traversing down the delta chain of an inner node
   * If the search key lies in the range between sep_key and
   * next_key then we know we should go to new_node_id
   */
  class InnerInsertNode : public DeltaNode {
   public:
    KeyType insert_key;
    KeyType next_key;
    NodeID new_node_id;

    /*
     * Constructor
     */
    InnerInsertNode(const KeyType &p_insert_key,
                    const KeyType &p_next_key,
                    NodeID p_new_node_id,
                    int p_depth,
                    const BaseNode *p_child_node_p) :
      DeltaNode{NodeType::InnerInsertType, p_depth, p_child_node_p},
      insert_key{p_insert_key},
      next_key{p_next_key},
      new_node_id{p_new_node_id}
    {}
  };

  /*
   * class InnerDeleteNode - Delete node
   *
   * NOTE: There are three keys associated with this node, two of them
   * defining the new range after deleting this node, the remaining one
   * describing the key being deleted
   */
  class InnerDeleteNode : public DeltaNode {
   public:
    KeyType delete_key;
    // These two defines a new range associated with this delete node
    KeyType next_key;
    KeyType prev_key;

    NodeID prev_node_id;

    /*
     * Constructor
     *
     * NOTE: We need to provide three keys, two for defining a new
     * range, and one for removing the index term from base node
     */
    InnerDeleteNode(const KeyType &p_delete_key,
                    const KeyType &p_next_key,
                    const KeyType &p_prev_key,
                    NodeID p_prev_node_id,
                    int p_depth,
                    const BaseNode *p_child_node_p) :
      DeltaNode{NodeType::InnerDeleteType, p_depth, p_child_node_p},
      delete_key{p_delete_key},
      next_key{p_next_key},
      prev_key{p_prev_key},
      prev_node_id{p_prev_node_id}
    {}
  };

  /*
   * class InnerSplitNode - Split inner nodes into two
   *
   * It has the same layout as leaf split node except for
   * the base class type variable. We make such distinguishment
   * to facilitate identifying current delta chain type
   */
  class InnerSplitNode : public DeltaNode {
   public:
    KeyType split_key;
    NodeID split_sibling;

    /*
     * Constructor
     */
    InnerSplitNode(const KeyType &p_split_key,
                   NodeID p_split_sibling,
                   int p_depth,
                   const BaseNode *p_child_node_p) :
      DeltaNode{NodeType::InnerSplitType, p_depth, p_child_node_p},
      split_key{p_split_key},
      split_sibling{p_split_sibling}
    {}
  };

  /*
   * class InnerRemoveNode
   */
  class InnerRemoveNode : public DeltaNode {
   public:

    /*
     * Constructor
     */
    InnerRemoveNode(int p_depth,
                    const BaseNode *p_child_node_p) :
      DeltaNode{NodeType::InnerRemoveType, p_depth, p_child_node_p}
    {}
  };

  /*
   * class InnerMergeNode - Merge delta for inner nodes
   */
  class InnerMergeNode : public DeltaNode {
   public:
    KeyType merge_key;

    const BaseNode *right_merge_p;

    /*
     * Constructor
     */
    InnerMergeNode(const KeyType &p_merge_key,
                   const BaseNode *p_right_merge_p,
                   int p_depth,
                   const BaseNode *p_child_node_p) :
      DeltaNode{NodeType::InnerMergeType, p_depth, p_child_node_p},
      merge_key{p_merge_key},
      right_merge_p{p_right_merge_p}
    {}
  };

  /*
   * class InnerAbortNode - Same as LeafAbortNode
   */
  class InnerAbortNode : public DeltaNode {
   public:

    /*
     * Constructor
     */
    InnerAbortNode(const BaseNode *p_child_node_p) :
      DeltaNode{NodeType::InnerAbortType, -1, p_child_node_p}
    {}
  };

  ///////////////////////////////////////////////////////////////////
  // Logical node definition
  ///////////////////////////////////////////////////////////////////

  /*
   * class BaseLogicalNode - Base class of logical node
   *
   * Both inner logical node and leaf logical node have a high key,
   * low key, and next node NodeID
   *
   * NOTE: This structure is used as part of the NodeSnapshot structure
   */
  class BaseLogicalNode {
   public:
    const KeyType *lbound_p;
    const KeyType *ubound_p;

    NodeID next_node_id;

    /*
     * Constructor - Initialize everything to initial invalid state
     */
    BaseLogicalNode() :
      lbound_p{nullptr},
      ubound_p{nullptr},
      next_node_id{INVALID_NODE_ID}
    {}

    /*
     * Copy Constructor - Copies three members
     */
    BaseLogicalNode(const BaseLogicalNode &node) :
      lbound_p{node.lbound_p},
      ubound_p{node.ubound_p},
      next_node_id{node.next_node_id}
    {}
  };

  /*
   * LogicalLeafNode() - A logical representation of a logical node
   *                     which is physically one or more chains of
   *                     delta node, SMO node and base nodes.
   *
   * This structure is usually used as a container to hold intermediate
   * results for a whole page operation. The caller fills in ID and pointer
   * field, while callee would fill the other fields
   */
  class LogicalLeafNode : public BaseLogicalNode {
   public:
    // We need to access wrapped key comparator later
    BwTree *tree_p;

    // These fields are filled by callee
    KeyValueSet key_value_set;

    // This is used to temporarily hold results, and should be empty
    // after all deltas has been applied
    std::vector<const BaseNode *> pointer_list;

    /*
     * Constructor - Initialize logical ID and physical pointer
     *               as the tree snapshot
     */
    LogicalLeafNode(BwTree *p_tree_p) :
      BaseLogicalNode{},
      tree_p{p_tree_p},
      // We need to use the given object to initialize std::map
      key_value_set{tree_p->wrapped_key_cmp_obj},
      pointer_list{}
    {}

    /*
     * Deleted Constructor - To explicitly state that an argument
     *                       must be given
     */
    LogicalLeafNode() = delete;

    /*
     * Copy Constructor - Copies metadata and data associated with the
     *                    logical leaf node into a newly allocated instance
     *
     * NOTE: It is reauired that the pointer list being empty, since it makes
     * no sense copy constructing a logical leaf node object using another
     * instance in its intermediate state.
     * This also helps catching some bugs
     */
    LogicalLeafNode(const LogicalLeafNode &node) :
      // Parent copy constructor
      BaseLogicalNode{node},
      tree_p{node.tree_p},
      // This is the most important one
      key_value_set{node.key_value_set},
      pointer_list{} {
      // We cannot copy construct a logical leaf node in its intermediate
      // state (i.e. pointer list not being empty)
      assert(node.pointer_list.size() == 0UL);

      return;
    }
    
    /*
     * operator= - Copies from source to current instance
     */
    LogicalLeafNode &operator=(const LogicalLeafNode &node) {
      // Self-assignment prevention
      if(this == &node) {
        return *this;
      }
      
      // To access parent class's member we use scope resolution operator "::"
      BaseLogicalNode::lbound_p = node.lbound_p;
      BaseLogicalNode::ubound_p = node.ubound_p;
      BaseLogicalNode::next_node_id = node.next_node_id;
      
      // Same reason; do not copy assign logcal leaf node in its
      // intermediate state
      assert(node.pointer_list.size() == 0UL);
      assert(pointer_list.size() == 0UL);
      
      tree_p = node.tree_p;
      key_value_set = node.key_value_set;
      
      return *this;
    }

    /*
     * Destructor
     *
     * The destructor checks the size of pointer list to make sure we do not
     * destroy a logical leaf node in its intermediate state. It is required
     * that pointer list being empty on destruction.
     */
    ~LogicalLeafNode() {
      assert(pointer_list.size() == 0UL);

      return;
    }

    /*
     * GetContainter() - Reuturn the container that holds key - multi value
     *                   mapping relation
     */
    inline KeyValueSet &GetContainer() {
      return key_value_set;
    }

    /*
     * BulkLoadValue() - Load a vector of ValueType into the given key
     *
     * Parameter is given by a DataItem reference which points to the
     * data field from some leaf base node
     */
    void BulkLoadValue(const DataItem &item) {
      auto ret = key_value_set.emplace(std::make_pair(item.key, ValueSet{}));

      //bwt_printf("key = %d\n", item.key.key);

      // Make sure it does not already exist in key value set
      // This always holds since we could not have overlaping
      // key range inside two base nodes
      assert(ret.second == true);

      // This points to the newly constructed mapping value_type
      typename KeyValueSet::iterator &map_item_it = ret.first;
      ValueSet &target = map_item_it->second;

      // Take advantange of the bulk load method in unordered_map
      target.insert(item.value_list.begin(), item.value_list.end());

      //bwt_printf("Key = %d\n", item.key.key);

      return;
    }

    /*
     * RemoveEmptyValueSet() - Remove empty ValueSets and associated keys
     *
     * This needs to be called after we have collected all values, but we
     * can choose to call this after every log replay at the cost of losing
     * performance
     *
     * NOTE: DO NOT REMOVE DIRECTLY ON THE MAP USING ITERATOR
     */
    void RemoveEmptyValueSet() {
      KeyValueSet temp_map{tree_p->wrapped_key_cmp_obj};

      // Move every non-empty ValueSet into container
      for(auto &item : GetContainer()) {
        if(item.second.size() != 0) {
          // We use move semantics to move the ValueSet
          temp_map[item.first] = std::move(item.second);
        }
      }

      // Use move semantics to copy the map
      key_value_set = std::move(temp_map);

      return;
    }

    /*
     * ReplayLog() - This function iterates through delta nodes
     *               in the reverse order that they are pushed
     *               and apply them to the key value set
     *
     * NOTE: This method always remove empty value set and its associated
     * key. So we could safely use it to determine the size of the node
     *
     * NOTE 2: This function takes an argument as the number of
     * pointers to replay. It may not equal total number of pointers
     * in the logical node, since if we have seen a merge delta, then
     * we only want to replay the log on two branches first and then
     * replay those on top of merge delta
     */
    void ReplayLog(size_t replay_count) {
      // For each insert/delete delta, replay the change on top
      // of base page values
      for(auto node_p_it = pointer_list.rbegin();
          node_p_it != pointer_list.rend();
          node_p_it++) {

        // If we have finished all logs just exit
        if(replay_count == 0LU) {
          //bwt_printf("Exit log replay because log count reaches 0\n");
          break;
        }

        const BaseNode *node_p = *node_p_it;
        NodeType type = node_p->GetType();

        switch(type) {
          case NodeType::LeafInsertType: {
            const LeafInsertNode *insert_node_p = \
              static_cast<const LeafInsertNode *>(node_p);

            auto it = key_value_set.find(insert_node_p->insert_key);
            if(it == key_value_set.end()) {
              // If the key does not exist yet we create a new value set
              auto ret = key_value_set.emplace( \
                std::make_pair(insert_node_p->insert_key,
                               ValueSet{})
              );

              // Insertion always happen
              assert(ret.second == true);

              // Now assign the newly allocated slot for the new key to it
              // so that we could use it later
              it = ret.first;
            }

            // it is either newly allocated or already exists
            it->second.insert(insert_node_p->value);

            break;
          } // case LeafInsertType
          case NodeType::LeafDeleteType: {
            const LeafDeleteNode *delete_node_p = \
              static_cast<const LeafDeleteNode *>(node_p);

            auto it = key_value_set.find(delete_node_p->delete_key);
            if(it == key_value_set.end()) {
              bwt_printf("ERROR: Delete a value that does not exist\n");

              assert(false);
            }

            // NOTE: This might erase a value that is not in the
            // value set. But we do not detect it here (i.e. the
            // detection of deleting a non-exiting value is detected
            // when insertion happens)
            it->second.erase(delete_node_p->value);

            break;
          } // case LeafDeleteType
          case NodeType::LeafUpdateType: {
            const LeafUpdateNode *update_node_p = \
              static_cast<const LeafUpdateNode *>(node_p);

            auto it = key_value_set.find(update_node_p->update_key);
            if(it == key_value_set.end()) {
              bwt_printf("ERROR: Update a value whose key does not exist\n");

              assert(false);
            }

            // Same as above: We do not detect whether value is present
            // here or not
            // Just blindly delete and then insert
            it->second.erase(update_node_p->old_value);
            it->second.insert(update_node_p->new_value);

            break;
          } // case LeafUpdateType
          default: {
            bwt_printf("ERROR: Unknown delta node type: %d\n",
                       static_cast<int>(type));

            assert(false);
          } // default
        } // case type

        // When this reaches 0 we break
        replay_count--;

        // Get rid of the last delta node
        // TODO: This is inefficient, and we could use a more effective
        // way to replay the log
        pointer_list.pop_back();
      } // for node_p in node_list

      RemoveEmptyValueSet();

      return;
    }

    /*
     * ToLeafNode() - Convert this logical node into a leaf node
     *
     * The most significant change would be to marshal the hash table
     * and map used to store keys and values into a two dimentional vector
     * of keys and values
     *
     * NOTE: This routine allocates memory for leaf page!!!!!!!!!!!!!
     */
    LeafNode *ToLeafNode() {
      assert(BaseLogicalNode::lbound_p != nullptr);
      assert(BaseLogicalNode::ubound_p != nullptr);
      // NOTE: This could be INVALID_NODE_ID for the last node
      //assert(BaseLogicalNode::next_node_id != INVALID_NODE_ID);

      LeafNode *leaf_node_p = \
        new LeafNode(*BaseLogicalNode::lbound_p,
                     *BaseLogicalNode::ubound_p,
                     BaseLogicalNode::next_node_id);

      // The key is already ordered, we just need to check for value
      // emptiness
      for(auto &it : key_value_set) {
        if(it.second.size() == 0) {
          bwt_printf("Skip empty value set\n");

          continue;
        }

        // Construct a data item in-place
        leaf_node_p->data_list.push_back(DataItem{it.first, it.second});
      }

      return leaf_node_p;
    }

  };

  /*
   * LogicalInnerNode - Logical representation of an inner node
   *
   * This corresponds to a single-key single-value version logical
   * page node with all delta changes applied to separator list
   *
   * The structure of this logical node is even more similar to
   * InnerNode than its counterpart for leaf. The reason is that
   * for inner nodes, we do not have to worry about multi-value
   * single-key, and thus the reconstruction algorithm does not
   * require replaying the log.
   *
   * In order to do down-traversal we do not have to compress
   * delta chain into a single logical node since the inner delta
   * node is already optimized for doing traversal. However, when
   * we perform left sibling locating, it is crucial for us to
   * be able to sequentially access all separators
   */
  class LogicalInnerNode : public BaseLogicalNode {
   public:
    KeyNodeIDMap key_value_map;

    /*
     * Constructor - Accept an initial starting point and init others
     */
    LogicalInnerNode(BwTree *tree_p) :
      BaseLogicalNode{},
      key_value_map{tree_p->wrapped_key_cmp_obj}
    {}

    /*
     * GetContainter() - Return the key - NodeID mapping relation
     */
    inline KeyNodeIDMap &GetContainer() {
      return key_value_map;
    }

    /*
     * ToInnerNode() - Convert to inner node object
     *
     * This function allocates a chunk of memory from the heap, which
     * should be freed by epoch manager.
     */
    InnerNode *ToInnerNode() {
      assert(BaseLogicalNode::lbound_p != nullptr);
      assert(BaseLogicalNode::ubound_p != nullptr);
      // NOTE: For the last inner node this could be the case
      //assert(BaseLogicalNode::next_node_id != INVALID_NODE_ID);

      // Memory allocated here should be freed by epoch
      InnerNode *inner_node_p = \
        new InnerNode{*BaseLogicalNode::lbound_p,
                      *BaseLogicalNode::ubound_p,
                      BaseLogicalNode::next_node_id};

      // Iterate through the ordered map and push separator items
      for(auto &it : key_value_map) {
        // We should not use INVALID_NODE_ID in inner node
        assert(it.second != INVALID_NODE_ID);

        inner_node_p->sep_list.push_back(SepItem{it.first, it.second});
      }

      return inner_node_p;
    }
  };

  /*
   * struct NodeSnapshot - Describes the states in a tree when we see them
   *
   * node_id and node_p are pairs that represents the state when we traverse
   * the node and use GetNode() to resolve the node ID.
   *
   * logical_node_p points to a consolidated view of the node, which might or
   * might not contain actual data (indicated by the boolean member), or even
   * might not be valid (if the pointer is nullptr).
   *
   * Also we need to distinguish between leaf snapshot and inner node snapshots
   * which is achieved by a flag is_leaf
   */
  class NodeSnapshot {
   public:
    NodeID node_id;
    const BaseNode *node_p;

    BaseLogicalNode *logical_node_p;

    // Whether there is data or only metadata
    // NOTE: Single key data is not classified as has data
    bool has_data;

    // Whether the logical node contains metadata
    bool has_metadata;

    // Whether logical node pointer points to a logical leaf node
    // or logical inner node
    // NOTE: This will not change once it is fixed
    const bool is_leaf;

    // This is true if the node is the left most node in its parent
    // We could not remove the leftmost node in a parent
    // NOTE: This is false for root node
    bool is_leftmost_child;

    // Set to true only if the snapshot is created for a root node
    // such that we know there is no more parent node.
    // A node with this flag set to true must be the first node
    // in the stack
    bool is_root;

    // The low key we uses when entering this node
    // This is used for finding the left sibling
    const KeyType *lbound_p;

    /*
     * Constructor - Initialize every member to invalid state
     *
     * Identity of leaf or inner needs to be provided as constructor
     * argument.
     *
     * NOTE: We allocate logical node inside this structure
     * during construction, so when replacing logical nodes please
     * remember to destroy the previous one
     */
    NodeSnapshot(bool p_is_leaf,
                 BwTree *tree_p) :
      node_id{INVALID_NODE_ID},
      node_p{nullptr},
      logical_node_p{nullptr},
      has_data{false},
      has_metadata{false},
      is_leaf{p_is_leaf},
      is_leftmost_child{false},
      is_root{false},
      lbound_p{nullptr} {
      // Allocate a logical node
      if(is_leaf == true) {
        logical_node_p = new LogicalLeafNode{tree_p};
      } else {
        logical_node_p = new LogicalInnerNode{tree_p};
      }

      assert(logical_node_p != nullptr);

      return;
    }

    // Explicitly forbid copy construct since this is likely to
    // cause memory leak
    NodeSnapshot(const NodeSnapshot &p_ns) = delete;
    NodeSnapshot &operator=(const NodeSnapshot &p_ns) = delete;

    /*
     * Move Constructor - Safely nullify right hand side logical pointer
     *
     * NOTE: DO NOT USE const qualifier with move semantics!
     */
    NodeSnapshot(NodeSnapshot &&p_ns) :
      node_id{p_ns.node_id},
      node_p{p_ns.node_p},
      logical_node_p{p_ns.logical_node_p},
      has_data{p_ns.has_data},
      has_metadata{p_ns.has_metadata},
      is_leaf{p_ns.is_leaf},
      is_leftmost_child{p_ns.is_leftmost_child},
      is_root{p_ns.is_root},
      lbound_p{p_ns.lbound_p} {

      // Avoid the node being destroyed on destruction of the old node
      p_ns.logical_node_p = nullptr;

      return;
    }

    /*
     * Move assignment - Safely move the logical node into another object
     *
     * This function will delete the current logical node for this node
     * and replace it with the pointer on RHS
     *
     * NOTE: This function destroys logical node pointer for the move-in
     * node first, and when we delete the logical node pointer we need
     * to first convert it to the correct type
     */
    NodeSnapshot &operator=(NodeSnapshot &&snapshot) {
      // Need to prevent self move assignment
      if(this == &snapshot) {
        return *this;
      }
      
      // We do not allow any NodeSnapshot object to have non-empty
      // logical pointer
      assert(logical_node_p != nullptr);

      // NOTE: We need to delete the correct type of logical node
      // pointer since we called child class constructor
      if(is_leaf == true) {
        delete (LogicalLeafNode *)logical_node_p;
      } else {
        delete (LogicalInnerNode *)logical_node_p;
      }

      node_id = snapshot.node_id;
      node_p = snapshot.node_p;
      logical_node_p = snapshot.logical_node_p;
      has_data = snapshot.has_data;
      has_metadata = snapshot.has_metadata;
      is_leaf = snapshot.is_leaf;
      is_leftmost_child = snapshot.is_leftmost_child;
      is_root = snapshot.is_root;
      lbound_p = snapshot.lbound_p;

      // This completes the move semantics
      snapshot.logical_node_p = nullptr;

      // Return a reference of LHS of the assignment
      return *this;
    }

    /*
     * Destructor - Automatically destroy the logical node object
     *
     * NOTE: It is possible for LogicalNode to have an empty logical node
     * pointer (being moved out), so we check for pointer emptiness before
     * deleting
     *
     * NOTE 2: When deleting the base class pointer, we need to
     * convert it to the appropriate child class type first in order
     * to call the child type destructor to free all associated
     * resources
     */
    ~NodeSnapshot() {
      if(logical_node_p != nullptr) {
        // We need to call derived class destructor
        // to properly destroy all associated resources
        // Same think needs to be done whenever destroying
        // logical_node_p in other contexts
        if(is_leaf == true) {
          delete (LogicalLeafNode *)logical_node_p;
        } else {
          delete (LogicalInnerNode *)logical_node_p;
        }
      }

      return;
    }

    /*
     * ResetLogicalNode() - Clear data and metadata
     *
     * We need to clear metadata before calling collectAll* function
     * since they rely on the fact that all metadata being invalid on entering
     */
    void ResetLogicalNode() {
      // First reset metadata
      logical_node_p->lbound_p = nullptr;
      logical_node_p->ubound_p = nullptr;
      logical_node_p->next_node_id = INVALID_NODE_ID;
      has_metadata = false;

      // Then reset data
      if(is_leaf == true) {
        LogicalLeafNode *node_p = GetLogicalLeafNode();

        node_p->key_value_set.clear();
        node_p->pointer_list.clear();
      } else {
        LogicalInnerNode *node_p = GetLogicalInnerNode();

        node_p->key_value_map.clear();
      }

      has_data = false;

      return;
    }

    /*
     * SwitchPhysicalPointer() - Changes physical pointer, and invalidate
     *                           cached data & metadata of logical node
     *
     * After installing a new delta node using CAS, we need to switch the
     * snapshot to the newly installed delta node. This requires invalidation
     * of cached logical node data.
     *
     * NOTE: This function does not change root node identity
     */
    void SwitchPhysicalPointer(const BaseNode *p_node_p) {
      node_p = p_node_p;

      // Since physical pointer has changed, we invalidate it here
      ResetLogicalNode();

      return;
    }

    /*
     * GetLogicalLeafNode() - Return the logical leaf node
     *
     * We use this as a wrapper to save a type casting
     */
    inline LogicalLeafNode *GetLogicalLeafNode() {
      assert(is_leaf == true);
      // Make sure we are reading valid pointer (i.e. die at
      // the place where misuse happens)
      assert(logical_node_p != nullptr);

      return static_cast<LogicalLeafNode *>(logical_node_p);
    }

    /*
     * GetLogicalInnerNode() - Reutrn the logical inner node
     *
     * We use this as a wrapper to save a type casting
     */
    inline LogicalInnerNode *GetLogicalInnerNode() {
      assert(is_leaf == false);
      assert(logical_node_p != nullptr);

      return static_cast<LogicalInnerNode *>(logical_node_p);
    }

    /*
     * GetHighKey() - Return the high key of a snapshot
     *
     * This deals with different node types
     */
    inline const KeyType *GetHighKey() {
      if(is_leaf == true) {
        return GetLogicalLeafNode()->ubound_p;
      } else {
        return GetLogicalInnerNode()->ubound_p;
      }
    }

    /*
     * GetLowKey() - Return the low key of a snapshot
     *
     * This is useful sometimes for assertion conditions
     */
    inline const KeyType *GetLowKey() {
      if(is_leaf == true) {
        return GetLogicalLeafNode()->lbound_p;
      } else {
        return GetLogicalInnerNode()->lbound_p;
      }
    }

    /*
     * GetNextNodeID() - Return the NodeID of its right sibling
     *
     * NOTE: Do not use const NodeID as return type - it will be ignored
     * by compiler and induce an error
     */
    inline NodeID GetNextNodeID() {
      if(is_leaf == true) {
        return GetLogicalLeafNode()->next_node_id;
      } else {
        return GetLogicalInnerNode()->next_node_id;
      }
    }

    /*
     * GetRightSiblingNodeID() - Return the node ID of right sibling
     *                           node (could be INVALID_NODE_ID)
     *
     * This function deals with different types of nodes
     */
    inline NodeID GetRightSiblingNodeID() {
      if(is_leaf == true) {
        return GetLogicalLeafNode()->next_node_id;
      } else {
        return GetLogicalInnerNode()->next_node_id;
      }
    }

    /*
     * SetLeftMostChildFlag() - Set a snapshot to be the left most child
     *
     * This function could only be called once to enforce the invariant that
     * once we see a node as left most node, it is impossible for us to
     * mark it as non-leftmost
     */
    inline void SetLeftMostChildFlag() {
      assert(is_leftmost_child == false);

      is_leftmost_child = true;

      return;
    }

    /*
     * SetRootFlag() - Set is_root flag for a NodeSnapshot instance
     */
    inline void SetRootFlag() {
      assert(is_root == false);

      is_root = true;

      return;
    }

    /*
     * SetLowKey() - Set the low key of a NodeSnapshot to indicate
     *               the low key we use when entering current node
     *
     * This could be called for multiple times since we allow changing
     * low key during repositioning
     */
    inline void SetLowKey(const KeyType *p_lbound_p) {
      lbound_p = p_lbound_p;

      return;
    }

    /*
     * MoveLogicalNode() - Move out the logical node
     *
     * This function returns the base logical node pointer to caller,
     * and set the pointer inside this instance as nullptr. The semantics
     * is similar to the move semantics in a sense that the "read" of
     * base logical node pointer is destructive and will cause the container
     * to drop ownership of that pointer.
     *
     * After calling this function, the destruction of NodeSnapshot instance
     * does not free memory allocated for logical node. Caller needs to track
     * the correct type of logical node returned, and call corresponding
     * destructors accordingly
     */
    inline BaseLogicalNode *MoveLogicalNode() {
      BaseLogicalNode *temp = logical_node_p;

      // Need to save original value before setting this to nullptr
      // After setting this to nullptr, the destruction of NodeSnaoshot
      // does not call logical node's destructor
      logical_node_p = nullptr;

      return temp;
    }

    /*
     * MoveLogicalLeafNode() - Move out logical node as a logical leaf node
     *
     * Please refer to MoveLogicalNode for more information about how the
     * semantics being similar to the move semantics
     */
    inline LogicalLeafNode *MoveLogicalLeafNode() {
      LogicalLeafNode *temp = \
        static_cast<LogicalLeafNode *>(logical_node_p);

      logical_node_p = nullptr;

      return temp;
    }

    /*
     * MoveLogicalInnerNode() - Move out logical node as a logical inner node
     *
     * Please refer to MoveLogicalNode for more information
     */
    inline LogicalInnerNode *MoveLogicalInnerNode() {
      LogicalInnerNode *temp = \
        static_cast<LogicalInnerNode *>(logical_node_p);

      logical_node_p = nullptr;

      return temp;
    }
  };


  ////////////////////////////////////////////////////////////////////
  // Interface Method Implementation
  ////////////////////////////////////////////////////////////////////

 public:
  /*
   * Constructor - Set up initial environment for BwTree
   *
   * Any tree instance must start with an intermediate node as root, together
   * with an empty leaf node as child
   */
  BwTree(KeyComparator p_key_cmp_obj = KeyComparator{},
         KeyEqualityChecker p_key_eq_obj = KeyEqualityChecker{},
         ValueEqualityChecker p_value_eq_obj = ValueEqualityChecker{},
         ValueHashFunc p_value_hash_obj = ValueHashFunc{},
         bool p_key_dup = false) :
      key_cmp_obj{p_key_cmp_obj},        // Raw key comparator
                                         // (copy construct)
      wrapped_key_cmp_obj{&key_cmp_obj}, // Wrapped key comparator
                                         // (needs raw key comparator)
      key_eq_obj{p_key_eq_obj},
      value_eq_obj{p_value_eq_obj},
      value_hash_obj{p_value_hash_obj},
      key_dup{p_key_dup},
      next_unused_node_id{0},
      insert_op_count{0},
      insert_abort_count{0},
      delete_op_count{0},
      delete_abort_count{0},
      update_op_count{0},
      update_abort_count{0},
      idb{this},
      epoch_manager{} {
    bwt_printf("Bw-Tree Constructor called. "
               "Setting up execution environment...\n");

    InitMappingTable();
    InitNodeLayout();

    bwt_printf("Starting epoch manager thread...\n");
    epoch_manager.StartThread();

    return;
  }

  /*
   * Destructor - Destroy BwTree instance
   *
   * NOTE: object member's destructor is called first prior to
   * the destructor of the container. So epoch manager's destructor
   * has been called before we free the whole tree
   */
  ~BwTree() {
    bwt_printf("Constructor: Free tree nodes\n");

    // Free all nodes recursively
    //FreeAllNodes(GetNode(root_id.load()));

    return;
  }

  /*
   * FreeAllNodes() - Free all nodes currently in the tree
   *
   * For normal destruction, we do not accept InnerAbortNode,
   * InnerRemoveNode, and LeafRemoveNode, since these three types
   * only appear as temporary states, and must be completed before
   * a thread could finish its job and exit (after posting remove delta
   * the thread always aborts and restarts traversal from the root)
   *
   * NOTE: Do not confuse this function with FreeEpochDeltaChain
   * in EpochManager. These two functions differs in details (see above)
   * and could not be used interchangeably.
   *
   * NOTE 2: This function should only be called in single threaded
   * environment since it assumes sole ownership of the entire tree
   * This is trivial during normal destruction, but care must be taken
   * when this is not true.
   *
   * This node calls destructor according to the type of the node, considering
   * that there is not virtual destructor defined for sake of running speed.
   */
  void FreeAllNodes(const BaseNode *node_p) {
    const BaseNode *next_node_p = node_p;
    int freed_count = 0;

    // These two tells whether we have seen InnerSplitNode
    // NOTE: ubound does not need to be passed between function calls
    // since ubound is only used to prevent InnerNode containing
    // SepItem that has been invalidated by a split
    bool has_ubound = false;
    KeyType ubound{RawKeyType{}};

    while(1) {
      node_p = next_node_p;
      assert(node_p != nullptr);

      NodeType type = node_p->GetType();
      bwt_printf("type = %d\n", (int)type);

      switch(type) {
        case NodeType::LeafInsertType:
          next_node_p = ((LeafInsertNode *)node_p)->child_node_p;

          delete (LeafInsertNode *)node_p;
          freed_count++;

          break;
        case NodeType::LeafDeleteType:
          next_node_p = ((LeafDeleteNode *)node_p)->child_node_p;

          delete (LeafDeleteNode *)node_p;

          break;
        case NodeType::LeafSplitType:
          next_node_p = ((LeafSplitNode *)node_p)->child_node_p;

          delete (LeafSplitNode *)node_p;
          freed_count++;

          break;
        case NodeType::LeafMergeType:
          FreeAllNodes(((LeafMergeNode *)node_p)->child_node_p);
          FreeAllNodes(((LeafMergeNode *)node_p)->right_merge_p);

          delete (LeafMergeNode *)node_p;
          freed_count++;

          // Leaf merge node is an ending node
          return;
        case NodeType::LeafType:
          delete (LeafNode *)node_p;
          freed_count++;

          // We have reached the end of delta chain
          return;
        case NodeType::InnerInsertType:
          next_node_p = ((InnerInsertNode *)node_p)->child_node_p;

          delete (InnerInsertNode *)node_p;
          freed_count++;

          break;
        case NodeType::InnerDeleteType:
          next_node_p = ((InnerDeleteNode *)node_p)->child_node_p;

          delete (InnerDeleteNode *)node_p;
          freed_count++;

          break;
        case NodeType::InnerSplitType: {
          const InnerSplitNode *split_node_p = \
            static_cast<const InnerSplitNode *>(node_p);

          next_node_p = split_node_p->child_node_p;

          // We only save ubound for the first split delta from top
          // to bottom
          // Since the first is always the most up-to-date one
          if(has_ubound == false) {
            // Save the upper bound of the node such that we do not
            // free child nodes that no longer belong to this inner
            // node when we see the InnerNode
            // NOTE: Cannot just save pointer since the pointer
            // will be invalidated after deleting node
            ubound = split_node_p->split_key;
            has_ubound = true;
          }

          delete (InnerSplitNode *)node_p;
          freed_count++;

          break;
        }
        case NodeType::InnerMergeType:
          FreeAllNodes(((InnerMergeNode *)node_p)->child_node_p);
          FreeAllNodes(((InnerMergeNode *)node_p)->right_merge_p);

          delete (InnerMergeNode *)node_p;
          freed_count++;

          // Merge node is also an ending node
          return;
        case NodeType::InnerType: { // Need {} since we initialized a variable
          const InnerNode *inner_node_p = \
            static_cast<const InnerNode *>(node_p);

          // Grammar issue: Since the inner node is const, all its members
          // are const, and so we could only declare const iterator on
          // the vector member, and therefore ":" only returns const reference
          for(const SepItem &sep_item : inner_node_p->sep_list) {
            NodeID node_id = sep_item.node;

            // Load the node pointer using child node ID of the inner node
            const BaseNode *child_node_p = GetNode(node_id);

            // If there is a split node and the item key >= split key
            // then we know the item has been invalidated by the split
            if((has_ubound == true) && \
               (KeyCmpGreaterEqual(sep_item.key, ubound))) {
              break;
            }

            // Then free all nodes in the child node (which is
            // recursively defined as a tree)
            FreeAllNodes(child_node_p);
          }

          // Access its content first and then delete the node itself
          delete inner_node_p;
          freed_count++;

          // Since we free nodes recursively, after processing an inner
          // node and recursively with its child nodes we could return here
          return;
        }
        default:
          // This does not include INNER ABORT node
          bwt_printf("Unknown node type: %d\n", (int)type);

          assert(false);
          return;
      } // switch

      bwt_printf("Freed node of type %d\n", (int)type);
    } // while 1

    bwt_printf("Freed %d nodes during destruction\n", freed_count);

    return;
  }

  /*
   * InitNodeLayout() - Initialize the nodes required to start BwTree
   *
   * We need at least a root inner node and a leaf node in order
   * to guide the first operation to the right place
   */
  void InitNodeLayout() {
    bwt_printf("Initializing node layout for root and first page...\n");

    root_id = GetNextNodeID();
    assert(root_id == 0UL);

    first_node_id = GetNextNodeID();
    assert(first_node_id == 1UL);

    SepItem neg_si {GetNegInfKey(), first_node_id};

    InnerNode *root_node_p = \
      new InnerNode{GetNegInfKey(), GetPosInfKey(), INVALID_NODE_ID};

    root_node_p->sep_list.push_back(neg_si);

    bwt_printf("root id = %lu; first leaf id = %lu\n",
               root_id.load(),
               first_node_id);
    bwt_printf("Plugging in new node\n");

    InstallNewNode(root_id, root_node_p);

    LeafNode *left_most_leaf = \
      new LeafNode{GetNegInfKey(), GetPosInfKey(), INVALID_NODE_ID};

    InstallNewNode(first_node_id, left_most_leaf);

    return;
  }

  /*
   * InitMappingTable() - Initialize the mapping table
   *
   * It initialize all elements to NULL in order to make
   * first CAS on the mapping table would succeed
   */
  void InitMappingTable() {
    bwt_printf("Initializing mapping table.... size = %lu\n",
               MAPPING_TABLE_SIZE);

    for(NodeID i = 0;i < MAPPING_TABLE_SIZE;i++) {
      mapping_table[i] = nullptr;
    }

    return;
  }

  /*
   * GetWrappedKey() - Return an internally wrapped key type used
   *                   to traverse the index
   */
  inline KeyType GetWrappedKey(RawKeyType key) {
    return KeyType {key};
  }

  /*
   * GetPosInfKey() - Get a positive infinite key
   *
   * Assumes there is a trivial constructor for RawKeyType
   */
  inline KeyType GetPosInfKey() {
    return KeyType {ExtendedKeyValue::PosInf};
  }

  /*
   * GetNegInfKey() - Get a negative infinite key
   *
   * Assumes there is a trivial constructor for RawKeyType
   */
  inline KeyType GetNegInfKey() {
    return KeyType {ExtendedKeyValue::NegInf};
  }

  /*
   * GetNextNodeID() - Thread-safe lock free method to get next node ID
   *
   * This function will not return until we have successfully obtained the
   * ID and increased counter by 1
   */
  NodeID GetNextNodeID() {
    bool ret = false;
    NodeID current_id, next_id;

    do {
      current_id = next_unused_node_id.load();
      next_id = current_id + 1;

      // Optimistic approach: If nobody has touched next_id then we are done
      // Otherwise CAS would fail, and we try again
      ret = next_unused_node_id.compare_exchange_strong(current_id, next_id);
    } while(ret == false);

    return current_id;
  }

  /*
   * InstallNodeToReplace() - Install a node to replace a previous one
   *
   * If installation fails because CAS returned false, then return false
   * This function does not retry
   */
  bool InstallNodeToReplace(NodeID node_id,
                            const BaseNode *node_p,
                            const BaseNode *prev_p) {
    // Make sure node id is valid and does not exceed maximum
    assert(node_id != INVALID_NODE_ID);
    assert(node_id < MAPPING_TABLE_SIZE);

    // If idb is activated, then all operation will be blocked before
    // they could call CAS and change the key
    #ifdef INTERACTIVE_DEBUG
    debug_stop_mutex.lock();
    debug_stop_mutex.unlock();
    #endif

    bool ret = mapping_table[node_id].compare_exchange_strong(prev_p, node_p);

    return ret;
  }

  /*
   * InstallRootNode() - Replace the old root with a new one
   *
   * There is change that this function would fail. In that case it returns
   * false, which implies there are some other threads changing the root ID
   */
  bool InstallRootNode(NodeID old_root_node_id,
                       NodeID new_root_node_id) {
    bool ret = \
      root_id.compare_exchange_strong(old_root_node_id,
                                      new_root_node_id);

    return ret;
  }

  /*
   * InstallNewNode() - Install a new node into the mapping table
   *
   * This function does not return any value since we assume new node
   * installation would always succeed
   */
  void InstallNewNode(NodeID node_id,
                      const BaseNode *node_p) {
    // We initialize the mapping table to always have 0 for
    // unused entries
    bool ret = InstallNodeToReplace(node_id, node_p, nullptr);

    // So using nullptr to CAS must succeed
    assert(ret == true);
    (void)ret;

    return;
  }

  /*
   * GetNode() - Return the pointer mapped by a node ID
   *
   * This function checks the validity of the node ID
   *
   * NOTE: This function fixes a snapshot; its counterpart using
   * CAS instruction to install a new node creates new snapshot
   * and the serialization order of these two atomic operations
   * determines actual serialization order
   *
   * If we want to keep the same snapshot then we should only
   * call GetNode() once and stick to that physical pointer
   */
  const BaseNode *GetNode(const NodeID node_id) {
    assert(node_id != INVALID_NODE_ID);
    assert(node_id < MAPPING_TABLE_SIZE);

    return mapping_table[node_id].load();
  }

  /*
   * IsLeafDeltaChainType() - Returns whether type is on of the
   *                          possible types on a leaf delta chain
   *
   * NOTE: Since we distinguish between leaf and inner node on all
   * SMOs it is easy to just judge underlying data node type using
   * the top of delta chain
   */
  bool IsLeafDeltaChainType(const NodeType type) const {
    return (type == NodeType::LeafDeleteType ||
            type == NodeType::LeafInsertType ||
            type == NodeType::LeafMergeType ||
            type == NodeType::LeafRemoveType ||
            type == NodeType::LeafSplitType ||
            type == NodeType::LeafType);
  }

  /*
   * Traverse() - Traverse down the tree structure, handles abort
   *
   * This function is implemented as a state machine that allows a thread
   * to jump back to Init state when necessary (probably a CAS failed and
   * it does not want to resolve it from bottom up)
   *
   * It stops at page level, and then the top on the context stack is a
   * leaf level snapshot, with all SMOs, consolidation, and possibly
   * split/remove finished
   *
   * NOTE: We could choose whether to let this function collect value given
   * the key, or not collect value. Because sometimes we just want to locate
   * the correct page that contains a certain key, so the option is useful
   */
  void Traverse(Context *context_p,
                bool collect_value) {
    // We must start from a clean state
    assert(context_p->path_list.size() == 0);

    // This will use lock
    #ifdef INTERACTIVE_DEBUG
    idb.AddKey(context_p->search_key);
    #endif

    // This is the low key (i.e. "virtual sep key")
    // for root node
    // NOTE: This should be put inside the scope in case it is
    // corrupted
    KeyType root_lbound_key = GetNegInfKey();

    while(1) {
      // NOTE: break only breaks out this switch
      switch(context_p->current_state) {
        case OpState::Init: {
          assert(context_p->path_list.size() == 0);
          assert(context_p->abort_flag == false);
          assert(context_p->current_level == 0);

          // This is the serialization point for reading/writing root node
          NodeID start_node_id = root_id.load();

          // We need to call this even for root node since there could
          // be split delta posted on to root node
          LoadNodeID(start_node_id,
                     context_p,
                     &root_lbound_key,  // Root node has a "virtual sep"
                     true);             // root ID is implicitly leftmost

          // There could be an abort here, and we could not directly jump
          // to Init state since we would like to do some clean up or
          // statistics after aborting
          if(context_p->abort_flag == true) {
            context_p->current_state = OpState::Abort;

            // This goes to the beginning of loop
            break;
          }

          // Make sure this flag is set
          NodeSnapshot *snapshot_p = GetLatestNodeSnapshot(context_p);
          assert(snapshot_p->is_root == true);
          (void)snapshot_p;

          bwt_printf("Successfully loading root node ID\n");

          // root node must be an inner node
          // NOTE: We do not traverse down in this state, just hand it
          // to Inner state and let it do all generic job
          context_p->current_state = OpState::Inner;
          context_p->current_level++;

          break;
        } // case Init
        case OpState::Inner: {
          // For inner nodes, when traversing down we should keep track of the
          // low key for the NodeID returned, and whether it is the left most
          // child of the parent node
          const KeyType *lbound_p = nullptr;

          NodeID child_node_id = \
            NavigateInnerNode(context_p, &lbound_p);

          // Navigate could abort since it goes to another NodeID
          if(context_p->abort_flag == true) {
            bwt_printf("Navigate Inner Node abort. ABORT\n");

            // This behavior is defined in Navigate function
            assert(child_node_id == INVALID_NODE_ID);

            context_p->current_state = OpState::Abort;

            break;
          }

          bool is_leftmost_child = false;

          // This returns the current inner node that we are traversing into
          // There must be at least one at this stage
          NodeSnapshot *snapshot_p = GetLatestNodeSnapshot(context_p);

          // This is the low key of current inner node
          const KeyType *current_lbound_p = snapshot_p->lbound_p;

          // If the sep key equals the low key of current node then we
          // know it definitely is the left most child since lbound is always
          // tight
          // NOTE: For root node, current_lbound_p is -Inf
          if(KeyCmpEqual(*current_lbound_p, *lbound_p) == true) {
            bwt_printf("Child node id = %lu is a left most child\n",
                       child_node_id);

            is_leftmost_child = true;
          }

          // This might load a leaf child
          LoadNodeID(child_node_id,
                     context_p,
                     lbound_p,
                     is_leftmost_child);

          if(context_p->abort_flag == true) {
            bwt_printf("LoadNodeID aborted. ABORT\n");

            context_p->current_state = OpState::Abort;

            break;
          }

          // This is the node we have just loaded
          snapshot_p = GetLatestNodeSnapshot(context_p);
          // We need to deal with leaf node and inner node when traversing down
          // an inner node
          bool is_leaf = snapshot_p->is_leaf;

          // Take care: Do not use conflict names in the
          // outer scope
          const KeyType *snapshot_lbound_p = nullptr;
          (void)snapshot_lbound_p;
          const KeyType *snapshot_ubound_p = nullptr;

          // Get metadata from the next node we are going to
          // navigate through
          if(is_leaf == true) {
            CollectMetadataOnLeaf(snapshot_p);

            LogicalLeafNode *logical_node_p = \
              snapshot_p->GetLogicalLeafNode();

            snapshot_lbound_p = logical_node_p->lbound_p;
            snapshot_ubound_p = logical_node_p->ubound_p;

            //assert(snapshot_lbound_p != nullptr);
          } else {
            CollectMetadataOnInner(snapshot_p);

            LogicalInnerNode *logical_node_p = \
              snapshot_p->GetLogicalInnerNode();

            snapshot_lbound_p = logical_node_p->lbound_p;
            snapshot_ubound_p = logical_node_p->ubound_p;

            //assert(snapshot_lbound_p != nullptr);
          }

          // Make sure the lbound is not empty
          idb_assert_key(snapshot_p->node_id,
                         context_p->search_key,
                         context_p,
                         snapshot_lbound_p != nullptr);

          // This must be true since the NodeID to low key mapping
          // is constant
          assert(KeyCmpGreaterEqual(context_p->search_key,
                                    *snapshot_lbound_p) == true);

          // If this happen then it means between the moment we get NodeID
          // and we get actual physical pointer, it has splited, changing
          // the high key to a smaller value
          // In that case we need to abort and re-traverse
          if(KeyCmpGreaterEqual(context_p->search_key,
                                *snapshot_ubound_p) == true) {
            bwt_printf("Child node high key has changed. ABORT\n");

            // TODO: In the future if we observed an inconsistent
            // state here (need to go right) then we need to
            // help along the SMO
            context_p->abort_flag = true;
            context_p->current_state = OpState::Abort;

            break;
          }


          if(is_leaf == true) {
            bwt_printf("The next node is a leaf\n");

            // Then we start traversing leaf node, and deal
            // with potential aborts
            context_p->current_state = OpState::Leaf;
          }

          // go to next level
          context_p->current_level++;

          break;
        } // case Inner
        case OpState::Leaf: {
          // We do not collect value for this call
          // since we only want to go to the right sibling
          // if there is one
          NavigateLeafNode(context_p,
                           collect_value); // This is argument

          if(context_p->abort_flag == true) {
            bwt_printf("NavigateLeafNode aborts. ABORT\n");

            context_p->current_state = OpState::Abort;

            break;
          }

          bwt_printf("Found leaf node. Abort count = %lu, level = %d\n",
                     context_p->abort_counter,
                     context_p->current_level);

          // If there is no abort then we could safely return
          return;

          break;
        }
        case OpState::Abort: {
          std::vector<NodeSnapshot> *path_list_p = \
            &context_p->path_list;

          assert(path_list_p->size() > 0);

          // We roll back for at least one level
          // and stop at the first
          NodeSnapshot *snapshot_p = nullptr;
          do {
            path_list_p->pop_back();
            context_p->current_level--;

            if(path_list_p->size() == 0) {
              context_p->current_state = OpState::Init;
              context_p->current_level = 0;

              break;
            }

            snapshot_p = &path_list_p->back();

            // Even if we break on leaf level we are now
            // on inner level
            context_p->current_state = OpState::Inner;

            // If we see a match after popping the first one
            // then quit aborting
            if(snapshot_p->node_p == GetNode(snapshot_p->node_id)) {
              break;
            }
          }while(1);
          // This calls destructor for all NodeSnapshot object, and
          // will release memory for logical node object
          //path_list_p->clear();

          context_p->abort_counter++;
          //context_p->current_level = 0;
          //context_p->current_state = OpState::Init;
          context_p->abort_flag = false;

          break;
        } // case Abort
        default: {
          bwt_printf("ERROR: Unknown State: %d\n",
                     static_cast<int>(context_p->current_state));
          assert(false);
          break;
        }
      } // switch current_state
    } // while(1)

    return;
  }

  /*
   * CollectNewNodesSinceLastSnapshot() - This function compares two different
   *                                      snapshots
   *
   * If CAS fails then we know the snaoshot is no longer most up-to-date
   * in which case we need to find out what happened between the time
   * we took the snapshot and the time we did CAS
   *
   * There are three possibilities:
   * (1) There are delta nodes appended onto the delta chain
   *     In this case we could find pointer after traversing down
   *     for some number of nodes
   *     All new nodes excluding the common one and below are
   *     pushed into node_list_p, and return false
   * (2) The old delta chain has been consolidated and replaced
   *     with a brand new one. There is no old pointers in the
   *     new delta chain (epoch manager guarantees they will not
   *     be recycled before the current thread exits)
   *     In this case we will traverse down to the base node
   *     and could not find a matching pointer
   *     In this case the entire delta chain is pushed (naturally
   *     because there is no common pointer) and return true
   *     NOTE: In this case the base page is also in the node list
   * (3) The NodeID has been deleted (so that new_pointer_p is nullptr)
   *     because the NodeID has been removed and merged into its left
   *     sibling.
   *     Return true and node_list_p is empty
   *
   */
  bool CollectNewNodesSinceLastSnapshot(
    const BaseNode *old_node_p,
    const BaseNode *new_node_p,
    std::vector<const BaseNode *> *node_list_p) const {
    // We only call this function if CAS fails, so these two pointers
    // must be different
    assert(new_node_p != old_node_p);

    // Return true means the entire delta chain has changed
    if(new_node_p == nullptr) {
      bwt_printf("The NodeID has been released permanently\n");

      return true;
    }

    while(1) {
      if(new_node_p != old_node_p) {
        node_list_p->push_back(new_node_p);
      } else {
        bwt_printf("Find common pointer! Delta chain append.\n")

        // We have found two equivalent pointers
        // which implies the delta chain is only prolonged
        // but not consolidated or removed
        return false;
      }

      if(!new_node_p->IsDeltaNode()) {
        bwt_printf("Did not find common pointer! Delta chian consolidated\n");

        // If we have reached the bottom and still do not
        // see equivalent pointers then the entire delta
        // chain has been consolidated
        return true;
      }

      // Try next one
      const DeltaNode *delta_node_p = \
        static_cast<const DeltaNode *>(new_node_p);

      new_node_p = delta_node_p->child_node_p;
    }

    assert(false);
    return false;
  }

  /*
   * LocateSeparatorByKey() - Locate the child node for a key
   *
   * This functions works with any non-empty inner nodes. However
   * it fails assertion with empty inner node
   *
   * NOTE: This function takes a pointer that points to a new high key
   * if we have met a split delta node before reaching the base node.
   * The new high key is used to test against the search key
   *
   * NOTE 2: This function will hit assertion failure if the key
   * range is not correct OR the node ID is invalid
   *
   * NOTE 3: This function takes a fourth argument as return value
   * to record the separator key associated with the NodeID
   */
  NodeID LocateSeparatorByKey(const KeyType &search_key,
                              const InnerNode *inner_node_p,
                              const KeyType *ubound_p,
                              const KeyType **lbound_p_p) {
    // If the upper bound is not set because we have not met a
    // split delta node, then just set to the current upper bound
    if(ubound_p == nullptr) {
      ubound_p = &inner_node_p->ubound;
    }

    const std::vector<SepItem> *sep_list_p = &inner_node_p->sep_list;

    // We do not know what to do for an empty inner node
    assert(sep_list_p->size() != 0UL);

    auto iter1 = sep_list_p->begin();
    auto iter2 = iter1 + 1;

    // NOTE: If there is only one element then we would
    // not be able to go into while() loop
    // and in that case we just check for upper bound
    //assert(iter2 != sep_list_p->end());

    // TODO: Replace this with binary search
    while(iter2 != sep_list_p->end()) {
      if(KeyCmpGreaterEqual(search_key, iter1->key) && \
         KeyCmpLess(search_key, iter2->key)) {
        // We set separator key before returning
        *lbound_p_p = &iter1->key;

        return iter1->node;
      }

      iter1++;
      iter2++;
    }

    // This assertion failure could only happen if we
    // hit +Inf as separator
    assert(iter1->node != INVALID_NODE_ID);

    // If search key >= upper bound (natural or artificial) then
    // we have hit the wrong inner node
    idb_assert(KeyCmpLess(search_key, *ubound_p));

    // Search key must be greater than or equal to the lower bound
    // which is assumed to be a constant associated with a NodeID
    assert(KeyCmpGreaterEqual(search_key, inner_node_p->lbound));

    // In this corner case, iter1 is the correct sep/NodeID pair
    *lbound_p_p = &iter1->key;

    return iter1->node;
  }

  /*
   * LocateLeftSiblingByKey() - Locate the left sibling given a key
   *
   * If the search key matches the left most separator then return
   * INVALID_NODE_ID to imply the left sibling is in another node of
   * the same level. For searching removed node's left sibling this
   * should be treated as an error; but if we are searching for the
   * real left sibling then it is a signal for us to jump to the left
   * sibling of the current node to search again
   *
   * If the key is not in the right range of the inner node (this is possible
   * if the key was once included in the node, but then node splited)
   * then assertion fail. This condition should be checked before calling
   * this routine
   */
  NodeID LocateLeftSiblingByKey(const KeyType &search_key,
                                const LogicalInnerNode *logical_inner_p)
    const {
    // This could happen when the inner node splits
    if(KeyCmpGreaterEqual(search_key, *logical_inner_p->ubound_p)) {
      bwt_printf("ERROR: Search key >= inner node upper bound!\n");

      assert(false);
    }

    // This is definitely an error no matter what happened
    if(KeyCmpLess(search_key, *logical_inner_p->lbound_p)) {
      bwt_printf("ERROR: Search key < inner node lower bound!\n");

      assert(false);
    }

    size_t node_size = logical_inner_p->key_value_map.size();

    if(node_size == 0) {
      bwt_printf("Logical inner node is empty\n");

      assert(false);
    } else if(node_size == 1) {
      bwt_printf("There is only 1 entry, implying jumping left\n");
      // We already guaranteed the key is in the range of the node
      // So as long as the node is consistent (bounds and actual seps)
      // then the key must match this sep
      // So we need to go left
      return INVALID_NODE_ID;
    }

    // From this line we know the number of seps is >= 2

    // Initialize two iterators from an ordered map
    // We are guaranteed that it1 and it2 are not null
    auto it1 = logical_inner_p->key_value_map.begin();

    // NOTE: std::map iterators are only bidirectional, which means it does
    // not support random + operation. Must use std::move or std::advance
    auto it2 = std::next(it1, 1);
    // Since we only restrict # of elements to be >= 2, it3 could be end()
    auto it3 = std::next(it2, 1);

    // We should not match first entry since there is no direct left
    // and in the situation of remove, we always do not remove
    // the left most child in any node since this would change
    // the lower bound of that node which is guaranteed not to change
    if(KeyCmpGreaterEqual(search_key, it1->first) && \
       KeyCmpLess(search_key, it2->first)) {
      bwt_printf("First entry is matched. Implying jumping left\n");

      return INVALID_NODE_ID;
    }

    while(1) {
      // If we already reached the last sep-ID pair
      // then only test the last sep
      if(it3 == logical_inner_p->key_value_map.end()) {
        if(KeyCmpGreaterEqual(search_key, it2->first)) {
          return it1->second;
        } else {
          // This should not happen since we already tested it
          // by upper bound
          assert(false);
        }
      }

      // If the second iterator matches the key (tested also with it3)
      // then we return the first iterator's node ID
      if(KeyCmpLess(search_key, it3->first) && \
         KeyCmpGreaterEqual(search_key, it2->first)) {
        return it1->second;
      }

      // This points to the left sib
      it1++;
      // This points to current node under testing
      it2++;
      // This points to next node as upperbound for current node
      it3++;
    }

    // After this line we already know that for every sep
    // the key is not matched

    bwt_printf("ERROR: Left sibling not found; Please check key before entering\n");

    // Loop will not break to here
    assert(false);
    return INVALID_NODE_ID;
  }

  /*
   * NavigateInnerNode() - Traverse down through the inner node delta chain
   *                       and probably horizontally to right sibling nodes
   *
   * This function does not have to always reach the base node in order to
   * find the target since we know for inner nodes it is always single key
   * single node mapping. Therefore there is neither need to keep a delta
   * pointer list to recover full key-value mapping, nor returning a base node
   * pointer to test low key and high key.
   *
   * However, if we have reached a base node, for debugging purposes we
   * need to test current search key against low key and high key
   *
   * NOTE: This function returns a NodeID, instead of NodeSnapshot since
   * its behaviour is not dependent on the actual content of the physical
   * pointer associated with the node ID, so we could choose to fix the
   * snapshot later
   *
   * NOTE: This function will jump to a sibling if the current node is on
   * a half split state. If this happens, then the flag inside snapshot_p
   * is set to true, and also the corresponding NodeId and BaseNode *
   * will be updated to reflect the newest sibling ID and pointer.
   * After returning of this function please remember to check the flag
   * and update path history. (Such jump may happen multiple times, so
   * do not make any assumption about how jump is performed)
   *
   * NOTE 2: This function is different from Navigate leaf version because it
   * takes an extra argument for remembering the separator key associated
   * with the NodeID.
   */
  NodeID NavigateInnerNode(Context *context_p,
                           const KeyType **lbound_p_p) {
    // First get the snapshot from context
    NodeSnapshot *snapshot_p = GetLatestNodeSnapshot(context_p);

    // This search key will not be changed during navigation
    const KeyType &search_key = context_p->search_key;

    // Make sure the structure is valid
    assert(snapshot_p->is_leaf == false);
    assert(snapshot_p->node_p != nullptr);
    assert(snapshot_p->node_id != INVALID_NODE_ID);

    // Even if it has data we need to keep traversing, since we want to
    // traverse to its sibling if there is one
    if(snapshot_p->has_data == true) {
      bwt_printf("Inner snapshot already has data.\n");
    }

    bool first_time = true;
    (void)first_time;

    // Save some keystrokes
    const BaseNode *node_p = snapshot_p->node_p;

    // We track current artificial high key brought about by split node
    const KeyType *ubound_p = nullptr;

    while(1) {
      NodeType type = node_p->GetType();

      switch(type) {
        case NodeType::InnerType: {
          const InnerNode *inner_node_p = \
            static_cast<const InnerNode *>(node_p);

          NodeID target_id = \
            LocateSeparatorByKey(search_key,
                                 inner_node_p,
                                 ubound_p,
                                 lbound_p_p); // This records the sep

          bwt_printf("Found child in inner node; child ID = %lu\n",
                     target_id);

          return target_id;
        } // case InnerType
        case NodeType::InnerRemoveType: {
          bwt_printf("ERROR: InnerRemoveNode not allowed\n");

          assert(first_time == true);
          assert(false);
        } // case InnerRemoveType
        case NodeType::InnerInsertType: {
          const InnerInsertNode *insert_node_p = \
            static_cast<const InnerInsertNode *>(node_p);

          const KeyType &insert_low_key = insert_node_p->insert_key;
          const KeyType &insert_high_key = insert_node_p->next_key;
          NodeID target_id = insert_node_p->new_node_id;

          if(KeyCmpGreaterEqual(search_key, insert_low_key) && \
             KeyCmpLess(search_key, insert_high_key)) {
            bwt_printf("Find target ID = %lu in insert delta\n", target_id);

            // Assign the separator key that brings us here
            *lbound_p_p = &insert_low_key;

            return target_id;
          }

          node_p = insert_node_p->child_node_p;

          break;
        } // InnerInsertType
        case NodeType::InnerDeleteType: {
          const InnerDeleteNode *delete_node_p = \
            static_cast<const InnerDeleteNode *>(node_p);

          // For inner delete node, we record its left and right sep
          // as a fast path
          // The node ID stored inside inner delete node is the NodeID
          // of its left sibling before deletion
          const KeyType &delete_low_key = delete_node_p->prev_key;
          const KeyType &delete_high_key = delete_node_p->next_key;
          NodeID target_id = delete_node_p->prev_node_id;

          if(KeyCmpGreaterEqual(search_key, delete_low_key) && \
             KeyCmpLess(search_key, delete_high_key)) {
            bwt_printf("Find target ID = %lu in delete delta\n", target_id);

            // For delete node, if there is a match then it must be
            // the prev separator key for deleted key
            *lbound_p_p = &delete_low_key;

            return target_id;
          }

          node_p = delete_node_p->child_node_p;

          break;
        } // InnerDeleteType
        case NodeType::InnerSplitType: {
          const InnerSplitNode *split_node_p = \
            static_cast<const InnerSplitNode *>(node_p);

          const KeyType &split_key = split_node_p->split_key;
          // If current key is on the new node side,
          // we need to update tree snapshot to reflect the fact that we have
          // traversed to a new NodeID
          if(KeyCmpGreaterEqual(search_key, split_key)) {
            bwt_printf("Go to split branch\n");

            NodeID branch_id = split_node_p->split_sibling;

            // Try to jump to the right branch
            // If jump fails just abort
            JumpToNodeId(branch_id,
                         context_p,
                         &split_key, // Equals low key of the target node
                         false);     // Always could not be left child

            if(context_p->abort_flag == true) {
              bwt_printf("JumpToNodeID aborts. ABORT\n");

              // Before returning an invalid node ID, set it to NULL
              *lbound_p_p = nullptr;

              return INVALID_NODE_ID;
            }

            snapshot_p = GetLatestNodeSnapshot(context_p);
            node_p = snapshot_p->node_p;

            // NOTE: IT IS POSSIBLE THAT JUMP TO NODE ID
            // ALSO LOAD INTO THE SNAPSHOT
            // But we do not care since data will not interfere here
            // with our search (no map insertion conflict as in leaf node)
            if(snapshot_p->has_data == true) {
              bwt_printf("After inner jumping there is data\n");
            }

            // Since we have jumped to a new NodeID, we could see a remove node
            first_time = true;

            // Continue in the while loop to avoid setting first_time to false
            continue;
          } else {
            // If we do not take the branch, then the high key has changed
            // since the splited half takes some keys from the logical node
            // downside
            ubound_p = &split_key;

            node_p = split_node_p->child_node_p;
          }

          break;
        } // case InnerSplitType
        case NodeType::InnerMergeType: {
          const InnerMergeNode *merge_node_p = \
            static_cast<const InnerMergeNode *>(node_p);

          const KeyType &merge_key = merge_node_p->merge_key;

          // Here since we will only take one branch, so
          // high key does not need to be updated
          // Since we still could not know the high key
          if(KeyCmpGreaterEqual(search_key, merge_key)) {
            node_p = merge_node_p->right_merge_p;
          } else {
            node_p = merge_node_p->child_node_p;
          }

          break;
        } // InnerMergeType
        default: {
          bwt_printf("ERROR: Unknown node type = %d",
                     static_cast<int>(type));

          assert(false);
        }
      } // switch type

      first_time = false;
    } // while 1

    // Should not reach here
    assert(false);
    return INVALID_NODE_ID;
  }

  /*
   * CollectAllSepsOnInner() - Collect all separators given a snapshot
   *
   * This function is just a wrapper for the recursive version, and the
   * difference is that this wrapper accepts snapshot pointer, and also
   * it collects all metadata (low key, high key and next node id)
   *
   * After this function returns, it is guaranteed that has_data and
   * has_metadata flag inside snapshot object is set to true
   *
   * NOTE: This function makes use of cached logical node if there is one
   */
  void CollectAllSepsOnInner(NodeSnapshot *snapshot_p) {
    // If there is cached version of value then just make use of
    // it. The cache will be invalidated when we switch physical pointer
    if(snapshot_p->has_data == true) {
      bwt_printf("Fast path: Use previous cached value\n");

      assert(snapshot_p->has_metadata == true);

      return;
    }

    // Because metadata will interfere will sep collection
    // which also collects metadata
    if(snapshot_p->has_metadata == true) {
      snapshot_p->ResetLogicalNode();
    }

    assert(snapshot_p->has_metadata == false);

    LogicalInnerNode *logical_node_p = snapshot_p->GetLogicalInnerNode();

    CollectAllSepsOnInnerRecursive(snapshot_p->node_p,
                                   logical_node_p,
                                   true,
                                   true,
                                   true);

    // NOTE: We need to provide a comparator object in the tree
    KeyNodeIDMap temp_map{wrapped_key_cmp_obj};

    // Remove all key with INVALID_NODE_ID placeholder
    // since they are deleted nodes. This could not be
    // done inside the recursive function since it is
    // possible that a deleted key reappearing
    // NOTE: DO NOT USE ITERATOR AND erase() TOGETHER!
    // WE WOULD LEAVE OUT SOME VALUES IF DO SO
    for(auto &item : logical_node_p->key_value_map) {
      if(item.second != INVALID_NODE_ID) {
        temp_map[item.first] = item.second;
      }
    }

    // Move temporary map into the object to save copy
    // efforts of the content
    logical_node_p->key_value_map = std::move(temp_map);

    /*
    for(auto &item : logical_node_p->key_value_map) {
      assert(item.second != INVALID_NODE_ID);
    }
    */

    snapshot_p->has_data = true;
    snapshot_p->has_metadata = true;

    return;
  }

  /*
   * CollectMetadataOnInner() - Collect metadata on a logical inner node
   *
   * This function collects high key, low key and next node ID for an inner
   * node and stores it in the logical node
   *
   * After this function returns, it is guarantted that has_data flag is set
   * to false
   */
  void CollectMetadataOnInner(NodeSnapshot *snapshot_p) {
    // If there is cached metadata then we just use cached version
    if(snapshot_p->has_metadata == true) {
      bwt_printf("Fast path: Use previous cached metadata\n");

      return;
    }

    assert(snapshot_p->has_data == false);

    CollectAllSepsOnInnerRecursive(snapshot_p->node_p,
                                   snapshot_p->GetLogicalInnerNode(),
                                   true,
                                   true,
                                   false);

    snapshot_p->has_data = false;
    snapshot_p->has_metadata = true;

    return;
  }

  /*
   * CollectAllSpesOnInnerRecursive() - This is the counterpart on inner node
   *
   * Please refer to the function on leaf node for details. These two have
   * almost the same logical flow
   */
  void
  CollectAllSepsOnInnerRecursive(const BaseNode *node_p,
                                 LogicalInnerNode *logical_node_p,
                                 bool collect_lbound,
                                 bool collect_ubound,
                                 bool collect_sep) const {
    // Validate remove node, if any
    bool first_time = true;
    (void)first_time;

    // Used to restrict the upper bound in a local branch
    // If we are collecting upper bound, then this will finally
    // be assign to the logical node
    const KeyType *ubound_p = nullptr;

    while(1) {
      NodeType type = node_p->GetType();

      switch(type) {
        case NodeType::InnerType: {
          const InnerNode *inner_node_p = \
            static_cast<const InnerNode *>(node_p);

          // If the caller cares about the actual content
          if(collect_sep) {
            for(const SepItem &item : inner_node_p->sep_list) {
              // Since we are using INVALID_NODE_ID to mark deleted nodes
              // we check for other normal node id to avoid accidently
              // having a bug and deleting random keys
              assert(item.node != INVALID_NODE_ID);

              // If we observed an out of range key (brought about by split)
              // just ignore it (>= high key, if exists a high key)
              if(ubound_p != nullptr && \
                 KeyCmpGreaterEqual(item.key, *ubound_p)) {
                //bwt_printf("Detected a out of range key in inner base node\n");

                continue;
              }

              // If the sep key has already been collected, then ignore
              logical_node_p->key_value_map.insert( \
                typename decltype(logical_node_p->key_value_map)::value_type( \
                  item.key, item.node));
            } // for item : sep_list
          } // if collect_sep

          // If we collect low key (i.e. on the left most branch
          // of a merge tree)
          if(collect_lbound == true) {
            assert(logical_node_p->lbound_p == nullptr);

            logical_node_p->lbound_p = &inner_node_p->lbound;
          }

          // A base node also defines the high key
          if(ubound_p == nullptr) {
            ubound_p = &inner_node_p->ubound;
          }

          // If we clooect high key, then it is set to the local branch
          if(collect_ubound == true) {
            assert(logical_node_p->ubound_p == nullptr);

            logical_node_p->ubound_p = ubound_p;
          }

          // If it is the rightmost node, and we have not seen
          // a split node that would change its next node ID
          if(collect_ubound == true && \
             logical_node_p->next_node_id == INVALID_NODE_ID) {
            logical_node_p->next_node_id = inner_node_p->next_node_id;
          }


          // NOTE: SHOULD NOT DO IT HERE
          // If there is a merge, then this will make some deleted keys
          // reappear since its INVALID_NODE_ID has been removed

          // Remove all keys with INVALID_NODE_ID as target node id
          // since they should have been deleted, and we just put them
          // into the map to block any further key operation
          //for(auto &item : logical_node_p->key_value_map) {
          //  if(item.second == INVALID_NODE_ID) {
          //    logical_node_p->key_value_map.erase(item.first);
          //  }
          //}

          return;
        } // case InnerType
        case NodeType::InnerRemoveType: {
          bwt_printf("ERROR: InnerRemoveNode not allowed\n");

          assert(first_time == true);
          assert(false);
          return;
        } // case InnerRemoveType
        case NodeType::InnerInsertType: {
          const InnerInsertNode *insert_node_p = \
            static_cast<const InnerInsertNode *>(node_p);

          const KeyType &insert_key = insert_node_p->insert_key;
          assert(insert_node_p->new_node_id != INVALID_NODE_ID);

          if(collect_sep == true) {
            // If there is a ubound and the key is inside the bound, or
            // there is no bound
            if((ubound_p != nullptr && \
                KeyCmpLess(insert_key, *ubound_p)) || \
                ubound_p == nullptr) {
              // This will insert if key does not exist yet
              logical_node_p->key_value_map.insert( \
                typename decltype(logical_node_p->key_value_map)::value_type( \
                  insert_key, insert_node_p->new_node_id));
            }
          }

          // Go to next node
          node_p = insert_node_p->child_node_p;

          break;
        } // case InnerInsertType
        case NodeType::InnerDeleteType: {
          const InnerDeleteNode *delete_node_p = \
            static_cast<const InnerDeleteNode *>(node_p);

          // For deleted keys, we first add it with INVALID ID
          // to block all further updates
          // In the last stage we just remove all keys with
          // INVALID_NODE_ID as node ID
          const KeyType &delete_key = delete_node_p->delete_key;

          if(collect_sep == true) {
            // Only delete key if it is in range (not in a split node)
            if((ubound_p != nullptr && \
                KeyCmpLess(delete_key, *ubound_p)) || \
                ubound_p == nullptr) {
              //bwt_printf("Key is deleted by a delete delta\n");

              auto ret = logical_node_p->key_value_map.insert( \
                typename decltype(logical_node_p->key_value_map)::value_type( \
                  delete_node_p->delete_key,
                  INVALID_NODE_ID));

              // We might want to use the return value to validate?
              (void)ret;
            }
          }

          node_p = delete_node_p->child_node_p;

          break;
        } // case InnerDeleteType
        case NodeType::InnerSplitType: {
          const InnerSplitNode *split_node_p = \
            static_cast<const InnerSplitNode *>(node_p);

          // If the high key has not been set yet, just set it
          if(ubound_p == nullptr) {
            //bwt_printf("Updating high key with split node\n");

            ubound_p = &split_node_p->split_key;
          }

          // If we are the right most branch, then also update next ID
          if(collect_ubound == true && \
             logical_node_p->next_node_id == INVALID_NODE_ID) {
            logical_node_p->next_node_id = split_node_p->split_sibling;
          }

          node_p = split_node_p->child_node_p;

          break;
        } // case InnerSplitType
        case NodeType::InnerMergeType: {
          const InnerMergeNode *merge_node_p = \
            static_cast<const InnerMergeNode *>(node_p);

          // Use different flags to collect on left and right branch
          CollectAllSepsOnInnerRecursive(merge_node_p->child_node_p,
                                         logical_node_p,
                                         collect_lbound, // Take care!
                                         false,
                                         collect_sep);

          CollectAllSepsOnInnerRecursive(merge_node_p->right_merge_p,
                                         logical_node_p,
                                         false,
                                         collect_ubound, // Take care!
                                         collect_sep);

          // There is no unvisited node
          return;
        } // case InnerMergeType
        default: {
          bwt_printf("ERROR: Unknown inner node type = %d\n",
                 static_cast<int>(type));

          assert(false);
          return;
        }
      } // switch type

      first_time = false;
    } // while(1)

    // Should not get to here
    assert(false);
    return;
  }

  /*
   * NavigateLeafNode() - Find search key on a logical leaf node
   *
   * This function correctly deals with merge and split, starting on
   * the topmost node of a delta chain
   *
   * It pushes pointers of nodes into a vector, and stops at the leaf node.
   * After that it bulk loads the leaf page's data item of the search key
   * into logical leaf node, and then replay log
   *
   * In order to reflect the fact that we might jump to a split sibling using
   * NodeID due to a half split state, the function will modify snapshot_p's
   * NodeID and BaseNode pointer if this happens, and furthermore it sets
   * is_sibling flag to true to notify the caller that path history needs to
   * be updated
   *
   * This function takes an extra argument to decide whether it collects any
   * value. If collect_value is set to false, then this function only traverses
   * the leaf node without collecting any actual value, while still being
   * able to traverse to its left sibling and potentially abort
   *
   * NOTE: If there is prior data in its logical node when calling this function
   * then we simply turn off collect_value flag in order to make use of
   * these values and avoid map insert conflict.
   *
   * NOTE 2: After jumping to a new NodeID in this function there will be similar
   * problems since now the snapshot has been changed, and we need to check
   * whether there is data in the map again to make sure there is no
   * map insert conflict
   */
  void NavigateLeafNode(Context *context_p,
                        bool collect_value) {
    // This contains information for current node
    NodeSnapshot *snapshot_p = GetLatestNodeSnapshot(context_p);
    const KeyType &search_key = context_p->search_key;

    assert(snapshot_p->is_leaf == true);
    assert(snapshot_p->node_p != nullptr);
    assert(snapshot_p->logical_node_p != nullptr);
    assert(snapshot_p->node_id != INVALID_NODE_ID);

    // Even if it has cached data, we need to clean it since
    // we want to traverse to sibling node
    if(snapshot_p->has_data == true) {
      bwt_printf("Leaf snapshot already has data. Stop collecting value\n");

      // This voids having value conflict problem
      // since we simply rely on previously collected data
      collect_value = false;
    }

    const BaseNode *node_p = snapshot_p->node_p;

    // Since upperbound could be updated by split delta, we just
    // record the newest ubound using a pointer; If this is nullptr
    // at leaf page then just use the ubound in leaf page
    const KeyType *ubound_p = nullptr;
    const KeyType *lbound_p = nullptr;

    // This is used to test whether a remove node is valid
    // since it could only be the first node on a delta chain
    bool first_time = true;
    (void)first_time;

    // There is no recursion, but we still need to count the
    // number of delta records to replay
    size_t log_count = 0;

    // We use this as a container, and write key-valuelist pair into
    // its map
    LogicalLeafNode *logical_node_p = snapshot_p->GetLogicalLeafNode();

    while(1) {
      NodeType type = node_p->GetType();
      switch(type) {
        case NodeType::LeafType: {
          const LeafNode *leaf_node_p = \
            static_cast<const LeafNode *>(node_p);

          if(lbound_p == nullptr) {
            lbound_p = &leaf_node_p->lbound;
          }

          if(ubound_p == nullptr) {
            ubound_p = &leaf_node_p->ubound;
          }

          // Even if we have seen merge and split this always hold
          // since merge and split would direct to the correct page by sep key
          idb_assert_key(snapshot_p->node_id,
                         search_key,
                         context_p,
                         (KeyCmpGreaterEqual(search_key, *lbound_p) && \
                         KeyCmpLess(search_key, *ubound_p))
                         );

          // If value is not collected then we do not play log
          if(collect_value == true) {
            // First bulk load data item for the search key, if exists
            for(const DataItem &item : leaf_node_p->data_list) {
              if(KeyCmpEqual(item.key, search_key)) {
                logical_node_p->BulkLoadValue(item);

                break;
              }
            }

            // Then replay log
            // NOTE: This does not have to be recursive since we only
            // follow one path down to the bottom
            logical_node_p->ReplayLog(log_count);
          }

          return;
        }
        case NodeType::LeafInsertType: {
          const LeafInsertNode *insert_node_p = \
            static_cast<const LeafInsertNode *>(node_p);

          if(collect_value == true) {
            // If there is a match then push the delta
            if(KeyCmpEqual(search_key, insert_node_p->insert_key)) {
              logical_node_p->pointer_list.push_back(node_p);
              log_count++;
            }
          }

          node_p = insert_node_p->child_node_p;

          break;
        } // case LeafInsertType
        case NodeType::LeafDeleteType: {
          const LeafDeleteNode *delete_node_p = \
            static_cast<const LeafDeleteNode *>(node_p);

          if(collect_value == true) {
            if(KeyCmpEqual(search_key, delete_node_p->delete_key)) {
              logical_node_p->pointer_list.push_back(node_p);
              log_count++;
            }
          }

          node_p = delete_node_p->child_node_p;

          break;
        } // case LeafDeleteType
        case NodeType::LeafUpdateType: {
          const LeafUpdateNode *update_node_p = \
            static_cast<const LeafUpdateNode *>(node_p);

          // Internally we treat update node as two operations
          // but they must be packed together to make an atomic step
          if(collect_value == true) {
            if(KeyCmpEqual(search_key, update_node_p->update_key)) {
              logical_node_p->pointer_list.push_back(node_p);
              log_count++;
            }
          }

          node_p = update_node_p->child_node_p;

          break;
        } // case LeafUpdateType
        case NodeType::LeafRemoveType: {
          bwt_printf("ERROR: Observed LeafRemoveNode in delta chain\n");

          assert(first_time == true);
          assert(false);
        } // case LeafRemoveType
        case NodeType::LeafMergeType: {
          bwt_printf("Observed a merge node on leaf delta chain\n");

          const LeafMergeNode *merge_node_p = \
            static_cast<const LeafMergeNode *>(node_p);

          // Decide which side we should choose
          // Using >= for separator key
          if(KeyCmpGreaterEqual(search_key, merge_node_p->merge_key)) {
            bwt_printf("Take leaf merge right branch\n");

            node_p = merge_node_p->right_merge_p;
          } else {
            bwt_printf("Take leaf merge left branch\n");

            node_p = merge_node_p->child_node_p;
          }

          break;
        } // case LeafMergeType
        case NodeType::LeafSplitType: {
          bwt_printf("Observed a split node on leaf delta chain\n");

          const LeafSplitNode *split_node_p = \
            static_cast<const LeafSplitNode *>(node_p);

          const KeyType &split_key = split_node_p->split_key;

          if(KeyCmpGreaterEqual(search_key, split_key)) {
            bwt_printf("Take leaf split right (NodeID branch)\n");

            // Since we are on the branch side of a split node
            // there should not be any record with search key in
            // the chain from where we come since otherwise these
            // records are misplaced
            assert(logical_node_p->pointer_list.size() == 0);
            assert(logical_node_p->key_value_set.size() == 0);

            NodeID split_sibling_id = split_node_p->split_sibling;

            // Jump to right sibling, with possibility that it aborts
            JumpToNodeId(split_sibling_id,
                         context_p,
                         &split_key, // This is the low key for target node
                         false);     // Always could not be leftmost child

            if(context_p->abort_flag == true) {
              bwt_printf("JumpToNodeID aborts. ABORT\n");

              // It might help if we clear the stack of pointers before
              // returning
              // But they will eventually be deallocated so this is not a
              // big problem
              return;
            }

            // These three needs to be refreshed after switching node
            snapshot_p = GetLatestNodeSnapshot(context_p);
            node_p = snapshot_p->node_p;
            logical_node_p = snapshot_p->GetLogicalLeafNode();

            // NOTE: IT IS POSSIBLE THAT JUMP TO NODE ID
            // ALSO LOAD INTO THE SNAPSHOT
            if(snapshot_p->has_data == true) {
              bwt_printf("After leaf jumping there is data. Use cached value\n");

              // This avoids map key insert conflict problem
              // since we make use of cached data
              collect_value = false;
            }

            // Since we have switched to a new NodeID
            first_time = true;

            // Avoid setting first_time = false at the end
            continue;
          } else {
            // Since we follow the child physical pointer, it is necessary
            // to update upper bound to have a better bounds checking
            if(ubound_p == nullptr) {
              ubound_p = &split_node_p->split_key;
            }

            node_p = split_node_p->child_node_p;
          }

          break;
        } // case LeafSplitType
        default: {
          bwt_printf("ERROR: Unknown leaf delta node type: %d\n",
                     static_cast<int>(node_p->GetType()));

          assert(false);
        } // default
      } // switch

      // After first loop most nodes will set this to false
      // If we continue inside the switch then we will not reach this
      // line
      first_time = false;
    } // while

    // We cannot reach here
    assert(false);
    return;
  }

  /*
   * CollectAllValuesOnLeafRecursive() - Collect all values given a
   *                                     pointer recursively
   *
   * It does not need NodeID to collect values since only read-only
   * routine calls this one, so no validation is ever needed even in
   * its caller.
   *
   * This function only travels using physical pointer, which implies
   * that it does not deal with LeafSplitNode and LeafRemoveNode
   * For LeafSplitNode it only collects value on child node
   * For LeafRemoveNode it fails assertion
   * If LeafRemoveNode is not the topmost node it also fails assertion
   *
   * NOTE: This function calls itself to collect values in a merge node
   * since logically speaking merge node consists of two delta chains
   * DO NOT CALL THIS DIRECTLY - Always use the wrapper (the one without
   * "Recursive" suffix)
   *
   * NOTE 2: This function tracks the upperbound of a logical
   * page using split node and merge node, since logically they are both
   * considered to be overwriting the uppberbound. All keys larger than
   * the upperbound will not be collected, since during a node split,
   * it is possible that there are obsolete keys left in the base leaf
   * page.
   *
   * NOTE 3: Despite the seemingly obsecure naming, this function actually
   * has an option that serves as an optimization when we only want metadata
   * (e.g. ubound, lbound, next ID) but not actual value. This saves some
   * computing resource. We need metadata only e.g. when we are going to verify
   * whether the upperbound of a page matches the lower bound of a given page
   * to confirm that we have found the left sibling of a node
   */
  void
  CollectAllValuesOnLeafRecursive(const BaseNode *node_p,
                                  LogicalLeafNode *logical_node_p,
                                  bool collect_lbound,
                                  bool collect_ubound,
                                  bool collect_value) const {
    bool first_time = true;
    (void)first_time;
    
    // This is the high key for local branch
    // At the end of the loop if we are collecting high key then
    // this value will be set into logical node as its high key
    const KeyType *ubound_p = nullptr;

    // The number of delta records we have collected in this recursive context
    // NOTE: If we have seen a merge delta
    size_t log_count = 0;

    while(1) {
      NodeType type = node_p->GetType();

      switch(type) {
        // When we see a leaf node, just copy all keys together with
        // all values into the value set
        case NodeType::LeafType: {
          const LeafNode *leaf_base_p = \
            static_cast<const LeafNode *>(node_p);

          // If we collect values into logical node pointer
          if(collect_value == true) {
            for(auto &data_item : leaf_base_p->data_list) {
              // If we find a key in the leaf page which is >= the latest
              // separator key of a split node (if there is one) then ignore
              // these key since they have been now stored in another leaf
              if(ubound_p != nullptr && \
                 KeyCmpGreaterEqual(data_item.key, *ubound_p)) {
                //bwt_printf("Obsolete key on Leaf Base Node\n");

                continue;
              }

              // Load all values in the vector using the given key
              logical_node_p->BulkLoadValue(data_item);
            } // for auto it : data_item ...
          } // if collect value == true

          // Then try to fill in ubound and lbound
          if(collect_lbound == true) {
            // Since lbound should not be changed by delta nodes
            // it must be not set by any other nodes
            assert(logical_node_p->lbound_p == nullptr);

            logical_node_p->lbound_p = &leaf_base_p->lbound;
          }

          // fill in next node id if it has not been changed by
          // some split delta way down the delta chain
          if(logical_node_p->next_node_id == INVALID_NODE_ID && \
             collect_ubound == true) {
            // This logically updates the next node pointer for a
            // logical node
            logical_node_p->next_node_id = leaf_base_p->next_node_id;
          }

          // We set ubound_p here to avoid having to compare keys above
          if(ubound_p == nullptr) {
            ubound_p = &leaf_base_p->ubound;
          }

          // If we collect high key, then local high key is set to be
          // logical node's high key
          if(collect_ubound == true) {
            logical_node_p->ubound_p = ubound_p;
          }

          if(collect_value == false) {
            assert(log_count == 0);
          } else {
            // After setting up all bounds, replay the log
            logical_node_p->ReplayLog(log_count);
          }

          return;
        } // case LeafType
        case NodeType::LeafInsertType: {
          const LeafInsertNode *insert_node_p = \
            static_cast<const LeafInsertNode *>(node_p);

          // Only collect delta pointer if values are collected
          if(collect_value == true) {
            // Only collect split delta if its key is in
            // the range
            if(ubound_p != nullptr && \
               KeyCmpGreaterEqual(insert_node_p->insert_key,
                                  *ubound_p)) {
              //bwt_printf("Insert key not in range (>= high key)\n");
            } else {
              logical_node_p->pointer_list.push_back(node_p);

              log_count++;
            }
          }

          node_p = insert_node_p->child_node_p;

          break;
        } // case LeafInsertType
        case NodeType::LeafDeleteType: {
          const LeafDeleteNode *delete_node_p = \
            static_cast<const LeafDeleteNode *>(node_p);

          if(collect_value == true) {
            // Only collect delete delta if it is in the range
            // i.e. < newest high key, since otherwise it will
            // be in splited nodes
            if(ubound_p != nullptr && \
               KeyCmpGreaterEqual(delete_node_p->delete_key,
                                  *ubound_p)) {
              //bwt_printf("Delete key not in range (>= high key)\n");
            } else {
              logical_node_p->pointer_list.push_back(node_p);

              log_count++;
            }
          }

          node_p = delete_node_p->child_node_p;

          break;
        } // case LeafDeleteType
        case NodeType::LeafUpdateType: {
          const LeafUpdateNode *update_node_p = \
            static_cast<const LeafUpdateNode *>(node_p);

          if(collect_value == true) {
            // For update nodes we also need to check whether
            // the updated key is in the range or not
            if(ubound_p != nullptr && \
               KeyCmpGreaterEqual(update_node_p->update_key,
                                  *ubound_p)) {
              //bwt_printf("Update key not in range (>= high key)\n");
            } else {
              logical_node_p->pointer_list.push_back(node_p);

              log_count++;
            }
          }

          node_p = update_node_p->child_node_p;

          break;
        } // case LeafUpdateType
        case NodeType::LeafRemoveType: {
          bwt_printf("ERROR: LeafRemoveNode not allowed\n");

          // If we see a remove node, then this node is removed
          // and in that case we just return silently
          assert(first_time == true);

          // These two are trivial but just put them here for safety
          assert(logical_node_p->key_value_set.size() == 0);
          assert(logical_node_p->pointer_list.size() == 0);

          // Fail here
          assert(false);
        } // case LeafRemoveType
        case NodeType::LeafSplitType: {
          const LeafSplitNode *split_node_p = \
            static_cast<const LeafSplitNode *>(node_p);

          // If we have not seen a split node, then this is the first one
          // and we need to remember the upperbound for the logical page
          // Since this is the latest change to its upperbound
          if(ubound_p == nullptr) {
            ubound_p = &split_node_p->split_key;
          }

          // Must test collect_ubound since we only collect
          // next node id for the right most node
          if(collect_ubound == true && \
             logical_node_p->next_node_id == INVALID_NODE_ID) {
            // This logically updates the next node pointer for a
            // logical node
            logical_node_p->next_node_id = split_node_p->split_sibling;
          }

          node_p = split_node_p->child_node_p;

          break;
        } // case LeafSplitType
        case NodeType::LeafMergeType: {
          //bwt_printf("Observe LeafMergeNode; recursively collect nodes\n");

          const LeafMergeNode *merge_node_p = \
            static_cast<const LeafMergeNode *>(node_p);

          /**** RECURSIVE CALL ON LEFT AND RIGHT SUB-TREE ****/
          CollectAllValuesOnLeafRecursive(merge_node_p->child_node_p,
                                          logical_node_p,
                                          collect_lbound,
                                          false,  // Always not collect ubound
                                          collect_value);

          CollectAllValuesOnLeafRecursive(merge_node_p->right_merge_p,
                                          logical_node_p,
                                          false, // Always not collect lbound
                                          collect_ubound,
                                          collect_value);

          if(collect_value == false) {
            assert(log_count == 0);
          } else {
            // Replay the remaining log records on top of merge delta
            logical_node_p->ReplayLog(log_count);
          }

          return;
        } // case LeafMergeType
        default: {
          bwt_printf("ERROR: Unknown node type: %d\n",
                     static_cast<int>(type));

          assert(false);
        } // default
      }

      first_time = false;
    }

    return;
  }

  /*
   * CollectAllValuesOnLeaf() - Consolidate delta chain for a single logical
   *                            leaf node
   *
   * This function is the non-recursive wrapper of the resursive core function.
   * It calls the recursive version to collect all base leaf nodes, and then
   * it replays delta records on top of them.
   *
   * NOTE 4: This function makes use of cached logical node if there is one
   */
  void
  CollectAllValuesOnLeaf(NodeSnapshot *snapshot_p) {
    if(snapshot_p->has_data == true) {
      bwt_printf("Fast path: There is cached value.\n");

      assert(snapshot_p->has_metadata == true);

      return;
    }

    // No data but has metadata
    // Then we should clean metadata since they collide with value collection
    // which also collect metadata
    if(snapshot_p->has_metadata == true) {
      snapshot_p->ResetLogicalNode();
    }

    assert(snapshot_p->is_leaf == true);
    assert(snapshot_p->logical_node_p != nullptr);

    // We want to collect both ubound and lbound in this call
    // These two flags will be set to false for every node
    // that is neither a left not right most node
    CollectAllValuesOnLeafRecursive(snapshot_p->node_p,
                                    snapshot_p->GetLogicalLeafNode(),
                                    true,
                                    true,
                                    true);  // collect data

    snapshot_p->has_data = true;
    snapshot_p->has_metadata = true;

    return;
  }


  /*
   * CollectMetadataOnLeaf() - Collect high key, low key and next node ID
   *
   * This function will not collect actual data, but only metadata including
   * high key, low key and next node ID in a logical leaf node.
   *
   * After this function returns snapshot has its has_data set to false
   */
  void
  CollectMetadataOnLeaf(NodeSnapshot *snapshot_p) {
    // If there is cached metadata then we just use cached version
    if(snapshot_p->has_metadata == true) {
      bwt_printf("Fast path: Use previous cached metadata\n");

      return;
    }

    // If there is no metadata then there is definitely no data
    assert(snapshot_p->has_data == false);

    assert(snapshot_p->is_leaf == true);
    assert(snapshot_p->logical_node_p != nullptr);

    // We want to collect both ubound and lbound in this call
    // These two flags will be set to false for every node
    // that is neither a left not right most node
    CollectAllValuesOnLeafRecursive(snapshot_p->node_p,
                                    snapshot_p->GetLogicalLeafNode(),
                                    true,
                                    true,
                                    false); // Do not collect data

    snapshot_p->has_data = false;
    snapshot_p->has_metadata = true;

    return;
  }

  /*
   * GetLatestNodeSnapshot() - Return a pointer to the most recent snapshot
   *
   * This is wrapper that includes size checking
   *
   * NOTE: The snapshot object is not valid after it is popped out of
   * the vector, since at that time the snapshot object will be destroyed
   * which also freed up the logical node object
   */
  inline NodeSnapshot *GetLatestNodeSnapshot(Context *context_p) const {
    std::vector<NodeSnapshot> *path_list_p = \
      &context_p->path_list;

    assert(path_list_p->size() > 0);

    return &(path_list_p->back());
  }

  /*
   * GetLatestParentNodeSnapshot() - Return the pointer to the parent node of
   *                                 the current node
   *
   * This function assumes the current node is always the one on the top of
   * the stack.
   *
   * NOTE: The same as GetLatestNodeSnapshot(), be careful when popping
   * snapshots out of the stack
   */
  NodeSnapshot *GetLatestParentNodeSnapshot(Context *context_p) {
    std::vector<NodeSnapshot> *path_list_p = \
      &context_p->path_list;

    // Make sure the current node has a parent
    size_t path_list_size = path_list_p->size();
    assert(path_list_size >= 2);

    // This is the address of the parent node
    return &((*path_list_p)[path_list_size - 2]);
  }

  /*
   * JumpToLeftSibling() - Jump to the left sibling given a node
   *
   * When this function is called, we assume path_list_p includes the
   * NodeSnapshot object for the current node being inspected, since
   * we want to pass the information of whether the current node is
   * the left most child.
   *
   * This function uses the fact that the mapping relationship between NodeID
   * and low key does not change during the lifetime of the NodeID. This way,
   * after we have fixed a snapshot of the current node, even though the node
   * could keep changing, using the information given in the snapshot
   * we could know at least a starting point for us to traverse right until
   * we see a node whose high key equals the low key we are searching for,
   * or whose range covers the low key (in the latter case, we know the
   * merge delta has already been posted)
   *
   * NOTE: This function might abort. Please check context and make proper
   * decisions
   */
  void JumpToLeftSibling(Context *context_p) {
    bwt_printf("Jumping to the left sibling\n");

    std::vector<NodeSnapshot> *path_list_p = &context_p->path_list;
    (void)path_list_p;

    // Make sure the path list is not empty, and that it has a parent node
    // Even if the original root node splits, the node we are on must be the
    // left most node of the new root
    assert(path_list_p->size() != 0 && \
           path_list_p->size() != 1);

    // Get last record which is the current node's context
    // and we must make sure the current node is not left mode node
    NodeSnapshot *snapshot_p = GetLatestNodeSnapshot(context_p);
    assert(snapshot_p->is_leftmost_child == false);

    // Check currently we are on a remove node
    NodeType type = snapshot_p->node_p->GetType();
    assert(type == NodeType::LeafRemoveType || \
           type == NodeType::InnerRemoveType);
    (void)type;

    // This is the low key of current removed node. Also
    // the low key of the separator-ID pair
    const KeyType *lbound_p = snapshot_p->lbound_p;
    // We use this in assertion
    const NodeID removed_node_id = snapshot_p->node_id;

    // Get its parent node
    snapshot_p = GetLatestParentNodeSnapshot(context_p);
    assert(snapshot_p->is_leaf == false);

    // Load data into the parent node if there is not
    if(snapshot_p->has_data == false) {
      CollectAllSepsOnInner(snapshot_p);
    }

    // After this point there must be data associated with the
    // node snapshot
    assert(snapshot_p->has_data == true);
    assert(snapshot_p->has_metadata == true);

    KeyNodeIDMap &key_value_map = \
      snapshot_p->GetLogicalInnerNode()->GetContainer();
    typename KeyNodeIDMap::reverse_iterator it{};

    for(it = key_value_map.rbegin();
        it != key_value_map.rend();
        it++) {
      // We break at the first key from right to left that is
      // strictly less than the low key we are from
      if(KeyCmpLess(it->first, *lbound_p) == true) {
        break;
      }
    } // for right to left iteration of SepItem

    // This could not be true since the left most SepItem
    // will always be valid (i.e. we always hit break)
    assert(it != key_value_map.rend());

    // This is our starting point to traverse right
    NodeID left_sibling_id = it->second;
    idb_assert(left_sibling_id != INVALID_NODE_ID);

    // This is used for LoadNodeID() if the left sibling itself
    // is removed, then we need to recursively call this function
    // to find the left left sibling, with the entry key
    const KeyType *entry_key_p = &it->first;

    // If it is the leftmost child node we mark this as true
    bool is_leftmost_child = false;
    if(KeyCmpEqual(*entry_key_p,
                   *snapshot_p->GetLogicalInnerNode()->lbound_p) == true) {
      is_leftmost_child = true;
    }

    while(1) {
      // It is impossible that the current node is merged
      // and its next node ID is INVALID_NODE_ID
      // In this case we would catch that by the range check
      if(left_sibling_id == INVALID_NODE_ID) {
        bwt_printf("Have reached the end of current level. "
                   "But it should be caught by range check\n");

        assert(false);
      }

      // This might incur recursive update
      // We need to pass in the low key of left sibling node
      JumpToNodeId(left_sibling_id, context_p, entry_key_p, is_leftmost_child);

      if(context_p->abort_flag == true) {
        bwt_printf("CALLEE ABORT\n");

        return;
      }

      // Read the potentially redirected snapshot
      // (but do not pop it - so we directly return)
      snapshot_p = GetLatestNodeSnapshot(context_p);

      // try to consolidate the node for meta data
      if(snapshot_p->is_leaf) {
        CollectMetadataOnLeaf(snapshot_p);
      } else {
        CollectMetadataOnInner(snapshot_p);
      }

      // After this point the new node has metadata
      assert(snapshot_p->has_metadata == true);

      // Get high key and left sibling NodeID
      const KeyType *ubound_p = snapshot_p->GetHighKey();

      // This will be used in next iteration
      // NOTE: The variable name is a little bit confusing, because
      // this variable was created outside of the loop
      left_sibling_id = snapshot_p->GetRightSiblingNodeID();

      // If the high key of the left sibling equals the low key
      // then we know it is the real left sibling
      // Or the node has already been consolidated, then the range
      // of the node would cover current low key
      if(KeyCmpEqual(*ubound_p, *lbound_p)) {
        bwt_printf("Find a exact match of low/high key\n");

        // We need to take care that high key is not sufficient for identifying
        // the correct left sibling
        if(left_sibling_id == removed_node_id) {
          bwt_printf("Find real left sibling, next id == removed id\n");

          // Quit the loop
          break;
        } else {
          bwt_printf("key match but next node ID does not match. ABORT\n");
          bwt_printf("    (Maybe it has been merged and then splited?)\n");

          // If key match but next node ID does not match then just abort
          // since it is the case that left sibling has already been merged
          // with the removed node, and then it splits
          // We could not handle such case, so just abort
          context_p->abort_flag = true;

          // Return and propagate abort information
          return;
        }
      } else if(KeyCmpGreater(*ubound_p, *lbound_p)) {
        bwt_printf("The range of left sibling covers current node\n");
        bwt_printf("    Don't know for sure what happened\n");

        // We also do not know what happened since it could be possible
        // that the removed node is merged, consolidated, and then splited
        // on a split key greater than the previous merge key
        context_p->abort_flag = true;

        return;
      } else {
        // We know it will never be invalid node id since ubound_p < lbound_p
        // which implies the high key is not +Inf, so there are other nodes to
        // its right side
        assert(left_sibling_id != INVALID_NODE_ID);
      }

      // For the next iteration, we traverse to the node to its right
      // using the next node ID and its high key as next node's low key
      entry_key_p = ubound_p;

      // As long as we traverse right, this becomes not true
      is_leftmost_child = false;
    } // while(1)

    return;
  }

  /*
   * TakeNodeSnapshot() - Take the snapshot of a node by pushing node information
   *
   * This function simply copies NodeID, low key, and physical pointer into
   * the snapshot object, and pushes snapshot into the path list
   *
   * NOTE: We use move semantics to push the snapshot, such that the instance will
   * be invalidated (logical node pointer being set to nullptr) after the move
   */
  void TakeNodeSnapshot(NodeID node_id,
                        Context *context_p,
                        const KeyType *lbound_p,
                        bool is_leftmost_child) {
    const BaseNode *node_p = GetNode(node_id);
    std::vector<NodeSnapshot> *path_list_p = &context_p->path_list;

    // Need to check whether it is leaf or inner node
    // NOTE: EVEN IF IT IS A LeafAbortNode WE STILL NEED
    // TO CALL THIS FUNCTION TO KNOW WE ARE ON LEAF
    bool is_leaf = node_p->IsOnLeafDeltaChain();
    NodeSnapshot snapshot{is_leaf, this};

    snapshot.node_id = node_id;
    // This makes sure the cached version of logical node is removed
    snapshot.SwitchPhysicalPointer(node_p);

    // This is useful for finding left sibling
    snapshot.lbound_p = lbound_p;

    // Mark it if it is the leftmost child
    snapshot.is_leftmost_child = is_leftmost_child;

    // Put this into the list in case that a remove node causes
    // backtracking
    // NOTE: 1. snapshot is invalid after this point
    //       2. We need to retrieve it using GetLatestNodeSnapshot()
    path_list_p->push_back(std::move(snapshot));

    return;
  }

  /*
   * UpdateNodeSnapshot() - Update an existing node snapshot
   *
   * This function does not create and push new NodeSnapshot object into the
   * stack; instead it modifies existing NodeSnapshot object on top of the
   * path stack, using the given NodeID and low key
   *
   * NOTE: The left most child flag will be ignored because in current
   * design when this is called we always go within the same parent node
   *
   * NOTE 2: If right traversal cross parent node boundary then NOTE 1
   * is no longer true
   *
   * NOTE 3: Since we always traverse on the same level, the leaf/inner identity
   * does not change
   *
   * NOTE 4: We need to clear all cached data and metadata inside the logical
   * node since it is not changed to another NodeID
   *
   * NOTE 5: IF NODE ID DOES NOT CHANGE, DO NOT CALL THIS FUNCTION
   * SINCE THIS FUNCTION RESETS ROOT IDENTITY
   * Call SwitchPhysicalPointer() instead
   *
   * TODO: In the future add an option to make it turn off self-checking
   * (i.e. checking whether switching to itself) whenever requested
   */
  void UpdateNodeSnapshot(NodeID node_id,
                          Context *context_p,
                          const KeyType *lbound_p,
                          bool is_leftmost_child) {
    const BaseNode *node_p = GetNode(node_id);

    // We operate on the latest snapshot instead of creating a new one
    NodeSnapshot *snapshot_p = GetLatestNodeSnapshot(context_p);

    // Assume we always use this function to traverse on the same
    // level
    assert(node_p->IsOnLeafDeltaChain() == snapshot_p->is_leaf);

    // Make sure we are not switching to itself
    assert(snapshot_p->node_id != node_id);

    // We change these three
    snapshot_p->is_leftmost_child = is_leftmost_child;
    snapshot_p->node_id = node_id;
    snapshot_p->lbound_p = lbound_p;

    // We only call UpdateNodeSnapshot() when we switch to split
    // sibling in Navigate function and jump to left sibling
    // When updating the physical pointer of a root node after posting
    // a delta, please call SwitchPhysicalPointer() instead
    snapshot_p->is_root = false;

    // Resets all cached metadata and data
    // including flags
    // and also replace physical pointer
    snapshot_p->SwitchPhysicalPointer(node_p);

    return;
  }

  /*
   * LoadNodeID() - Push a new snapshot for the node pointed to by node_id
   *
   * If we just want to modify existing snapshot object in the stack, we should
   * call JumpToNodeID() instead
   *
   * NOTE: If this function is called when we are still in Init mode, then
   * root flag is set as a side-effect, since we know now we are definitely
   * loading the ID for root node
   */
  void LoadNodeID(NodeID node_id,
                  Context *context_p,
                  const KeyType *lbound_p,
                  bool is_leftmost_child) {
    bwt_printf("Loading NodeID = %lu\n", node_id);

    // This pushes a new snapshot into stack
    TakeNodeSnapshot(node_id, context_p, lbound_p, is_leftmost_child);

    // If we are in init state, then set root flag
    if(context_p->current_state == OpState::Init) {
      bwt_printf("Loading NodeID for root; set root flag\n");

      NodeSnapshot *snapshot_p = GetLatestNodeSnapshot(context_p);
      snapshot_p->SetRootFlag();
    }

    FinishPartialSMO(context_p);

    if(context_p->abort_flag == true) {
      return;
    }

    ConsolidateNode(context_p);

    if(context_p->abort_flag == true) {
      return;
    }

    AdjustNodeSize(context_p);

    if(context_p->abort_flag == true) {
      return;
    }

    return;
  }

  /*
   * JumpToNodeID() - Given a NodeID, change the top of the path list
   *                  by loading the delta chain of the node ID
   *
   * NOTE: This function could also be called to traverse right, in which case
   * we need to check whether the target node is the left most child
   */
  void JumpToNodeId(NodeID node_id,
                    Context *context_p,
                    const KeyType *lbound_p,
                    bool is_leftmost_child) {
    bwt_printf("Jumping to node ID = %lu\n", node_id);

    // This updates the current snapshot in the stack
    UpdateNodeSnapshot(node_id, context_p, lbound_p, is_leftmost_child);
    FinishPartialSMO(context_p);

    if(context_p->abort_flag == true) {
      return;
    }

    ConsolidateNode(context_p);

    if(context_p->abort_flag == true) {
      return;
    }

    AdjustNodeSize(context_p);

    if(context_p->abort_flag == true) {
      return;
    }

    return;
  }

  /*
   * FinishPartialSMO() - Finish partial completed SMO if there is one
   *
   * This function defines the help-along protocol, i.e. if we find
   * a SMO on top of the delta chain, we should help-along that SMO before
   * doing our own job. This might incur a recursive call of this function.
   *
   * If we see a remove node, then the actual NodeID pushed into the path
   * list stack may not equal the NodeID passed in this function. So when
   * we need to read NodeID, always use the one stored in the NodeSnapshot
   * vector instead of using a previously passed one
   */
  void FinishPartialSMO(Context *context_p) {
    // We assume the last node is the node we will help for
    NodeSnapshot *snapshot_p = GetLatestNodeSnapshot(context_p);

    // NOTE: Need to update these three variables if we jump to
    // the left sibling
    const BaseNode *node_p = snapshot_p->node_p;
    NodeID node_id = snapshot_p->node_id;
    // NOTE: For root node this is nullptr
    // But we do not allow posting remove delta on root node
    // which means it would not be
    const KeyType *lbound_p = snapshot_p->lbound_p;

    // And also need to update this if we post a new node
    // NOTE: This will be updated by remove node, so do not make
    // it as constant
    NodeType type = node_p->GetType();

before_switch:
    switch(type) {
      case NodeType::InnerAbortType: {
        bwt_printf("Observed Inner Abort Node; ABORT\n");

        // This block is an optimization - when seeing an ABORT
        // node, we continue but set the physical pointer to be ABORT's
        // child, to make CAS always fail on this node to avoid
        // posting on ABORT, especially posting split node
        {
          node_p = \
            static_cast<const DeltaNode *>(node_p)->child_node_p;
          snapshot_p->SwitchPhysicalPointer(node_p);
          type = node_p->GetType();

          goto before_switch;
        }

        //context_p->abort_flag = true;

        //return;
      }
      case NodeType::LeafRemoveType:
      case NodeType::InnerRemoveType: {
        bwt_printf("Helping along remove node...\n");

        // If snapshot indicates this is a leftmost child, but
        // actually we have seen a remove, then the parent must have
        // been removed into its left sibling
        // so we just abort and retry
        if(snapshot_p->is_leftmost_child == true) {
          bwt_printf("Snapshot indicates this is left most child. ABORT\n");

          context_p->abort_flag = true;

          return;
        }

        // We could not find remove delta on left most child
        idb_assert_key(node_id,
                       context_p->search_key,
                       context_p,
                       snapshot_p->is_leftmost_child == false);

        const DeltaNode *delta_node_p = \
          static_cast<const DeltaNode *>(node_p);

        const BaseNode *merge_right_branch = \
          delta_node_p->child_node_p;

        JumpToLeftSibling(context_p);

        // If this aborts then we propagate this to the state machine driver
        if(context_p->abort_flag == true) {
          bwt_printf("Jump to left sibling in Remove help along ABORT\n");

          return;
        }

        // That is the left sibling's snapshot
        NodeSnapshot *left_snapshot_p = \
          GetLatestNodeSnapshot(context_p);
        // Make sure we are still on the same level
        assert(snapshot_p->is_leaf == left_snapshot_p->is_leaf);

        const BaseNode *left_sibling_p = left_snapshot_p->node_p;

        // We save this before lbound_p which was once remove node lbound_p
        // is overwritten with the lbound_p of left sibling
        const KeyType *remove_node_lbound_p = lbound_p;
        NodeID remove_node_id = node_id;
        (void)remove_node_id;

        // Update current node information
        // NOTE: node_p will not be used here since we have already
        // saved merge sibling in merge_right_branch
        node_p = left_sibling_p;
        node_id = left_snapshot_p->node_id;
        lbound_p = left_snapshot_p->lbound_p;

        // Now they are equal
        snapshot_p = left_snapshot_p;

        // if debug is on, make sure it is the left sibling
        #ifdef BWTREE_DEBUG
        if(left_snapshot_p->is_leaf == true) {
          CollectMetadataOnLeaf(left_snapshot_p);
        } else {
          CollectMetadataOnInner(left_snapshot_p);
        }

        // Make sure it is the real left sibling
        assert(KeyCmpEqual(*left_snapshot_p->GetHighKey(),
                           *remove_node_lbound_p));
        assert(left_snapshot_p->GetNextNodeID() == remove_node_id);
        #endif

        bool ret = false;

        // If we are currently on leaf, just post leaf merge delta
        // NOTE: node_p will be reset to the merge delta if CAS succeeds
        if(left_snapshot_p->is_leaf == true) {
          ret = \
            PostMergeNode<LeafMergeNode>(left_snapshot_p,
                                         remove_node_lbound_p,
                                         merge_right_branch,
                                         &node_p);
        } else {
          ret = \
            PostMergeNode<InnerMergeNode>(left_snapshot_p,
                                          remove_node_lbound_p,
                                          merge_right_branch,
                                          &node_p);
        }

        // If CAS fails just abort and return
        if(ret == true) {
          bwt_printf("Merge delta CAS succeeds. ABORT\n");

          // Update the new merge node back to snapshot
          snapshot_p->SwitchPhysicalPointer(node_p);

          // Then we abort, because it is possible for us to immediately
          // posting on the merge delta
          context_p->abort_flag = true;

          return;
        } else {
          bwt_printf("Merge delta CAS fails. ABORT\n");

          // TODO: We could choose to look at the delta chain and find out
          // what happened. If that's a trivial case (just another insert/delete
          // get posted then we could retry)
          // Currently we just abort
          context_p->abort_flag = true;

          return;
        } // if ret == true

        bwt_printf("Continue to post index term delete delta\n");

        // Update type information and make sure it is the correct node
        type = node_p->GetType();
        assert(type == NodeType::InnerMergeType ||
               type == NodeType::LeafMergeType);

        // FALL THROUGH TO MERGE
      } // case Inner/LeafRemoveType
      case NodeType::InnerMergeType:
      case NodeType::LeafMergeType: {
        bwt_printf("Helping along merge delta, ID = %lu\n", node_id);

        // First consolidate parent node and find the left/right
        // sep pair plus left node ID
        NodeSnapshot *parent_snapshot_p = \
          GetLatestParentNodeSnapshot(context_p);

        // Consolidate parent node
        if(parent_snapshot_p->has_data == false) {
          CollectAllSepsOnInner(parent_snapshot_p);
        }

        assert(parent_snapshot_p->has_data == true);
        assert(parent_snapshot_p->is_leaf == false);

        // DUMMY PLACEHOLDER; DO NOT USE OR MODIFY
        NodeID deleted_node_id = INVALID_NODE_ID;

        const KeyType *merge_key_p = nullptr;

        // These three needs to be found in the parent node
        // Since we do not alter parent node's snapshot since last time we
        // saw it, we could always find the sep and its prev/next/node ID
        const KeyType *prev_key_p = nullptr;
        const KeyType *next_key_p = nullptr;
        NodeID prev_node_id = INVALID_NODE_ID;

        int depth = 1;

        // This is a faster way of determining depth:
        // Since we know the parent must be inner node
        // the only exception is InnerType, in which we need to
        // set depth to 1. In all other cases just cast them to
        // DeltaNode and use its depth
        NodeType parent_type = parent_snapshot_p->node_p->GetType();
        if(parent_type != NodeType::InnerType) {
          const DeltaNode *delta_node_p = \
            static_cast<const DeltaNode *>(parent_snapshot_p->node_p);

          depth = delta_node_p->depth + 1;
        }

        // Determine deleted key using merge node
        if(type == NodeType::InnerMergeType) {
          const InnerMergeNode *merge_node_p = \
            static_cast<const InnerMergeNode *>(node_p);

          merge_key_p = &merge_node_p->merge_key;
        } else if(type == NodeType::LeafMergeType) {
          const LeafMergeNode *merge_node_p = \
            static_cast<const LeafMergeNode *>(node_p);

          merge_key_p = &merge_node_p->merge_key;
        } else {
          bwt_printf("ERROR: Illegal node type: %d\n",
                     static_cast<int>(type));

          assert(false);
        } // If on type of merge node

        // if this is false then we have already deleted the index term
        bool merge_key_found = \
          FindMergePrevNextKey(parent_snapshot_p,
                               merge_key_p,
                               &prev_key_p,
                               &next_key_p,
                               &prev_node_id,
                               deleted_node_id);

        // If merge key is not found then we know we have already deleted the
        // index term
        if(merge_key_found == false) {
          bwt_printf("Index term is absent; No need to remove\n");

          // Out of switch
          break;
        }

        // NOTE: InnerDeleteNode should use parent node's current
        // delta chain as its child node
        // DO NOT USE node_p AS ITS CHILD!!!!!!
        const InnerDeleteNode *delete_node_p = \
          new InnerDeleteNode{*merge_key_p,
                              *next_key_p,
                              *prev_key_p,
                              prev_node_id,
                              depth,
                              parent_snapshot_p->node_p};

        // Assume parent has not changed, and CAS the index term delete delta
        // If CAS failed then parent has changed, and we have no idea how it
        // could be modified. The safest way is just abort
        bool ret = InstallNodeToReplace(parent_snapshot_p->node_id,
                                        delete_node_p,
                                        parent_snapshot_p->node_p);
        if(ret == true) {
          bwt_printf("Index term delete delta installed, ID = %lu; ABORT\n",
                     parent_snapshot_p->node_id);

          // Update in parent snapshot
          parent_snapshot_p->SwitchPhysicalPointer(delete_node_p);

          // NOTE: We also abort here
          context_p->abort_flag = true;

          return;
        } else {
          bwt_printf("Index term delete delta install failed. ABORT\n");

          context_p->abort_flag = true;
          // DO NOT FORGET TO DELETE THIS
          delete delete_node_p;

          return;
        }

        break;
      } // case Inner/LeafMergeNode
      case NodeType::InnerSplitType:
      case NodeType::LeafSplitType: {
        bwt_printf("Helping along split node (ID = %lu)\n", node_id);

        // We need to read these three from split delta node
        const KeyType *split_key_p = nullptr;
        const KeyType *next_key_p = nullptr;
        NodeID split_node_id = INVALID_NODE_ID;

        // NOTE: depth should not be read here, since we
        // need to know the depth on its parent node
        if(type == NodeType::InnerSplitType) {
          const InnerSplitNode *split_node_p = \
            static_cast<const InnerSplitNode *>(node_p);

          split_key_p = &split_node_p->split_key;
          split_node_id = split_node_p->split_sibling;
        } else {
          const LeafSplitNode *split_node_p = \
            static_cast<const LeafSplitNode *>(node_p);

          split_key_p = &split_node_p->split_key;
          split_node_id = split_node_p->split_sibling;
        }

        assert(context_p->path_list.size() > 0);

        // SPLIT ROOT NODE!!
        if(context_p->path_list.size() == 1) {
          bwt_printf("Root splits!\n");

          // Allocate a new node ID for the newly created node
          NodeID new_root_id = GetNextNodeID();

          // [-Inf, +Inf] -> INVALID_NODE_ID, for root node
          // NOTE: DO NOT MAKE IT CONSTANT since we will push separator into it
          InnerNode *inner_node_p = \
            new InnerNode{GetNegInfKey(), GetPosInfKey(), INVALID_NODE_ID};

          SepItem item1{GetNegInfKey(), node_id};
          SepItem item2{*split_key_p, split_node_id};

          inner_node_p->sep_list.push_back(item1);
          inner_node_p->sep_list.push_back(item2);

          // First we need to install the new node with NodeID
          // This makes it visible
          InstallNewNode(new_root_id, inner_node_p);
          bool ret = InstallRootNode(snapshot_p->node_id,
                                     new_root_id);

          if(ret == true) {
            bwt_printf("Install root CAS succeeds\n");
          } else {
            bwt_printf("Install root CAS failed. ABORT\n");

            delete inner_node_p;
            // TODO: REMOVE THE NEWLY ALLOCATED ID
            context_p->abort_flag = true;

            return;
          } // if CAS succeeds/fails

          // NOTE: We do not have to update the snapshot to reflect newly assigned
          // root node
        } else {
          /***********************************************************
           * Index term insert for non-root nodes
           ***********************************************************/

          // First consolidate parent node and find the right sep
          NodeSnapshot *parent_snapshot_p = \
            GetLatestParentNodeSnapshot(context_p);

          // Make sure there is data in the snapshot
          if(parent_snapshot_p->has_data == false) {
            CollectAllSepsOnInner(parent_snapshot_p);
          }

          assert(parent_snapshot_p->has_data == true);

          // If this is false then we know the index term has already
          // been inserted
          bool split_key_absent = \
            FindSplitNextKey(parent_snapshot_p,
                             split_key_p,
                             &next_key_p,
                             split_node_id);

          if(split_key_absent == false) {
            bwt_printf("Index term is present. No need to insert\n");

            // Break out of switch
            break;
          }

          // Determine the depth on parent delta chain
          // If the parent node has delta node, then derive
          // depth from delta node
          int depth = 0;
          if(parent_snapshot_p->node_p->IsDeltaNode()) {
            const DeltaNode *delta_node_p = \
              static_cast<const DeltaNode *>(parent_snapshot_p->node_p);

            depth = delta_node_p->depth + 1;
          } else {
            depth = 1;
          }

          const InnerInsertNode *insert_node_p = \
            new InnerInsertNode{*split_key_p,
                                *next_key_p,
                                split_node_id,
                                depth,
                                parent_snapshot_p->node_p};

          // CAS
          bool ret = InstallNodeToReplace(parent_snapshot_p->node_id,
                                          insert_node_p,
                                          parent_snapshot_p->node_p);
          if(ret == true) {
            bwt_printf("Index term insert (from %lu to %lu) delta CAS succeeds\n",
                       node_id,
                       split_node_id);

            // Invalidate cached version of logical node
            parent_snapshot_p->SwitchPhysicalPointer(insert_node_p);

            context_p->abort_flag = true;

            return;
          } else {
            bwt_printf("Index term insert (from %lu to %lu) delta CAS failed. ABORT\n",
                       node_id,
                       split_node_id);

            // Set abort, and remove the newly created node
            context_p->abort_flag = true;
            delete insert_node_p;

            return;
          } // if CAS succeeds/fails
        } // if split root / else not split root

        break;
      } // case split node
      default: {
        // By default we do not do anything special
        break;
      }
    } // switch

    return;
  }

  /*
   * ConsolidateNode() - Consolidate current node if its length exceeds the
   *                     threshold value
   *
   * If the length of delta chain does not exceed the threshold then this
   * function does nothing
   *
   * NOTE: If consolidation fails then this function does not do anything
   * and will just continue with its own job. There is no chance of abort
   *
   * TODO: If strict mode is on, then whenever consolidation fails we should
   * always abort and start from the beginning, to keep delta chain length
   * upper bound intact
   */
  void ConsolidateNode(Context *context_p) {
    NodeSnapshot *snapshot_p = GetLatestNodeSnapshot(context_p);

    const BaseNode *node_p = snapshot_p->node_p;
    NodeID node_id = snapshot_p->node_id;

    // We could only perform consolidation on delta node
    // because we want to see depth field
    if(node_p->IsDeltaNode() == false) {
      return;
    }

    const DeltaNode *delta_node_p = \
      static_cast<const DeltaNode *>(node_p);

    // If depth does not exceeds threshold then just return
    const int depth = delta_node_p->depth;
    if(depth < DELTA_CHAIN_LENGTH_THRESHOLD) {
      return;
    }

    // After this pointer we decide to consolidate node

    // This is for debugging
    if(snapshot_p->is_root == true) {
      bwt_printf("Consolidate root node\n");
    }

    if(snapshot_p->has_data == false) {
      if(snapshot_p->is_leaf) {
        CollectAllValuesOnLeaf(snapshot_p);
      } else {
        CollectAllSepsOnInner(snapshot_p);
      }
    }

    assert(snapshot_p->has_data == true);
    assert(snapshot_p->has_metadata == true);

    if(snapshot_p->is_leaf) {
      // This function implicitly allocates a leaf node
      const LeafNode *leaf_node_p = \
        snapshot_p->GetLogicalLeafNode()->ToLeafNode();

      // CAS leaf node
      bool ret = InstallNodeToReplace(node_id, leaf_node_p, node_p);
      if(ret == true) {
        bwt_printf("Leaf node consolidation (ID %lu) CAS succeeds\n",
                   node_id);

        // Update current snapshot using our best knowledge
        snapshot_p->SwitchPhysicalPointer(leaf_node_p);

        // Add the old delta chain to garbage list and its
        // deallocation is delayed
        epoch_manager.AddGarbageNode(node_p);
      } else {
        bwt_printf("Leaf node consolidation CAS failed. NO ABORT\n");

        // TODO: If we want to keep delta chain length constant then it
        // should abort here

        delete leaf_node_p;
      } // if CAS succeeds / fails
    } else {
      const InnerNode *inner_node_p = \
        snapshot_p->GetLogicalInnerNode()->ToInnerNode();

      bool ret = InstallNodeToReplace(node_id, inner_node_p, node_p);
      if(ret == true) {
        bwt_printf("Inner node consolidation (ID %lu) CAS succeeds\n",
                   node_id);

        snapshot_p->SwitchPhysicalPointer(inner_node_p);

        // Add the old delta into garbage list
        epoch_manager.AddGarbageNode(node_p);
      } else {
        bwt_printf("Inner node consolidation CAS failed. NO ABORT\n");

        context_p->abort_flag = true;

        delete inner_node_p;

        return;
      } // if CAS succeeds / fails
    } // if it is leaf / is inner

    return;
  }

  /*
   * AdjustNodeSize() - Post split or merge delta if a node becomes overflow
   *                    or underflow
   *
   * For leftmost children nodes and for root node (which has is_leftmost
   * flag set) we never post remove delta, the merge of which would change
   * its parent node's low key
   *
   * NOTE: This function will abort after installing a node remove delta,
   * in order not to have to call LoadNodeID recursively
   *
   * TODO: In the future we might want to change this
   */
  void AdjustNodeSize(Context *context_p) {
    NodeSnapshot *snapshot_p = GetLatestNodeSnapshot(context_p);
    const BaseNode *node_p = snapshot_p->node_p;
    NodeID node_id = snapshot_p->node_id;

    // We do not adjust size for delta nodes
    if(node_p->IsDeltaNode() == true) {
      // TODO: If we want to strictly enforce the size of any node
      // then we should aggressively consolidate here and always
      // Though it brings huge overhead because now we are consolidating
      // every node on the path

      return;
    }

    if(snapshot_p->is_leaf == true) {
      const LeafNode *leaf_node_p = \
        static_cast<const LeafNode *>(node_p);

      // No matter what we want to do, this is necessary
      // since we need to
      // NOTE: This causes problem for later uses since it
      // fills the node with actual value
      if(snapshot_p->has_data == false) {
        CollectAllValuesOnLeaf(snapshot_p);
      }

      assert(snapshot_p->has_data == true);

      size_t node_size = leaf_node_p->data_list.size();

      // Perform corresponding action based on node size
      if(node_size >= LEAF_NODE_SIZE_UPPER_THRESHOLD) {
        bwt_printf("Node size >= leaf upper threshold. Split\n");

        const LeafNode *new_leaf_node_p = leaf_node_p->GetSplitSibling();
        const KeyType *split_key_p = &new_leaf_node_p->lbound;

        NodeID new_node_id = GetNextNodeID();

        // TODO: If we add support to split base node with a delta chain
        // then this is the length of the delta chain
        int depth = 1;

        const LeafSplitNode *split_node_p = \
          new LeafSplitNode{*split_key_p, new_node_id, depth, node_p};

        //  First install the NodeID -> split sibling mapping
        InstallNewNode(new_node_id, new_leaf_node_p);
        // Then CAS split delta into current node's NodeID
        bool ret = InstallNodeToReplace(node_id, split_node_p, node_p);

        if(ret == true) {
          bwt_printf("Leaf split delta (from %lu to %lu) CAS succeeds. ABORT\n",
                     node_id,
                     new_node_id);

          // NOTE: WE SHOULD SWITCH TO SPLIT NODE
          snapshot_p->SwitchPhysicalPointer(split_node_p);

          // TODO: WE ABORT HERE TO AVOID THIS THREAD POSTING ANYTHING
          // ON TOP OF IT WITHOUT HELPING ALONG AND ALSO BLOCKING OTHER
          // THREAD TO HELP ALONG
          context_p->abort_flag = true;

          return;
        } else {
          bwt_printf("Leaf split delta CAS fails\n");

          // We have two nodes to delete here
          delete split_node_p;
          delete new_leaf_node_p;

          return;
        }

      } else if(node_size <= LEAF_NODE_SIZE_LOWER_THRESHOLD) {
        if(snapshot_p->is_leftmost_child == true || \
           snapshot_p->is_root == true) {
          bwt_printf("Left most leaf node cannot be removed\n");

          return;
        }

        bwt_printf("Node size <= leaf lower threshold. Remove\n");

        // Install an abort node on parent
        const BaseNode *abort_node_p = nullptr;
        const BaseNode *abort_child_node_p = nullptr;
        NodeID parent_node_id = INVALID_NODE_ID;

        bool abort_node_ret = \
          PostAbortOnParent(context_p,
                            &parent_node_id,
                            &abort_node_p,
                            &abort_child_node_p);

        // If we could not block the parent then the parent has changed
        // (splitted, etc.)
        if(abort_node_ret == true) {
          bwt_printf("Blocked parent node (current node is leaf)\n");
        } else {
          bwt_printf("Unable to block parent node "
                     "(current node is leaf). ABORT\n");

          // ABORT and return
          context_p->abort_flag = true;

          return;
        }

        // TODO: If we support removing non-base nodes, then we need
        // to fill into this will actual depth of the delta node
        int depth = 1;

        const LeafRemoveNode *remove_node_p = \
          new LeafRemoveNode{depth, node_p};

        bool ret = InstallNodeToReplace(node_id, remove_node_p, node_p);
        if(ret == true) {
          bwt_printf("LeafRemoveNode CAS succeeds. ABORT.\n");

          snapshot_p->SwitchPhysicalPointer(node_p);

          context_p->abort_flag = true;

          RemoveAbortOnParent(parent_node_id,
                              abort_node_p,
                              abort_child_node_p);

          return;
        } else {
          bwt_printf("LeafRemoveNode CAS failed\n");

          delete remove_node_p;

          context_p->abort_flag = true;

          RemoveAbortOnParent(parent_node_id,
                              abort_node_p,
                              abort_child_node_p);

          return;
        }
      }
    } else {   // If this is an inner node
      const InnerNode *inner_node_p = \
        static_cast<const InnerNode *>(node_p);

      if(snapshot_p->has_data == false) {
        CollectAllSepsOnInner(snapshot_p);
      }

      assert(snapshot_p->has_data == true);

      size_t node_size = inner_node_p->sep_list.size();

      if(node_size >= INNER_NODE_SIZE_UPPER_THRESHOLD) {
        bwt_printf("Node size >= inner upper threshold. Split\n");

        // This flag is only set when
        if(snapshot_p->is_root) {
          bwt_printf("Posting split delta on root node\n");
        }

        const InnerNode *new_inner_node_p = inner_node_p->GetSplitSibling();
        const KeyType *split_key_p = &new_inner_node_p->lbound;

        // New node has at least one item (this is the basic requirement)
        assert(new_inner_node_p->sep_list.size() > 0);

        const SepItem &first_item = new_inner_node_p->sep_list[0];
        // This points to the left most node on the right split sibling
        // If this node is being removed then we abort
        NodeID split_key_child_node_id = first_item.node;

        // This must be the split key
        assert(KeyCmpEqual(first_item.key, *split_key_p));

        // NOTE: WE FETCH THE POINTER WITHOUT HELP ALONG SINCE WE ARE
        // NOW ON ITS PARENT
        const BaseNode *split_key_child_node_p = \
          GetNode(split_key_child_node_id);

        // If the type is remove then we just continue without abort
        // If we abort then it might introduce deadlock
        NodeType split_key_child_node_type = split_key_child_node_p->GetType();
        if(split_key_child_node_type == NodeType::LeafRemoveType || \
           split_key_child_node_type == NodeType::InnerRemoveType) {
          bwt_printf("Found a removed node (type %d) on split key child "
                     "CONTINUE \n",
                     static_cast<int>(split_key_child_node_type));

          //context_p->abort_flag = true;

          return;
        }

        NodeID new_node_id = GetNextNodeID();

        // TODO: If we add support to split base node with a delta chain
        // then this is the length of the delta chain
        int depth = 1;

        const InnerSplitNode *split_node_p = \
          new InnerSplitNode{*split_key_p, new_node_id, depth, node_p};

        //  First install the NodeID -> split sibling mapping
        InstallNewNode(new_node_id, new_inner_node_p);
        // Then CAS split delta into current node's NodeID
        bool ret = InstallNodeToReplace(node_id, split_node_p, node_p);

        if(ret == true) {
          bwt_printf("Inner split delta (from %lu to %lu) CAS succeeds. ABORT\n",
                     node_id,
                     new_node_id);

          snapshot_p->SwitchPhysicalPointer(split_node_p);

          // Same reason as in leaf node
          context_p->abort_flag = true;

          return;
        } else {
          bwt_printf("Inner split delta CAS fails\n");

          // We have two nodes to delete here
          delete split_node_p;
          delete new_inner_node_p;

          return;
        } // if CAS fails
      } else if(node_size <= INNER_NODE_SIZE_LOWER_THRESHOLD) {
        // We could not remove leftmost node
        if(snapshot_p->is_leftmost_child == true || \
           snapshot_p->is_root == true) {
          bwt_printf("Left most inner node cannot be removed\n");

          return;
        }

        bwt_printf("Node size <= inner lower threshold. Remove\n");

        // Then we abort its parent node
        // These two are used to hold abort node and its previous child
        const BaseNode *abort_node_p = nullptr;
        const BaseNode *abort_child_node_p = nullptr;
        NodeID parent_node_id = INVALID_NODE_ID;

        bool abort_node_ret = \
          PostAbortOnParent(context_p,
                            &parent_node_id,
                            &abort_node_p,
                            &abort_child_node_p);

        // If we could not block the parent then the parent has changed
        // (splitted, etc.)
        if(abort_node_ret == true) {
          bwt_printf("Blocked parent node (current node is inner)\n");
        } else {
          bwt_printf("Unable to block parent node "
                     "(current node is inner). ABORT\n");

          // ABORT and return
          context_p->abort_flag = true;

          return;
        }

        // TODO: If we support removing non-base inner nodes then we should
        // fill in actual delta chain depth here
        int depth = 1;

        const InnerRemoveNode *remove_node_p = \
          new InnerRemoveNode{depth, node_p};

        bool ret = InstallNodeToReplace(node_id, remove_node_p, node_p);
        if(ret == true) {
          bwt_printf("LeafRemoveNode CAS succeeds. ABORT\n");

          snapshot_p->SwitchPhysicalPointer(node_p);

          // We abort after installing a node remove delta
          context_p->abort_flag = true;

          // Even if we success we need to remove the abort
          // on the parent, and let parent split thread to detect the remove
          // delta on child
          RemoveAbortOnParent(parent_node_id,
                              abort_node_p,
                              abort_child_node_p);

          return;
        } else {
          bwt_printf("LeafRemoveNode CAS failed\n");

          delete remove_node_p;

          // We must abort here since otherwise it might cause
          // merge nodes to underflow
          context_p->abort_flag = true;

          // Same as above
          RemoveAbortOnParent(parent_node_id,
                              abort_node_p,
                              abort_child_node_p);

          return;
        }
      } // if split/remove
    }

    return;
  }

  /*
   * RemoveAbortOnParent() - Removes the abort node on the parent
   *
   * This operation must succeeds since only the thread that installed
   * the abort node could remove it
   */
  void RemoveAbortOnParent(NodeID parent_node_id,
                           const BaseNode *abort_node_p,
                           const BaseNode *abort_child_node_p) {
    bwt_printf("Remove abort on parent node\n");

    // We switch back to the child node (so it is the target)
    bool ret = \
      InstallNodeToReplace(parent_node_id, abort_child_node_p, abort_node_p);

    // This CAS must succeed since nobody except this thread could remove
    // the ABORT delta on parent node
    assert(ret == true);
    (void)ret;

    // NOTE: DO NOT FORGET TO REMOVE THE ABORT AFTER
    // UNINSTALLING IT FROM THE PARENT NODE
    // NOTE 2: WE COULD NOT DIRECTLY DELETE THIS NODE
    // SINCE SOME OTHER NODES MIGHT HAVE TAKEN A SNAPSHOT
    // AND IF ABORT NODE WAS REMOVED, THE TYPE INFORMATION
    // CANNOT BE PRESERVED AND IT DOES NOT ABORT; INSTEAD
    // IT WILL TRY TO CALL CONSOLIDATE ON ABORT
    // NODE, CAUSING TYPE ERROR
    //delete (InnerAbortNode *)abort_node_p;

    // This delays the deletion of abort node until all threads has exited
    // so that existing pointers to ABORT node remain valid
    epoch_manager.AddGarbageNode(abort_node_p);

    return;
  }

  /*
   * PostAbortOnParent() - Posts an inner abort node on the parent
   *
   * This function blocks all accesses to the parent node, and blocks
   * all CAS efforts for threads that took snapshots before the CAS
   * in this function.
   *
   * Return false if CAS failed. In that case the memory is freed
   * by this function
   *
   * This function DOES NOT ABORT. Do not have to check for abort flag. But
   * if CAS fails then returns false, and caller needs to abort after
   * checking the return value.
   */
  bool PostAbortOnParent(Context *context_p,
                         NodeID *parent_node_id_p,
                         const BaseNode **abort_node_p_p,
                         const BaseNode **abort_child_node_p_p) {
    // This will make sure the path list has length >= 2
    NodeSnapshot *parent_snapshot_p = \
      GetLatestParentNodeSnapshot(context_p);

    const BaseNode *parent_node_p = parent_snapshot_p->node_p;
    NodeID parent_node_id = parent_snapshot_p->node_id;

    // Save original node pointer
    *abort_child_node_p_p = parent_node_p;
    *parent_node_id_p = parent_node_id;

    InnerAbortNode *abort_node_p = new InnerAbortNode{parent_node_p};

    bool ret = InstallNodeToReplace(parent_node_id,
                                    abort_node_p,
                                    parent_node_p);

    if(ret == true) {
      bwt_printf("Inner Abort node CAS succeeds\n");

      // Copy the new node to caller since after posting remove delta we will
      // remove this abort node to enable accessing again
      *abort_node_p_p = abort_node_p;
    } else {
      bwt_printf("Inner Abort node CAS failed\n");

      delete abort_node_p;
    }

    return ret;
  }

  /*
   * FindSplitNextKey() - Given a parent snapshot, find the next key of
   *                      the current split key
   *
   * This function will search for the next key of the current split key
   * If the split key is found, it just return false. In that case
   * the key has already been inserted, and we should not post duplicate
   * record
   *
   * Returns true if split key is not found (i.e. we could insert index term)
   *
   * NOTE: This function assumes the snapshot already has data and metadata
   *
   * NOTE 2: This function checks whether the PID has already been inserted
   * using both the sep key and NodeID. These two must match, otherwise we
   * observed an inconsistent state
   */
  bool FindSplitNextKey(NodeSnapshot *snapshot_p,
                        const KeyType *split_key_p,
                        const KeyType **next_key_p_p,
                        const NodeID insert_pid) {
    assert(snapshot_p->is_leaf == false);
    assert(snapshot_p->has_data == true);
    assert(snapshot_p->has_metadata == true);
    
    (void)insert_pid;

    KeyNodeIDMap &sep_map = \
      snapshot_p->GetLogicalInnerNode()->GetContainer();

    assert(sep_map.size() >= 1);

    typename KeyNodeIDMap::iterator it1 = sep_map.begin();

    while(it1 != sep_map.end()) {
      // We have inserted the same key before
      if(KeyCmpEqual(it1->first, *split_key_p) == true) {
        // Make sure the SepItem is used to map newly splitted node
        //idb_assert_key(snapshot_p->node_id,
        //               , insert_pid == it1->second);

        return false;
      }

      // if we have found its next key, return it in the parameter
      if(KeyCmpGreater(it1->first, *split_key_p) == true) {
        *next_key_p_p = &it1->first;

        return true;
      }

      it1++;
    }

    // If we reach here, then the split key is larger than all
    // existing keys in the inner node
    *next_key_p_p = snapshot_p->GetHighKey();

    return true;
  }

  /*
   * FindMergePrevNextKey() - Find merge next and prev key and node ID
   *
   * This function loops through keys in the snapshot, and if there is a
   * match of the merge key, its previous key and next key are collected.
   * Also the node ID of its previous key is collected
   *
   * NOTE: There is possibility that the index term has already been
   * deleted in the parent node, in which case this function does not
   * fill in the given pointer but returns false instead.
   *
   * NOTE 2: This function assumes the snapshot already has data and metadata
   *
   * Return true if the merge key is found. false if merge key not found
   */
  bool FindMergePrevNextKey(NodeSnapshot *snapshot_p,
                            const KeyType *merge_key_p,
                            const KeyType **prev_key_p_p,
                            const KeyType **next_key_p_p,
                            NodeID *prev_node_id_p,
                            NodeID deleted_node_id) {
    // We could only post merge key on merge node
    assert(snapshot_p->is_leaf == false);
    assert(snapshot_p->has_data == true);
    assert(snapshot_p->has_metadata == true);
    
    (void)deleted_node_id;

    // This is the map that stores key to node ID Mapping
    KeyNodeIDMap &sep_map = \
      snapshot_p->GetLogicalInnerNode()->GetContainer();

    // This does not have to be >= 2, since it is possible
    // that there is only 1 separator (we just return false
    // to let the caller know the key has been deleted no
    // matter what the key is)
    idb_assert(sep_map.size() != 0);

    // If there is only one key then we know
    // and we know the key being deleted is not here
    if(sep_map.size() == 1) {
      bwt_printf("Only 1 key to delete. Return false\n");

      return false;
    }

    // it1 should not be the key since we do not allow
    // merge node on the leftmost child
    // FUCK YOU C++ for not having non-random iterator's operator+
    typename KeyNodeIDMap::iterator it1 = sep_map.begin();
    decltype(it1) it2 = it1;
    it2++;
    decltype(it2) it3 = it2;
    it3++;

    while(it3 != sep_map.end()) {
      if(KeyCmpEqual(it2->first, *merge_key_p) == true) {
        *prev_key_p_p = &it1->first;
        *next_key_p_p = &it3->first;
        // We use prev key's node ID
        *prev_node_id_p = it1->second;

        return true;
      }

      it1++;
      it2++;
      it3++;
    }

    // Special case: sep is the last in parent node
    // In this case we need to use high key as the next sep key
    if(KeyCmpEqual(it2->first, *merge_key_p) == true) {
      *prev_key_p_p = &it1->first;
      *next_key_p_p = snapshot_p->GetHighKey();
      *prev_node_id_p = it1->second;

      return true;
    } else {
      bwt_printf("Did not find merge key in parent node - already deleted\n");

      return false;
    }

    assert(false);
    return false;
  }

  /*
   * PostMergeNode() - Post a leaf merge node on top of a delta chain
   *
   * This function is made to be a template since all logic except the
   * node type is same.
   *
   * NOTE: This function takes an argument, such that if CAS succeeds it
   * sets that pointer's value to be the new node we just created
   * If CAS fails we do not touch that pointer
   *
   * NOTE: This function deletes memory for the caller, so caller do
   * not have to free memory when this function returns false
   */
  template <typename MergeNodeType>
  bool PostMergeNode(const NodeSnapshot *snapshot_p,
                     const KeyType *merge_key_p,
                     const BaseNode *merge_branch_p,
                     const BaseNode **node_p_p) {
    // This is the child node of merge delta
    const BaseNode *node_p = snapshot_p->node_p;
    NodeID node_id = snapshot_p->node_id;

    // If this is the first delta node, then we just set depth to 1
    // Otherwise we need to use from the one from previous node
    int depth = 1;

    if(node_p->IsDeltaNode() == true) {
      const DeltaNode *delta_node_p = \
        static_cast<const DeltaNode *>(node_p);

      depth = delta_node_p->depth + 1;
    }

    // NOTE: DO NOT FORGET TO DELETE THIS IF CAS FAILS
    const MergeNodeType *merge_node_p = \
      new MergeNodeType{*merge_key_p,
                        merge_branch_p,
                        depth,
                        node_p};

    // Compare and Swap!
    bool ret = InstallNodeToReplace(node_id, merge_node_p, node_p);

    // If CAS fails we delete the node and return false
    if(ret == false) {
      delete merge_node_p;
    } else {
      *node_p_p = merge_node_p;
    }

    return ret;
  }

  /*
   * IsKeyPresent() - Check whether the key is present in the tree
   *
   * This function calls Traverse with value option in order to
   * collect all values associated with the search key. If there is
   * no such key in the resulting key value map, then return false
   *
   * TODO: This function is not the optimal one since there is overhead
   * for copying the values which is discarded. In the future we may want
   * to revise Traverse to make it able to detect for key existence request
   */
  bool IsKeyPresent(const KeyType &search_key) {
    Context context{&search_key};

    Traverse(&context, true);

    NodeSnapshot *snapshot = GetLatestNodeSnapshot(&context);
    LogicalLeafNode *logical_node_p = snapshot->GetLogicalLeafNode();
    KeyValueSet &container = logical_node_p->GetContainer();

    if(container.find(search_key) == container.end()) {
      return false;
    } else {
      return true;
    }

    assert(false);
    return false;
  }

  /*
   * Insert() - Insert a key-value pair
   *
   * This function returns false if value already exists
   * If CAS fails this function retries until it succeeds
   */
  bool Insert(const KeyType &key, const ValueType &value) {
    bwt_printf("Insert called\n");

    insert_op_count.fetch_add(1);

    EpochNode *epoch_node_p = epoch_manager.JoinEpoch();

    while(1) {
      Context context{key};

      // Collect values with node navigation
      Traverse(&context, true);

      NodeSnapshot *snapshot_p = GetLatestNodeSnapshot(&context);
      LogicalLeafNode *logical_node_p = snapshot_p->GetLogicalLeafNode();
      KeyValueSet &container = logical_node_p->GetContainer();

      // For insertion for should iterate through existing values first
      // to make sure the key-value pair does not exist
      typename KeyValueSet::iterator it = container.find(key);
      if(it != container.end()) {
        // Look for value in consolidated leaf with the given key
        auto it2 = it->second.find(value);

        if(it2 != it->second.end()) {
          epoch_manager.LeaveEpoch(epoch_node_p);

          return false;
        }
      }

      // We will CAS on top of this
      const BaseNode *node_p = snapshot_p->node_p;
      NodeID node_id = snapshot_p->node_id;

      // If node_p is a delta node then we have to use its
      // delta value
      int depth = 1;

      if(node_p->IsDeltaNode() == true) {
        const DeltaNode *delta_node_p = \
          static_cast<const DeltaNode *>(node_p);

        depth = delta_node_p->depth + 1;
      }

      const LeafInsertNode *insert_node_p = \
        new LeafInsertNode{key, value, depth, node_p};

      bool ret = InstallNodeToReplace(node_id,
                                      insert_node_p,
                                      node_p);
      if(ret == true) {
        bwt_printf("Leaf Insert delta CAS succeed\n");

        // This will actually not be used anymore, so maybe
        // could save this assignment
        snapshot_p->SwitchPhysicalPointer(insert_node_p);

        // If install is a success then just break from the loop
        // and return
        break;
      } else {
        bwt_printf("Leaf insert delta CAS failed\n");

        context.abort_counter++;

        delete insert_node_p;
      }

      // Update abort counter
      // NOTW 1: We could not do this before return since the context
      // object is cleared at the end of loop
      // NOTE 2: Since Traverse() might abort due to other CAS failures
      // context.abort_counter might be larger than 1 when
      // LeafInsertNode installation fails
      insert_abort_count.fetch_add(context.abort_counter);

      // We reach here only because CAS failed
      bwt_printf("Retry installing leaf insert delta from the root\n");
    }

    epoch_manager.LeaveEpoch(epoch_node_p);

    return true;
  }
  
#ifdef BWTREE_PELOTON

  /*
   * ConditionalInsert() - Insert a key-value pair only if a given
   *                       predicate fails for all values with a key
   *
   * If return true then the value has been inserted
   * If return false then the value is not inserted. The reason could be
   * predicates returning true for one of the values of a given key
   * or because the value is already in the index
   *
   * Argument value_p is set to nullptr if all predicate tests returned false
   *
   * NOTE: We first test the predicate, and then test for duplicated values
   * so predicate test result is always available
   */
  bool ConditionalInsert(const KeyType &key,
                         const ValueType &value,
                         std::function<bool(const ItemPointer &)> predicate,
                         bool *predicate_satisfied) {
    bwt_printf("Consitional Insert called\n");

    insert_op_count.fetch_add(1);

    EpochNode *epoch_node_p = epoch_manager.JoinEpoch();

    while(1) {
      Context context{key};

      // Collect values with node navigation
      Traverse(&context, true);

      NodeSnapshot *snapshot_p = GetLatestNodeSnapshot(&context);
      LogicalLeafNode *logical_node_p = snapshot_p->GetLogicalLeafNode();
      KeyValueSet &container = logical_node_p->GetContainer();
      
      // At the beginning of each iteration we just set the value pointer
      // to be empty
      // Note that in Peloton we always store value as ItemPointer * so
      // in order for simplicity we just assume the value itself
      // is a pointer type
      *predicate_satisfied = false;

      // For insertion for should iterate through existing values first
      // to make sure the key-value pair does not exist
      typename KeyValueSet::iterator it = container.find(key);
      if(it != container.end()) {
        // v is a reference to ValueType
        for(auto &v : it->second) {
          if(predicate(*v) == true) {
            // To notify the caller that predicate
            // has been satisfied and we cannot insert
            *predicate_satisfied = true;
            
            // Do not forget this!
            epoch_manager.LeaveEpoch(epoch_node_p);
            
            return false;
          }
        }
        
        // After evaluating predicate on all values we continue to find
        // whether there is duplication for the value
        auto it2 = it->second.find(value);

        if(it2 != it->second.end()) {
          epoch_manager.LeaveEpoch(epoch_node_p);

          // In this case, value_p is set to nullptr
          // and return value is false
          return false;
        }
      }

      // We will CAS on top of this
      const BaseNode *node_p = snapshot_p->node_p;
      NodeID node_id = snapshot_p->node_id;

      // If node_p is a delta node then we have to use its
      // delta value
      int depth = 1;

      if(node_p->IsDeltaNode() == true) {
        const DeltaNode *delta_node_p = \
          static_cast<const DeltaNode *>(node_p);

        depth = delta_node_p->depth + 1;
      }

      const LeafInsertNode *insert_node_p = \
        new LeafInsertNode{key, value, depth, node_p};

      bool ret = InstallNodeToReplace(node_id,
                                      insert_node_p,
                                      node_p);
      if(ret == true) {
        bwt_printf("Leaf Insert delta (cond) CAS succeed\n");

        // This will actually not be used anymore, so maybe
        // could save this assignment
        snapshot_p->SwitchPhysicalPointer(insert_node_p);

        // If install is a success then just break from the loop
        // and return
        break;
      } else {
        bwt_printf("Leaf Insert delta (cond) CAS failed\n");

        context.abort_counter++;

        delete insert_node_p;
      }

      // Update abort counter
      // NOTE 1: We could not do this before return since the context
      // object is cleared at the end of loop
      // NOTE 2: Since Traverse() might abort due to other CAS failures
      // context.abort_counter might be larger than 1 when
      // LeafInsertNode installation fails
      insert_abort_count.fetch_add(context.abort_counter);

      // We reach here only because CAS failed
      bwt_printf("Retry installing leaf insert delta from the root\n");
    }

    epoch_manager.LeaveEpoch(epoch_node_p);

    return true;
  }
  
#endif

  /*
   * Update() - Update an existing value for a key to a new value
   *
   * This function returns false if the key does not exist or
   * value does not exist with the key
   *
   * This function also returns false if the new value already exists
   *
   * TODO: Use a flag set to represent all these error conditions
   * instead of only one boolean flag
   */
  bool Update(const KeyType &key,
              const ValueType &old_value,
              const ValueType &new_value) {
    bwt_printf("Update called\n");

    update_op_count.fetch_add(1);

    EpochNode *epoch_node_p = epoch_manager.JoinEpoch();

    while(1) {
      Context context{key};

      Traverse(&context, true);

      NodeSnapshot *snapshot_p = GetLatestNodeSnapshot(&context);
      LogicalLeafNode *logical_node_p = snapshot_p->GetLogicalLeafNode();
      KeyValueSet &container = logical_node_p->GetContainer();

      // first check whether the key exists or not
      // if key does not exist return false
      typename KeyValueSet::iterator it = container.find(key);
      if(it == container.end()) {
        epoch_manager.LeaveEpoch(epoch_node_p);

        return false;
      }

      // Then check whether the value exists for this key
      // given the value set for the key
      // and if value does not exist return false
      auto it2 = it->second.find(old_value);
      if(it2 == it->second.end()) {
        epoch_manager.LeaveEpoch(epoch_node_p);

        return false;
      }

      // If the new value already exists in the leaf
      // we also will not do update and simply return false
      auto it3 = it->second.find(new_value);
      if(it3 != it->second.end()) {
        epoch_manager.LeaveEpoch(epoch_node_p);

        return false;
      }

      // We will CAS on top of this
      const BaseNode *node_p = snapshot_p->node_p;
      NodeID node_id = snapshot_p->node_id;

      // If node_p is a delta node then we have to use its
      // delta value
      int depth = 1;

      if(node_p->IsDeltaNode() == true) {
        const DeltaNode *delta_node_p = \
          static_cast<const DeltaNode *>(node_p);

        depth = delta_node_p->depth + 1;
      }

      const LeafUpdateNode *update_node_p = \
        new LeafUpdateNode{key, old_value, new_value, depth, node_p};

      bool ret = InstallNodeToReplace(node_id,
                                      update_node_p,
                                      node_p);
      if(ret == true) {
        bwt_printf("Leaf Update delta CAS succeed\n");

        // This will actually not be used anymore, so maybe
        // could save this assignment
        snapshot_p->SwitchPhysicalPointer(update_node_p);

        // If install is a success then just break from the loop
        // and return
        break;
      } else {
        bwt_printf("Leaf update delta CAS failed\n");

        delete update_node_p;

        context.abort_counter++;
      }

      update_abort_count.fetch_add(context.abort_counter);

      // We reach here only because CAS failed
      bwt_printf("Retry installing leaf update delta from the root\n");
    }

    epoch_manager.LeaveEpoch(epoch_node_p);

    return true;
  }

  /*
   * Delete() - Remove a key-value pair from the tree
   *
   * This function returns false if the key and value pair does not
   * exist. Return true if delete succeeds
   *
   * This functions shares a same structure with the Insert() one
   */
  bool Delete(const KeyType &key,
              const ValueType &value) {
    bwt_printf("Delete called\n");

    delete_op_count.fetch_add(1);

    EpochNode *epoch_node_p = epoch_manager.JoinEpoch();

    while(1) {
      Context context{key};

      // Collect values with node navigation
      Traverse(&context, true);

      NodeSnapshot *snapshot_p = GetLatestNodeSnapshot(&context);
      LogicalLeafNode *logical_node_p = snapshot_p->GetLogicalLeafNode();
      KeyValueSet &container = logical_node_p->GetContainer();

      // If the key or key-value pair does not exist then we just
      // return false
      typename KeyValueSet::iterator it = container.find(key);
      if(it == container.end()) {
        epoch_manager.LeaveEpoch(epoch_node_p);

        return false;
      }

      // Look for value in consolidated leaf with the given key
      auto it2 = it->second.find(value);
      if(it2 == it->second.end()) {
        epoch_manager.LeaveEpoch(epoch_node_p);

        return false;
      }

      // We will CAS on top of this
      const BaseNode *node_p = snapshot_p->node_p;
      NodeID node_id = snapshot_p->node_id;

      // If node_p is a delta node then we have to use its
      // delta value
      int depth = 1;

      if(node_p->IsDeltaNode() == true) {
        const DeltaNode *delta_node_p = \
          static_cast<const DeltaNode *>(node_p);

        depth = delta_node_p->depth + 1;
      }

      const LeafDeleteNode *delete_node_p = \
        new LeafDeleteNode{key, value, depth, node_p};

      bool ret = InstallNodeToReplace(node_id,
                                      delete_node_p,
                                      node_p);
      if(ret == true) {
        bwt_printf("Leaf Delete delta CAS succeed\n");

        // This will actually not be used anymore, so maybe
        // could save this assignment
        snapshot_p->SwitchPhysicalPointer(delete_node_p);

        // If install is a success then just break from the loop
        // and return
        break;
      } else {
        bwt_printf("Leaf Delete delta CAS failed\n");

        delete delete_node_p;

        context.abort_counter++;
      }

      delete_abort_count.fetch_add(context.abort_counter);

      // We reach here only because CAS failed
      bwt_printf("Retry installing leaf delete delta from the root\n");
    }

    epoch_manager.LeaveEpoch(epoch_node_p);

    return true;
  }
  
  #ifdef BWTREE_PELOTON
  
  /*
   * DeleteItemPointer() - Deletes an item pointer from the index by comparing
   *                       the target of the pointer, rather than pointer itself
   *
   * Note that this function assumes the value always being ItemPointer *
   * and in this function we compare item pointer's target rather than
   * the value of pointers themselves. Also when a value is deleted, we
   * free the memory, which is allocated when the value is inserted
   */
  bool DeleteItemPointer(const KeyType &key,
                         const ItemPointer &value) {
    bwt_printf("Delete Item Pointer called\n");

    delete_op_count.fetch_add(1);

    EpochNode *epoch_node_p = epoch_manager.JoinEpoch();

    while(1) {
      Context context{key};

      // Collect values with node navigation
      Traverse(&context, true);

      NodeSnapshot *snapshot_p = GetLatestNodeSnapshot(&context);
      LogicalLeafNode *logical_node_p = snapshot_p->GetLogicalLeafNode();
      KeyValueSet &container = logical_node_p->GetContainer();

      // If the key or key-value pair does not exist then we just
      // return false
      typename KeyValueSet::iterator it = container.find(key);
      if(it == container.end()) {
        epoch_manager.LeaveEpoch(epoch_node_p);

        return false;
      }

      bool found_flag = false;
      ItemPointer *found_value = nullptr;
      // We iterator over values stored with the given key
      // v should be ItemPointer *
      for(ItemPointer *v : it->second) {
        if((v->block == value.block) &&
           (v->offset == value.offset)) {
          found_flag = true;
          found_value = v;
        }
      }
      
      // If the value was not found, then just leave the epoch
      // and return false to notify the caller
      if(found_flag == false) {
        assert(found_value == nullptr);
        
        epoch_manager.LeaveEpoch(epoch_node_p);

        return false;
      }

      // We will CAS on top of this
      const BaseNode *node_p = snapshot_p->node_p;
      NodeID node_id = snapshot_p->node_id;

      // If node_p is a delta node then we have to use its
      // delta value
      int depth = 1;

      if(node_p->IsDeltaNode() == true) {
        const DeltaNode *delta_node_p = \
          static_cast<const DeltaNode *>(node_p);

        depth = delta_node_p->depth + 1;
      }

      const LeafDeleteNode *delete_node_p = \
        new LeafDeleteNode{key, found_value, depth, node_p};

      bool ret = InstallNodeToReplace(node_id,
                                      delete_node_p,
                                      node_p);
      if(ret == true) {
        bwt_printf("Leaf Delete delta CAS succeed\n");

        // This will actually not be used anymore, so maybe
        // could save this assignment
        snapshot_p->SwitchPhysicalPointer(delete_node_p);
        
        // This piece of memory holds ItemPointer, and is allocated by
        // InsertEntry() in its wrapper class. We need to free the memory
        // when the index is deleted
        delete found_value;

        // If install is a success then just break from the loop
        // and return
        break;
      } else {
        bwt_printf("Leaf Delete delta CAS failed\n");

        delete delete_node_p;

        context.abort_counter++;
      }

      delete_abort_count.fetch_add(context.abort_counter);

      // We reach here only because CAS failed
      bwt_printf("Retry installing leaf delete delta from the root\n");
    }

    epoch_manager.LeaveEpoch(epoch_node_p);

    return true;
  }
  
  #endif

  /*
   * GetValue() - Fill a value list with values stored
   *
   * This function accepts a value list as argument,
   * and will copy all values into the list
   *
   * The return value is used to indicate whether the value set
   * is empty or not
   */
  bool GetValue(const KeyType &search_key,
                std::vector<ValueType> &value_list) {
    EpochNode *epoch_node_p = epoch_manager.JoinEpoch();

    Context context{search_key};

    Traverse(&context, true);

    NodeSnapshot *snapshot = GetLatestNodeSnapshot(&context);
    LogicalLeafNode *logical_node_p = snapshot->GetLogicalLeafNode();
    KeyValueSet &container = logical_node_p->GetContainer();

    typename KeyValueSet::iterator it = container.find(search_key);
    if(it != container.end()) {
      value_list = std::move(std::vector<ValueType>{it->second.begin(),
                                                    it->second.end()});

      epoch_manager.LeaveEpoch(epoch_node_p);

      return true;
    } else {
      epoch_manager.LeaveEpoch(epoch_node_p);

      return false;
    }

    assert(false);
    return false;
  }

  /*
   * GetValue() - Return an unordered set of values given the key
   *
   * This is an overridden version of the first GetValue(). The
   * overhead is slightly less than the previous one since it does
   * not construct a vector
   */
  ValueSet GetValue(const KeyType &search_key) {
    EpochNode *epoch_node_p = epoch_manager.JoinEpoch();

    Context context{search_key};

    Traverse(&context, true);

    NodeSnapshot *snapshot = GetLatestNodeSnapshot(&context);
    LogicalLeafNode *logical_node_p = snapshot->GetLogicalLeafNode();
    KeyValueSet &container = logical_node_p->GetContainer();

    typename KeyValueSet::iterator it = container.find(search_key);
    if(it != container.end()) {
      epoch_manager.LeaveEpoch(epoch_node_p);

      return ValueSet{it->second};
    } else {
      epoch_manager.LeaveEpoch(epoch_node_p);

      return ValueSet{};
    }
  }

 /*
  * Private Method Implementation
  */
#ifndef ALL_PUBLIC
 private:
#else
 public:
#endif

 /*
  * Data Member Definition
  */
#ifndef ALL_PUBLIC
 private:
#else
 public:
#endif
  // Compare key in a less than manner
  // NOTE: We need this being passed around to do tuple comparison
  // in Peloton
  const KeyComparator key_cmp_obj;

  // Wrapped key comparator
  // We use this in general to compare wrapped keys. This object should
  // also be passed around for std::map to use as the key comparator since
  // it could not be trivially constructed
  const WrappedKeyComparator wrapped_key_cmp_obj;

  // It is faster to compare equality using this than using two <
  const KeyEqualityChecker key_eq_obj;
  // Check whether values are equivalent
  const ValueEqualityChecker value_eq_obj;
  // Hash ValueType into a size_t
  const ValueHashFunc value_hash_obj;

  // Whether we should allow keys to have multiple values
  // This does not affect data layout, and will introduce extra overhead
  // for a given key. But it simplifies coding for duplicated values
  const bool key_dup;

  std::atomic<NodeID> root_id;
  NodeID first_node_id;
  std::atomic<NodeID> next_unused_node_id;
  std::array<std::atomic<const BaseNode *>, MAPPING_TABLE_SIZE> mapping_table;

  std::atomic<uint64_t> insert_op_count;
  std::atomic<uint64_t> insert_abort_count;

  std::atomic<uint64_t> delete_op_count;
  std::atomic<uint64_t> delete_abort_count;

  std::atomic<uint64_t> update_op_count;
  std::atomic<uint64_t> update_abort_count;

  InteractiveDebugger idb;

  EpochManager epoch_manager;

 /*
  * Debug function definition
  */
 public:

  /*
   * DebugGetInnerNode() - Debug routine. Return inner node given two
   *                       lists of keys and values
   *
   * This is only used for debugging externally - please do not call
   * this function in side the class
   *
   * NOTE: Since this function does not actually require "this"
   * pointer, we could make it as static. However, making it static might
   * cause complex grammar, so we make it a member function
   */
  InnerNode *DebugGetInnerNode(const RawKeyType &p_lbound,
                               const RawKeyType &p_ubound,
                               NodeID p_next_node_id,
                               std::vector<RawKeyType> raw_key_list,
                               std::vector<NodeID> node_id_list) {
    InnerNode *temp_node_p = \
      new InnerNode{p_lbound, p_ubound, p_next_node_id};

    assert(raw_key_list.size() == node_id_list.size());

    // Push item into the node 1 by 1
    for(int i = 0;i < (int)raw_key_list.size();i++) {
      SepItem item{KeyType{raw_key_list[i]}, node_id_list[i]};

      temp_node_p->sep_list.push_back(item);
    }

    return temp_node_p;
  }

  /*
   * DebugGetLeafNode() - Return a leaf node given a list of keys
   *                      and a list of list of values
   *
   * This function is only used for debugging.
   */
  LeafNode *DebugGetLeafNode(const RawKeyType &p_lbound,
                             const RawKeyType &p_ubound,
                             NodeID p_next_node_id,
                             std::vector<RawKeyType> raw_key_list,
                             std::vector<std::vector<ValueType>> value_list_list) {
    LeafNode *temp_node_p = \
      new LeafNode{p_lbound, p_ubound, p_next_node_id};

    assert(raw_key_list.size() == value_list_list.size());

    for(int i = 0;i < (int)raw_key_list.size();i++) {
      // Construct the data item using a key and a list of values
      DataItem item{KeyType{raw_key_list[i]},
                    value_list_list[i]};

      temp_node_p->data_list.push_back(item);
    }

    return temp_node_p;
  }

  /*
   * DebugUninstallNode() - Uninstall a node forcibally
   *
   * This could not be used inside normal operation since this operation
   * is not thread safe (not using CAS)
   */
  void DebugUninstallNode(NodeID node_id) {
    mapping_table[node_id] = 0LU;
  }

  /*
   * DebugGetContext() - Get a context object pointer for debugging
   *
   * It assumes non-leftmost, root, no lbound_p context
   * If you need more than that just change the member of Context
   */
  Context *DebugGetContext(const KeyType &search_key,
                           NodeSnapshot *snapshot_p) {
    Context *context_p = new Context{search_key};

    context_p->path_list.push_back(std::move(snapshot_p));

    return context_p;
  }
  
  /*
   * Iterator Interface
   */
  class ForwardIterator;
  
  /*
   * Begin() - Return an iterator pointing the first element in the tree
   *
   * If the tree is currently empty, then the iterator is both a begin
   * iterator and an end iterator (i.e. both flags are set to true). This
   * is a valid state.
   */
  ForwardIterator Begin() {
    return ForwardIterator{this};
  }
  
  /*
   * Begin() - Return an iterator using a given key
   *
   * The iterator returned will points to a data item whose key is greater than
   * or equal to the given start key. If such key does not exist then it will
   * be the smallest key that is greater than start_key
   */
  ForwardIterator Begin(const KeyType &start_key) {
    return ForwardIterator{this, start_key};
  }

  /*
   * Iterators
   */

  /*
   * class ForwardIterator - Iterator that supports forward iteration of
   *                         tree elements
   *
   * NOTE: An iterator could be begin() and end() iterator at the same time,
   * as long as the container is empty. To correctly identify this case, we
   * need to try loading the first page with key = -Inf to test whether
   * there is any element stored in the tree. And if not then the iterator
   * is both begin() and end() iterator at the same time
   */
  class ForwardIterator {
   public:
    /*
     * Default Constructor
     *
     * NOTE: We try to load the first page using -Inf as the next key
     * during construction in order to correctly identify the case where
     * the tree is empty, and such that begin() iterator equals end() iterator
     */
    ForwardIterator(BwTree *p_tree_p) :
      tree_p{p_tree_p},
      logical_node_p{nullptr},
      raw_key_p{nullptr},
      value_set_p{nullptr},
      key_it{},
      value_it{},
      // On initialization, next key is set to -Inf
      // to indicate it is begin() vector
      next_key{tree_p->GetNegInfKey()},
      // These two will be set in LoadNextKey()
      is_begin{false},
      is_end{false},
      key_distance{0},
      value_distance{0} {
      // 1. If the tree is not empty then this prepares the first page
      // 2. If LoadNextKey() sees +Inf on the page, then it set is_end flag
      // 3. If LoadNextKey() sees next_key being -Inf then it set is_begin flag
      // and now the iterator is a begin() and end() iterator at the same time
      LoadNextKey();

      return;
    }
    
    /*
     * Constructor - Construct an iterator given a key
     *
     * The given key would locate the iterator on an data item whose
     * key is >= the given key. This is useful in range query if
     * a starting key could be derived according to conditions
     */
    ForwardIterator(BwTree *p_tree_p,
                    const KeyType start_key) :
      tree_p{p_tree_p},
      logical_node_p{nullptr},
      raw_key_p{nullptr},
      value_set_p{nullptr},
      key_it{},
      value_it{},
      // We set next_key here, and it will be loaded by LoadNextKey()
      next_key{start_key},
      // These two will be set in LoadNextKey()
      is_begin{false},
      is_end{false},
      key_distance{0},
      value_distance{0} {
      // For a given key rather than -Inf, noe we have an iterator
      // whose first element is on a key >= given start key
      LoadNextKey();

      return;
    }
    
    /*
     * Copy Constructor - Constructs a new iterator instance from existing one
     *
     * In ForwardIterator we always maintain the property that logical leaf node
     * is not shared between iterators. This implies we need to also copy the
     * logical leaf node during copy construction.
     */
    ForwardIterator(const ForwardIterator &it) :
      tree_p{it.tree_p},
      // We allocate memory here and call copy constructor
      logical_node_p{new LogicalLeafNode{*it.logical_node_p}},
      next_key{it.next_key},
      // These two also need to be copied from the source
      is_begin{it.is_begin},
      is_end{it.is_end},
      key_distance{it.key_distance},
      value_distance{it.value_distance} {
      // First locate key using key distance
      key_it = logical_node_p->key_value_set.begin();
      std::advance(key_it, key_distance);
      
      // Next locate value using value distance
      value_it = key_it->second.begin();
      std::advance(value_it, value_distance);
      
      // This refers to the raw key pointer in the new object
      raw_key_p = &key_it->first.key;
      value_set_p = &key_it->second;
      
      return;
    }
    
    /*
     * operator= - Assigns one object to another
     *
     * DONE: As an optimization we could define an assignment operation
     * for logical leaf node, and direct assign the logical leaf node from
     * the source object to the current object
     */
    ForwardIterator &operator=(const ForwardIterator &it) {
      // It is crucial to prevent self assignment since we do pointer
      // operation here
      if(this == &it) {
        return *this;
      }
      
      // First copy the logical node into current instance
      // DO NOT NEED delete; JUST DO A VALUE COPY
      assert(logical_node_p != nullptr);
      *logical_node_p = *it.logical_node_p;
      
      // Copy everything that could be copied
      tree_p = it.tree_p;
      next_key = it.next_key;
      
      is_begin = it.is_begin;
      is_end = it.is_end;
      
      key_distance = it.key_distance;
      value_distance = it.value_distance;
      
      // The following is copied from the copy constructor
      
      // NOTE: It is possible that for an empty container
      // the key value set is empty. In that case key_distance
      // and value_distance must be 0
      key_it = logical_node_p->key_value_set.begin();
      std::advance(key_it, key_distance);

      value_it = key_it->second.begin();
      std::advance(value_it, value_distance);

      raw_key_p = &key_it->first.key;
      value_set_p = &key_it->second;
      
      // The above is copied from the copy constructor
      
      return *this;
    }
    
    /*
     * operator*() - Return the value reference currently pointed to by this
     *               iterator
     *
     * NOTE: We need to return a constant reference to both save a value copy
     * and also to prevent caller modifying value using the reference
     */
    inline const ValueType &operator*() {
      // This itself is a ValueType reference
      return (*value_it);
    }
    
    /*
     * operator->() - Returns the value pointer pointed to by this iterator
     *
     * Note that this function returns a contsnat pointer which can be used
     * to access members of the value, but cannot modify
     */
    inline const ValueType *operator->() {
      return &(*value_it);
    }
    
    /*
     * operator< - Compares two iterators by comparing their current key
     *
     * Since the structure of the tree keeps changing, there is not a
     * universal coordinate system that allows us to compare absolute
     * positions of two iterators, all comparisons are done using the current
     * key associated with values.
     *
     * NOTE: It is possible that for an iterator, no raw keys are stored. This
     * happens when the tree is empty, or the requested key is larger than
     * all existing keys in the tree. In that case, end flag is set, so
     * in this function we check end flag first
     */
    inline bool operator<(const ForwardIterator &it) const {
      if(it.is_end == true) {
        if(is_end == true) {
          // If both are end iterator then no one is less than another
          return false;
        } else {
          // Otherwise, the left one is smaller as long as the
          // RHS is an end iterator
          return true;
        }
      }
      
      // If none of them is end iterator, we simply do a key comparison
      return tree_p->RawKeyCmpLess(*raw_key_p, *it.raw_key_p);
    }
    
    /*
     * operator==() - Compares whether two iterators refer to the same key
     *
     * If both iterators are end iterator then we return true
     * If one of them is not then the result is false
     * Otherwise the result is the comparison result of current key
     */
    inline bool operator==(const ForwardIterator &it) const {
      if(it.is_end == true) {
        if(is_end == true) {
          // Two end iterators are equal to each other
          return true;
        } else {
          // Otherwise they are not equal
          return false;
        }
      }
      
      return tree_p->RawKeyCmpEqual(*raw_key_p, *it.raw_key_p);
    }

    /*
     * Destructor
     *
     * NOTE: Since we always make copies of logical node object when copy
     * constructing the iterator, we could always safely delete the logical
     * node, because its memory is not shared between iterators
     */
    ~ForwardIterator() {
      // This holds even if the tree is empty
      assert(logical_node_p != nullptr);

      delete logical_node_p;

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
      if(is_end == true) {
        return *this;
      }
      
      is_begin = false;
      MoveAheadByOne();
      
      return *this;
    }
    
    /*
     * Postfix operator++ - Move the iterator ahead, and return the old one
     *
     * For end() iterator we do not do anything but return the same iterator
     */
    inline ForwardIterator operator++(int) {
      if(is_end == true) {
        return *this;
      }
      
      // Make a copy of the current one before advancing
      ForwardIterator temp = *this;
      
      // Since it is forward iterator, it is sufficient
      // to set begin flag to false everytime the iterator is advanced
      // But this has to be done after saving temp
      is_begin = false;
      
      MoveAheadByOne();
      
      return temp;
    }
    
    /*
     * IsEnd() - Returns true if we have reached the end of iteration
     *
     * This is just a wrapper of the private member is_end
     */
    inline bool IsEnd() const {
      return is_end;
    }
    
    /*
     * IsBegin() - Returns true if this iterator points to the first element
     *             in the tree
     *
     * NOTE: Since the tree keeps changing potentially, this method only
     * guarantees "first element" at some time in the past, which may not
     * reflect the current state of the container
     */
    inline bool IsBegin() const {
      return is_begin;
    }
    
    /*
     * GetCurrentKey() - Returns a const pointer to current key asociated with
     *                   the value
     *
     * We need to return constant pointer to avoid caller modifying the key
     * returned by this method.
     */
    inline const RawKeyType *GetCurrentKey() {
      // OK This is not elegant
      // but the wrapper insists that
      return raw_key_p;
    }

   private:
    // We need access to the tree in order to traverse down using
    // a low key to leaf node level
    BwTree *tree_p;

    // The consolidated version of current leaf node
    // the following four members all refer to objects inside this
    // logical node
    // NOTE: If iterator is copy constructed, then we need to allocate a new
    // logical node and copy-construct the new logical leaf node using this one
    // since iterator is responsible for deallocating memory of the logical
    // leaf node, we need to prevent sharing of logical node pointers.
    // NOTE 2: However, as an optimization we move the logical leaf node
    // out of NodeSnapshot to save unnecessary memory (de)allocation
    LogicalLeafNode *logical_node_p;

    // Pointers tracking current key and value set
    const RawKeyType *raw_key_p;
    const ValueSet *value_set_p;

    // Iterators tracking current position inside the bwtree leaf node
    typename KeyValueSet::iterator key_it;
    typename ValueSet::iterator value_it;

    // The upper bound of current logical leaf node. Used to access the next
    // position (i.e. leaf node) inside bwtree
    // NOTE: We cannot use next_node_id to access next node since
    // it might become invalid since the logical node was created. The only
    // pivotal point we could rely on is the high key, which indicates a
    // lowerbound of keys we have not seen
    // NOTE 2: This has to be an object rather than pointer. The reason is that
    // after this iterator is returned to the user, the actual bwtree node
    // might be recycled by the epoch manager since thread has exited current
    // epoch. Therefore we need to copy the wrapped key from bwtree physical
    // node into the iterator
    KeyType next_key;

    // The flag indicating whether the iterator is the begin() iterator
    // This flag is set by LoadNextKey() when the function sees next_key being
    // -Inf (which implies we are scanning pages from the left most one)
    // And on each ++ or ++(int) operation, this flag is set back to false
    // since this is a forward iterator, any operation would make it
    // unqualified for begin iterator
    bool is_begin;
    
    // We use this flag to indicate whether we have reached the end of
    // iteration.
    // NOTE: We could not directly check for next_key being +Inf, since
    // there might still be keys not scanned yet even if next_key is +Inf
    // LoadNextKey() checks whether the current page has no key >= next_key
    // and the next key is +Inf for current page. If these two conditions hold
    // then we know we have reached the last key of the last page
    bool is_end;
    
    // The following two variables are used in copy construction
    // since copy construction would invaidate the key and value
    // iterator in the object being copied from for the new object,
    // we need numerical values to record current position
    
    // How many ++ operations have been performed on key-value set map
    int key_distance;
    // How many ++ operations have been performed on value set
    int value_distance;

    /*
     * LoadNextKey() - Load logical leaf page whose low key <= next_key
     *
     * NOTE: Consider the case where there are two nodes [1, 2, 3] [4, 5, 6]
     * after we have scanned [1, 2, 3] (i.e. got its logical leaf node object)
     * the node [4, 5, 6] is merged into [1, 2, 3], forming [1, 2, 3, 4, 5, 6]
     * then when we query again using the saved high key = 4, we would still
     * be reading [1, 2, 3] first, which is not consistent (duplicated values)
     *
     * To address this problem, after loading a logical node, we need to advance
     * the key iterator to locate the first key that >= next_key
     */
    void LoadNextKey() {
      // Caller needs to guarantee this function not being called if
      // we have already reached the end
      assert(is_end == false);

      // Special case: If the next key is NegInf
      // then we know this is begin() iterator
      // and the other members are either nullptr or empty
      if(next_key.IsNegInf() == true) {
        assert(logical_node_p == nullptr);
        assert(raw_key_p == nullptr);
        assert(value_set_p == nullptr);
        
        // When we load -Inf then we know it is the first time
        // we load a page, from the first page
        is_begin = true;
      } else {
        assert(is_begin == false);
      }

      // First join the epoch to prevent physical nodes being deallocated
      // too early
      EpochNode *epoch_node_p = tree_p->epoch_manager.JoinEpoch();

    load_next_key:
      Context context{next_key};

      // Traverse to the leaf page whose range includes the key
      // NOTE: We do not collect values associated with the key
      // by setting the second argument to false
      // Since we are actually just using the low key to find page
      // rather than trying to collect its values
      tree_p->Traverse(&context, false);

      // Then collect all data in the logical leaf node
      NodeSnapshot *snapshot_p = tree_p->GetLatestNodeSnapshot(&context);
      if(snapshot_p->has_data == true) {
        assert(snapshot_p->has_metadata == true);
      } else {
        tree_p->CollectAllValuesOnLeaf(snapshot_p);

        assert(snapshot_p->has_data == true);
        assert(snapshot_p->has_metadata == true);
      }

      // First move out the logical leaf node from the last snapshot
      // in path histoty
      logical_node_p = snapshot_p->MoveLogicalLeafNode();

      // It is a map interator
      key_it = logical_node_p->key_value_set.begin();
      key_distance = 0;
      
      do {
        // This way we could hit the end of key value set
        // Consider the case [1, 2, 3] [4, 5, 6], after we
        // have scanned [1, 2, 3], all keys were deleted except 1
        // then we query the tree with next key = 4. In that case
        // we cannot find a key >= 4 in the page returned
        // In this case we should proceed to the next page using 
        // the high key of current page
        if(key_it == logical_node_p->key_value_set.end()) {
          if(logical_node_p->ubound_p->IsPosInf() == true) {
            // Set the flag to direcyly indicate caller that we
            // have reached the end of iteration
            is_end = true;

            // NOTE: At this point, raw_key_p is not set
            // so we cannot compare the iterator with other
            // iterators using keys
            // We need to inspect is_end first
            return;
          } else {
            // Need to set next key as the high key of current page, since
            // in this situation, current node does not contain any key
            // that >= the next key, so we go to the next page by using high
            // key as the search key
            next_key = *logical_node_p->ubound_p;

            goto load_next_key;
          }
        }
        
        if(tree_p->KeyCmpLess(key_it->first, next_key) == true) {
          // These two should always be increamented together
          key_it++;
          key_distance++;
        } else {
          break;
        }
      } while(1);

      // It is the first value associated with the key
      value_it = key_it->second.begin();
      value_distance = 0;

      // Then prepare for next key
      // NOTE: Must copy the key, since the key needs to be preserved
      // across many query sessions
      // NOTE 2: Once we have reached the last logical leaf page, next_key
      // would be +Inf. That is the reason we could not check for next_key
      // being +Inf to decide whether end has been reached
      next_key = *logical_node_p->ubound_p;

      // Set the two shortcut pointers (they are not really needed
      // but we keep them as a shortcut)
      raw_key_p = &key_it->first.key;
      value_set_p = &key_it->second;

      // Leave the epoch, since we have already had all information
      // NOTE: Before this point we need to collect all data inside
      // shared data structure since after this point they might be
      // deallocated by other threads
      tree_p->epoch_manager.LeaveEpoch(epoch_node_p);

      return;
    }
    
    /*
     * IsLastLeafPage() - Return true if the leaf page is the last one
     *
     * We check for last page by checking whether the next key is +Inf
     * If it is true then we know we are now on the last page, and if keys
     * run out then we further know we are at the end of iteration
     */
    bool IsLastLeafPage() {
      // If the next key is +Inf then we are on the last page
      return next_key.IsPosInf();
    }
    
    /*
     * MoveAheadByOne() - Move the iterator ahead by one
     *
     * The caller is responsible for checking whether the iterator has reached
     * its end. If iterator has reached end then assertion fails.
     */
    inline void MoveAheadByOne() {
      // This invalidates the iterator as the begin iterator
      is_begin = false;
      
      value_it++;
      value_distance++;
      
      // If we have reached the last element for a given key,
      // then change to the next key (in key order)
      // and readjust value iterator to the first element
      if(value_it == key_it->second.end()) {
        // Try to increament the key iterator first, and if we
        // run out of keys we know this is the end of page
        key_it++;
        key_distance++;
        
        if(key_it == logical_node_p->key_value_set.end()) {
          // If we ran out of keys, and we are on the last page
          if(IsLastLeafPage() == true) {
            is_end = true;
            
            return;
          } else {
            // This will set key_it to the first key >= next_key
            // but not sure about key distance
            // NOTE: This function could result in reaching the end iterator
            // in that case key_it is invalid
            LoadNextKey();
            
            // If current page is not last page, but load next key
            // sets is_end flag, then we know the page after this page
            // has been merged into current page, and all keys >= next_key
            // has been deleted
            // This is an extremely complicated case, and it worth that many
            // lines of comments to make it clear
            if(is_end == true) {
              return;
            }
          } // if is last leaf page == true
        } // if key_it == end()
        
        // If we switched to the next key in current page
        // or we have loaded a new page and readjusted the key to be
        // the first element in the page, we need also to readjust the
        // value iterator to the first element for the current key
        value_it = key_it->second.begin();
        value_distance = 0;
      } // if value_it == end()
      
      return;
    }
  };

  /*
   * Interactive Debugger
   */

  /*
   * class InteractiveDebugger - Allows us to explore the tree interactively
   */
  class InteractiveDebugger {
   public:
    BwTree *tree;

    NodeID current_node_id;

    // This always points to the current top node of
    // delta chain which is exactly the node pointer
    // for current_pid
    const BaseNode *top_node_p;

    const BaseNode *current_node_p;
    NodeType current_type;

    // We use unique, order-preserving int as identity for
    // opaque key and value types
    uint64_t next_key_id;
    uint64_t next_value_id;

    // This stores mapping from key to value
    // These values are irrelevant to actual values
    // except that they preserve order
    std::map<KeyType, uint64_t, WrappedKeyComparator> key_map;
    std::unordered_map<ValueType,
                       uint64_t,
                       ValueHashFunc,
                       ValueEqualityChecker> value_map;

    // Stacks to record history position and used for backtracking
    std::stack<const BaseNode *> node_p_stack;
    std::stack<const BaseNode *> top_node_p_stack;
    std::stack<NodeID> node_id_stack;
    std::stack<bool> need_switch_stack;

    // Used as a buffer to hold keys
    std::vector<KeyType> key_list;

    // Also used as a buffer to hold PID
    std::vector<NodeID> node_id_list;

    // We catch this for debugging from the place where we put it
    Context *context_p;

    std::mutex key_map_mutex;

    /*
     * Constructor - Takes BwTree object pointer as argument
     *
     * NOTE: key_map is an std::map object which uses WrappedKeyComparator
     * to compare keys. However, WrappedKeyComparator cannot be trivially
     * constructed. As a solution, we pass comparator object to std::map
     * as construction argument to force std::map to use our
     * given constructor instead of trying to construct one itself
     */
    InteractiveDebugger(BwTree *p_tree)
        : tree(p_tree),
          current_node_id(0),
          current_node_p(nullptr),
          next_key_id(0),
          next_value_id(0),
          // The map object could take a comparator as argument
          // and use the given comparator to compare keys
          key_map{p_tree->wrapped_key_cmp_obj} {
      InitKeyMap();

      return;
    }

    /*
     * Destructor
     */
    ~InteractiveDebugger() {}

    /*
     * AddKey() - Record the key in its map such that we could sort them
     *
     * This should be called in Traverse() since all function must
     * call Traverse()
     */
    void AddKey(const KeyType &key) {
      key_map_mutex.lock();

      key_map[key] = 0;

      key_map_mutex.unlock();

      return;
    }

    /*
     * GetKeyID() - Return a unique ID for each unique key
     *
     * The ID is just tentative, and they will be sorted once all keys
     * have been collected
     */
    std::string GetKeyID(const KeyType& key) {
      // We append this to every value
      std::string type_str = \
        "(type " + std::to_string(static_cast<int>(key.type)) + ")";

      auto it = key_map.find(key);
      if (it == key_map.end()) {
        uint64_t id = next_key_id++;
        key_map[key] = id;

        // This value is usually not used
        return std::string("key-") + std::to_string(id) + type_str;
      }

      // 0 is -inf, max is +inf
      if (key_map[key] == 0) {
        return "-Inf" + type_str;
      } else if (key_map[key] == key_map.size() - 1) {
        return "+Inf" + type_str;
      } else {
        return std::string("key-") + std::to_string(key_map[key]) + type_str;
      }

      assert(false);
      return "";
    }

    /*
     * GetValueID() - Get a unique ID for each unique value
     */
    std::string GetValueID(const ValueType& value) {
      // If the value has not yet been recorded, allocate
      // an ID for it, and return the new ID
      auto it = value_map.find(value);
      if (it == value_map.end()) {
        uint64_t id = next_value_id++;
        value_map[value] = id;

        return std::string("val-") + std::to_string(id);
      }

      return std::string("val-") + std::to_string(value_map[value]);
    }

    /*
     * PrintPrompt() - Print the debugger prompt
     */
    void PrintPrompt() {
      std::cout << "[(" << NodeTypeToString(current_type)
                << ") NodeID=" << current_node_id << "]>> ";

      return;
    }

    /*
     * PrepareNode() - Save current information for switching to new node
     *
     * This function does not deal with any NodeID change
     */
    void PrepareNode(const BaseNode *node_p,
                     bool need_switch = false) {
      // Node pointer must be valid
      assert(node_p != nullptr);

      node_p_stack.push(current_node_p);

      current_node_p = node_p;
      current_type = node_p->GetType();

      need_switch_stack.push(need_switch);

      return;
    }

    /*
     * PrepareNodeByID() - Change NodeID to a new one, and switch node
     *
     * NOTE: If the node ID is invalid, this function will return false
     * This would happen if we are on the last node
     */
    bool PrepareNodeByID(NodeID node_id,
                         bool init_flag = false) {
      // If NodeId is invalid just return false
      if(node_id == INVALID_NODE_ID) {
        return false;
      }

      // If We are loading root node
      if(init_flag == true) {
        current_node_id = node_id;
        current_node_p = tree->GetNode(node_id);

        // The top pointer of a delta chain
        top_node_p = current_node_p;
        current_type = current_node_p->GetType();

        return true;
      } else {
        // Save the root of delta chain and the corresponding NodeID
        node_id_stack.push(current_node_id);
        top_node_p_stack.push(top_node_p);

        // Also need to check for each element's identity somewhere else
        assert(node_id_stack.size() == top_node_p_stack.size());

        current_node_id = node_id;

        // It calls push for node_stack and renew the type
        // We also need to switch in this case
        PrepareNode(tree->GetNode(node_id), true);

        // We only change this node when PID changes
        // NOTE: This must be called after PrepareNode() since
        // only after that current_node_p is updated
        top_node_p = current_node_p;

        return true;
      }

      assert(false);
      return false;
    }

    /*
     * NodeTypeToString() - Convert NodeType enum variable to a string
     */
    std::string NodeTypeToString(NodeType type) {
      switch(type) {
        case NodeType::LeafType:
          return "Leaf";
        case NodeType::InnerType:
          return "Inner";
        case NodeType::LeafInsertType:
          return "Leaf Insert";
        case NodeType::LeafDeleteType:
          return "Leaf Delete";
        case NodeType::LeafSplitType:
          return "Leaf Split";
        case NodeType::LeafRemoveType:
          return "Leaf Remove";
        case NodeType::LeafMergeType:
          return "LeafMerge";
        case NodeType::InnerInsertType:
          return "Inner Insert";
        case NodeType::InnerDeleteType:
          return "Inner Delete";
        case NodeType::InnerSplitType:
          return "Inner Split";
        case NodeType::InnerRemoveType:
          return "Inner Remove";
        case NodeType::InnerMergeType:
          return "InnerMerge";
        case NodeType::LeafUpdateType:
          return "LeafUpdate";
        default:
          return "Unknown Type (Error!)";
      }

      assert(false);
      return "";
    }

    /*
     * ProcessPrint() - Process print command
     *
     * print node-pointer - Prints current pointer address
     * print type - Prints current type
     */
    void ProcessPrint() {
      std::string s;
      std::cin >> s;

      if (s == "") {
        std::cout << "Nothing to print!" << std::endl;
        return;
      } else if (s == "node-pointer") {
        std::cout << current_node_p << std::endl;
      } else if (s == "type") {
        std::cout << static_cast<int>(current_type)
                  << " ("
                  << NodeTypeToString(current_type)
                  << ")"
                  << std::endl;
      } else {
        std::cout << "Unknown print argument: " << s << std::endl;
      }

      return;
    }

    /*
     * ProcessGotoChild() - Go to the child node of a delta node
     *
     * This does not work for inner and leaf nodes
     */
    void ProcessGotoChild() {
      switch (current_node_p->type) {
        case NodeType::LeafInsertType:
        case NodeType::LeafDeleteType:
        case NodeType::LeafSplitType:
        case NodeType::LeafRemoveType:
        case NodeType::LeafMergeType:
        case NodeType::InnerInsertType:
        case NodeType::InnerDeleteType:
        case NodeType::InnerSplitType:
        case NodeType::InnerRemoveType:
        case NodeType::InnerMergeType:
        case NodeType::LeafUpdateType: {
          const DeltaNode *delta_node_p = \
            static_cast<const DeltaNode *>(current_node_p);

          // Make this current node, do not refresh PID
          PrepareNode(delta_node_p->child_node_p);
          break;
        }
        case NodeType::LeafType:
        case NodeType::InnerType:
          std::cout << "Type (" << NodeTypeToString(current_type)
                    << ") does not have child node" << std::endl;
          break;
      }

      return;
    }

    /*
     * ProcessGotoSplitSibling() - Go to split sibling of a split delta node
     */
    void ProcessGotoSplitSibling() {
      NodeID sibling_node_id = INVALID_NODE_ID;

      if(current_type == NodeType::InnerSplitType) {
        const InnerSplitNode *split_node_p = \
          static_cast<const InnerSplitNode *>(current_node_p);

        sibling_node_id = split_node_p->split_sibling;
      } else if(current_type == NodeType::LeafSplitType) {
        const LeafSplitNode *split_node_p = \
          static_cast<const LeafSplitNode *>(current_node_p);

        sibling_node_id = split_node_p->split_sibling;
      } else {
        std::cout << "Type (" << NodeTypeToString(current_type)
                  << ") does not have split sibling" << std::endl;
        return;
      }

      // This should not happen, but just in case
      if(sibling_node_id == INVALID_NODE_ID) {
        std::cout << "NodeID is INVALID_NODE_ID";

        return;
      }

      PrepareNodeByID(sibling_node_id);

      return;
    }

    /*
     * ProcessGotoSibling() - Goto sibling node (only for leaf nodes)
     */
    void ProcessGotoSibling() {
      NodeID next_node_id = INVALID_NODE_ID;

      if(current_type == NodeType::InnerType) {
        const InnerNode *inner_node_p = \
          static_cast<const InnerNode *>(current_node_p);

        next_node_id = inner_node_p->next_node_id;
      } else if(current_type == NodeType::LeafType) {
        const LeafNode *leaf_node_p = \
          static_cast<const LeafNode *>(current_node_p);

        next_node_id = leaf_node_p->next_node_id;
      } else {
        std::cout << "Type (" << NodeTypeToString(current_type)
                  << ") does not have sibling node" << std::endl;
        return;
      }

      // This should not happen, but just in case
      if(next_node_id == INVALID_NODE_ID) {
        std::cout << "NodeID is INVALID_NODE_ID";

        return;
      }

      PrepareNodeByID(next_node_id);

      return;
    }

    /*
     * ProcessGotoMergeSibling() - Go to the right branch of a merge node
     */
    void ProcessGotoMergeSibling() {
      const BaseNode *node_p = nullptr;

      if(current_type == NodeType::InnerMergeType) {
        const InnerMergeNode *merge_node_p = \
          static_cast<const InnerMergeNode *>(current_node_p);

        node_p = merge_node_p->right_merge_p;
      } else if(current_type == NodeType::LeafMergeType) {
        const LeafMergeNode *merge_node_p = \
          static_cast<const LeafMergeNode *>(current_node_p);

        node_p = merge_node_p->right_merge_p;
      } else {
        std::cout << "Type (" << NodeTypeToString(current_type)
                  << ") does not have merge sibling" << std::endl;
        return;
      }

      // Physical pointer in merge delta (PID in split delta)
      PrepareNode(node_p);

      return;
    }

    /*
     * ProcessPrintSep() - Print separator for inner node
     */
    void ProcessPrintSep() {
      if (current_type != NodeType::InnerType) {
        std::cout << "Type (" << NodeTypeToString(current_type)
                  << ") does not have separator array" << std::endl;
        return;
      }

      const InnerNode *inner_node_p = \
        static_cast<const InnerNode *>(current_node_p);

      std::cout << "Number of separators: " << inner_node_p->sep_list.size()
                << std::endl;

      for (SepItem it : inner_node_p->sep_list) {
        std::cout << "[" << GetKeyID(it.key) << ", " << it.node << "]"
                  << ", ";
      }

      std::cout << std::endl;

      return;
    }

    /*
     * ProcessPrintBound() - Process high key and low key printing
     */
    void ProcessPrintBound() {
      if(current_type == NodeType::InnerType) {
        const InnerNode *inner_node_p = \
          static_cast<const InnerNode *>(current_node_p);

        std::cout << "Lower, Upper: "
                  << GetKeyID(inner_node_p->lbound)
                  << ", "
                  << GetKeyID(inner_node_p->ubound)
                  << std::endl;
      } else if(current_type == NodeType::LeafType) {
        const LeafNode *leaf_node_p = \
          static_cast<const LeafNode *>(current_node_p);

        std::cout << "Lower, Upper: "
                  << GetKeyID(leaf_node_p->lbound)
                  << ", "
                  << GetKeyID(leaf_node_p->ubound)
                  << std::endl;
      } else {
        std::cout << "Type ("
                  << NodeTypeToString(current_type)
                  << ") does not have bound key"
                  << std::endl;
      }

      return;
    }

    /*
     * ProcessPrintLeaf() - Print content of leaf page
     */
    void ProcessPrintLeaf() {
      if (current_type != NodeType::LeafType) {
        std::cout << "Type ("
                  << NodeTypeToString(current_type)
                  << ") does not have leaf array"
                  << std::endl;

        return;
      }

      const LeafNode *leaf_node_p = \
        static_cast<const LeafNode *>(current_node_p);

      std::cout << "Node size: "
                << leaf_node_p->data_list.size()
                << std::endl;

      // Iterate through keys
      for (const DataItem &it : leaf_node_p->data_list) {
        std::cout << GetKeyID(it.key)
                  << ": [";

        // Print all values in one line, separated by comma
        for (auto &it2 : it.value_list) {
          std::cout << GetValueID(it2) << ", ";
        }

        std::cout << "], " << std::endl;
      }

      return;
    }

    /*
     * ProcessPrintDelta() - Print delta node contents
     */
    void ProcessPrintDelta() {
      std::cout << "Node type: "
                << static_cast<int>(current_type)
                << " ("
                << NodeTypeToString(current_type) << ")"
                << std::endl;

      switch (current_type) {
        case NodeType::LeafType:
        case NodeType::InnerType:
        case NodeType::LeafRemoveType:
        case NodeType::InnerRemoveType: {
          std::cout << "Type ("
                    << NodeTypeToString(current_type)
                    << ") does not have record"
                    << std::endl;

          return;
        }
        case NodeType::LeafSplitType: {
          const LeafSplitNode *split_node_p = \
            static_cast<const LeafSplitNode *>(current_node_p);

          std::cout << "Separator key: "
                    << GetKeyID(split_node_p->split_key)
                    << std::endl;

          std::cout << "Sibling NodeID: "
                    << split_node_p->split_sibling
                    << std::endl;
          break;
        }
        case NodeType::InnerSplitType: {
          const InnerSplitNode *split_node_p = \
            static_cast<const InnerSplitNode *>(current_node_p);

          std::cout << "Separator key: "
                    << GetKeyID(split_node_p->split_key)
                    << std::endl;

          std::cout << "Sibling NodeID: "
                    << split_node_p->split_sibling
                    << std::endl;
          break;
        }
        case NodeType::LeafMergeType: {
          const LeafMergeNode *merge_node_p = \
            static_cast<const LeafMergeNode *>(current_node_p);

          std::cout << "Separator key: "
                    << GetKeyID(merge_node_p->merge_key)
                    << std::endl;

          break;
        }
        case NodeType::InnerMergeType: {
          const InnerMergeNode *merge_node_p = \
            static_cast<const InnerMergeNode *>(current_node_p);

          std::cout << "Separator key: "
                    << GetKeyID(merge_node_p->merge_key)
                    << std::endl;

          break;
        }
        case NodeType::LeafInsertType: {
          const LeafInsertNode *insert_node_p = \
            static_cast<const LeafInsertNode *>(current_node_p);

          std::cout << "key, value = ["
                    << GetKeyID(insert_node_p->insert_key)
                    << ", "
                    << GetValueID(insert_node_p->value)
                    << "]"
                    << std::endl;

          break;
        }
        case NodeType::LeafDeleteType: {
          const LeafDeleteNode *delete_node_p = \
            static_cast<const LeafDeleteNode*>(current_node_p);

          std::cout << "key, value = ["
                    << GetKeyID(delete_node_p->delete_key)
                    << ", "
                    << GetValueID(delete_node_p->value)
                    << "]"
                    << std::endl;

          break;
        }
        case NodeType::LeafUpdateType: {
          const LeafUpdateNode *update_node_p = \
            static_cast<const LeafUpdateNode *>(current_node_p);

          std::cout << "key, old value, new value = ["
                    << GetKeyID(update_node_p->update_key)
                    << ", "
                    << GetValueID(update_node_p->old_value)
                    << ", "
                    << GetValueID(update_node_p->new_value)
                    << "]"
                    << std::endl;

          break;
        }
        case NodeType::InnerInsertType: {
          const InnerInsertNode *insert_node_p = \
              static_cast<const InnerInsertNode *>(current_node_p);

          std::cout << "New split sep: "
                    << GetKeyID(insert_node_p->insert_key)
                    << std::endl;

          std::cout << "Next split sep: "
                    << GetKeyID(insert_node_p->next_key)
                    << std::endl;

          std::cout << "New child PID: "
                    << insert_node_p->new_node_id
                    << std::endl;

          break;
        }
        case NodeType::InnerDeleteType: {
          const InnerDeleteNode *delete_node_p =
              static_cast<const InnerDeleteNode *>(current_node_p);

          std::cout << "Deleted key: "
                    << GetKeyID(delete_node_p->delete_key)
                    << std::endl;

          std::cout << "Low key: "
                    << GetKeyID(delete_node_p->prev_key)
                    << std::endl;

          std::cout << "High key: "
                    << GetKeyID(delete_node_p->next_key)
                    << std::endl;

          break;
        }
        default:
          std::cout << "Node a delta node type: "
                    << NodeTypeToString(current_type)
                    << std::endl;

          assert(false);
          return;
      }

      // We need to print out depth for debugging
      std::cout << "Delta depth: "
                << static_cast<const DeltaNode *>(current_node_p)->depth
                << std::endl;

      return;
    }

    /*
     * ProcessBack() - Process go back command
     */
    void ProcessBack() {
      assert(node_id_stack.size() == top_node_p_stack.size());

      if (node_p_stack.size() == 0) {
        std::cout << "Already at root. Cannot go back" << std::endl;
        return;
      }

      // We know we are on top of a PID delta chain
      if (need_switch_stack.top() == true) {
        std::cout << "Return to previous PID: "
                  << node_id_stack.top()
                  << std::endl;

        top_node_p = top_node_p_stack.top();
        top_node_p_stack.pop();

        current_node_id = node_id_stack.top();
        node_id_stack.pop();
      }

      need_switch_stack.pop();

      current_node_p = node_p_stack.top();
      node_p_stack.pop();

      current_type = current_node_p->type;

      return;
    }

    /*
     * ProcessPrintPath() - Print path in context_p object
     */
    void ProcessPrintPath() {
      if(context_p == nullptr) {
        std::cout << "Context object does not exist" << std::endl;

        return;
      } else {
        std::cout << "Path list length: "
                  << context_p->path_list.size()
                  << std::endl;
        std::cout << "Root NodeID: "
                  << tree->root_id.load() << std::endl;
      }

      for(auto &snapshot : context_p->path_list) {
        std::cout << snapshot.node_id;
        std::cout << "; leftmost = "
                  << snapshot.is_leftmost_child;
        std::cout << "; is_leaf = "
                  << snapshot.is_leaf;
        std::cout << "; low key: "
                  << GetKeyID(*snapshot.lbound_p);

        std::cout << std::endl;
      }

      return;
    }

    /*
     * ProcessConsolidate() - Consolidate current node and print
     */
    void ProcessConsolidate() {
      bool is_leaf = current_node_p->IsOnLeafDeltaChain();

      NodeSnapshot snapshot{is_leaf};
      snapshot.node_p = current_node_p;

      if(is_leaf == true) {
        tree->CollectAllValuesOnLeaf(&snapshot);

        std::cout << "Value list = [";
        for(auto &item : snapshot.GetLogicalLeafNode()->GetContainer()) {
          std::cout << GetKeyID(item.first)
                    << ", ";
        }
      } else {
        tree->CollectAllSepsOnInner(&snapshot);

        std::cout << "Sep list = [";
        for(auto &item : snapshot.GetLogicalInnerNode()->GetContainer()) {
          std::cout << "("
                    << GetKeyID(item.first)
                    << ", "
                    << item.second
                    << "), ";
        }
      }

      NodeID id = snapshot.GetNextNodeID();
      std::cout << "]" << std::endl;
      std::cout << "Low key = "
                << GetKeyID(*snapshot.GetLowKey())
                << std::endl;
      std::cout << "High key = "
                << GetKeyID(*snapshot.GetHighKey())
                << std::endl;
      std::cout << "Next ID = "
                << ((id == INVALID_NODE_ID) ? "INVALID_ID" : std::to_string(id))
                << std::endl;

      return;
    }

    /*
     * InitKeyMap() - Initialize representation for +/-Inf key
     *
     * This function only makes sure that +/-Inf are in the map
     */
    void InitKeyMap() {
      GetKeyID(tree->GetNegInfKey());
      GetKeyID(tree->GetPosInfKey());

      return;
    }

    /*
     * SortKeyMap() - Sort all keys so that key ID reflects key order
     *
     * Assign order-preserve identifier to keys
     */
    void SortKeyMap() {
      uint64_t counter = 0;

      key_map_mutex.lock();

      for (auto it = key_map.begin(); it != key_map.end(); it++) {
        // -inf = 0; + inf = key_map.size() - 1
        it->second = counter++;
      }

      key_map_mutex.unlock();

      return;
    }

    void Start() {
      // We could not start with empty root node
      assert(PrepareNodeByID(tree->root_id.load(), true) == true);
      SortKeyMap();

      std::cout << "********* Interactive Debugger *********\n";

      while (1) {
        PrintPrompt();

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
          ProcessPrint();
        } else if (opcode == "print-sep") {
          // print sep-PID pairs for inner node
          ProcessPrintSep();
        } else if (opcode == "print-leaf") {
          // print key-value_list pairs for leaf node
          ProcessPrintLeaf();
        } else if (opcode == "print-bound") {
          // print lower and upper bound for leaf and inenr node
          ProcessPrintBound();
        } else if (opcode == "type") {
          std::cout << static_cast<int>(current_type)
                    << " ("
                    << NodeTypeToString(current_type)
                    << ")"
                    << std::endl;
        } else if (opcode == "goto-child") {
          // Goto child node pointed to by a physical pointer
          // in delta nodes
          ProcessGotoChild();
        } else if (opcode == "goto-split-sibling") {
          ProcessGotoSplitSibling();
        } else if (opcode == "goto-sibling") {
          ProcessGotoSibling();
        } else if (opcode == "goto-merge-sibling") {
          ProcessGotoMergeSibling();
        } else if (opcode == "print-delta") {
          // For delta nodes, print the content
          ProcessPrintDelta();
        } else if (opcode == "back") {
          // Go back to the previous position
          // (switch between PID and PID root and current node
          // is done using stacks)
          ProcessBack();
        } else if (opcode == "goto-id") {
          // Goto the first page pointed to by a PID
          NodeID target_node_id;

          std::cin >> target_node_id;
          PrepareNodeByID(target_node_id);
        } else if (opcode == "get-key-id") {
          // We push the target key into key_list
          // when we hit the assertion, and then invoke the debugger
          // then we could use its index to see the relative position of the key
          int key_index;
          std::cin >> key_index;

          if (key_index < 0 || key_index >= key_list.size()) {
            std::cout << "Key index " << key_index << " invalid!" << std::endl;
          } else {
            std::cout << GetKeyID(key_list[key_index]) << std::endl;
          }
        } else if (opcode == "get-id") {
          int node_id_index;
          std::cin >> node_id_index;

          if (node_id_index < 0 || node_id_index >= node_id_list.size()) {
            std::cout << "PID index " << node_id_index << " invalid!" << std::endl;
          } else {
            std::cout << "pid_list[" << node_id_index
                      << "] = " << node_id_list[node_id_index] << std::endl;
          }
        } else if(opcode == "get-thread-id") {
          printf("%8lX\n", std::hash<std::thread::id>()(std::this_thread::get_id()));
        } else if(opcode == "print-path") {
          ProcessPrintPath();
        } else if(opcode == "consolidate") {
          ProcessConsolidate();
        } else {
          std::cout << "Unknown command: " << opcode << std::endl;
        }
      }

      return;
    }
  };  // class Interactive Debugger



  /*
   * class EpochManager - Maintains a linked list of deleted nodes
   *                      for threads to access until all threads
   *                      entering epochs before the deletion of
   *                      nodes have exited
   */
  class EpochManager {
   public:
    // Garbage collection interval (milliseconds)
    constexpr static int GC_INTERVAL = 50;

    /*
     * struct GarbageNode - A linked list of garbages
     */
    struct GarbageNode {
      const BaseNode *node_p;

      // This does not have to be atomic, since we only
      // insert at the head of garbage list
      GarbageNode *next_p;
    };

    /*
     * struct EpochNode - A linked list of epoch node that records thread count
     *
     * This struct is also the head of garbage node linked list, which must
     * be made atomic since different worker threads will contend to insert
     * garbage into the head of the list
     */
    struct EpochNode {
      // We need this to be atomic in order to accurately
      // counte the number of threads
      std::atomic<size_t> active_thread_count;
      
      // We need this to be atomic to be able to
      // add garbage nodes without any race condition
      // i.e. GC nodes are CASed onto this pointer
      std::atomic<GarbageNode *> garbage_list_p;

      // This does not need to be atomic since it is
      // only maintained by the epoch thread
      EpochNode *next_p;
    };

    // The head pointer does not need to be atomic
    // since it is only accessed by epoch manager
    EpochNode *head_epoch_p;

    // This does not need to be atomic because it is only written
    // by the epoch manager and read by worker threads. But it is
    // acceptable that allocations are delayed to the next epoch
    EpochNode *current_epoch_p;

    // This flag will be set to true sometime after the bwtree destructor is
    // called - no synchronization is guaranteed, but it is acceptable
    // since this flag is checked by the epoch thread periodically
    // so even if it missed one, it will not miss the next
    bool exited_flag;

    std::thread *thread_p;

    // The counter that counts how many free() is called
    // inside the epoch manager
    // NOTE: We cannot precisely count the size of memory freed
    // since sizeof(Node) does not reflect the true size, since
    // some nodes are embedded with complicated data structure that
    // maintains its own memory
    #ifdef BWTREE_DEBUG
    std::atomic<size_t> freed_count;
    #endif

    /*
     * Constructor - Initialize the epoch list to be a single node
     *
     * NOTE: We do not start thread here since the init of bw-tree itself
     * might take a long time
     */
    EpochManager() {
      current_epoch_p = new EpochNode{};
      // These two are atomic variables but we could
      // simply assign to them
      current_epoch_p->active_thread_count = 0UL;
      current_epoch_p->garbage_list_p = nullptr;

      current_epoch_p->next_p = nullptr;

      head_epoch_p = current_epoch_p;

      // We allocate and run this later
      thread_p = nullptr;

      // This is used to notify the cleaner thread that it has ended
      exited_flag = false;

      // Initialize atomic counter to record how many
      // freed has been called inside epoch manager
      #ifdef BWTREE_DEBUG
      freed_count = 0UL;
      #endif

      return;
    }

    /*
     * Destructor - Stop the worker thread and cleanup resources not freed
     *
     * This function waits for the worker thread using join() method. After the
     * worker thread has exited, it synchronously clears all epochs that have
     * not been recycled by calling ClearEpoch()
     */
    ~EpochManager() {
      // Set stop flag and let thread terminate
      exited_flag = true;

      bwt_printf("Waiting for thread\n");
      thread_p->join();

      // Free memory
      delete thread_p;

      // So that in the following function the comparison
      // would always fail, until we have cleaned all epoch nodes
      current_epoch_p = nullptr;

      // If all threads has exited then all thread counts are
      // 0, and therefore this should proceed way to the end
      ClearEpoch();

      assert(head_epoch_p == nullptr);
      bwt_printf("Clean up for garbage collector\n");

      #ifdef BWTREE_DEBUG
      bwt_printf("Stat: Freed %lu nodes by epoch manager\n",
                 freed_count.load());
      #endif

      return;
    }

    /*
     * CreateNewEpoch() - Create a new epoch node
     *
     * This functions does not have to consider race conditions
     */
    void CreateNewEpoch() {
      bwt_printf("Creating new epoch...\n");

      // TODO: There is currently a bug that throws malloc: memory corruption
      // message on this statement
      EpochNode *epoch_node_p = new EpochNode{};

      epoch_node_p->active_thread_count = 0UL;
      epoch_node_p->garbage_list_p = nullptr;

      // We always append to the tail of the linked list
      // so this field for new node is always nullptr
      epoch_node_p->next_p = nullptr;

      // Update its previous node (current tail)
      current_epoch_p->next_p = epoch_node_p;

      // And then switch current epoch pointer
      current_epoch_p = epoch_node_p;

      return;
    }

    /*
     * AddGarbageNode() - Add garbage node into the current epoch
     *
     * NOTE: This function is called by worker threads so it has
     * to consider race conditions
     */
    void AddGarbageNode(const BaseNode *node_p) {
      // We need to keep a copy of current epoch node
      // in case that this pointer is increased during
      // the execution of this function
      //
      // NOTE: Current epoch must not be recycled, since
      // the current thread calling this function must
      // come from an epoch <= current epoch
      // in which case all epochs before that one should
      // remain valid
      EpochNode *epoch_p = current_epoch_p;

      // These two could be predetermined
      GarbageNode *garbage_node_p = new GarbageNode{};
      garbage_node_p->node_p = node_p;

      while(1) {
        garbage_node_p->next_p = epoch_p->garbage_list_p.load();

        // Then CAS previous node with new garbage node
        bool ret = \
          epoch_p->garbage_list_p.compare_exchange_strong(garbage_node_p->next_p,
                                                          garbage_node_p);

        // If CAS succeeds then just return
        if(ret == true) {
          break;
        } else {
          bwt_printf("Add garbage node CAS failed. Retry\n");
        }
      } // while 1

      return;
    }

    /*
     * JoinEpoch() - Let current thread join this epoch
     *
     * The effect is that all memory deallocated on and after
     * current epoch will not be freed before current thread leaves
     */
    EpochNode *JoinEpoch() {
      // We must make sure the epoch we join and the epoch we
      // return are the same one because the current point
      // could change in the middle of this function
      EpochNode *epoch_p = current_epoch_p;

      epoch_p->active_thread_count.fetch_add(1);

      return epoch_p;
    }

    /*
     * LeaveEpoch() - Leave epoch a thread has once joined
     *
     * After an epoch has been cleared all memories allocated on
     * and before that epoch could safely be deallocated
     */
    void LeaveEpoch(EpochNode *epoch_p) {
      epoch_p->active_thread_count.fetch_sub(1);

      return;
    }

    /*
     * FreeEpochDeltaChain() - Free a delta chain (used by EpochManager)
     *
     * This function differs from the one of the same name in BwTree definition
     * in the sense that for tree destruction there are certain node
     * types not being accepted. But in EpochManager we must support a wider
     * range of node types.
     *
     * For more details please refer to FreeDeltaChain in BwTree class definition
     */
    void FreeEpochDeltaChain(const BaseNode *node_p) {
      const BaseNode *next_node_p = node_p;

      while(1) {
        node_p = next_node_p;
        assert(node_p != nullptr);

        NodeType type = node_p->GetType();

        switch(type) {
          case NodeType::LeafInsertType:
            next_node_p = ((LeafInsertNode *)node_p)->child_node_p;

            delete (LeafInsertNode *)node_p;
            #ifdef BWTREE_DEBUG
            freed_count.fetch_add(1);
            #endif
            break;
          case NodeType::LeafDeleteType:
            next_node_p = ((LeafDeleteNode *)node_p)->child_node_p;

            delete (LeafDeleteNode *)node_p;
            #ifdef BWTREE_DEBUG
            freed_count.fetch_add(1);
            #endif
            break;
          case NodeType::LeafSplitType:
            next_node_p = ((LeafSplitNode *)node_p)->child_node_p;

            delete (LeafSplitNode *)node_p;
            #ifdef BWTREE_DEBUG
            freed_count.fetch_add(1);
            #endif
            break;
          case NodeType::LeafMergeType:
            FreeEpochDeltaChain(((LeafMergeNode *)node_p)->child_node_p);
            FreeEpochDeltaChain(((LeafMergeNode *)node_p)->right_merge_p);

            delete (LeafMergeNode *)node_p;
            #ifdef BWTREE_DEBUG
            freed_count.fetch_add(1);
            #endif

            // Leaf merge node is an ending node
            return;
          case NodeType::LeafRemoveType:
            delete (LeafRemoveNode *)node_p;
            #ifdef BWTREE_DEBUG
            freed_count.fetch_add(1);
            #endif

            // We never try to free those under remove node
            // since they will be freed by recursive call from
            // merge node
            //
            // TODO: Put remove node into garbage list after
            // IndexTermDeleteDelta was posted (this could only be done
            // by one thread that succeeds CAS)
            return;
          case NodeType::LeafType:
            delete (LeafNode *)node_p;
            #ifdef BWTREE_DEBUG
            freed_count.fetch_add(1);
            #endif

            // We have reached the end of delta chain
            return;
          case NodeType::InnerInsertType:
            next_node_p = ((InnerInsertNode *)node_p)->child_node_p;

            delete (InnerInsertNode *)node_p;
            #ifdef BWTREE_DEBUG
            freed_count.fetch_add(1);
            #endif
            break;
          case NodeType::InnerDeleteType:
            next_node_p = ((InnerDeleteNode *)node_p)->child_node_p;

            delete (InnerDeleteNode *)node_p;
            #ifdef BWTREE_DEBUG
            freed_count.fetch_add(1);
            #endif
            break;
          case NodeType::InnerSplitType:
            next_node_p = ((InnerSplitNode *)node_p)->child_node_p;

            delete (InnerSplitNode *)node_p;
            #ifdef BWTREE_DEBUG
            freed_count.fetch_add(1);
            #endif
            break;
          case NodeType::InnerMergeType:
            FreeEpochDeltaChain(((InnerMergeNode *)node_p)->child_node_p);
            FreeEpochDeltaChain(((InnerMergeNode *)node_p)->right_merge_p);

            delete (InnerMergeNode *)node_p;
            #ifdef BWTREE_DEBUG
            freed_count.fetch_add(1);
            #endif

            // Merge node is also an ending node
            return;
          case NodeType::InnerRemoveType:
            delete (InnerRemoveNode *)node_p;
            #ifdef BWTREE_DEBUG
            freed_count.fetch_add(1);
            #endif

            // We never free nodes under remove node
            return;
          case NodeType::InnerType:
            delete (InnerNode *)node_p;
            #ifdef BWTREE_DEBUG
            freed_count.fetch_add(1);
            #endif

            return;
          case NodeType::InnerAbortType:
            // NOTE: Deleted abort node is also appended to the
            // garbage list, to prevent other threads reading the
            // wrong type after the node has been put into the
            // list (if we delete it directly then this will be
            // a problem)
            delete (InnerAbortNode *)node_p;

            #ifdef BWTREE_DEBUG
            freed_count.fetch_add(1);
            #endif

            // Inner abort node is also a terminating node
            // so we do not delete the beneath nodes, but just return
            return;
          default:
            // This does not include INNER ABORT node
            bwt_printf("Unknown node type: %d\n", (int)type);

            assert(false);
            return;
        } // switch
      } // while 1

      return;
    }

    /*
     * ClearEpoch() - Sweep the chain of epoch and free memory
     *
     * The minimum number of epoch we must maintain is 1 which means
     * when current epoch is the head epoch we should stop scanning
     *
     * NOTE: There is no race condition in this function since it is
     * only called by the cleaner thread
     */
    void ClearEpoch() {
      bwt_printf("Start to clear epoch\n");

      while(1) {
        // Even if current_epoch_p is nullptr, this should work
        if(current_epoch_p == head_epoch_p) {
          bwt_printf("Current epoch is head epoch. Do not clean\n");

          break;
        }

        // If we have seen an epoch whose count is not zero then all
        // epochs after that are protected and we stop
        if(head_epoch_p->active_thread_count.load() != 0UL) {
          bwt_printf("Head epoch is not empty. Return\n");

          break;
        }

        // If the epoch has cleared we just loop through its garbage chain
        // and then free each delta chain

        GarbageNode *next_garbage_node_p = nullptr;

        // Walk through its garbage chain
        for(GarbageNode *garbage_node_p = head_epoch_p->garbage_list_p.load();
            garbage_node_p != nullptr;
            garbage_node_p = next_garbage_node_p) {
          FreeEpochDeltaChain(garbage_node_p->node_p);

          // Save the next pointer so that we could
          // delete current node directly
          next_garbage_node_p = garbage_node_p->next_p;

          // This invalidates any further reference to its
          // members (so we saved next pointer above)
          delete garbage_node_p;
        } // for

        // First need to save this in order to delete current node
        // safely
        EpochNode *next_epoch_node_p = head_epoch_p->next_p;
        delete head_epoch_p;

        // Then advance to the next epoch
        // It is possible that head_epoch_p becomes nullptr
        // this happens during destruction, and should not
        // cause any problem since that case we also set current epoch
        // pointer to nullptr
        head_epoch_p = next_epoch_node_p;
      } // while(1) through epoch nodes

      return;
    }

    /*
     * ThreadFunc() - The cleaner thread executes this every GC_INTERVAL ms
     *
     * This function exits when exit flag is set to true
     */
    void ThreadFunc() {
      // While the parent is still running
      // We do not worry about race condition here
      // since even if we missed one we could always
      // hit the correct value on next try
      while(exited_flag == false) {
        CreateNewEpoch();
        ClearEpoch();

        // Sleep for 50 ms
        std::chrono::milliseconds duration(GC_INTERVAL);
        std::this_thread::sleep_for(duration);
      }

      bwt_printf("exit flag is true; thread return\n");

      return;
    }

    /*
     * StartThread() - Start cleaner thread for garbage collection
     *
     * NOTE: This is not called in the constructor, and needs to be
     * called manually
     */
    void StartThread() {
      thread_p = new std::thread{[this](){this->ThreadFunc();}};

      return;
    }

  }; // Epoch manager

}; // class BwTree

#ifdef BWTREE_PELOTON
}  // End index namespace
}  // End peloton namespace
#endif

