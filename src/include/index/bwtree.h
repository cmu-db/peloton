//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// bwtree.h
//
// Identification: src/index/bwtree.h
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

#ifndef _BWTREE_H
#define _BWTREE_H

#pragma once

#include <algorithm>
#include <array>
#include <atomic>
#include <cassert>
#include <chrono>
#include <thread>
#include <unordered_set>
#include <vector>

/*
 * class PointerComparator - A template class for comparing pointer types
 *
 * If the ValueType of BwTree is pointer type, we usually do not want to
 * compare pointers directly, but instead we hope to dereference the pointer
 * and do comparison with the dereferenced type
 */
template <typename DereferencedType,
          typename DereferencedTypeComparator = std::equal_to<DereferencedType>>
class PointerComparator {
  inline bool operator()(const DereferencedType *p1, const DereferencedType *p2) {
    return DereferencedTypeComparator(*p1, *p2);
  }
};

/*
 * BWTREE_PELOTON - Specifies whether Peloton-specific features are
 *                  Compiled or not
 *                  We strive to make BwTree a standalone and independent
 *                  module that can be plugged-and-played in any situation
 */
#define BWTREE_PELOTON

/*
 * BWTREE_NODEBUG - This flag disables usage of print_flag, which greatly
 *                  reduces performance
 */
#define BWTREE_NODEBUG

#ifdef BWTREE_PELOTON

#include "index/index.h"

#endif

#include "sorted_small_set.h"
#include "bloom_filter.h"
#include "atomic_stack.h"

// We use this to control from the compiler
#ifndef BWTREE_NODEBUG
/*
 * BWTREE_DEBUG - This flag enables assertions that check for
 *                structural consistency
 *
 * REMOVING THIS FLAG FOR RELEASE
 */
#define BWTREE_DEBUG
#endif

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
                                            typename KeyHashFunc, \
                                            typename ValueEqualityChecker, \
                                            typename ValueHashFunc>


#ifdef BWTREE_PELOTON
namespace peloton {
namespace index {
#endif

// This needs to be always here
static void dummy(const char*, ...) {}

#ifdef BWTREE_DEBUG

#define bwt_printf(fmt, ...)                              \
  do {                                                    \
    if(print_flag == false) break;                        \
    fprintf(stderr, "%-24s(%8lX): " fmt, __FUNCTION__, std::hash<std::thread::id>()(std::this_thread::get_id()), ##__VA_ARGS__); \
    fflush(stdout);                                       \
  } while (0);

#else

#define bwt_printf(fmt, ...)   \
  do {                         \
    dummy(fmt, ##__VA_ARGS__); \
  } while (0);

#endif

/*
 * class DummyOutObject - Mimics std::cout interface and avoids std::cout
 *                       appearing in the source
 *
 * It is a validation requirement that std::cout should not appear in the
 * source code, and all output should use logger function
 */
class DummyOutObject {
 public:

  /*
   * operator<<() - accepts any type and chauin them up
   *
   * The template argument can be automatically deducted from the actual
   * argument, which is done by the compiler
   */
  template <typename T>
  DummyOutObject &operator<<(const T &value) {
    (void)value;

    return *this;
  }

  /*
   * operator<<() - The following three are to support std::endl()
   */
  DummyOutObject &operator<<(std::ostream& (*f)(std::ostream &)) {
    (void)f;

    return *this;
  }

  DummyOutObject &operator<<(std::ostream& (*f)(std::ios &)) {
    (void)f;

    return *this;
  }

  DummyOutObject &operator<<(std::ostream& (*f)(std::ios_base &)) {
    (void)f;

    return *this;
  }
};

/*
static DummyOutObject dummy_out;
*/

#define dummy_out std::cout

using NodeID = uint64_t;

extern bool print_flag;

#define INVALID_NODE_ID (0UL)
#define FIRST_LEAF_NODE_ID (2UL)

/*
 * class BwTree - Lock-free BwTree index implementation
 *
 * Template Arguments:
 *
 * template <typename KeyType,
 *           typename ValueType,
 *           typename KeyComparator = std::less<KeyType>,
 *           typename KeyEqualityChecker = std::equal_to<KeyType>,
 *           typename KeyHashFunc = std::hash<KeyType>,
 *           typename ValueEqualityChecker = std::equal_to<ValueType>,
 *           typename ValueHashFunc = std::hash<ValueType>>
 *
 * Explanation:
 *
 *  - KeyType: Key type of the map
 *
 *  - ValueType: Value type of the map. Note that it is possible
 *               that a single key is mapped to multiple values
 *
 *  - KeyComparator: "less than" relation comparator for KeyType
 *                   Returns true if "less than" relation holds
 *                   *** NOTE: THIS OBJECT DO NOT NEED TO HAVE A DEFAULT
 *                   CONSTRUCTOR. THIS MODIFICATION WAS MADE TO FIT
 *                   INTO Peloton's ARCHITECTURE
 *                   Please refer to main.cpp, class KeyComparator for more
 *                   information on how to define a proper key comparator
 *
 *  - KeyEqualityChecker: Equality checker for KeyType
 *                        Returns true if two keys are equal
 *
 *  - KeyHashFunc: Hashes KeyType into size_t. This is used in unordered_set
 *
 *  - ValueEqualityChecker: Equality checker for value type
 *                          Returns true for ValueTypes that are equal
 *
 *  - ValueHashFunc: Hashes ValueType into a size_t
 *                   This is used in unordered_set
 *
 * If not specified, then by default all arguments except the first two will
 * be set as the standard operator in C++ (i.e. the operator for primitive types
 * AND/OR overloaded operators for derived types)
 */
template <typename KeyType,
          typename ValueType,
          typename KeyComparator = std::less<KeyType>,
          typename KeyEqualityChecker = std::equal_to<KeyType>,
          typename KeyHashFunc = std::hash<KeyType>,
          typename ValueEqualityChecker = std::equal_to<ValueType>,
          typename ValueHashFunc = std::hash<ValueType>>
class BwTree {
 /*
  * Private & Public declaration
  */
#ifndef ALL_PUBLIC
 private:
#else
 public:
#endif
  // This does not have to be the friend class of BwTree
  class EpochManager;

 public:
  class BaseNode;
  class NodeSnapshot;

  class KeyNodeIDPairHashFunc;
  class KeyNodeIDPairEqualityChecker;
  class KeyValuePairHashFunc;
  class KeyValuePairEqualityChecker;

 /*
  * private: Basic type definition
  */
#ifndef ALL_PUBLIC
 private:
#else
 public:
#endif
  // KeyType-NodeID pair
  using KeyNodeIDPair = std::pair<KeyType, NodeID>;
  using KeyNodeIDPairSet = std::unordered_set<KeyNodeIDPair,
                                              KeyNodeIDPairHashFunc,
                                              KeyNodeIDPairEqualityChecker>;
  using KeyNodeIDPairBloomFilter = BloomFilter<KeyNodeIDPair,
                                               KeyNodeIDPairEqualityChecker,
                                               KeyNodeIDPairHashFunc>;

  // KeyType-ValueType pair
  using KeyValuePair = std::pair<KeyType, ValueType>;
  using KeyValuePairBloomFilter = BloomFilter<KeyValuePair,
                                              KeyValuePairEqualityChecker,
                                              KeyValuePairHashFunc>;

  using ValueSet = std::unordered_set<ValueType,
                                      ValueHashFunc,
                                      ValueEqualityChecker>;


  using EpochNode = typename EpochManager::EpochNode;

  // The maximum number of nodes we could map in this index
  constexpr static NodeID MAPPING_TABLE_SIZE = 1 << 20;

  // If the length of delta chain exceeds ( >= ) this then we consolidate the node
  constexpr static int INNER_DELTA_CHAIN_LENGTH_THRESHOLD = 10;
  constexpr static int LEAF_DELTA_CHAIN_LENGTH_THRESHOLD = 8;

  // If node size goes above this then we split it
  constexpr static size_t INNER_NODE_SIZE_UPPER_THRESHOLD = 128;
  constexpr static size_t INNER_NODE_SIZE_LOWER_THRESHOLD = 32;
  
  constexpr static size_t LEAF_NODE_SIZE_UPPER_THRESHOLD = 64;
  constexpr static size_t LEAF_NODE_SIZE_LOWER_THRESHOLD = 16;

  constexpr static int max_thread_count = 0x7FFFFFFF;

  // This constant represents INVALID_NODE_ID which is used as an indication
  // that the node is actually the last node on that level
  //constexpr static NodeID INVALID_NODE_ID = 0;

  // The NodeID for the first leaf is fixed, which is 2
  //constexpr static NodeID FIRST_LEAF_NODE_ID = 2;

  /*
   * enum class NodeType - Bw-Tree node type
   */
  enum class NodeType {
    LeafStart = 0,
    // Data page type
    LeafType = 1,

    // Only valid for leaf
    LeafInsertType,
    LeafSplitType,
    LeafDeleteType,
    LeafRemoveType,
    LeafMergeType,

    // This serves as sentinel
    LeafEnd,

    // We separate leaf and inner into two different intervals
    // to make it possible for compiler to optimize

    InnerStart,

    InnerType,

    // Only valid for inner
    InnerInsertType,
    InnerSplitType,
    InnerDeleteType,
    InnerRemoveType,
    InnerMergeType,
    InnerAbortType, // Unconditional abort

    InnerEnd,
  };

  ///////////////////////////////////////////////////////////////////
  // Comparator, equality checker and hasher for key-NodeID pair
  ///////////////////////////////////////////////////////////////////

  /*
   * class KeyNodeIDPairComparator - Compares key-value pair for < relation
   *
   * Only key values are compares. However, we should use WrappedKeyComparator
   * instead of the raw one, since there could be -Inf involved in inner nodes
   */
  class KeyNodeIDPairComparator {
   public:
    const KeyComparator *key_cmp_obj_p;

    /*
     * Default constructor - deleted
     */
    KeyNodeIDPairComparator() = delete;

    /*
     * Constructor - Initialize a key-NodeID pair comparator using
     *               wrapped key comparator
     */
    KeyNodeIDPairComparator(BwTree *p_tree_p) :
      key_cmp_obj_p{&p_tree_p->key_cmp_obj}
    {}

    /*
     * operator() - Compares whether a key NodeID pair is less than another
     *
     * We only compare keys since there should not be duplicated
     * keys inside an inner node
     */
    inline bool operator()(const KeyNodeIDPair &knp1,
                           const KeyNodeIDPair &knp2) const {
      // First compare keys for relation
      return (*key_cmp_obj_p)(knp1.first, knp2.first);
    }
  };

  /*
   * class KeyNodeIDPairEqualityChecker - Checks KeyNodeIDPair equality
   *
   * Only keys are checked since there should not be duplicated keys inside
   * inner nodes. However we should always use wrapped key eq checker rather
   * than wrapped raw key eq checker
   */
  class KeyNodeIDPairEqualityChecker {
   public:
    const KeyEqualityChecker *key_eq_obj_p;

    /*
     * Default constructor - deleted
     */
    KeyNodeIDPairEqualityChecker() = delete;

    /*
     * Constructor - Initialize a key node pair eq checker
     */
    KeyNodeIDPairEqualityChecker(BwTree *p_tree_p) :
      key_eq_obj_p{&p_tree_p->key_eq_obj}
    {}

    /*
     * operator() - Compares key-NodeID pair by comparing keys
     */
    inline bool operator()(const KeyNodeIDPair &knp1,
                           const KeyNodeIDPair &knp2) const {
      return (*key_eq_obj_p)(knp1.first, knp2.first);
    }
  };

  /*
   * class KeyNodeIDPairHashFunc - Hashes a key-NodeID pair into size_t
   */
  class KeyNodeIDPairHashFunc {
   public:
    const KeyHashFunc *key_hash_obj_p;

    /*
     * Default constructor - deleted
     */
    KeyNodeIDPairHashFunc() = delete;

    /*
     * Constructor - Initialize a key value pair hash function
     */
    KeyNodeIDPairHashFunc(BwTree *p_tree_p) :
      key_hash_obj_p{&p_tree_p->key_hash_obj}
    {}

    /*
     * operator() - Hashes a key-value pair by hashing each part and
     *              combine them into one size_t
     *
     * We use XOR to combine hashes of the key and value together into one
     * single hash value
     */
    inline size_t operator()(const KeyNodeIDPair &knp) const {
      return (*key_hash_obj_p)(knp.first);
    }
  };

  ///////////////////////////////////////////////////////////////////
  // Equality checker and hasher for key-value pair
  ///////////////////////////////////////////////////////////////////

  /*
   * class KeyValuePairEqualityChecker - Checks KeyValuePair equality
   */
  class KeyValuePairEqualityChecker {
   public:
    const KeyEqualityChecker *key_eq_obj_p;
    const ValueEqualityChecker *value_eq_obj_p;

    /*
     * Default constructor - deleted
     */
    KeyValuePairEqualityChecker() = delete;

    /*
     * Constructor - Initialize a key value pair equality checker with
     *               WrappedKeyEqualityChecker and ValueEqualityChecker
     */
    KeyValuePairEqualityChecker(BwTree *p_tree_p) :
      key_eq_obj_p{&p_tree_p->key_eq_obj},
      value_eq_obj_p{&p_tree_p->value_eq_obj}
    {}

    /*
     * operator() - Compares key-value pair by comparing each component
     *              of them
     *
     * NOTE: This function only compares keys with KeyType. For +/-Inf
     * the wrapped raw key comparator will fail
     */
    inline bool operator()(const KeyValuePair &kvp1,
                           const KeyValuePair &kvp2) const {
      return ((*key_eq_obj_p)(kvp1.first, kvp2.first)) && \
             ((*value_eq_obj_p)(kvp1.second, kvp2.second));
    }
  };

  /*
   * class KeyValuePairHashFunc - Hashes a key-value pair into size_t
   *
   * This is used as the hash function of unordered_map
   */
  class KeyValuePairHashFunc {
   public:
    const KeyHashFunc *key_hash_obj_p;
    const ValueHashFunc *value_hash_obj_p;

    /*
     * Default constructor - deleted
     */
    KeyValuePairHashFunc() = delete;

    /*
     * Constructor - Initialize a key value pair hash function
     */
    KeyValuePairHashFunc(BwTree *p_tree_p) :
      key_hash_obj_p{&p_tree_p->key_hash_obj},
      value_hash_obj_p{&p_tree_p->value_hash_obj}
    {}

    /*
     * operator() - Hashes a key-value pair by hashing each part and
     *              combine them into one size_t
     *
     * We use XOR to combine hashes of the key and value together into one
     * single hash value
     */
    inline size_t operator()(const KeyValuePair &kvp) const {
      return ((*key_hash_obj_p)(kvp.first)) ^ \
             ((*value_hash_obj_p)(kvp.second));
    }
  };

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

  /*
   * class Context - Stores per thread context data that is used during
   *                 tree traversal
   *
   * NOTE: For each thread there could be only 1 instance of this object
   * so we forbid copy construction and assignment and move
   */
  class Context {
   public:
    // We choose to keep the search key as a member rather than pointer
    // inside the context object
    const KeyType search_key;

    // We only need to keep current snapshot and parent snapshot
    NodeSnapshot current_snapshot;
    NodeSnapshot parent_snapshot;

    // Counts abort in one traversal
    int abort_counter;

    // Represents current level we are on the tree
    // root is level 0
    // On initialization this is set to -1
    int current_level;

    // Whether to abort current traversal, and start a new one
    // after seeing this flag, all function should return without
    // any further action, and let the main driver to perform abort
    // and restart
    // NOTE: Only the state machine driver could abort
    // and other functions just return on seeing this flag
    bool abort_flag;

    /*
     * Constructor - Initialize a context object into initial state
     */
    Context(const KeyType p_search_key) :
      #ifdef BWTREE_PELOTON

      // Because earlier versions of g++ does not support
      // initializer list so must use () form
      search_key(p_search_key),

      #else

      search_key{p_search_key},

      #endif
      abort_counter{0},
      current_level{-1},
      abort_flag{false}
    {}

    /*
     * Destructor - Cleanup
     */
    ~Context() {}

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

    /*
     * HasParentNode() - Returns whether the current node (top of path list)
     *                   has a parent node
     */
    inline bool HasParentNode() const {
      return current_level >= 1;
    }
  };

  /*
   * class NodeMetaData - Holds node metadata in an object
   *
   * Node metadata includes a pointer to the range object, the depth
   * of the current delta chain (NOTE: If there is a merge node then the
   * depth is the sum of the length of its two children rather than
   * the larger one)
   *
   * Since we need to query for high key and low key in every step of
   * traversal down the tree (i.e. on each level we need to verify we
   * are on the correct node). It would be wasteful if we traverse down the
   * delta chain down to the bottom everytime to collect these metadata
   * therefore as an optimization we store them inside each delta node
   * and leaf/inner node to optimize for performance
   *
   * NOTE: We do not count node type as node metadata
   */
  class NodeMetaData {
   public:
    // For inner nodes, low key always points to the KeyNodeIDPair inside the
    // InnerNode
    // For LeafInsert and LeafDelete nodes, this field is set to the index for
    // the change that will be applied to on LeafNode data_list
    // For other leaf nodes this field is set to nullptr and has no special
    // meaning
    union LowKeyOrIndex{
      const KeyNodeIDPair *low_key_p;
      
      std::pair<int, bool> index_pair;
      
      /*
       * Constructor - We always initialize the first element
       *               even if it is a BaseNode *
       */
      LowKeyOrIndex(const KeyNodeIDPair *p_low_key_p) :
        low_key_p{p_low_key_p}
      {}
      
      LowKeyOrIndex(std::pair<int, bool> p_index_pair) :
        index_pair{p_index_pair}
      {}
    } low_key_or_index;

    // For inner nodes, high key points to the first element inside its
    // separator list if there is neither InnerSplitNode nor InnerMergeNode
    // However if there is one then the pointer points to the item inside
    // InnerSplitNode or InnerMergeNode
    // For leaf nodes, high key points to the KeyNodeIDPair inside the LeafNode
    // if there is neither LeafSplitNode nor LeafMergeNode. Otherwise it
    // points to the item inside split node or merge node
    const KeyNodeIDPair *high_key_p;

    // This is the depth of current delta chain
    // For merge delta node, the depth of the delta chain is the
    // sum of its two children
    int depth;

    // This counts the number of items alive inside the Node
    // when consolidating nodes, we use this piece of information
    // to reserve space for the new node
    int item_count;

    /*
     * Constructor
     *
     * NOTE: This constructor uses template type auto deduction which
     * allows us to assign to the union without having to write two
     * different constructors (the compiler will synthesize for us)
     */
    template <typename T>
    NodeMetaData(T p_low_key_or_index,
                 const KeyNodeIDPair *p_high_key_p,
                 int p_depth,
                 int p_item_count) :
      low_key_or_index{p_low_key_or_index},
      high_key_p{p_high_key_p},
      depth{p_depth},
      item_count{p_item_count}
    {}
  };

  /*
   * class BaseNode - Generic node class; inherited by leaf, inner
   *                  and delta node
   */
  class BaseNode {
   // We hold its data structure as private to force using member functions
   // for member access
   private:
    NodeType type;

    // This holds low key, high key, and next node ID
    NodeMetaData metadata;
   public:

    /*
     * Constructor - Initialize type and metadata
     */
    template <typename T>
    BaseNode(NodeType p_type,
             T p_low_key_or_index,
             const KeyNodeIDPair *p_high_key_p,
             int p_depth,
             int p_item_count) :
      type{p_type},
      metadata{p_low_key_or_index, p_high_key_p, p_depth, p_item_count}
    {}

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
     * GetNodeMetaData() - Returns a const reference to node metadata
     *
     * Please do not override this method
     */
    inline const NodeMetaData &GetNodeMetaData() const {
      return metadata;
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
     * IsInnerNode() - Returns true if the node is an inner node
     *
     * This is useful if we want to collect all seps on an inner node
     * If the top of delta chain is an inner node then just do not collect
     * and use the node directly
     */
    inline bool IsInnerNode() const {
      return type == NodeType::InnerType;
    }

    /*
     * IsRemoveNode() - Returns true if the node is of inner/leaf remove type
     *
     * This is used in JumpToLeftSibling() as an assertion
     */
    inline bool IsRemoveNode() const {
      return (type == NodeType::InnerRemoveType) || \
             (type == NodeType::LeafRemoveType);
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
     *
     * This function makes use of the fact that leaf types occupy a
     * continuous region of NodeType numerical space, so that we could
     * the identity of leaf or Inner using only one comparison
     *
     */
    inline bool IsOnLeafDeltaChain() const {
      return type < NodeType::LeafEnd;
    }

    /*
     * GetLowKey() - Returns the low key of the current base node
     *
     * NOTE: Since it is defined that for LeafNode the low key is undefined
     * and pointers should be set to nullptr, accessing the low key of
     * a leaf node would result in Segmentation Fault
     */
    inline const KeyType &GetLowKey() const {
      // Make sure this is not called on leaf nodes
      assert(IsOnLeafDeltaChain() == false);

      return metadata.low_key_or_index.low_key_p->first;
    }

    /*
     * GetHighKey() - Returns a reference to the high key of current node
     *
     * This function could be called for all node types including leaf nodes
     * and inner nodes.
     */
    inline const KeyType &GetHighKey() const {
      return metadata.high_key_p->first;
    }

    /*
     * GetHighKeyPair() - Returns the pointer to high key node id pair
     */
    inline const KeyNodeIDPair &GetHighKeyPair() const {
      return *metadata.high_key_p;
    }

    /*
     * GetLowKeyPair() - Returns the pointer to low key node id pair
     *
     * The return value is nullptr for LeafNode and its delta chain
     */
    inline const KeyNodeIDPair &GetLowKeyPair() const {
      // Make sure this is not called on leaf nodes
      assert(IsOnLeafDeltaChain() == false);

      return *metadata.low_key_or_index.low_key_p;
    }
    
    

    /*
     * GetNextNodeID() - Returns the next NodeID of the current node
     */
    inline NodeID GetNextNodeID() const {
      return metadata.high_key_p->second;
    }

    /*
     * GetLowKeyNodeID() - Returns the NodeID for low key
     *
     * Since low key is not defined for LeafNode and its delta chian
     * this must not be called on leafNode and its delta chain
     */
    inline NodeID GetLowKeyNodeID() const {
      assert(IsOnLeafDeltaChain() == false);

      return metadata.low_key_or_index.low_key_p->second;
    }

    /*
     * GetDepth() - Returns the depth of the current node
     */
    inline int GetDepth() const {
      return metadata.depth;
    }

    /*
     * GetItemCount() - Returns the item count of the current node
     */
    inline int GetItemCount() const {
      return metadata.item_count;
    }

    /*
     * SetLowKeyPair() - Sets the low key pair of metadata
     *
     * This function could only be called by InnerNodes and its delta chain
     * node since for LeafNodes their low key is always nullptr on initilization
     * and could not be reset
     */
    inline void SetLowKeyPair(const KeyNodeIDPair *p_low_key_p) {
      assert(IsOnLeafDeltaChain() == false);

      metadata.low_key_or_index.low_key_p = p_low_key_p;

      return;
    }
    
    /*
     * GetIndexPair() - Returns the index-flag pair inside metadata
     *
     * This function must be called only on LeafInsertNode or LeafDeleteNode
     * On other node types it would fail
     */
    inline std::pair<int, bool> GetIndexPair() const {
      assert(GetType() == NodeType::LeafInsertType ||
             GetType() == NodeType::LeafDeleteType);
             
      return metadata.low_key_or_index.index_pair;
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
    const BaseNode *child_node_p;

    /*
     * Constructor
     */
    template <typename T>
    DeltaNode(NodeType p_type,
              const BaseNode *p_child_node_p,
              T p_low_key_or_index,
              const KeyNodeIDPair *p_high_key_p,
              int p_depth,
              int p_item_count) :
      BaseNode{p_type,
               p_low_key_or_index,
               p_high_key_p,
               p_depth,
               p_item_count},
      child_node_p{p_child_node_p}
    {}
  };

  /*
   * class InnerDataNode - Base class for InnerInsertNode and InnerDeleteNode
   *
   * We need this node since we want to sort pointers to such nodes using
   * stable sorting algorithm
   */
  class InnerDataNode : public DeltaNode {
   public:
    KeyNodeIDPair item;

    InnerDataNode(const KeyNodeIDPair &p_item,
                  NodeType p_type,
                  const BaseNode *p_child_node_p,
                  const KeyNodeIDPair *p_low_key_p,
                  const KeyNodeIDPair *p_high_key_p,
                  int p_depth,
                  int p_item_count) :
      DeltaNode{p_type,
                p_child_node_p,
                p_low_key_p,
                p_high_key_p,
                p_depth,
                p_item_count},
      item{p_item}
    {}
  };
  
  /*
   * class LeafDataNode - Holds LeafInsertNode and LeafDeleteNode's data
   *
   * This class is used in node consolidation to provide a uniform
   * interface for the log-structured merge process
   */
  class LeafDataNode : public DeltaNode {
   public:
    KeyValuePair item;

    template <typename T>
    LeafDataNode(const KeyValuePair &p_item,
                 NodeType p_type,
                 const BaseNode *p_child_node_p,
                 T p_low_key_or_index,
                 const KeyNodeIDPair *p_high_key_p,
                 int p_depth,
                 int p_item_count) :
      DeltaNode{p_type,
                p_child_node_p,
                p_low_key_or_index,
                p_high_key_p,
                p_depth,
                p_item_count},
      item{p_item}
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
    // We always hold data within a vector of KeyValuePair
    std::vector<KeyValuePair> data_list;

    // This holds high key and the next node ID inside a pair
    // so that they could be accessed in a compacted manner
    KeyNodeIDPair high_key;

    /*
     * Constructor - Initialize bounds and next node ID
     *
     * NOTE: Since low key is not defined for LeafNode and its delta chain,
     * the low key field of LeafNode and its delta chain is always nullptr
     *
     * NOTE 2: For leaf pages we could first assign a high key and them push
     * items into the vector, since the high key does not come from vector
     * items unlike the InnerNode
     */
    LeafNode(const KeyNodeIDPair &p_high_key_p, int p_item_count) :
      BaseNode{NodeType::LeafType,
               nullptr,         // Low key is not defined for leaf node
               &high_key,       // The high key is stored inside leaf node
               0,               // Depth of the node - always 0
               p_item_count},
      high_key{p_high_key_p}
    {}

    /*
     * FindSplitPoint() - Find the split point that could divide the node
     *                    into two even siblings
     *
     * If such point does not exist then we manage to find a point that
     * divides the node into two halves that are as even as possible (i.e.
     * make the size difference as small as possible)
     *
     * This function works by first finding the key on the exact central
     * position, after which it scans forward to find a KeyValuePair
     * with a different key. If this fails then it scans backwards to find
     * a KeyValuePair with a different key.
     *
     * NOTE: If both split points would make an uneven division with one of
     * the node size below the merge threshold, then we do not split,
     * and return -1 instead. Otherwise the index of the spliting point
     * is returned
     */
    int FindSplitPoint(const BwTree *t) const {
      int central_index = static_cast<int>(data_list.size()) / 2;
      assert(central_index > 1);

      // This will used as upper_bound and lower_bound key
      const KeyValuePair &central_kvp = data_list[central_index];

      // Move it to the element before data_list
      auto it = data_list.begin() + central_index - 1;

      // If iterator has reached the begin then we know there could not
      // be any split points
      while((it != data_list.begin()) && \
            (t->KeyCmpEqual(it->first, central_kvp.first) == true)) {
        it--;
      }

      // This is the real split point
      it++;

      // This size is exactly the index of the split point
      int left_sibling_size = std::distance(data_list.begin(), it);

      if(left_sibling_size > static_cast<int>(LEAF_NODE_SIZE_LOWER_THRESHOLD)) {
        return left_sibling_size;
      }

      // Move it to the element after data_list
      it = data_list.begin() + central_index + 1;

      // If iterator has reached the end then we know there could not
      // be any split points
      while((it != data_list.end()) && \
            (t->KeyCmpEqual(it->first, central_kvp.first) == true)) {
        it++;
      }

      int right_sibling_size = std::distance(it, data_list.end());

      if(right_sibling_size > static_cast<int>(LEAF_NODE_SIZE_LOWER_THRESHOLD)) {
        return std::distance(data_list.begin(), it);
      }

      return -1;
    }

    /*
     * GetSplitSibling() - Split the node into two halves
     *
     * Although key-values are stored as independent pairs, we always split
     * on the point such that no keys are separated on two leaf nodes
     * This decision is made to make searching easier since now we could
     * just do a binary search on the base page to decide whether a
     * given key-value pair exists or not
     *
     * This function splits a leaf node into halves based on key rather
     * than items. This implies the number of keys would be even, but no
     * guarantee is made about the number of key-value items, which might
     * be very unbalanced, cauing node size varing much.
     *
     * NOTE: This function allocates memory, and if it is not used
     * e.g. by a CAS failure, then caller needs to delete it
     *
     * NOTE 2: Split key is stored in the low key of the new leaf node
     *
     * NOTE 3: This function assumes no out-of-bound key, i.e. all keys
     * stored in the leaf node are < high key. This is valid since we
     * already filtered out those key >= high key in consolidation.
     *
     * NOTE 4: On failure of split (i.e. could not find a split key that evenly
     * or almost evenly divide the leaf node) then the return value of this
     * function is nullptr
     */
    LeafNode *GetSplitSibling(const BwTree *t) const {
      // When we split a leaf node, it is certain that there is no delta
      // chain on top of it. As a result, the number of items must equal
      // the actual size of the data list
      assert(static_cast<int>(data_list.size()) == this->GetItemCount());

      // This is the index of the actual key-value pair in data_list
      // We need to substract this value from the prefix sum in the new
      // inner node
      int split_item_index = FindSplitPoint(t);
      
      // Could not split because we could not find a split point
      // and the caller is responsible for not spliting the node
      // This should happen relative infrequently, in a sense that
      // oversized page would affect performance
      if(split_item_index == -1) {
        return nullptr;
      }

      // This is an iterator pointing to the split point in the vector
      // note that std::advance() operates efficiently on std::vector's
      // RandomAccessIterator
      auto copy_start_it = data_list.begin() + split_item_index;

      // This is the end point for later copy of data
      auto copy_end_it = data_list.end();

      // This is the key part of the key-value pair, also the low key
      // of the new node and new high key of the current node (will be
      // reflected in split delta later in its caller)
      //const KeyType &split_key = copy_start_it->first;

      // This will call SetMetaData inside its constructor
      LeafNode *leaf_node_p = \
        new LeafNode{this->GetHighKeyPair(),
                     static_cast<int>(std::distance(copy_start_it,
                                                    copy_end_it))};

      // Copy data item into the new node using batch assign()
      leaf_node_p->data_list.assign(copy_start_it, copy_end_it);

      return leaf_node_p;
    }
  };

  /*
   * class LeafInsertNode - Insert record into a leaf node
   */
  class LeafInsertNode : public LeafDataNode {
   public:

    /*
     * Constructor
     */
    LeafInsertNode(const KeyType &p_insert_key,
                   const ValueType &p_value,
                   const BaseNode *p_child_node_p,
                   std::pair<int, bool> p_index_pair) :
      LeafDataNode{std::make_pair(p_insert_key, p_value),
                   NodeType::LeafInsertType,
                   p_child_node_p,
                   p_index_pair,
                   &p_child_node_p->GetHighKeyPair(),
                   p_child_node_p->GetDepth() + 1,
                   // For insert nodes, the item count is inheried from the child
                   // node + 1 since it inserts new item
                   p_child_node_p->GetItemCount() + 1}
    {}
  };

  /*
   * class LeafDeleteNode - Delete record from a leaf node
   *
   * In multi-value mode, it takes a value to identify which value
   * to delete. In single value mode, value is redundant but what we
   * could use for sanity check
   */
  class LeafDeleteNode : public LeafDataNode {
   public:

    /*
     * Constructor
     */
    LeafDeleteNode(const KeyType &p_delete_key,
                   const ValueType &p_value,
                   const BaseNode *p_child_node_p,
                   std::pair<int, bool> p_index_pair) :
      LeafDataNode{std::make_pair(p_delete_key, p_value),
                   NodeType::LeafDeleteType,
                   p_child_node_p,
                   p_index_pair,
                   &p_child_node_p->GetHighKeyPair(),
                   p_child_node_p->GetDepth() + 1,
                   // For delete node it inherits item count from its child
                   // and - 1 from it since one element was deleted
                   p_child_node_p->GetItemCount() - 1}
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
    // Split key is the first element and the sibling NodeID
    // is the second element
    // This also specifies a new high key pair for the leaf
    // delta chain
    KeyNodeIDPair insert_item;

    /*
     * Constructor
     *
     * NOTE: The constructor requires that the physical pointer to the split
     * sibling being passed as an argument. It will not be stored inside the
     * split delta, but it will be used to compute the new item count for
     * the current node
     */
    LeafSplitNode(const KeyNodeIDPair &p_insert_item,
                  const BaseNode *p_child_node_p,
                  const BaseNode *p_split_node_p) :
      DeltaNode{NodeType::LeafSplitType,
                p_child_node_p,
                nullptr,
                // High key is redirected to the split item inside the node
                &insert_item,
                // NOTE: split node is SMO and does not introduce
                // new piece of data
                // So we set its depth the same as its child
                p_child_node_p->GetDepth(),
                // For split node it is a little bit tricky - we must
                // know the item count of its sibling to decide how many
                // items were removed by the split delta
                p_child_node_p->GetItemCount() - \
                  p_split_node_p->GetItemCount()},
      insert_item{p_insert_item}
    {}
  };

  /*
   * class LeafRemoveNode - Remove all physical children and redirect
   *                        all access to its logical left sibling
   *
   * The removed ID field is not used in SMO protocol, but it helps
   * EpochManager to recycle NodeID correctly, since when a LeafRemoveNode
   * is being recycled, the removed NodeID is also recycled
   */
  class LeafRemoveNode : public DeltaNode {
   public:
    // This is used for EpochManager to recycle NodeID
    NodeID removed_id;

    /*
     * Constructor
     */
    LeafRemoveNode(NodeID p_removed_id,
                   const BaseNode *p_child_node_p) :
      DeltaNode{NodeType::LeafRemoveType,
                p_child_node_p,
                nullptr,
                &p_child_node_p->GetHighKeyPair(),
                // REMOVE node is an SMO and does not introduce data
                p_child_node_p->GetDepth(),
                p_child_node_p->GetItemCount()},
      removed_id{p_removed_id}
    {}
  };

  /*
   * class LeafMergeNode - Merge two delta chian structure into one node
   *
   * This structure uses two physical pointers to indicate that the right
   * half has become part of the current node and there is no other way
   * to access it
   *
   * NOTE: Merge node also contains the NodeID of the node being removed
   * as the result of the merge operation. We keep it here to simplify
   * NodeID searching in the parent node
   */
  class LeafMergeNode : public DeltaNode {
   public:
    KeyNodeIDPair delete_item;

    // For merge nodes we use actual physical pointer
    // to indicate that the right half is already part
    // of the logical node
    const BaseNode *right_merge_p;

    /*
     * Constructor
     */
    LeafMergeNode(const KeyType &p_merge_key,
                  const BaseNode *p_right_merge_p,
                  NodeID p_deleted_node_id,
                  const BaseNode *p_child_node_p) :
      DeltaNode{NodeType::LeafMergeType,
                p_child_node_p,
                nullptr,
                // The high key of the merge node is inherited
                // from the right sibling
                &p_right_merge_p->GetHighKeyPair(),
                //std::max(p_child_node_p->metadata.depth,
                //         p_right_merge_p->metadata.depth) + 1,
                p_child_node_p->GetDepth() + \
                  p_right_merge_p->GetDepth(),
                // For merge node the item count should be the
                // sum of items inside both branches
                p_child_node_p->GetItemCount() + \
                  p_right_merge_p->GetItemCount()},
      delete_item{p_merge_key, p_deleted_node_id},
      right_merge_p{p_right_merge_p}
    {}
  };

  ///////////////////////////////////////////////////////////////////
  // Leaf Delta Chain Node Type End
  ///////////////////////////////////////////////////////////////////

  ///////////////////////////////////////////////////////////////////
  // Inner Delta Chain Node Type
  ///////////////////////////////////////////////////////////////////

  /*
   * class InnerNode - Inner node that holds separators
   */
  class InnerNode : public BaseNode {
   public:
    // The vector stores the separators, with the first element being the
    // low key-NodeID pair (low key is not used)
    std::vector<KeyNodeIDPair> sep_list;

    // This stores the high key of the node
    KeyNodeIDPair high_key;

    /*
     * Constructor
     */
    InnerNode(const KeyNodeIDPair &p_high_key_p,
              int p_item_count,
              int p_depth = 0) :
      BaseNode{NodeType::InnerType,
               nullptr,        // Low key should be initialized after reserve()
               &high_key,      // High key is stored inside InnerNode
               p_depth,        // Depth of InnerNode defaults to 0
               p_item_count},  // We use this to reserve storage
      sep_list{},              // This will be initialized later
      high_key{p_high_key_p} {
      // First reserve that much space (it should not be changed once set)
      // The first element is the low key, and we maintain a pointer to it
      // So it is important that the vector does not reallocate its internal
      // memory segment otherwise the pointer to low key is invalidated
      sep_list.reserve(p_item_count);

      // Then direct high key and low key pointer to the first and last element
      // NOTE: Once the vector reallocates its memory, this pointer is
      // invalidated. So we should avoid that
      // NOTE 2: Should use "this" pointer to call the low key pair
      this->SetLowKeyPair(&sep_list[0]);

      return;
    }

    /*
     * GetSplitSibling() - Split InnerNode into two halves.
     *
     * This function does not change the current node since all existing nodes
     * should be read-only to avoid data race. It copies half of the inner node
     * into the split sibling, and return the sibling node.
     */
    InnerNode *GetSplitSibling() const {
      int key_num = static_cast<int>(sep_list.size());

      // Inner node size must be > 2 to avoid empty split node
      assert(key_num >= 2);

      // Same reason as in leaf node - since we only split inner node
      // without a delta chain on top of it, the sep list size must equal
      // the recorded item count
      assert(static_cast<int>(sep_list.size()) == this->GetItemCount());

      int split_item_index = key_num / 2;

      // This is the split point of the inner node
      auto copy_start_it = sep_list.begin();
      std::advance(copy_start_it, split_item_index);

      // We copy key-NodeID pairs till the end of the inner node
      auto copy_end_it = sep_list.end();

      // This sets metddata inside BaseNode by calling SetMetaData()
      // inside inner node constructor
      InnerNode *inner_node_p = \
        new InnerNode{this->GetHighKeyPair(), // It will be copied into new node
                      static_cast<int>(std::distance(copy_start_it,
                                                     copy_end_it))};

      // Batch copy from the current node to the new node
      // It does not cause reallocation
      inner_node_p->sep_list.assign(copy_start_it, copy_end_it);

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
  class InnerInsertNode : public InnerDataNode {
   public:
    // This is the next right after the item being inserted
    // This could be set to the +Inf high key
    // in that case next_item.second == INVALID_NODE_ID
    KeyNodeIDPair next_item;

    /*
     * Constructor
     */
    InnerInsertNode(const KeyNodeIDPair &p_insert_item,
                    const KeyNodeIDPair &p_next_item,
                    const BaseNode *p_child_node_p) :
      InnerDataNode{p_insert_item,
                    NodeType::InnerInsertType,
                    p_child_node_p,
                    &p_child_node_p->GetLowKeyPair(),
                    &p_child_node_p->GetHighKeyPair(),
                    p_child_node_p->GetDepth() + 1,
                    p_child_node_p->GetItemCount() + 1},
      next_item{p_next_item}
    {}
  };

  /*
   * class InnerDeleteNode - Delete node
   *
   * NOTE: There are three keys associated with this node, two of them
   * defining the new range after deleting this node, the remaining one
   * describing the key being deleted
   *
   * NOTE 2: We also store the InnerDeleteNode in BwTree to facilitate
   * tree destructor to avoid traversing NodeID that has already been
   * deleted and gatbage collected
   */
  class InnerDeleteNode : public InnerDataNode {
   public:
    KeyNodeIDPair delete_item;

    // This holds the previous key-NodeID item in the inner node
    // if the NodeID matches the low key of the inner node (which
    // should be a constant and kept inside each node on the delta chain)
    // then do not need to compare to it since the search key must >= low key
    // But if the prev_item.second != low key node id then need to compare key
    KeyNodeIDPair prev_item;

    // This holds the next key-NodeID item in the inner node
    // If the NodeID inside next_item is INVALID_NODE_ID then we do not have
    // to conduct any comparison since we know it is the high key
    KeyNodeIDPair next_item;

    /*
     * Constructor
     *
     * NOTE: We need to provide three keys, two for defining a new
     * range, and one for removing the index term from base node
     *
     * NOTE 2: We also needs to provide the key being deleted, though
     * it does make sense for tree traversal. But when the tree destructor
     * runs it needs the deleted NodeID information in order to avoid
     * traversing to a node that has already been deleted and been recycled
     */
    InnerDeleteNode(const KeyNodeIDPair &p_delete_item,
                    const KeyNodeIDPair &p_prev_item,
                    const KeyNodeIDPair &p_next_item,
                    const BaseNode *p_child_node_p) :
      InnerDataNode{p_delete_item,
                    NodeType::InnerDeleteType,
                    p_child_node_p,
                    &p_child_node_p->GetLowKeyPair(),
                    &p_child_node_p->GetHighKeyPair(),
                    p_child_node_p->GetDepth() + 1,
                    p_child_node_p->GetItemCount() - 1},
      prev_item{p_prev_item},
      next_item{p_next_item}
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
    KeyNodeIDPair insert_item;

    /*
     * Constructor
     */
    InnerSplitNode(const KeyNodeIDPair &p_insert_item,
                   const BaseNode *p_child_node_p,
                   const BaseNode *p_split_node_p) :
      DeltaNode{NodeType::InnerSplitType,
                p_child_node_p,
                &p_child_node_p->GetLowKeyPair(), // Low key does not change
                &insert_item,                      // High key are defined by this
                // For split node depth does not change since it does not
                // introduce new data
                p_child_node_p->GetDepth(),
                // For split node we need the physical pointer to the
                // split sibling to compute item count
                p_child_node_p->GetItemCount() - \
                  p_split_node_p->GetItemCount()},
      insert_item{p_insert_item}
    {}
  };

  /*
   * class InnerRemoveNode
   */
  class InnerRemoveNode : public DeltaNode {
   public:
    // We also need this to recycle NodeID
    NodeID removed_id;

    /*
     * Constructor
     */
    InnerRemoveNode(NodeID p_removed_id,
                    const BaseNode *p_child_node_p) :
      DeltaNode{NodeType::InnerRemoveType,
                p_child_node_p,
                &p_child_node_p->GetLowKeyPair(),
                &p_child_node_p->GetHighKeyPair(),
                p_child_node_p->GetDepth(),
                p_child_node_p->GetItemCount()},
      removed_id{p_removed_id}
    {}
  };

  /*
   * class InnerMergeNode - Merge delta for inner nodes
   */
  class InnerMergeNode : public DeltaNode {
   public:
    // This is exactly the item being deleted in the parent node
    KeyNodeIDPair delete_item;

    const BaseNode *right_merge_p;

    /*
     * Constructor
     */
    InnerMergeNode(const KeyType &p_merge_key,
                   const BaseNode *p_right_merge_p,
                   NodeID p_deleted_node_id,
                   const BaseNode *p_child_node_p) :
      DeltaNode{NodeType::InnerMergeType,
                p_child_node_p,
                &p_child_node_p->GetLowKeyPair(),
                &p_right_merge_p->GetHighKeyPair(),
                // Note: Since both children under merge node is considered
                // as part of the same node, we use the larger one + 1
                // as the depth of the merge node
                //std::max(p_child_node_p->metadata.depth,
                //         p_right_merge_p->metadata.depth) + 1,
                p_child_node_p->GetDepth() + \
                  p_right_merge_p->GetDepth(),
                // For merge node the item count is the sum of its two
                // branches
                p_child_node_p->GetItemCount() + \
                  p_right_merge_p->GetItemCount()},
      delete_item{p_merge_key, p_deleted_node_id},
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
      DeltaNode{NodeType::InnerAbortType,
                p_child_node_p,
                &p_child_node_p->GetLowKeyPair(),
                &p_child_node_p->GetHighKeyPair(),
                p_child_node_p->GetDepth(),
                p_child_node_p->GetItemCount()}
    {}
  };

  /*
   * struct NodeSnapshot - Describes the states in a tree when we see them
   *
   * node_id and node_p are pairs that represents the state when we traverse
   * the node and use GetNode() to resolve the node ID.
   */
  class NodeSnapshot {
   public:
    NodeID node_id;
    const BaseNode *node_p;

    /*
     * Constructor - Initialize every member to invalid state
     *
     * Identity of leaf or inner needs to be provided as constructor
     * argument.
     *
     * NOTE: We do not allocate any logical node structure here
     */
    NodeSnapshot(NodeID p_node_id, const BaseNode *p_node_p) :
      node_id{p_node_id},
      node_p{p_node_p}
    {}

    /*
     * Default Constructor - Fast path
     */
    NodeSnapshot() {}

    /*
     * IsLeaf() - Test whether current snapshot is on leaf delta chain
     *
     * This function is just a wrapper of IsOnLeafDeltaChain() in BaseNode
     */
    inline bool IsLeaf() const {
      return node_p->IsOnLeafDeltaChain();
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
         KeyHashFunc p_key_hash_obj = KeyHashFunc{},
         ValueEqualityChecker p_value_eq_obj = ValueEqualityChecker{},
         ValueHashFunc p_value_hash_obj = ValueHashFunc{}) :
      // Key comparator, equality checker and hasher
      key_cmp_obj{p_key_cmp_obj},
      key_eq_obj{p_key_eq_obj},
      key_hash_obj{p_key_hash_obj},

      // Value equality checker and hasher
      value_eq_obj{p_value_eq_obj},
      value_hash_obj{p_value_hash_obj},

      // key-node ID pair equality checker and hasher
      key_node_id_pair_eq_obj{this},
      key_node_id_pair_hash_obj{this},

      // key-value pair equality checker and hasher
      key_value_pair_eq_obj{this},
      key_value_pair_hash_obj{this},

      tree_height{2UL},

      // NodeID counter
      next_unused_node_id{1},

      // Initialize free NodeID stack
      free_node_id_list{},

      // Statistical information
      insert_op_count{0},
      insert_abort_count{0},
      delete_op_count{0},
      delete_abort_count{0},
      update_op_count{0},
      update_abort_count{0},

      // Epoch Manager that does garbage collection
      epoch_manager{this} {
    bwt_printf("Bw-Tree Constructor called. "
               "Setting up execution environment...\n");

    InitMappingTable();
    InitNodeLayout();

    bwt_printf("sizeof(NodeMetaData) = %lu is the overhead for each node\n",
               sizeof(NodeMetaData));
    bwt_printf("sizeof(KeyType) = %lu is the size of wrapped key\n",
               sizeof(KeyType));

    bwt_printf("Starting epoch manager thread...\n");
    epoch_manager.StartThread();

    dummy("Call it here to avoid compiler warning\n");
    
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
    bwt_printf("Next node ID at exit: %lu\n", next_unused_node_id.load());
    bwt_printf("Destructor: Free tree nodes\n");

    // Free all nodes recursively
    size_t node_count = FreeNodeByNodeID(root_id.load());

    bwt_printf("Freed %lu tree nodes\n", node_count);

    return;
  }

  /*
   * FreeNodeByNodeID() - Given a NodeID, free all nodes and its children
   *
   * This function returns if the mapping table entry is nullptr which implies
   * that the NodeID has already been recycled and we should not recycle it
   * twice.
   *
   * The return value represents the number of nodes recycled
   */
  size_t FreeNodeByNodeID(NodeID node_id) {
    const BaseNode *node_p = GetNode(node_id);
    if(node_p == nullptr) {
      return 0UL;
    }

    mapping_table[node_id] = nullptr;

    return FreeNodeByPointer(node_p);
  }

  /*
   * InvalidateNodeID() - Recycle NodeID
   *
   * This function is called when a NodeID has been invalidated due to a
   * node removal and it is guaranteed that the NodeID will not be used
   * anymore by any thread. This usually happens in the epoch manager
   *
   * Even if NodeIDs are not recycled (e.g. the counter monotonically increase)
   * this is necessary for destroying the tree since we want to avoid deleting
   * a removed node in InnerNode
   *
   * NOTE: This function only works in a single-threaded environment such
   * as epoch manager and destructor
   *
   * DO NOT call this in worker thread!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
   */
  inline void InvalidateNodeID(NodeID node_id) {
    mapping_table[node_id] = nullptr;

    // Next time if we need a node ID we just push back from this
    free_node_id_list.SingleThreadPush(node_id);

    return;
  }

  /*
   * FreeNodeByPointer() - Free all nodes currently in the tree
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
   * NOTE 3: This function does not consider InnerAbortNode, InnerRemoveNode
   * and LeafRemoveNode as valid, since these three are just temporary nodes,
   * and should not exist if a thread finishes its job (for remove nodes they
   * must be removed after posting InnerDeleteNode, which should be done by
   * the thread posting remove delta or other threads helping-along)
   *
   * This node calls destructor according to the type of the node, considering
   * that there is not virtual destructor defined for sake of running speed.
   */
  size_t FreeNodeByPointer(const BaseNode *node_p) {
    const BaseNode *next_node_p = node_p;
    size_t freed_count = 0;

    while(1) {
      node_p = next_node_p;
      assert(node_p != nullptr);

      NodeType type = node_p->GetType();

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

          freed_count += \
            FreeNodeByNodeID(((LeafSplitNode *)node_p)->insert_item.second);

          delete (LeafSplitNode *)node_p;
          freed_count++;

          break;
        case NodeType::LeafMergeType:
          freed_count += \
            FreeNodeByPointer(((LeafMergeNode *)node_p)->child_node_p);
          freed_count += \
            FreeNodeByPointer(((LeafMergeNode *)node_p)->right_merge_p);

          delete (LeafMergeNode *)node_p;
          freed_count++;

          // Leaf merge node is an ending node
          return freed_count;
        case NodeType::LeafType:
          delete (LeafNode *)node_p;
          freed_count++;

          // We have reached the end of delta chain
          return freed_count;
        case NodeType::InnerInsertType:
          next_node_p = ((InnerInsertNode *)node_p)->child_node_p;

          freed_count += \
            FreeNodeByNodeID(((InnerInsertNode *)node_p)->item.second);

          delete (InnerInsertNode *)node_p;
          freed_count++;

          break;
        case NodeType::InnerDeleteType:
          next_node_p = ((InnerDeleteNode *)node_p)->child_node_p;

          // For those already deleted, the delta chain has been merged
          // and the remove node should be freed (either before this point
          // or will be freed) epoch manager
          // NOTE: No need to call InvalidateNodeID since this function is
          // only called on destruction of the tree
          mapping_table[((InnerDeleteNode *)node_p)->item.second] = \
            nullptr;

          delete (InnerDeleteNode *)node_p;
          freed_count++;

          break;
        case NodeType::InnerSplitType:
          next_node_p = ((InnerSplitNode *)node_p)->child_node_p;

          freed_count += \
            FreeNodeByNodeID(((LeafSplitNode *)node_p)->insert_item.second);

          delete (InnerSplitNode *)node_p;
          freed_count++;

          break;
        case NodeType::InnerMergeType:
          freed_count += \
            FreeNodeByPointer(((InnerMergeNode *)node_p)->child_node_p);
          freed_count += \
            FreeNodeByPointer(((InnerMergeNode *)node_p)->right_merge_p);

          delete (InnerMergeNode *)node_p;
          freed_count++;

          return freed_count;
        case NodeType::InnerType: {
          const InnerNode *inner_node_p = \
            static_cast<const InnerNode *>(node_p);

          // Free NodeID one by one stored in its separator list
          // Even if they are already freed (e.g. a split delta has not
          // been consolidated would share a NodeID with its parent)
          for(auto &sep_item : inner_node_p->sep_list) {
            freed_count += FreeNodeByNodeID(sep_item.second);
          }

          // Access its content first and then delete the node itself
          delete inner_node_p;
          freed_count++;

          // Since we free nodes recursively, after processing an inner
          // node and recursively with its child nodes we could return here
          return freed_count;
        }
        default:
          // For InnerAbortNode, InnerRemoveNode and LeafRemoveNode
          // they are supposed to be removed as part of any operation on
          // the tree (i.e. they could not be left behind once a operation
          // has been finished)
          bwt_printf("Unknown node type: %d\n", (int)type);

          assert(false);
          return 0;
      } // switch
    } // while 1

    assert(false);
    return 0;
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
    assert(root_id == 1UL);

    // This is important since in the iterator we will use NodeID = 2
    // as the starting point of the traversal
    first_leaf_id = GetNextNodeID();
    assert(first_leaf_id == FIRST_LEAF_NODE_ID);

    #ifdef BWTREE_PELOTON

    // For the first inner node, it needs an empty low key
    // the search procedure will not look at it and only use it
    // if the search key could not be matched to anything after the first key
    KeyNodeIDPair first_sep{KeyType(), first_leaf_id};

    // Initially there is one element inside the root node
    // so we set item count to be 1
    // The high key is +Inf which is identified by INVALID_NODE_ID
    InnerNode *root_node_p = \
      new InnerNode{std::make_pair(KeyType(), INVALID_NODE_ID), 1};

    #else

    // For the first inner node, it needs an empty low key
    // the search procedure will not look at it and only use it
    // if the search key could not be matched to anything after the first key
    KeyNodeIDPair first_sep{KeyType{}, first_leaf_id};

    // Initially there is one element inside the root node
    // so we set item count to be 1
    // The high key is +Inf which is identified by INVALID_NODE_ID
    InnerNode *root_node_p = \
      new InnerNode{std::make_pair(KeyType{}, INVALID_NODE_ID), 1};

    #endif

    root_node_p->sep_list.push_back(first_sep);

    bwt_printf("root id = %lu; first leaf id = %lu\n",
               root_id.load(),
               first_leaf_id);

    InstallNewNode(root_id, root_node_p);

    // Initially there is no element inside the leaf node so we set element
    // count to be 0
    #ifdef BWTREE_PELOTON

    LeafNode *left_most_leaf = \
      new LeafNode{std::make_pair(KeyType(), INVALID_NODE_ID), 0};

    #else

    LeafNode *left_most_leaf = \
      new LeafNode{std::make_pair(KeyType{}, INVALID_NODE_ID), 0};

    #endif

    InstallNewNode(first_leaf_id, left_most_leaf);

    return;
  }

  /*
   * InitMappingTable() - Initialize the mapping table
   *
   * It initialize all elements to NULL in order to make
   * first CAS on the mapping table would succeed
   *
   * NOTE: As an optimization we do not set the mapping table to zero
   * since installing new node could be done as directly writing into
   * the mapping table rather than CAS with nullptr
   */
  void InitMappingTable() {
    bwt_printf("Initializing mapping table.... size = %lu\n",
               MAPPING_TABLE_SIZE);
    bwt_printf("Fast initialization: Do not set to zero\n");

    return;
  }

  /*
   * GetNextNodeID() - Thread-safe lock free method to get next node ID
   *
   * This function basically compiles to LOCK XADD instruction on x86
   * which is guaranteed to execute atomically
   */
  inline NodeID GetNextNodeID() {
    // This is a std::pair<bool, NodeID>
    // If the first element is true then the NodeID is a valid one
    // If the first element is false then NodeID is invalid and the
    // stack is either empty or being used (we cannot lock and wait)
    auto ret_pair = free_node_id_list.Pop();

    // If there is no free node id
    if(ret_pair.first == false) {
      // fetch_add() returns the old value and increase the atomic
      // automatically
      return next_unused_node_id.fetch_add(1);
    } else {
      return ret_pair.second;
    }
  }

  /*
   * InstallNodeToReplace() - Install a node to replace a previous one
   *
   * If installation fails because CAS returned false, then return false
   * This function does not retry
   */
  inline bool InstallNodeToReplace(NodeID node_id,
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

    return mapping_table[node_id].compare_exchange_strong(prev_p, node_p);
  }

  /*
   * InstallRootNode() - Replace the old root with a new one
   *
   * There is change that this function would fail. In that case it returns
   * false, which implies there are some other threads changing the root ID
   */
  inline bool InstallRootNode(NodeID old_root_node_id,
                              NodeID new_root_node_id) {
    return root_id.compare_exchange_strong(old_root_node_id,
                                           new_root_node_id);
  }

  /*
   * InstallNewNode() - Install a new node into the mapping table
   *
   * This function does not return any value since we assume new node
   * installation would always succeed
   */
  inline void InstallNewNode(NodeID node_id,
                             const BaseNode *node_p) {
    mapping_table[node_id] = node_p;

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
  inline const BaseNode *GetNode(const NodeID node_id) {
    assert(node_id != INVALID_NODE_ID);
    assert(node_id < MAPPING_TABLE_SIZE);

    return mapping_table[node_id].load();
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
   * NOTE: If value_p is given then this function calls NavigateLeafNode()
   * to detect whether the desired key value pair exists or not. If value_p
   * is nullptr then this function calls the overloaded version of
   *  NavigateLeafNode() to collect all values associated with the key
   * and put them into value_list_p.
   *
   * If value_p is not nullptr, then the return value of this function denotes
   * whether the key-value pair exists or not. Otherwise it always return true
   *
   * In both cases, when this function returns, the top of the NodeSnapshot
   * list is always a leaf node snapshot in which current key is included
   * in its range.
   *
   * For value_p and value_list_p, at most one of them could be non-nullptr
   * If both are nullptr then we just traverse and do not do anything
   */
  const KeyValuePair *Traverse(Context *context_p,
                               const ValueType *value_p,
                               std::pair<int, bool> *index_pair_p) {

    // For value collection it always returns nullptr
    const KeyValuePair *found_pair_p = nullptr;

retry_traverse:
    assert(context_p->abort_flag == false);
    assert(context_p->current_level == -1);

    // This is the serialization point for reading/writing root node
    NodeID start_node_id = root_id.load();

    // We need to call this even for root node since there could
    // be split delta posted on to root node
    LoadNodeID(start_node_id, context_p);

    // There could be an abort here, and we could not directly jump
    // to Init state since we would like to do some clean up or
    // statistics after aborting
    if(context_p->abort_flag == true) {
      goto abort_traverse;
    }

    bwt_printf("Successfully loading root node ID\n");

    while(1) {
      NodeID child_node_id = NavigateInnerNode(context_p);

      // Navigate could abort since it might go to another NodeID
      // if there is a split delta and the key is >= split key
      if(context_p->abort_flag == true) {
        bwt_printf("Navigate Inner Node abort. ABORT\n");

        // If NavigateInnerNode() aborts then it retrns INVALID_NODE_ID
        // as a double check
        // This is the only situation that this function returns
        // INVALID_NODE_ID
        assert(child_node_id == INVALID_NODE_ID);

        goto abort_traverse;
      }

      // This might load a leaf child
      // Also LoadNodeID() does not guarantee the node bound matches
      // seatch key. Since we could readjust using the split side link
      // during Navigate...Node()
      LoadNodeID(child_node_id, context_p);

      if(context_p->abort_flag == true) {
        bwt_printf("LoadNodeID aborted. ABORT\n");

        goto abort_traverse;
      }

      // This is the node we have just loaded
      NodeSnapshot *snapshot_p = GetLatestNodeSnapshot(context_p);

      if(snapshot_p->IsLeaf() == true) {
        bwt_printf("The next node is a leaf\n");

        break;
      }
    } //while(1)

    if(value_p == nullptr) {
      // We are using an iterator just to get a leaf page
      assert(index_pair_p == nullptr);
      
      // If both are nullptr then we just navigate the sibling chain
      // to find the correct node with the correct range
      // And then the iterator will consolidate the node without actually
      // going down with a specific key
      NavigateSiblingChain(context_p);
    } else {
      // If a value is given then use this value to Traverse down leaf
      // page to find whether the value exists or not
      found_pair_p = NavigateLeafNode(context_p, *value_p, index_pair_p);
    }

    if(context_p->abort_flag == true) {
      bwt_printf("NavigateLeafNode aborts. ABORT\n");

      goto abort_traverse;
    }

    bwt_printf("Found leaf node. Abort count = %d, level = %d\n",
               context_p->abort_counter,
               context_p->current_level);

    // If there is no abort then we could safely return
    return found_pair_p;

abort_traverse:
    assert(context_p->current_level >= 0);

    context_p->current_level = -1;

    context_p->abort_flag = false;
    context_p->abort_counter++;

    goto retry_traverse;

    assert(false);
    return nullptr;
  }

  ///////////////////////////////////////////////////////////////////
  // Data Storage Core
  ///////////////////////////////////////////////////////////////////
  
  /*
   * NavigateSiblingChain() - Traverse on sibling chain to adjust current
   *                          position so that the range of the node matches
   *                          the search key
   *
   * This function detects node range using the topmost node's high key
   * since it is guaranteed that topmost high key is always the correct
   * high key for us to use
   *
   * NOTE: This function cannot traverse to a sibling node of a different
   * parent node than the current parent node, because we also do
   * a key range validation on the parent node, it is impossible that the
   * parent node contains a child node whose range is outside the parent
   * node's range. Therefore when posting on the parent node there is no need
   * for us to validate key range.
   *
   * If the traverse aborts then this function returns with abort_flag
   * setting to true.
   */
  void NavigateSiblingChain(Context *context_p) {
    do {
      // These two will be updated everytime we switch to
      // a new node
      NodeSnapshot *snapshot_p = GetLatestNodeSnapshot(context_p);
      const BaseNode *node_p = snapshot_p->node_p;
      
      // Before node navigation, the first thing to do is to verify
      // that we are on the correct node according to the current
      // search key
      // This procedure does not necessarily need a split node to do right
      // traversal (but it needs to complete any SMO if there is one, so
      // it must be called after FinishPartialSMO() returns), because
      // the high key of split node will be inherited by all nodes
      // posted later and by the consolidated version of the node
      if((node_p->GetNextNodeID() != INVALID_NODE_ID) &&
         (KeyCmpGreaterEqual(context_p->search_key, node_p->GetHighKey()))) {
        bwt_printf("Bounds checking failed (id = %lu) - "
                   "Go right.\n",
                   snapshot_p->node_id);

        JumpToNodeID(node_p->GetNextNodeID(), context_p);

        if(context_p->abort_flag == true) {
          bwt_printf("JumpToNodeID aborts. ABORT\n");

          return;
        }

      } else {
        break;
      }
    } while(1);
    
    return;
  }

  /*
   * LocateSeparatorByKey() - Locate the child node for a key
   *
   * This functions works with any non-empty inner nodes. However
   * it fails assertion with empty inner node
   *
   * NOTE: This function ignores the first element in the sep list
   * since even if we know the low key of the first element
   */
  inline NodeID LocateSeparatorByKey(const KeyType &search_key,
                                     const InnerNode *inner_node_p) {
    const std::vector<KeyNodeIDPair> *sep_list_p = &inner_node_p->sep_list;

    // Inner node could not be empty
    assert(sep_list_p->size() != 0UL);

    // Hopefully std::upper_bound would use binary search here
    auto it = std::upper_bound(sep_list_p->begin() + 1,
                               sep_list_p->end(),
                               std::make_pair(search_key, INVALID_NODE_ID),
                               [this](const KeyNodeIDPair &knp1,
                                      const KeyNodeIDPair &knp2) {
                                  return this->key_cmp_obj(knp1.first, knp2.first);
                               });

    // Since upper_bound returns the first element > given key
    // so we need to decrease it to find the last element <= given key
    // which is out separator key
    return (it - 1)->second;
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
   */
  inline NodeID NavigateInnerNode(Context *context_p) {
    // This will go to the right sibling until we have seen
    // a node whose range match the search key
    NavigateSiblingChain(context_p);

    // If navigating sibling chain aborts then abort here
    if(context_p->abort_flag == true) {
      return INVALID_NODE_ID;
    }
    
    /////////////////////////////////////////////////////////////////
    // Only after this point could we get snapshot and node_p
    /////////////////////////////////////////////////////////////////
    
    // This search key will not be changed during navigation
    const KeyType &search_key = context_p->search_key;

    // First get the snapshot from context
    NodeSnapshot *snapshot_p = GetLatestNodeSnapshot(context_p);

    // Save some keystrokes
    const BaseNode *node_p = snapshot_p->node_p;

    // Make sure the structure is valid
    assert(snapshot_p->IsLeaf() == false);
    assert(snapshot_p->node_p != nullptr);
    assert(snapshot_p->node_id != INVALID_NODE_ID);

    

    bwt_printf("Navigating inner node delta chain...\n");

    while(1) {
      NodeType type = node_p->GetType();

      switch(type) {
        case NodeType::InnerType: {
          const InnerNode *inner_node_p = \
            static_cast<const InnerNode *>(node_p);

          // We always use the ubound recorded inside the top of the
          // delta chain
          NodeID target_id = LocateSeparatorByKey(search_key, inner_node_p);

          bwt_printf("Found child in inner node; child ID = %lu\n",
                     target_id);

          return target_id;
        } // case InnerType
        case NodeType::InnerRemoveType: {
          bwt_printf("ERROR: InnerRemoveNode not allowed\n");

          assert(false);
        } // case InnerRemoveType
        case NodeType::InnerInsertType: {
          const InnerInsertNode *insert_node_p = \
            static_cast<const InnerInsertNode *>(node_p);

          const KeyNodeIDPair &insert_item = insert_node_p->item;
          const KeyNodeIDPair &next_item = insert_node_p->next_item;

          // If the next item has +Inf as its key (checking that using
          // next_node_id), or it > search key
          if((next_item.second == INVALID_NODE_ID) ||
             (KeyCmpLess(search_key, next_item.first))) {
            // If search key >= insert key
            if(KeyCmpGreaterEqual(search_key, insert_item.first)) {
              bwt_printf("Find target ID = %lu in insert delta\n",
                         insert_item.second);

              return insert_item.second;
            }
          }

          node_p = insert_node_p->child_node_p;

          break;
        } // InnerInsertType
        case NodeType::InnerDeleteType: {
          const InnerDeleteNode *delete_node_p = \
            static_cast<const InnerDeleteNode *>(node_p);

          const KeyNodeIDPair &prev_item = delete_node_p->prev_item;
          const KeyNodeIDPair &next_item = delete_node_p->next_item;

          // NOTE: Low key ID will not be changed (i.e. being deleted or
          // being preceded by other key-NodeID pair)
          // If the prev item is the leftmost item then we do not need to
          // compare since we know the search key is definitely greater than
          // or equal to the low key (this is necessary to prevent comparing
          // with -Inf)
          // NOTE: Even if the inner node is merged into its left sibling
          // this is still true since we compared prev_item.second
          // with the low key of the current delete node which always
          // reflects the low key of this branch
          if((delete_node_p->GetLowKeyNodeID() == prev_item.second) ||
             (KeyCmpGreaterEqual(search_key, prev_item.first))) {
            // If the next item is +Inf key then we also choose not to compare
            // keys directly since we know the search key is definitely smaller
            // then +Inf
            if((next_item.second == INVALID_NODE_ID) ||
               (KeyCmpLess(search_key, next_item.first))) {
              bwt_printf("Find target ID = %lu in delete delta\n",
                         prev_item.second);

              return prev_item.second;
            }
          }

          node_p = delete_node_p->child_node_p;

          break;
        } // InnerDeleteType
        case NodeType::InnerSplitType: {
          const InnerSplitNode *split_node_p = \
            static_cast<const InnerSplitNode *>(node_p);

          // Since we have already finished jumping to the right
          // sibling on the top level, it is unnecessary to
          // jump when seeing an InnerSplitNode
          // (The split key information has been observed on top
          // node's high key)
          node_p = split_node_p->child_node_p;

          break;
        } // case InnerSplitType
        case NodeType::InnerMergeType: {
          const InnerMergeNode *merge_node_p = \
            static_cast<const InnerMergeNode *>(node_p);

          const KeyType &merge_key = merge_node_p->delete_item.first;

          // Here since we will only take one branch, so
          // high key does not need to be updated
          // Since we still could not know the high key
          if(KeyCmpGreaterEqual(search_key, merge_key)) {
            bwt_printf("Take merge right branch (ID = %lu)\n",
                       snapshot_p->node_id);

            node_p = merge_node_p->right_merge_p;
          } else {
            bwt_printf("Take merge left branch (ID = %lu)\n",
                       snapshot_p->node_id);

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
    } // while 1

    // Should not reach here
    assert(false);
    return INVALID_NODE_ID;
  }

  /*
   * CollectAllSepsOnInner() - Collect all separators given a snapshot
   *
   * This function consolidates a given delta chain given the node snapshot.
   * Consolidation takes place by using a deleted set and present set to record
   * values already deleted and still exist in the current node, to achieve
   * the same effect as a log replay.
   *
   * This function returns an inner node with all Key-NodeID pairs
   * sorted by the key order
   *
   * NOTE: This function could take an optional depth argument indicating the
   * depth of the newly constructed InnerNode. This is majorly used by other
   * proceures where parent node is consolidated and scanned in order to find
   * a certain key.
   */
  inline InnerNode *CollectAllSepsOnInner(NodeSnapshot *snapshot_p,
                                          int p_depth = 0) {

    // Note that in the recursive call node_p might change
    // but we should not change the metadata
    const BaseNode *node_p = snapshot_p->node_p;

    // This is the number of insert + delete records which contributes
    // to the size of the bloom filter
    int delta_record_num = node_p->GetDepth();

    // This array will hold sorted InnerDataNode pointers in order to
    // perform a log merging
    const InnerDataNode *data_node_list[delta_record_num];

    // These two are used to compare InnerDataNode for < and == relation
    auto f1 = [this](const InnerDataNode *idn_1, const InnerDataNode *idn_2) {
                      return this->key_cmp_obj(idn_1->item.first, idn_2->item.first);
                    };

    auto f2 = [this](const InnerDataNode *idn_1, const InnerDataNode *idn_2) {
                      return this->key_eq_obj(idn_1->item.first, idn_2->item.first);
                    };

    SortedSmallSet<const InnerDataNode *, decltype(f1), decltype(f2)> \
      sss{data_node_list, f1, f2};

    // The effect of this function is a consolidation into inner node
    InnerNode *inner_node_p = new InnerNode{node_p->GetHighKeyPair(),
                                            node_p->GetItemCount(),
                                            p_depth};

    // Save some typing
    auto sep_list_p = &inner_node_p->sep_list;

    // The first element is always the low key
    // In the recursive call below we should take care not to insert the low
    // key again (it is always the first element in data list)
    sep_list_p->push_back(node_p->GetLowKeyPair());

    // This will fill in two sets with values present in the inner node
    // and values deleted
    CollectAllSepsOnInnerRecursive(node_p,
                                   node_p->GetLowKeyNodeID(),
                                   sss,
                                   inner_node_p);

    // Since consolidation would not change item count they must be equal
    assert(static_cast<int>(sep_list_p->size()) == node_p->GetItemCount());

    return inner_node_p;
  }

  /*
   * CollectAllSepsOnInnerRecursive() - This is the counterpart on inner node
   *
   * Please refer to the function on leaf node for details. These two have
   * almost the same logical flow
   */
  template<typename T> // To make the f**king compiler
                       // to deduce SortedSmallSet template type
  void
  CollectAllSepsOnInnerRecursive(const BaseNode *node_p,
                                 NodeID low_key_node_id,
                                 T &sss,
                                 InnerNode *new_inner_node_p) const {
    // High key should be the high key of the branch (if there is a merge
    // then the high key of the branch may not always equal the high key
    // of the merged node)
    // Even if there is no merge, we still need the high key to rule out
    // keys that has already been splited
    const KeyNodeIDPair &high_key_pair = node_p->GetHighKeyPair();

    while(1) {
      NodeType type = node_p->GetType();

      switch(type) {
        case NodeType::InnerType: {
          const InnerNode *inner_node_p = \
            static_cast<const InnerNode *>(node_p);

          // These two will be set according to the high key and
          // low key
          typename std::vector<KeyNodeIDPair>::const_iterator copy_end_it{};
          typename std::vector<KeyNodeIDPair>::const_iterator copy_start_it{};

          if(high_key_pair.second == INVALID_NODE_ID) {
            copy_end_it = inner_node_p->sep_list.end();
          } else {
            // This search for the first key >= high key of the current node
            // being consolidated
            // This is exactly where we should stop copying
            // The return value might be end() iterator, but it is also
            // consistent
            copy_end_it = \
              std::lower_bound(inner_node_p->sep_list.begin() + 1,
                               inner_node_p->sep_list.end(),
                               high_key_pair,     // This contains the high key
                               [this](const KeyNodeIDPair &knp1,
                                      const KeyNodeIDPair &knp2) {
                                 return this->key_cmp_obj(knp1.first, knp2.first);
                               });
          }

          // Since we want to access its first element
          assert(static_cast<int>(inner_node_p->sep_list.size()) > 0);

          // If the first element in the sep list equals the low key pair's
          // NodeID then we are at the leftmost node of the merge tree
          // and we ignore the leftmost sep (since it could be -Inf)
          // For other nodes, the leftmost item inside sep list has a valid
          // key and could thus be pushed directly
          if(inner_node_p->sep_list[0].second == low_key_node_id) {
            copy_start_it = inner_node_p->sep_list.begin() + 1;
          } else {
            copy_start_it = inner_node_p->sep_list.begin();
          }

          // Find the end of copying
          auto sss_end_it = sss.GetEnd() - 1;

          // If the next key is +Inf then sss_end_it is the real end of the
          // sorted array
          if(high_key_pair.second != INVALID_NODE_ID) {
            // Corner case: If the first element is lower bound
            // then current_p will move outside
            // the valid range of the array but still we return
            // the first element in the array
            while(sss_end_it >= sss.GetBegin()) {
              if(key_cmp_obj((*sss_end_it)->item.first, high_key_pair.first) == true) {
                break;
              }

              sss_end_it--;
            }
          }

          // This points to the first element >= high key
          sss_end_it++;

          //printf("%ld\n", sss.GetSize());

          while(1) {
            bool sss_end_flag = (sss.GetBegin() == sss_end_it);
            bool array_end_flag = (copy_start_it == copy_end_it);

            //printf("sss_end_flag = %d; array_end_flag = %d\n", sss_end_flag, array_end_flag);

            if(sss_end_flag == true && array_end_flag == true) {
              // Both are drained
              break;
            } else if(sss_end_flag == true) {
              // If the sss has drained we continue to drain the array
              // We use range insertion which inserts before the first argument
              // with the range specified by the last two arguments
              new_inner_node_p->sep_list.insert(new_inner_node_p->sep_list.end(),
                                                copy_start_it,
                                                copy_end_it);

              break;
            } else if(array_end_flag == true) {
              // Then we drain all elements inside the delta chain
              while(sss.GetBegin() != sss_end_it) {
                // We could not pop here since we will use the value later
                NodeType data_node_type = (sss.GetFront())->GetType();

                // It is possible that there is an InnerDeleteNode:
                // InnerNode: [1, 2, 3, 4, 5]
                // Delta Chain:               Delete 6, Insert 6
                // Only the first "Delete 6" will appear in the set
                // and we just do not care
                if(data_node_type == NodeType::InnerInsertType) {
                  // Pop the value here
                  new_inner_node_p->sep_list.push_back(sss.PopFront()->item);
                } else {
                  // And here (InnerDeleteNode after we have drained InnerNode
                  // is useless so just ignore it)
                  sss.PopFront();
                }
              }

              break;
            }

            // Next is the normal case: Both are not drained
            // we do a comparison of their leading elements

            if(key_cmp_obj(copy_start_it->first, sss.GetFront()->item.first)) {
              // If array element is less than data node list element
              new_inner_node_p->sep_list.push_back(*copy_start_it);

              copy_start_it++;
            } else if(key_cmp_obj(sss.GetFront()->item.first, copy_start_it->first)) {
              NodeType data_node_type = (sss.GetFront())->GetType();

              // Delta Insert with array not having that element
              if(data_node_type == NodeType::InnerInsertType) {
                // Pop the value here
                new_inner_node_p->sep_list.push_back(sss.PopFront()->item);
              } else {
                // This is possible
                // InnerNode: [2, 3, 4, 5]
                // Delta Chain: Delete 1, Insert 1
                // So just ignore it
                sss.PopFront();
              }
            } else {
              // In this branch the values are equal

              NodeType data_node_type = (sss.GetFront())->GetType();

              // InsertDelta overrides InnerNode element
              if(data_node_type == NodeType::InnerInsertType) {
                new_inner_node_p->sep_list.push_back(sss.PopFront()->item);
              } else {
                // There is a value in InnerNode that does not exist
                // in consolidated node. Just ignore
                sss.PopFront();
              }

              copy_start_it++;
            } // Compare leading elements
          } // while(1)

          return;
        } // case InnerType
        case NodeType::InnerRemoveType: {
          bwt_printf("ERROR: InnerRemoveNode not allowed\n");

          assert(false);
          return;
        } // case InnerRemoveType
        case NodeType::InnerInsertType: {
          const InnerInsertNode *insert_node_p = \
            static_cast<const InnerInsertNode *>(node_p);

          // delta nodes must be consistent with the most up-to-date
          // node high key
          assert((high_key_pair.second == INVALID_NODE_ID) ||
                 (KeyCmpLess(insert_node_p->item.first, high_key_pair.first)));

          sss.Insert(static_cast<const InnerDataNode *>(node_p));

          // Go to next node
          node_p = insert_node_p->child_node_p;

          break;
        } // case InnerInsertType
        case NodeType::InnerDeleteType: {
          const InnerDeleteNode *delete_node_p = \
            static_cast<const InnerDeleteNode *>(node_p);

          // Since we do not allow any delta node under split node
          // this must be true
          // i.e. delta nodes must be consistent with the most up-to-date
          // node high key
          assert((high_key_pair.second == INVALID_NODE_ID) ||
                 (KeyCmpLess(delete_node_p->item.first, high_key_pair.first)));

          sss.Insert(static_cast<const InnerDataNode *>(node_p));

          node_p = delete_node_p->child_node_p;

          break;
        } // case InnerDeleteType
        case NodeType::InnerSplitType: {
          node_p = (static_cast<const DeltaNode *>(node_p))->child_node_p;

          break;
        } // case InnerSplitType
        case NodeType::InnerMergeType: {
          const InnerMergeNode *merge_node_p = \
            static_cast<const InnerMergeNode *>(node_p);

          // NOTE: We alaways use the same metadata object which is the
          // one passed by the wrapper. Though node_p changes for each
          // recursive call, metadata should not change and should remain
          // constant

          CollectAllSepsOnInnerRecursive(merge_node_p->child_node_p,
                                         low_key_node_id,
                                         sss,
                                         new_inner_node_p);

          CollectAllSepsOnInnerRecursive(merge_node_p->right_merge_p,
                                         low_key_node_id,
                                         sss,
                                         new_inner_node_p);

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
    } // while(1)

    // Should not get to here
    assert(false);
    return;
  }

  /*
   * NavigateLeafNode() - Find search key on a logical leaf node and collect
   *                      values associated with the key
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
                        std::vector<ValueType> &value_list) {
                          
    // This will go to the right sibling until we have seen
    // a node whose range match the search key
    NavigateSiblingChain(context_p);

    // If navigating sibling chain aborts then abort here
    if(context_p->abort_flag == true) {
      return;
    }
    
    /////////////////////////////////////////////////////////////////
    // Only after this point could we get snapshot and node_p
    /////////////////////////////////////////////////////////////////
    
    // This contains information for current node
    NodeSnapshot *snapshot_p = GetLatestNodeSnapshot(context_p);
    const BaseNode *node_p = snapshot_p->node_p;

    assert(snapshot_p->IsLeaf() == true);

    // We only collect values for this key
    const KeyType &search_key = context_p->search_key;

    // The maximum size of present set and deleted set is just
    // the length of the delta chain. Since when we reached the leaf node
    // we just probe and add to value set
    const int set_max_size = node_p->GetDepth();

    // 1. This works even if depth is 0
    // 2. We choose to store const ValueType * because we want to bound the
    // size of stack array. It should be upper bounded by max delta chain
    // length * pointer size. On the contrary, if the size of
    // ValueType is huge, and we store ValueType, then we might cause
    // a stack overflow
    const ValueType *present_set_data_p[set_max_size];
    const ValueType *deleted_set_data_p[set_max_size];

    BloomFilter<ValueType, ValueEqualityChecker, ValueHashFunc> \
      present_set{present_set_data_p,
                  value_eq_obj,
                  value_hash_obj};

    BloomFilter<ValueType, ValueEqualityChecker, ValueHashFunc> \
      deleted_set{deleted_set_data_p,
                  value_eq_obj,
                  value_hash_obj};

    while(1) {
      NodeType type = node_p->GetType();

      switch(type) {
        case NodeType::LeafType: {
          const LeafNode *leaf_node_p = \
            static_cast<const LeafNode *>(node_p);

          // Here we know the search key < high key of current node
          // NOTE: We only compare keys here, so it will get to the first
          // element >= search key
          auto copy_start_it = \
            std::lower_bound(leaf_node_p->data_list.begin(),
                             leaf_node_p->data_list.end(),
                             std::make_pair(search_key, ValueType{}),
                             [this](const KeyValuePair &kvp1, const KeyValuePair &kvp2) {
                                return this->key_cmp_obj(kvp1.first, kvp2.first);
                             });

          // If there is something to copy
          while((copy_start_it != leaf_node_p->data_list.end()) && \
                (KeyCmpEqual(search_key, copy_start_it->first))) {
            // If the value has not been deleted then just insert
            // Note that here we use ValueSet, so need to extract value from
            // the key value pair
            if(deleted_set.Exists(copy_start_it->second) == false) {
              if(present_set.Exists(copy_start_it->second) == false) {
                // Note: As an optimization, we do not have to Insert() the
                // value element into present set here. Since we know it
                // is already base leaf page, adding values into present set
                // definitely will not block the remaining values, since we
                // know they do not duplicate inside the leaf node

                value_list.push_back(copy_start_it->second);
              }
            }

            copy_start_it++;
          }

          return;
        }
        case NodeType::LeafInsertType: {
          const LeafInsertNode *insert_node_p = \
            static_cast<const LeafInsertNode *>(node_p);

          if(KeyCmpEqual(search_key, insert_node_p->item.first)) {
            if(deleted_set.Exists(insert_node_p->item.second) == false) {
              // We must do this, since inserted set does not detect for
              // duplication, and if the value has already been in present set
              // then we inserted the same value twice
              if(present_set.Exists(insert_node_p->item.second) == false) {
                present_set.Insert(insert_node_p->item.second);

                value_list.push_back(insert_node_p->item.second);
              }
            }
          }

          node_p = insert_node_p->child_node_p;

          break;
        } // case LeafInsertType
        case NodeType::LeafDeleteType: {
          const LeafDeleteNode *delete_node_p = \
            static_cast<const LeafDeleteNode *>(node_p);

          if(KeyCmpEqual(search_key, delete_node_p->item.first)) {
            if(present_set.Exists(delete_node_p->item.second) == false) {
              deleted_set.Insert(delete_node_p->item.second);
            }
          } 
          
          node_p = delete_node_p->child_node_p;

          break;
        } // case LeafDeleteType
        case NodeType::LeafRemoveType: {
          bwt_printf("ERROR: Observed LeafRemoveNode in delta chain\n");

          assert(false);
        } // case LeafRemoveType
        case NodeType::LeafMergeType: {
          bwt_printf("Observed a merge node on leaf delta chain\n");

          const LeafMergeNode *merge_node_p = \
            static_cast<const LeafMergeNode *>(node_p);

          // Decide which side we should choose
          // Using >= for separator key
          if(KeyCmpGreaterEqual(search_key, merge_node_p->delete_item.first)) {
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

          // Do not need to go right here since we have already
          // done that on the top level
          // and the high key has been observed on the top level high key
          node_p = split_node_p->child_node_p;

          break;
        } // case LeafSplitType
        default: {
          bwt_printf("ERROR: Unknown leaf delta node type: %d\n",
                     static_cast<int>(node_p->GetType()));

          assert(false);
        } // default
      } // switch
    } // while

    // We cannot reach here
    assert(false);
    return;
  }

  /*
   * NavigateLeafNode() - Check existence for a certain value
   *
   * Please note that this function is overloaded. This version is used for
   * insert/delete/update that only checks existence of values, rather than
   * collecting all values for a single key.
   *
   * If return value is nullptr, then the key-value pair is not found. Otherwise
   * a pointer to the matching item is returned. The caller should check return
   * value and act accordingly.
   *
   * This function works by traversing down the delta chain and compare
   * values with those in delta nodes and in the base node. No special data
   * structure is required
   *
   * This function calls JumpToNodeID() to switch to a split sibling node
   * There are possibility that the switch aborts, and in this case this
   * function returns with value false.
   */
  const KeyValuePair *NavigateLeafNode(Context *context_p,
                                       const ValueType &search_value,
                                       std::pair<int, bool> *index_pair_p) {
    
    // This will go to the right sibling until we have seen
    // a node whose range match the search key
    NavigateSiblingChain(context_p);

    // If navigating sibling chain aborts then abort here
    if(context_p->abort_flag == true) {
      return nullptr;
    }
    
    /////////////////////////////////////////////////////////////////
    // Only after this point could we get snapshot and node_p
    /////////////////////////////////////////////////////////////////
    
    // Snapshot pointer, node pointer, and metadata reference all need
    // updating once LoadNodeID() returns with success
    NodeSnapshot *snapshot_p = GetLatestNodeSnapshot(context_p);
    assert(snapshot_p->IsLeaf() == true);

    const BaseNode *node_p = snapshot_p->node_p;

    // Save some typing
    const KeyType &search_key = context_p->search_key;

    while(1) {
      NodeType type = node_p->GetType();

      switch(type) {
        case NodeType::LeafType: {
          const LeafNode *leaf_node_p = \
            static_cast<const LeafNode *>(node_p);

          // Here we know the search key < high key of current node
          // NOTE: We only compare keys here, so it will get to the first
          // element >= search key
          auto scan_start_it = \
            std::lower_bound(leaf_node_p->data_list.begin(),
                             leaf_node_p->data_list.end(),
                             std::make_pair(search_key, ValueType{}),
                             [this](const KeyValuePair &kvp1, const KeyValuePair &kvp2) {
                               return this->key_cmp_obj(kvp1.first, kvp2.first);
                             });

          // Search all values with the search key
          while((scan_start_it != leaf_node_p->data_list.end()) && \
                (KeyCmpEqual(scan_start_it->first, search_key))) {
                  
            // If there is a value matching the search value then return true
            // We do not need to check any delete set here, since if the
            // value has been deleted earlier then this function would
            // already have returned
            if(ValueCmpEqual(scan_start_it->second, search_value)) {
              // Since only Delete() will use this piece of information
              // we set exist flag to false to indicate that the value
              // has been invalidated
              index_pair_p->first = \
                scan_start_it - leaf_node_p->data_list.begin();
              index_pair_p->second = true;
              
              // Return a pointer to the item inside LeafNode;
              // This pointer should remain valid until epoch is exited
              return &(*scan_start_it);
            }

            scan_start_it++;
          }
          
          // Either key does not exist or key exists but value does not
          // exist will reach here
          // Since only Insert() will use the index we set exist flag to false
          index_pair_p->first = \
                scan_start_it - leaf_node_p->data_list.begin();
          index_pair_p->second = false;

          return nullptr;
        } // case LeafType
        case NodeType::LeafInsertType: {
          const LeafInsertNode *insert_node_p = \
            static_cast<const LeafInsertNode *>(node_p);

          if(KeyCmpEqual(search_key, insert_node_p->item.first)) {
            if(ValueCmpEqual(insert_node_p->item.second, search_value)) {
              // Only Delete() will use this
              // We just simply inherit from the first node
              *index_pair_p = node_p->GetIndexPair();
              
              return &insert_node_p->item;
            }
          }

          node_p = insert_node_p->child_node_p;

          break;
        } // case LeafInsertType
        case NodeType::LeafDeleteType: {
          const LeafDeleteNode *delete_node_p = \
            static_cast<const LeafDeleteNode *>(node_p);

          // If the value was deleted then return false
          if(KeyCmpEqual(search_key, delete_node_p->item.first)) {
            if(ValueCmpEqual(delete_node_p->item.second, search_value)) {
              // Only Insert() will use this
              // We just simply inherit from the first node
              *index_pair_p = node_p->GetIndexPair();
              
              return nullptr;
            }
          }
          
          node_p = delete_node_p->child_node_p;

          break;
        } // case LeafDeleteType
        case NodeType::LeafRemoveType: {
          bwt_printf("ERROR: Observed LeafRemoveNode in delta chain\n");

          assert(false);
        } // case LeafRemoveType
        case NodeType::LeafMergeType: {
          bwt_printf("Observed a merge node on leaf delta chain\n");

          const LeafMergeNode *merge_node_p = \
            static_cast<const LeafMergeNode *>(node_p);

          // Decide which side we should choose
          // Using >= for separator key
          if(KeyCmpGreaterEqual(search_key, merge_node_p->delete_item.first)) {
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

          node_p = split_node_p->child_node_p;

          break;
        } // case LeafSplitType
        default: {
          bwt_printf("ERROR: Unknown leaf delta node type: %d\n",
                     static_cast<int>(node_p->GetType()));

          assert(false);
        } // default
      } // switch
    } // while

    // We cannot reach here
    assert(false);
    return nullptr;
  }
  
  /*
   * NavigateLeafNode() - Apply predicate to all values, and detect for existing
   *                      value for insert
   *
   * This function cooperates with ConditionalInsert() which requires applying
   * a predicate to all existing values, and insert the value if and only if:
   *
   * 1. The predicate is not satisfied on all existing values
   * 2. The value does not yet exists in the leaf delta chain
   *
   * If any of the above two are true, then we should not insert:
   *
   * 1. If predicate is satisfied by an existing value, return nullptr and
   *    argument predicate_satisfied is set to true
   * 2. If predicate is not satisfied, but the value already exists, then
   *    return nullptr but argument predicate_satisfied is unchanged
   *
   * If both conditions are true then we always test predicate first
   * and then test for existence
   *
   * If only one condition are true then it depends on the order we
   * traverse the delta chain and leaf data node, and could not be
   * guaranteed a specific order
   */
  inline const KeyValuePair *
  NavigateLeafNode(Context *context_p,
                   const ValueType &value,
                   std::pair<int, bool> *index_pair_p,
                   std::function<bool(const ItemPointer &)> predicate,
                   bool *predicate_satisfied) {


    // NOTE: We do not have to traverse to the right sibling here
    // since Traverse() already traverses to the right sibling
    // if value pointer given to it is nullptr
    // So we are guaranteed that we are always on the correct leaf
    // page delta chain on entry of this function

    NodeSnapshot *snapshot_p = GetLatestNodeSnapshot(context_p);
    const BaseNode *node_p = snapshot_p->node_p;

    assert(snapshot_p->IsLeaf() == true);

    const KeyType &search_key = context_p->search_key;

    const int set_max_size = node_p->GetDepth();

    const ValueType *present_set_data_p[set_max_size];
    const ValueType *deleted_set_data_p[set_max_size];

    BloomFilter<ValueType, ValueEqualityChecker, ValueHashFunc> \
      present_set{present_set_data_p,
                  value_eq_obj,
                  value_hash_obj};

    BloomFilter<ValueType, ValueEqualityChecker, ValueHashFunc> \
      deleted_set{deleted_set_data_p,
                  value_eq_obj,
                  value_hash_obj};

    while(1) {
      NodeType type = node_p->GetType();

      switch(type) {
        case NodeType::LeafType: {
          const LeafNode *leaf_node_p = \
            static_cast<const LeafNode *>(node_p);

          auto copy_start_it = \
            std::lower_bound(leaf_node_p->data_list.begin(),
                             leaf_node_p->data_list.end(),
                             std::make_pair(search_key, ValueType{}),
                             [this](const KeyValuePair &kvp1, const KeyValuePair &kvp2) {
                                return this->key_cmp_obj(kvp1.first, kvp2.first);
                             });

          while((copy_start_it != leaf_node_p->data_list.end()) && \
                (KeyCmpEqual(search_key, copy_start_it->first))) {
            if(deleted_set.Exists(copy_start_it->second) == false) {
              if(present_set.Exists(copy_start_it->second) == false) {
                
                // If the predicate is satified by the value
                // then return with predicate flag set to true
                // Otherwise test for duplication
                if(predicate(*copy_start_it->second) == true) {
                  *predicate_satisfied = true;
                  
                  return nullptr;
                } else if(value_eq_obj(value, copy_start_it->second) == true) {
                  // We will not insert anyway....
                  return &(*copy_start_it);
                }
              }
            }

            copy_start_it++;
          }
          
          // The index is the last element (this is true even if we have seen a
          // leaf delete node, since the delete node must have deleted a
          // value that does not exist in leaf base node, so that inserted value
          // must have the same index)
          index_pair_p->first = copy_start_it - leaf_node_p->data_list.begin();
          // Value does not exist
          index_pair_p->second = false;

          // Only if we return here could we insert
          return nullptr;
        }
        case NodeType::LeafInsertType: {
          const LeafInsertNode *insert_node_p = \
            static_cast<const LeafInsertNode *>(node_p);

          if(KeyCmpEqual(search_key, insert_node_p->item.first)) {
            if(deleted_set.Exists(insert_node_p->item.second) == false) {
              if(present_set.Exists(insert_node_p->item.second) == false) {
                present_set.Insert(insert_node_p->item.second);

                // LeafInsertNode means this value exists
                if(predicate(*insert_node_p->item.second) == true) {
                  *predicate_satisfied = true;

                  // Could return here since we know the predicate is satisfied
                  return nullptr;
                } else if(value_eq_obj(value, insert_node_p->item.second) == true) {
                  // Could also return here since we know the value exists
                  // and we could not insert anyway
                  return &insert_node_p->item;
                } // test predicate and duplicates
              } // if value not seen
            } // if value not deleted
          } // if key equal

          node_p = insert_node_p->child_node_p;

          break;
        } // case LeafInsertType
        case NodeType::LeafDeleteType: {
          const LeafDeleteNode *delete_node_p = \
            static_cast<const LeafDeleteNode *>(node_p);

          if(KeyCmpEqual(search_key, delete_node_p->item.first)) {
            if(present_set.Exists(delete_node_p->item.second) == false) {
              // Even if we know the value does not exist, we still need
              // to test all predicates to the leaf base node
              deleted_set.Insert(delete_node_p->item.second);
            }
          }

          node_p = delete_node_p->child_node_p;

          break;
        } // case LeafDeleteType
        case NodeType::LeafRemoveType: {
          bwt_printf("ERROR: Observed LeafRemoveNode in delta chain\n");

          assert(false);
        } // case LeafRemoveType
        case NodeType::LeafMergeType: {
          bwt_printf("Observed a merge node on leaf delta chain\n");

          const LeafMergeNode *merge_node_p = \
            static_cast<const LeafMergeNode *>(node_p);

          if(KeyCmpGreaterEqual(search_key, merge_node_p->delete_item.first)) {
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

          node_p = split_node_p->child_node_p;

          break;
        } // case LeafSplitType
        default: {
          bwt_printf("ERROR: Unknown leaf delta node type: %d\n",
                     static_cast<int>(node_p->GetType()));

          assert(false);
        } // default
      } // switch
    } // while

    // We cannot reach here
    assert(false);
    return nullptr;
  }

  /*
   * CollectAllValuesOnLeaf() - Consolidate delta chain for a single logical
   *                            leaf node
   *
   * This function is the non-recursive wrapper of the resursive core function.
   * It calls the recursive version to collect all base leaf nodes, and then
   * it replays delta records on top of them.
   */
  inline LeafNode *CollectAllValuesOnLeaf(NodeSnapshot *snapshot_p) {
    assert(snapshot_p->IsLeaf() == true);

    const BaseNode *node_p = snapshot_p->node_p;
    
    /////////////////////////////////////////////////////////////////
    // Prepare new node
    /////////////////////////////////////////////////////////////////

    LeafNode *leaf_node_p = new LeafNode{node_p->GetHighKeyPair(),
                                         // The item count of the consolidated
                                         // leaf node is the set of items still
                                         // present in the node
                                         node_p->GetItemCount()};

    std::vector<KeyValuePair> *data_list_p = &leaf_node_p->data_list;

    // Reserve that much space for items to avoid allocation in the future
    // Since the iterator from unordered_set is not a RamdomAccessIterator
    // std::vector could not decide the size from these two iterators
    data_list_p->reserve(node_p->GetItemCount());
    
    /////////////////////////////////////////////////////////////////
    // Prepare Delta Set
    /////////////////////////////////////////////////////////////////
    
    // This is the number of delta records inside the logical node
    // including merged delta chains
    int delta_change_num = node_p->GetDepth();

    // We only need to keep those on the delta chian into a set
    // and those in the data list of leaf page do not need to be
    // put in the set
    // This is used to dedup already seen key-value pairs
    const KeyValuePair *delta_set_data_p[delta_change_num];

    // This set is used as the set for deduplicating already seen
    // key value pairs
    KeyValuePairBloomFilter delta_set{delta_set_data_p,
                                      key_value_pair_eq_obj,
                                      key_value_pair_hash_obj};
                                        
    /////////////////////////////////////////////////////////////////
    // Prepare Small Sorted Set
    /////////////////////////////////////////////////////////////////
    
    const LeafDataNode *sss_data_p[delta_change_num];
    
    auto f1 = [this](const LeafDataNode *ldn1, const LeafDataNode *ldn2) {
      // Compare using key first; if keys are equal then compare using
      // index (since we must pop those nodes in a key-order given the index)
      // NOTE: Since in leaf node keys are sorted, if keys are sorted then
      // indices only needs sorting inside the range of the key
      if(this->key_cmp_obj(ldn1->item.first, ldn2->item.first)) {
        return true;
      } else if(this->key_eq_obj(ldn1->item.first, ldn2->item.first)) {
        return ldn1->GetIndexPair().first < ldn2->GetIndexPair().first;
      } else {
        return false;
      }
    };
    
    // This is not used since in this case we do not need to compare
    // for equal relation
    auto f2 = [this](const LeafDataNode *ldn1, const LeafDataNode *ldn2) {
      (void)ldn1;
      (void)ldn2;
      
      assert(false);
      return false;
    };
    
    // Declare an sss object with the previously declared comparators and
    // null equality checkers (not used)
    SortedSmallSet<const LeafDataNode *, decltype(f1), decltype(f2)> \
      sss{sss_data_p, f1, f2};
    
    /////////////////////////////////////////////////////////////////
    // Start collecting values!
    /////////////////////////////////////////////////////////////////

    // We collect all valid values in present_set
    // and deleted_set is just for bookkeeping
    CollectAllValuesOnLeafRecursive(node_p,
                                    sss,
                                    delta_set,
                                    leaf_node_p);

    // Item count would not change during consolidation
    assert(static_cast<int>(leaf_node_p->data_list.size()) == \
           node_p->GetItemCount());

    return leaf_node_p;
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
   */
  template <typename T>  // To let the compiler deduce type of sss
  void
  CollectAllValuesOnLeafRecursive(const BaseNode *node_p,
                                  T &sss,
                                  KeyValuePairBloomFilter &delta_set,
                                  LeafNode *new_leaf_node_p) const {
    // The top node is used to derive high key
    // NOTE: Low key for Leaf node and its delta chain is nullptr
    const KeyNodeIDPair &high_key_pair = node_p->GetHighKeyPair();

    while(1) {
      NodeType type = node_p->GetType();

      switch(type) {
        // When we see a leaf node, just copy all keys together with
        // all values into the value set
        case NodeType::LeafType: {
          const LeafNode *leaf_node_p = \
            static_cast<const LeafNode *>(node_p);

          // We compute end iterator based on the high key
          typename std::vector<KeyValuePair>::const_iterator copy_end_it{};

          // If the high key is +Inf then all items could be copied
          if((high_key_pair.second == INVALID_NODE_ID)) {
            copy_end_it = leaf_node_p->data_list.end();
          } else {
            // This points copy_end_it to the first element >= current high key
            // If no such element exists then copy_end_it is end() iterator
            // which is also consistent behavior
            copy_end_it = std::lower_bound(leaf_node_p->data_list.begin(),
                                           leaf_node_p->data_list.end(),
                                           // It only compares key so we
                                           // just use high key pair
                                           std::make_pair(high_key_pair.first, ValueType{}),
                                           [this](const KeyValuePair &kvp1,
                                                  const KeyValuePair &kvp2) {
                                             return this->key_cmp_obj(kvp1.first, kvp2.first);
                                           });
          }
          
          // This is the index of the copy end it
          int copy_end_index = \
            static_cast<int>(copy_end_it - leaf_node_p->data_list.begin());
          int copy_start_index = 0;

          ///////////////////////////////////////////////////////////
          // Find the end index for sss
          ///////////////////////////////////////////////////////////

          // Find the end of copying
          auto sss_end_it = sss.GetEnd() - 1;

          // If the next key is +Inf then sss_end_it is the real end of the
          // sorted array
          if(high_key_pair.second != INVALID_NODE_ID) {
            // Corner case: If the first element is lower bound
            // then current_p will move outside
            // the valid range of the array but still we return
            // the first element in the array
            while(sss_end_it >= sss.GetBegin()) {
              if(key_cmp_obj((*sss_end_it)->item.first, high_key_pair.first) == true) {
                break;
              }

              sss_end_it--;
            }
          }

          // This points to the first element >= high key
          sss_end_it++;
          
          ///////////////////////////////////////////////////////////
          // Start merge loop
          ///////////////////////////////////////////////////////////

          // While the sss has not reached the end for this node
          while(sss.GetBegin() != sss_end_it) {
            int current_index = sss.GetFront()->GetIndexPair().first;
            
            // If we did not see any overwriting delta then
            // we also copy the old item in leaf node
            bool item_overwritten = false;
            
            assert(copy_start_index <= current_index);
            assert(current_index <= copy_end_index);
            
            // First copy all items before the current index
            new_leaf_node_p->data_list.insert(new_leaf_node_p->data_list.end(),
                                              leaf_node_p->data_list.begin() + copy_start_index,
                                              leaf_node_p->data_list.begin() + current_index);

            // Update copy start index for next copy
            copy_start_index = current_index;
            
            // Drain delta records on the same index
            while(sss.GetFront()->GetIndexPair().first == current_index) {
              // Update current status of the item on leaf base node
              // IndexPair.second == true if the value has been overwritten
              item_overwritten = item_overwritten || sss.GetFront()->GetIndexPair().second;
              
              // We only insert those in LeafInsertNode
              // and ignore all LeafDeleteNode
              if(sss.GetFront()->GetType() == NodeType::LeafInsertType) {
                // We remove the element from sss here
                new_leaf_node_p->data_list.push_back(sss.PopFront()->item);
              } else {
                assert(sss.GetFront()->GetType() == NodeType::LeafDeleteType);

                // ... and here
                sss.PopFront();
              }
              
              // If we have drained sss
              if(sss.GetBegin() == sss_end_it) {
                break;
              }
            }
            
            // If the element has been overwritten by some of the deltas
            // just advance the pointer
            if(item_overwritten == true) {
              copy_start_index++;
            }
          } // while sss has not reached the copy end
          
          // Also need to insert all other elements if there are some
          new_leaf_node_p->data_list.insert(new_leaf_node_p->data_list.end(),
                                            leaf_node_p->data_list.begin() + copy_start_index,
                                            leaf_node_p->data_list.begin() + copy_end_index);

          return;
        } // case LeafType
        case NodeType::LeafInsertType: {
          const LeafInsertNode *insert_node_p = \
            static_cast<const LeafInsertNode *>(node_p);

          if(delta_set.Exists(insert_node_p->item) == false) {
            delta_set.Insert(insert_node_p->item);

            sss.InsertNoDedup(insert_node_p);
          }

          node_p = insert_node_p->child_node_p;

          break;
        } // case LeafInsertType
        case NodeType::LeafDeleteType: {
          const LeafDeleteNode *delete_node_p = \
            static_cast<const LeafDeleteNode *>(node_p);

          if(delta_set.Exists(delete_node_p->item) == false) {
            delta_set.Insert(delete_node_p->item);

            sss.InsertNoDedup(delete_node_p);
          }

          node_p = delete_node_p->child_node_p;

          break;
        } // case LeafDeleteType
        case NodeType::LeafRemoveType: {
          bwt_printf("ERROR: LeafRemoveNode not allowed\n");

          assert(false);
        } // case LeafRemoveType
        case NodeType::LeafSplitType: {
          const LeafSplitNode *split_node_p = \
            static_cast<const LeafSplitNode *>(node_p);

          node_p = split_node_p->child_node_p;

          break;
        } // case LeafSplitType
        case NodeType::LeafMergeType: {
          const LeafMergeNode *merge_node_p = \
            static_cast<const LeafMergeNode *>(node_p);

          /**** RECURSIVE CALL ON LEFT AND RIGHT SUB-TREE ****/
          CollectAllValuesOnLeafRecursive(merge_node_p->child_node_p,
                                          sss,
                                          delta_set,
                                          new_leaf_node_p);

          CollectAllValuesOnLeafRecursive(merge_node_p->right_merge_p,
                                          sss,
                                          delta_set,
                                          new_leaf_node_p);

          return;
        } // case LeafMergeType
        default: {
          bwt_printf("ERROR: Unknown node type: %d\n",
                     static_cast<int>(type));

          assert(false);
        } // default
      } // switch
    } // while(1)

    return;
  }

  ///////////////////////////////////////////////////////////////////
  // Control Core
  ///////////////////////////////////////////////////////////////////

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
    assert(context_p->current_level >= 0);

    return &context_p->current_snapshot;
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
  inline NodeSnapshot *GetLatestParentNodeSnapshot(Context *context_p) {
    // Make sure the current node has a parent
    assert(context_p->current_level >= 1);

    // This is the address of the parent node
    return &context_p->parent_snapshot;
  }

  /*
   * IsOnLeftMostChild() - Returns true if latest node snapshot represents
   *                       the leftmost child in its parent
   *
   * This is done by checking whether the NodeID of the current node is
   * the same as the NodeID associated with the low key of its parent node
   *
   * NOTE: This function could only be called on a non-root node
   * since root node does not have any parent node
   */
  inline bool IsOnLeftMostChild(Context *context_p) {
    assert(context_p->current_level >= 1);

    return GetLatestParentNodeSnapshot(context_p)->node_p->GetLowKeyNodeID() == \
           GetLatestNodeSnapshot(context_p)->node_id;
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

    assert(context_p->HasParentNode());

    // Get last record which is the current node's context
    // and we must make sure the current node is not left mode node
    NodeSnapshot *snapshot_p = GetLatestNodeSnapshot(context_p);

    // Check currently we are on a remove node
    assert(snapshot_p->node_p->IsRemoveNode());

    // This is not necessarily true. e.g. if the parent node was merged into
    // its left sibling before we take the snapshot of its previou left child
    // then this no longer holds
    if(IsOnLeftMostChild(context_p)) {
      bwt_printf("Observed a remove node on left most child.\n"
                 "  Parent node must have been merged. ABORT\n");

      context_p->abort_flag = true;

      return;
    }

    // After here we know we are not on a left most child of the parent
    // InnerNode, and thus the low key is a valid key

    // We use this to test whether we have found the real
    // left sibling whose next node id equals this one
    const NodeID removed_node_id = snapshot_p->node_id;

    // After this point snapshot_p could be overwritten

    // Get its parent node
    NodeSnapshot *parent_snapshot_p = GetLatestParentNodeSnapshot(context_p);
    assert(parent_snapshot_p->IsLeaf() == false);

    // We either consolidate the parent node to get an inner node
    // or directly use the parent node_p if it is already inner node
    const InnerNode *inner_node_p = \
      static_cast<const InnerNode *>(parent_snapshot_p->node_p);

    // If the parent node is not inner node (i.e. has delta chain)
    // then consolidate it to get an inner node
    if(parent_snapshot_p->node_p->IsInnerNode() == false) {
      inner_node_p = \
        CollectAllSepsOnInner(parent_snapshot_p,
                              parent_snapshot_p->node_p->GetDepth() + 1);

      const BaseNode *old_node_p = parent_snapshot_p->node_p;

      // As an optimization, we CAS the consolidated version of InnerNode
      // here. If CAS fails, we SHOULD NOT delete the inner node immediately
      // since its value is still being used by this function
      // So we delay allocation by putting it into the garbage chain
      bool ret = InstallNodeToReplace(parent_snapshot_p->node_id,
                                      inner_node_p,
                                      old_node_p);

      // If CAS succeeds we update the node_p in parent snapshot
      // If CAS fails we delete inner node allocated in CollectAllSeps..()
      if(ret == true) {
        parent_snapshot_p->node_p = inner_node_p;

        epoch_manager.AddGarbageNode(old_node_p);
      } else {
        epoch_manager.AddGarbageNode(inner_node_p);
      }
    }

    // This returns the first iterator >= current low key
    // Note that we start searching on the second element
    auto it = std::find_if(inner_node_p->sep_list.begin() + 1,
                           inner_node_p->sep_list.end(),
                           [removed_node_id](const KeyNodeIDPair &knp) {
                             return knp.second == removed_node_id;
                           });

    // The removed node ID may not be found in the inner node
    if(it == inner_node_p->sep_list.end()) {
      context_p->abort_flag = true;

      return;
    }

    // it points to the current node (must be an exact match),
    // so it-- points to its possible left sibling
    it--;

    // Note that after this point the inner node is still being used since
    // we have iterator referring to the internal structure of the inner node
    // So we could not try CAS here

    // This is our starting point to traverse right
    NodeID left_sibling_id = it->second;


    // This might incur recursive update
    // We need to pass in the low key of left sibling node
    JumpToNodeID(left_sibling_id, context_p);

    if(context_p->abort_flag == true) {
      bwt_printf("JumpToLeftSibling()'s call to JumpToNodeID() ABORT\n")

      return;
    }

    // Read the potentially redirected snapshot
    // (but do not pop it - so we directly return)
    snapshot_p = GetLatestNodeSnapshot(context_p);

    // If the next node ID stored inside the "left sibling" does
    // not equal the removed node, then we know we have got to the
    // wrong left sibling, maybe because the parent node has changed
    // or because its left sibling has splited
    // In the latter case simply aborting seems to be a bad idea
    // but if it is the second case then aborting is beneficial
    if(removed_node_id != snapshot_p->node_p->GetNextNodeID()) {
      bwt_printf("Left sibling's next node ID does not match removed NodeID."
                 " ABORT\n");

      context_p->abort_flag = true;

      return;
    }

    return;
  }

  /*
   * TakeNodeSnapshot() - Take the snapshot of a node by pushing node information
   *
   * This function simply copies NodeID, and physical pointer into
   * the snapshot object. Node that the snapshot object itself is directly
   * constructed on the path list which is a vector. This avoids copying the
   * NodeSnapshot object from the stack to vector
   */
  void TakeNodeSnapshot(NodeID node_id,
                        Context *context_p) {
    const BaseNode *node_p = GetNode(node_id);

    bwt_printf("Is leaf node? - %d\n", node_p->IsOnLeafDeltaChain());

    // This is used to record how many levels we have traversed
    context_p->current_level++;

    // Copy assignment
    context_p->parent_snapshot = context_p->current_snapshot;

    context_p->current_snapshot.node_p = node_p;
    context_p->current_snapshot.node_id = node_id;

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
   */
  void UpdateNodeSnapshot(NodeID node_id,
                          Context *context_p) {
    const BaseNode *node_p = GetNode(node_id);

    // We operate on the latest snapshot instead of creating a new one
    NodeSnapshot *snapshot_p = GetLatestNodeSnapshot(context_p);

    // Assume we always use this function to traverse on the same
    // level
    assert(node_p->IsOnLeafDeltaChain() == snapshot_p->IsLeaf());

    // Make sure we are not switching to itself
    assert(snapshot_p->node_id != node_id);

    snapshot_p->node_id = node_id;
    snapshot_p->node_p = node_p;

    return;
  }

  /*
   * LoadNodeID() - Push a new snapshot for the node pointed to by node_id
   *
   * If we just want to modify existing snapshot object in the stack, we should
   * call JumpToNodeID() instead
   *
   * Note that there is currently no flag indicating whether the current node
   * being load is a leaf node or leftmost node or a root node. To check for
   * attributes mentioned above:
   *
   * (1) Call IsOnLeafDeltaChain() to determine whether we are on leaf node
   *     This call is lightweight and only requires an integer comparison
   *
   * (2) Check current node id with parent node's low key node id. If they
   *     match then we are on a left most child of the parent node
   *     (but that is only a strong hint; if the parent node is merged after
   *     being taken a snapshot then we will get a false positive, though it
   *     does not affect correctness)
   *
   * (3) Check current_level == 0 to determine whether we are on a root node
   */
  inline void LoadNodeID(NodeID node_id, Context *context_p) {
    bwt_printf("Loading NodeID = %lu\n", node_id);

    // This pushes a new snapshot into stack
    TakeNodeSnapshot(node_id, context_p);

    // If there are SMO (split; merge) that require much computation to
    // deal with (e.g. go to its parent and comsolidate parent first) then
    // we should aggressively comsolidate the SMO away to avoid further
    // access
    FinishPartialSMO(context_p);

    if(context_p->abort_flag == true) {
      return;
    }

    TryConsolidateNode(context_p);

    if(context_p->abort_flag == true) {
      return;
    }

    AdjustNodeSize(context_p);

    // Do not have to check for abort here because anyway we will return

    return;
  }

  /*
   * JumpToNodeID() - Given a NodeID, change the top of the path list
   *                  by loading the delta chain of the node ID
   *
   * NOTE: This function could also be called to traverse right, in which case
   * we need to check whether the target node is the left most child
   */
  void JumpToNodeID(NodeID node_id, Context *context_p) {
    bwt_printf("Jumping to node ID = %lu\n", node_id);

    // This updates the current snapshot in the stack
    UpdateNodeSnapshot(node_id, context_p);

    FinishPartialSMO(context_p);

    if(context_p->abort_flag == true) {
      return;
    }

    TryConsolidateNode(context_p);

    if(context_p->abort_flag == true) {
      return;
    }

    AdjustNodeSize(context_p);

    // Do not have to check for abort here because anyway we will return

    return;
  }

  ///////////////////////////////////////////////////////////////////
  // Read-Optimized functions
  ///////////////////////////////////////////////////////////////////

  /*
   * FinishPartialSMOReadOptimized() - Read optimized version of
   *                                   FinishPartialSMO()
   *
   * This function only delas with remove delta and abort node
   */
  void FinishPartialSMOReadOptimized(Context *context_p) {
    // Note: If the top of the path list changes then this pointer
    // must also be updated
    NodeSnapshot *snapshot_p = GetLatestNodeSnapshot(context_p);

before_switch:
    switch(snapshot_p->node_p->GetType()) {
      case NodeType::InnerAbortType: {
        bwt_printf("Observed Inner Abort Node; ABORT\n");

        snapshot_p->node_p = \
          (static_cast<const DeltaNode *>(snapshot_p->node_p))->child_node_p;

        goto before_switch;
      }
      case NodeType::LeafRemoveType:
      case NodeType::InnerRemoveType: {
        bwt_printf("Helping along remove node...\n");

        // The right branch for merging is the child node under remove node
        const BaseNode *merge_right_branch = \
          (static_cast<const DeltaNode *>(snapshot_p->node_p))->child_node_p;

        // This will also be recorded in merge delta such that when
        // we finish merge delta we could recycle the node id as well
        // as the RemoveNode
        NodeID deleted_node_id = snapshot_p->node_id;

        JumpToLeftSibling(context_p);

        // If this aborts then we propagate this to the state machine driver
        if(context_p->abort_flag == true) {
          bwt_printf("Jump to left sibling in Remove help along ABORT\n");

          return;
        }

        // That is the left sibling's snapshot
        NodeSnapshot *left_snapshot_p = GetLatestNodeSnapshot(context_p);

        // Update snapshot pointer if we fall through to posting
        // index term delete delta for merge node
        snapshot_p = left_snapshot_p;

        // This serves as the merge key (for left sibling there is always a
        // valid high key)
        const KeyType &merge_key = snapshot_p->node_p->GetHighKey();

        // This holds the merge node if installation is successful
        // Not changed if CAS fails
        const BaseNode *merge_node_p;

        bool ret;

        // If we are currently on leaf, just post leaf merge delta
        if(left_snapshot_p->IsLeaf() == true) {
          ret = \
            PostMergeNode<LeafMergeNode>(left_snapshot_p,
                                         &merge_key,
                                         merge_right_branch,
                                         deleted_node_id,
                                         &merge_node_p);
        } else {
          ret = \
            PostMergeNode<InnerMergeNode>(left_snapshot_p,
                                          &merge_key,
                                          merge_right_branch,
                                          deleted_node_id,
                                          &merge_node_p);
        }

        // If CAS fails just abort and return
        if(ret == true) {
          bwt_printf("Merge delta CAS succeeds. ABORT\n");

          context_p->abort_flag = true;

          return;
        } else {
          bwt_printf("Merge delta CAS fails. ABORT\n");

          context_p->abort_flag = true;

          return;
        } // if ret == true

        assert(false);
      } // case Inner/LeafRemoveType
      default: {
        return;
      }
    } // switch

    assert(false);
    return;
  }

  /*
   * LoadNodeIDReadOptimized() - Read optimized version of LoadNodeID()
   *
   * This only SMO that a read only thread cares about is remove delta,
   * since it has to jump to the left sibling in order to keep traversing
   * down. However, jumping to left sibling has the possibility to fail
   * so we still need to check abort flag after this function returns
   */
  inline void LoadNodeIDReadOptimized(NodeID node_id, Context *context_p) {
    bwt_printf("Loading NodeID (RO) = %lu\n", node_id);

    // This pushes a new snapshot into stack
    TakeNodeSnapshot(node_id, context_p);

    FinishPartialSMOReadOptimized(context_p);

    // Do not need to check for abort flag here

    return;
  }

  /*
   * TraverseReadOptimized() - Read optimized tree traversal
   *
   * This function differs from the standard version in a sense that it
   * does not try to adjust node size and consolidate node - what it does
   * is just navigating inner node and leaf node and return values for the
   * given search key.
   *
   * However, there is one thing we must do, that is to jump to the left
   * sibling of the current node when we see a remove delta. This is inevitable
   * even if the thread is read only, since otherwise traversing down
   * would become impossible.
   */
  inline void TraverseReadOptimized(Context *context_p,
                                    std::vector<ValueType> *value_list_p) {
retry_traverse:
    assert(context_p->abort_flag == false);
    assert(context_p->current_level == -1);

    // This is the serialization point for reading/writing root node
    NodeID child_node_id = root_id.load();
    
    LoadNodeIDReadOptimized(child_node_id, context_p);

    if(context_p->abort_flag == true) {
      bwt_printf("LoadNodeID aborted on loading root (RO)\n");

      goto abort_traverse;
    }
    
    bwt_printf("Successfully loading root node ID (RO)\n");

    while(1) {
      child_node_id = NavigateInnerNode(context_p);

      // Navigate could abort since it might go to another NodeID
      // if there is a split delta and the key is >= split key
      if(context_p->abort_flag == true) {
        bwt_printf("Navigate Inner Node abort (RO)\n");

        // If NavigateInnerNode() aborts then it retrns INVALID_NODE_ID
        // as a double check
        // This is the only situation that this function returns
        // INVALID_NODE_ID
        assert(child_node_id == INVALID_NODE_ID);

        goto abort_traverse;
      }
      
      // This might load a leaf child
      // Also LoadNodeID() does not guarantee the node bound matches
      // search key. Since we could readjust using the split side link
      // during Navigate...Node(), or abort if we reach the bottom
      // while still observing an inconsistent high key
      // (low key is always consistent)
      LoadNodeIDReadOptimized(child_node_id, context_p);

      if(context_p->abort_flag == true) {
        bwt_printf("LoadNodeID aborted (RO). ABORT\n");

        goto abort_traverse;
      }

      // This is the node we have just loaded
      NodeSnapshot *snapshot_p = GetLatestNodeSnapshot(context_p);

      if(snapshot_p->IsLeaf() == true) {
        bwt_printf("The next node is a leaf (RO)\n");

        NavigateLeafNode(context_p, *value_list_p);

        if(context_p->abort_flag == true) {
          bwt_printf("NavigateLeafNode aborts (RO). ABORT\n");

          goto abort_traverse;
        }

        bwt_printf("Found leaf node (RO). Abort count = %d, level = %d\n",
                   context_p->abort_counter,
                   context_p->current_level);

        // If there is no abort then we could safely return
        return;
      }
    } // while(1)

abort_traverse:
    assert(context_p->current_level >= 0);

    context_p->current_level = -1;

    context_p->abort_flag = false;
    context_p->abort_counter++;

    goto retry_traverse;

    assert(false);
    return;
  }

  ///////////////////////////////////////////////////////////////////
  ///////////////////////////////////////////////////////////////////
  ///////////////////////////////////////////////////////////////////

  /*
   * PostInnerInsertNode() - Posts an InnerInsertNode on the parent node
   *
   * This is used to finish the partial node split SMO as part of the help
   * along protocol
   *
   * NOTE: This function may abort
   */
  inline bool PostInnerInsertNode(Context *context_p,
                                  const KeyNodeIDPair &insert_item,
                                  const KeyNodeIDPair &next_item) {
    // We post on the parent node, after which we check for size and decide whether
    // to consolidate and/or split the node
    NodeSnapshot *parent_snapshot_p = GetLatestParentNodeSnapshot(context_p);

    // Three arguments are: inserted key-node id pair
    //                      next key-node id pair
    //                      child node in delta chain
    const InnerInsertNode *insert_node_p = \
      new InnerInsertNode{insert_item,
                          next_item,
                          parent_snapshot_p->node_p};

    // CAS Index Term Insert Delta onto the parent node
    bool ret = InstallNodeToReplace(parent_snapshot_p->node_id,
                                    insert_node_p,
                                    parent_snapshot_p->node_p);

    if(ret == true) {
      bwt_printf("Index term insert (from %lu to %lu) delta CAS succeeds\n",
                 GetLatestNodeSnapshot(context_p)->node_id,
                 insert_item.second);

      // Update parent node pointer to reflect the fact that it has been
      // updated
      parent_snapshot_p->node_p = insert_node_p;

      ConsolidateNode(GetLatestNodeSnapshot(context_p));

      return true;
    } else {
      bwt_printf("Index term insert (from %lu to %lu) delta CAS failed. "
                 "ABORT\n",
                 GetLatestNodeSnapshot(context_p)->node_id,
                 insert_item.second);

      // Set abort, and remove the newly created node
      context_p->abort_flag = true;
      delete insert_node_p;

      return false;
    } // if CAS succeeds/fails

    assert(false);
    return false;
  }

  /*
   * PostInnerDeleteNode() - Posts an InnerDeleteNode on the current parent node
   *
   * This function is called to complete node merges as part of the SMO help-along
   * protocol. It posts an InnerDeleteNode on the parent node and returns the result
   * of CAS instruction
   *
   * NOTE: This function may abort
   */
  inline bool PostInnerDeleteNode(Context *context_p,
                                  const KeyNodeIDPair &delete_item,
                                  const KeyNodeIDPair &prev_item,
                                  const KeyNodeIDPair &next_item) {
    NodeSnapshot *parent_snapshot_p = GetLatestParentNodeSnapshot(context_p);

    // Arguments are:
    // Deleted item; Prev item; next item (NodeID not used for next item)
    // and delta chail child node
    const InnerDeleteNode *delete_node_p = \
      new InnerDeleteNode{delete_item,
                          prev_item,
                          next_item,
                          parent_snapshot_p->node_p};

    // Assume parent has not changed, and CAS the index term delete delta
    // If CAS failed then parent has changed, and we have no idea how it
    // could be modified. The safest way is just abort
    bool ret = InstallNodeToReplace(parent_snapshot_p->node_id,
                                    delete_node_p,
                                    parent_snapshot_p->node_p);

    // If we installed the IndexTermDeleteDelta successfully the next
    // step is to put the remove node into garbage chain
    // and also recycle the deleted NodeID since now no new thread
    // could access that NodeID until it is reused
    if(ret == true) {
      bwt_printf("Index term delete delta installed, ID = %lu; ABORT\n",
                 parent_snapshot_p->node_id);

      // The deleted node ID must resolve to a RemoveNode of either
      // Inner or Leaf category
      const BaseNode *garbage_node_p = GetNode(delete_item.second);
      assert(garbage_node_p->IsRemoveNode());

      // Put the remove node into garbage chain
      // This will not remove the child node of the remove node, which
      // should be removed together with the merge node above it
      // Also, the remove node acts as a container for removed NodeID
      // which will be recycled when the remove node is recycled
      epoch_manager.AddGarbageNode(garbage_node_p);

      // To avoid this entry being recycled during tree destruction
      // Since the entry still remains in the old inner base node
      // but we should have already deleted it
      // NOTE: We could not do it here since some thread will fetch
      // nullptr from the mapping table
      // We should recycle the NodeID using EpochManager
      // and requires that epoch manager finishes all epoches before
      // the tree is destroyed
      //mapping_table[deleted_node_id] = nullptr;

      parent_snapshot_p->node_p = delete_node_p;

      ConsolidateNode(GetLatestNodeSnapshot(context_p));

      return true;
    } else {
      bwt_printf("Index term delete delta install failed. ABORT\n");

      // DO NOT FORGET TO DELETE THIS
      delete delete_node_p;

      // The caller just returns after this function, so do not have
      // to branch on abort_flag after this function returns
      context_p->abort_flag = true;

      return false;
    }

    assert(false);
    return false;
  }

  /*
   * FinishPartialSMO() - Finish partial completed SMO if there is one
   *
   * This function defines the help-along protocol, i.e. if we find
   * a SMO on top of the delta chain, we should help-along that SMO before
   * doing our own job. This might incur a recursive call of this function.
   *
   * If this function returns true, then a node consolidation on the current
   * top snapshot is recommended, because we have seen and processed (or
   * confirmed that it has been processed) a merge/split delta on current
   * top snapshot. To prevent other threads from seeing them and taking
   * a full snapshot of the parent snapshot everytime they are seen, we
   * return true to inform caller to do a consolidation
   *
   * If we see a remove node, then the actual NodeID pushed into the path
   * list stack may not equal the NodeID passed in this function. So when
   * we need to read NodeID, always use the one stored in the NodeSnapshot
   * vector instead of using a previously passed one
   */
  void FinishPartialSMO(Context *context_p) {
    // Note: If the top of the path list changes then this pointer
    // must also be updated
    NodeSnapshot *snapshot_p = GetLatestNodeSnapshot(context_p);

before_switch:
    switch(snapshot_p->node_p->GetType()) {
      case NodeType::InnerAbortType: {
        bwt_printf("Observed Inner Abort Node; ABORT\n");

        // This is an optimization - when seeing an ABORT
        // node, we continue but set the physical pointer to be ABORT's
        // child, to make CAS always fail on this node to avoid
        // posting on ABORT, especially posting split node
        snapshot_p->node_p = \
          (static_cast<const DeltaNode *>(snapshot_p->node_p))->child_node_p;

        goto before_switch;
      }
      case NodeType::LeafRemoveType:
      case NodeType::InnerRemoveType: {
        bwt_printf("Helping along remove node...\n");

        // The right branch for merging is the child node under remove node
        const BaseNode *merge_right_branch = \
          (static_cast<const DeltaNode *>(snapshot_p->node_p))->child_node_p;

        // This will also be recorded in merge delta such that when
        // we finish merge delta we could recycle the node id as well
        // as the RemoveNode
        NodeID deleted_node_id = snapshot_p->node_id;

        JumpToLeftSibling(context_p);

        // If this aborts then we propagate this to the state machine driver
        if(context_p->abort_flag == true) {
          bwt_printf("Jump to left sibling in Remove help along ABORT\n");

          // Here we are uncertain about the current status (it might have
          // jumped and observed an inconsistent next node ID, or it has not
          // jumped because of an inconsistent left child status)
          return;
        }

        // That is the left sibling's snapshot
        NodeSnapshot *left_snapshot_p = GetLatestNodeSnapshot(context_p);

        // Update snapshot pointer if we fall through to posting
        // index term delete delta for merge node
        snapshot_p = left_snapshot_p;

        // This serves as the merge key
        // Note that the left sibling node must have a valid high key
        const KeyType &merge_key = snapshot_p->node_p->GetHighKey();

        // This holds the merge node if installation is successful
        // Not changed (i.e.undefined) if CAS fails
        const BaseNode *merge_node_p = nullptr;

        bool ret;

        // If we are currently on leaf, just post leaf merge delta
        if(left_snapshot_p->IsLeaf() == true) {
          ret = \
            PostMergeNode<LeafMergeNode>(left_snapshot_p,
                                         &merge_key,
                                         merge_right_branch,
                                         deleted_node_id,
                                         &merge_node_p);
        } else {
          ret = \
            PostMergeNode<InnerMergeNode>(left_snapshot_p,
                                          &merge_key,
                                          merge_right_branch,
                                          deleted_node_id,
                                          &merge_node_p);
        }

        // If CAS fails just abort and return
        if(ret == true) {
          bwt_printf("Merge delta CAS succeeds. "
                     "Continue to finish merge SMO\n");

          left_snapshot_p->node_p = merge_node_p;

          // merge_node_p is set as the newest merge node above
          // after this point
          // Also snapshot_p has been to left_snapshot_p above
        } else {
          bwt_printf("Merge delta CAS fails. ABORT\n");

          context_p->abort_flag = true;

          return;
        } // if ret == true

        //
        // FALL THROUGH IF POSTING MERGE DELTA SUCCEEDS
        //

      } // case Inner/LeafRemoveType
      case NodeType::InnerMergeType:
      case NodeType::LeafMergeType: {
        bwt_printf("Helping along merge delta\n");

        // First consolidate parent node and find the left/right
        // sep pair plus left node ID
        NodeSnapshot *parent_snapshot_p = \
          GetLatestParentNodeSnapshot(context_p);

        // This is necessary - make sure the parent node snapshot
        // is always update to date such that we do not miss a
        // late InnerInsertNode that actually posts the deleted item
        // o.w. the thread will just continue and post on top of
        // an unfinished merge delta
        if(parent_snapshot_p->node_p != GetNode(parent_snapshot_p->node_id)) {
          context_p->abort_flag = true;
          
          return;
        }

        // This is the item being deleted inside parent node
        const KeyNodeIDPair *delete_item_p = nullptr;

        // Type of the merge delta
        // This is important since we might fall through
        // from the LeafRemoveNode/InnerRemoveNode branch
        NodeType type = snapshot_p->node_p->GetType();
        
        if(type == NodeType::InnerMergeType) {
          const InnerMergeNode *merge_node_p = \
            static_cast<const InnerMergeNode *>(snapshot_p->node_p);

          delete_item_p = &merge_node_p->delete_item;
        } else if(type == NodeType::LeafMergeType) {
          const LeafMergeNode *merge_node_p = \
            static_cast<const LeafMergeNode *>(snapshot_p->node_p);

          delete_item_p = &merge_node_p->delete_item;
        } else {
          bwt_printf("ERROR: Illegal node type: %d\n",
                     static_cast<int>(type));

          assert(false);
        } // If on type of merge node

        // These two are set to be pointing to the prev and next KeyNodeIDPair
        // Note that for prev key id pair the key does not matter much since
        // we could use the node ID later in the InnerDeleteNode to determine
        // whether we have hit the left most sep or not
        const KeyNodeIDPair *prev_item_p;
        const KeyNodeIDPair *next_item_p;

        // if this is false then we have already deleted the index term
        bool merge_key_found = \
          FindMergePrevNextKey(parent_snapshot_p,
                               delete_item_p,
                               &prev_item_p,
                               &next_item_p);

        // If merge key is not found then we know we have already deleted the
        // index term
        if(merge_key_found == false) {
          bwt_printf("Index term is absent; No need to remove\n");

          // If we have seen a merge delta but did not find
          // corresponding sep in parent then it has already been removed
          // so we propose a consolidation on current node to
          // get rid of the merge delta
          //ConsolidateNode(snapshot_p);

          return;
        }

        // It will post an InnerDeleteNode on the parent node
        // and the return value is the result of CAS
        // Note: Even if this function aborts, since we return immediately
        // so do not have to test abort_flag here
        PostInnerDeleteNode(context_p,
                            *delete_item_p,
                            *prev_item_p,
                            *next_item_p);

        return;
      } // case Inner/LeafMergeNode
      case NodeType::InnerSplitType:
      case NodeType::LeafSplitType: {
        bwt_printf("Helping along split node\n");

        // These two will be stored inside InnerInsertNode
        // The insert item is just the split item inside InnerSplitNode
        // but next item needs to be read from the parent node
        const KeyNodeIDPair *insert_item_p;

        // This is set to use the high key pair of the node under
        // split delta
        const KeyNodeIDPair *next_item_p;

        NodeType type = snapshot_p->node_p->GetType();

        // NOTE: depth should not be read here, since we
        // need to know the depth on its parent node
        if(type == NodeType::InnerSplitType) {
          const InnerSplitNode *split_node_p = \
            static_cast<const InnerSplitNode *>(snapshot_p->node_p);

          insert_item_p = &split_node_p->insert_item;
          next_item_p = &split_node_p->child_node_p->GetHighKeyPair();
        } else {
          const LeafSplitNode *split_node_p = \
            static_cast<const LeafSplitNode *>(snapshot_p->node_p);

          insert_item_p = &split_node_p->insert_item;
          next_item_p = &split_node_p->child_node_p->GetHighKeyPair();
        }

        assert(context_p->current_level >= 0);

        if(context_p->current_level == 0) {
          /***********************************************************
           * Root splits (don't have to consolidate parent node)
           ***********************************************************/
          bwt_printf("Root splits!\n");

          // Allocate a new node ID for the newly created node
          // If CAS fails we need to free the root ID
          NodeID new_root_id = GetNextNodeID();

          // InnerNode requires high key pair which is +Inf, INVALID NODE ID
          // low key pair will be set inside the constructor to be pointing
          // to the first element in the sep list
          // NOTE: Storage will be automatically reserved inside the
          // constructor
          #ifdef BWTREE_PELOTON

          InnerNode *inner_node_p = \
            new InnerNode{std::make_pair(KeyType(), INVALID_NODE_ID), 2};

          // NOTE: Since we never directly access the first element in the
          // InnerNode object, it is OK to just put an empty KeyType
          // object into the list
          inner_node_p->sep_list.push_back(std::make_pair(KeyType(),
                                                          snapshot_p->node_id));

          #else

          InnerNode *inner_node_p = \
            new InnerNode{std::make_pair(KeyType{}, INVALID_NODE_ID), 2};

          // NOTE: Since we never directly access the first element in the
          // InnerNode object, it is OK to just put an empty KeyType
          // object into the list
          inner_node_p->sep_list.push_back(std::make_pair(KeyType{},
                                                          snapshot_p->node_id));
          #endif

          inner_node_p->sep_list.push_back(*insert_item_p);

          // This needs to be done here to avoid some unfortunate thread
          // seeing an un-updated tree height and overflowed its stack
          tree_height.fetch_add(1);

          // First we need to install the new node with NodeID
          // This makes it visible
          InstallNewNode(new_root_id, inner_node_p);
          bool ret = InstallRootNode(snapshot_p->node_id,
                                     new_root_id);

          if(ret == true) {
            bwt_printf("Install root CAS succeeds. Height = %lu\n",
                       tree_height.load());

            // After installing new root we abort in order to load
            // the new root
            context_p->abort_flag = true;

            return;
          } else {
            bwt_printf("Install root CAS failed. ABORT\n");

            // If install fails we just sub 1 from the tree height
            tree_height.fetch_sub(1);

            // We need to make a new remove node and send it into EpochManager
            // for recycling the NodeID
            const InnerRemoveNode *fake_remove_node_p = \
              new InnerRemoveNode{new_root_id, inner_node_p};

            // Put the remove node into garbage chain, because
            // we cannot call InvalidateNodeID() here
            epoch_manager.AddGarbageNode(fake_remove_node_p);

            delete inner_node_p;

            context_p->abort_flag = true;

            return;
          } // if CAS succeeds/fails
        } else {
          /***********************************************************
           * Index term insert for non-root nodes
           ***********************************************************/

          // First consolidate parent node and find the right sep
          NodeSnapshot *parent_snapshot_p = \
            GetLatestParentNodeSnapshot(context_p);

          // If this is false then we know the index term has already
          // been inserted
          // This could also abort
          bool should_post = \
            CheckSplitItem(context_p, parent_snapshot_p, *insert_item_p);

          // If it aborts then we know there is an item in the parent
          // node that has the same key but different NodeID
          // This is totally legal
          if(context_p->abort_flag == true) {
            return;
          }

          // If the split key is out of range then we know the parent node has
          // changed
          if(should_post == false) {
            bwt_printf("Check split item failed\n");

            // Consolidate out the split node
            //ConsolidateNode(snapshot_p);

            // Do not need to abort here, since though the parent node
            // has changed, the range for the search key on the parent is
            // still correct

            return;
          }

          // Post InnerInsertNode on the parent node. If return value is true then
          // posting is successful
          // Also if this returns true then we have successfully completed split SMO
          // which means the SMO could be removed by a consolidation to avoid
          // consolidating the parent node again and again
          // Note: Even if this function aborts, since we return immediately
          // so do not have to test abort_flag here
          PostInnerInsertNode(context_p, *insert_item_p, *next_item_p);

          return;
        } // if split root / else not split root
      } // case split node
      default: {
        return;
        // By default we do not do anything special
        break;
      }
    } // switch

    assert(false);
    return;
  }
  
  /*
   * ConsolidateLeafNode() - Consolidates a leaf delta chian unconditionally
   *
   * This function does not check delta chain size
   */
  inline void ConsolidateLeafNode(NodeSnapshot *snapshot_p) {
    assert(snapshot_p->node_p->IsOnLeafDeltaChain() == true);
    
    LeafNode *leaf_node_p = CollectAllValuesOnLeaf(snapshot_p);

    bool ret = InstallNodeToReplace(snapshot_p->node_id,
                                    leaf_node_p,
                                    snapshot_p->node_p);

    if(ret == true) {
      epoch_manager.AddGarbageNode(snapshot_p->node_p);

      snapshot_p->node_p = leaf_node_p;
    } else {
      delete leaf_node_p;
    }
    
    return;
  }
  
  /*
   * ConsolidateInnerNode() - Consolidates inner node unconditionally
   *
   * This function does not check for inner delta chain length
   */
  inline void ConsolidateInnerNode(NodeSnapshot *snapshot_p) {
    assert(snapshot_p->node_p->IsOnLeafDeltaChain() == false);
    
    InnerNode *inner_node_p = CollectAllSepsOnInner(snapshot_p);

    bool ret = InstallNodeToReplace(snapshot_p->node_id,
                                    inner_node_p,
                                    snapshot_p->node_p);

    if(ret == true) {
      epoch_manager.AddGarbageNode(snapshot_p->node_p);

      snapshot_p->node_p = inner_node_p;
    } else {
      delete inner_node_p;
    }
    
    return;
  }

  /*
   * ConsolidateNode() - Consolidates current node unconditionally
   *
   * This function is called after finishing split/merge SMO, since we
   * want to prevent other threads from seeing the finished SMO and
   * do an useless consolidation on the parent node
   *
   * NOTE: This function does not return any value reflecting the status
   * of CAS operation, since consolidation is an optional operation, and it
   * would not have any effect even if it fails
   */
  void ConsolidateNode(NodeSnapshot *snapshot_p) {
    if(snapshot_p->node_p->IsOnLeafDeltaChain() == true) {
      ConsolidateLeafNode(snapshot_p);
    } else {
      ConsolidateInnerNode(snapshot_p);
    } // if on leaf/inner node

    return;
  }

  /*
   * TryConsolidateNode() - Consolidate current node if its length exceeds the
   *                        threshold value
   *
   * If the length of delta chain does not exceed the threshold then this
   * function does nothing
   *
   * If function returns true then we have successfully consolidated a node
   * Otherwise no consolidation happens. The return value might be used
   * as a hint for deciding whether to adjust node size or not
   *
   * NOTE: If consolidation fails then this function does not do anything
   * and will just continue with its own job. There is no chance of abort
   *
   * TODO: If strict mode is on, then whenever consolidation fails we should
   * always abort and start from the beginning, to keep delta chain length
   * upper bound intact
   */
  void TryConsolidateNode(Context *context_p) {
    NodeSnapshot *snapshot_p = GetLatestNodeSnapshot(context_p);

    // Do not overwrite this pointer since we will use this
    // to locate garbage delta chain
    const BaseNode *node_p = snapshot_p->node_p;

    // We could only perform consolidation on delta node
    // because we want to see depth field
    if(node_p->IsDeltaNode() == false) {
      // The depth of base node may not be 0
      // since if we consolidate parent node to finish the partial SMO,
      // then parent node will have non-0 depth in order to avoid being too
      // large (see FindSplitNextKey() and FindMergePrevNextKey() and
      // JumpToLeftSibling())
      // assert(node_p->metadata.depth == 0);

      return;
    }

    // If depth does not exceed threshold then we check recommendation flag
    int depth = node_p->GetDepth();

    if(snapshot_p->IsLeaf() == true) {
      if(depth < LEAF_DELTA_CHAIN_LENGTH_THRESHOLD) {
        return;
      }
    } else {
      if(depth < INNER_DELTA_CHAIN_LENGTH_THRESHOLD) {
        return;
      }
    }

    // After this point we decide to consolidate node

    ConsolidateNode(snapshot_p);

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

    // We do not adjust size for delta nodes
    if(node_p->IsDeltaNode() == true) {
      // TODO: If we want to strictly enforce the size of any node
      // then we should aggressively consolidate here and always
      // Though it brings huge overhead because now we are consolidating
      // every node on the path

      return;
    }

    NodeID node_id = snapshot_p->node_id;

    if(snapshot_p->IsLeaf() == true) {
      const LeafNode *leaf_node_p = \
        static_cast<const LeafNode *>(node_p);

      // NOTE: We use key number as the size of the node
      // because using item count might result in a very unstable
      // split, in a sense that we could not always split the node
      // evenly, and in the worst case if there is one key in the
      // node the node could not be splited while having a large
      // item count
      size_t node_size = leaf_node_p->GetItemCount();

      // Perform corresponding action based on node size
      if(node_size >= LEAF_NODE_SIZE_UPPER_THRESHOLD) {
        bwt_printf("Node size >= leaf upper threshold. Split\n");

        // Note: This function takes this as argument since it will
        // do key comparison
        const LeafNode *new_leaf_node_p = leaf_node_p->GetSplitSibling(this);

        // If the new leaf node pointer is nullptr then it means the
        // although the size of the leaf node exceeds split threshold
        // but we could not find a spliting point that evenly or almost
        // evenly divides the key space into two siblings whose sizes
        // are both larger than the merge threshold (o.w. the node will
        // be immediately merged)
        // NOTE: This is a potential problem if a leaf becomes very unbalanced
        // since all threads will try to split the leaf node when traversing
        // to it (not for reader threads)
        if(new_leaf_node_p == nullptr) {
          bwt_printf("LeafNode size exceeds overhead, "
                     "but could not find split point\n");

          return;
        }

        // Since we would like to access its first element to get the low key
        assert(new_leaf_node_p->data_list.size() > 0UL);

        // The split key must be a valid key
        // Note that the lowkey for leaf node is not defined, so in the
        // case that it is required we must manually goto its data list
        // and find the low key in its leftmost element
        const KeyType &split_key = new_leaf_node_p->data_list[0].first;

        // If leaf split fails this should be recyced using a fake remove node
        NodeID new_node_id = GetNextNodeID();

        const LeafSplitNode *split_node_p = \
          new LeafSplitNode{std::make_pair(split_key, new_node_id),
                            node_p,
                            // We need this to compute the item count
                            // of the current node being splited
                            new_leaf_node_p};

        //  First install the NodeID -> split sibling mapping
        // If CAS fails we also need to recycle the node ID allocated here
        InstallNewNode(new_node_id, new_leaf_node_p);

        // Then CAS split delta into current node's NodeID
        bool ret = InstallNodeToReplace(node_id, split_node_p, node_p);

        if(ret == true) {
          bwt_printf("Leaf split delta (from %lu to %lu) CAS succeeds. ABORT\n",
                     node_id,
                     new_node_id);

          // TODO: WE ABORT HERE TO AVOID THIS THREAD POSTING ANYTHING
          // ON TOP OF IT WITHOUT HELPING ALONG AND ALSO BLOCKING OTHER
          // THREAD TO HELP ALONG
          context_p->abort_flag = true;

          return;
        } else {
          bwt_printf("Leaf split delta CAS fails\n");

          // Need to use the epoch manager to recycle NodeID
          // NOTE: DO NOT CHANGE THE NEW LEAF NODE since the
          // constructor needs some pointer to initialize
          // the high key and low key
          const LeafRemoveNode *fake_remove_node_p = \
            new LeafRemoveNode{new_node_id, new_leaf_node_p};

          epoch_manager.AddGarbageNode(fake_remove_node_p);

          // We have two nodes to delete here
          delete split_node_p;
          delete new_leaf_node_p;

          return;
        }

      } else if(node_size <= LEAF_NODE_SIZE_LOWER_THRESHOLD) {
        // This might yield a false positive of left child
        // but correctness is not affected - sometimes the merge is delayed
        if(IsOnLeftMostChild(context_p) == true) {
          bwt_printf("Left most leaf node cannot be removed\n");

          return;
        }

        // After this point we decide to remove leaf node

        bwt_printf("Node size <= leaf lower threshold. Remove\n");

        // Install an abort node on parent
        const BaseNode *abort_node_p;
        const BaseNode *abort_child_node_p;
        NodeID parent_node_id;

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

        const LeafRemoveNode *remove_node_p = \
          new LeafRemoveNode{node_id, node_p};

        bool ret = InstallNodeToReplace(node_id, remove_node_p, node_p);
        if(ret == true) {
          bwt_printf("LeafRemoveNode CAS succeeds. ABORT.\n");

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
      const InnerNode *inner_node_p = static_cast<const InnerNode *>(node_p);

      size_t node_size = inner_node_p->sep_list.size();

      if(node_size >= INNER_NODE_SIZE_UPPER_THRESHOLD) {
        bwt_printf("Node size >= inner upper threshold. Split\n");

        const InnerNode *new_inner_node_p = inner_node_p->GetSplitSibling();

        // Since this is a split sibling, the low key must be a valid key
        // NOTE: Only for InnerNodes could we call GetLowKey()
        const KeyType &split_key = new_inner_node_p->GetLowKey();

        // New node has at least one item (this is the basic requirement)
        assert(new_inner_node_p->sep_list.size() > 0);

        const KeyNodeIDPair &first_item = new_inner_node_p->sep_list[0];
        // This points to the left most node on the right split sibling
        // If this node is being removed then we abort
        NodeID split_key_child_node_id = first_item.second;

        // This must be the split key
        assert(KeyCmpEqual(first_item.first, split_key));

        // NOTE: WE FETCH THE POINTER WITHOUT HELP ALONG SINCE WE ARE
        // NOW ON ITS PARENT
        const BaseNode *split_key_child_node_p = \
          GetNode(split_key_child_node_id);

        // If the type is remove then we just continue without abort
        // If we abort then it might introduce deadlock
        if(split_key_child_node_p->IsRemoveNode()) {
          bwt_printf("Found a removed node on split key child. CONTINUE \n");

          // NOTE: There was a memory leak caused by not removing the node
          // allocared above
          delete new_inner_node_p;

          return;
        }

        NodeID new_node_id = GetNextNodeID();

        const InnerSplitNode *split_node_p = \
          new InnerSplitNode{std::make_pair(split_key, new_node_id),
                             node_p,
                             // We need this to compute item count of the
                             // current node being splited
                             new_inner_node_p};

        //  First install the NodeID -> split sibling mapping
        InstallNewNode(new_node_id, new_inner_node_p);

        // Then CAS split delta into current node's NodeID
        bool ret = InstallNodeToReplace(node_id, split_node_p, node_p);

        if(ret == true) {
          bwt_printf("Inner split delta (from %lu to %lu) CAS succeeds."
                     " ABORT\n", node_id, new_node_id);

          // Same reason as in leaf node
          context_p->abort_flag = true;

          return;
        } else {
          bwt_printf("Inner split delta CAS fails\n");

          // Use the epoch manager to recycle NodeID in single threaded
          // environment
          const InnerRemoveNode *fake_remove_node_p = \
            new InnerRemoveNode{new_node_id, new_inner_node_p};

          epoch_manager.AddGarbageNode(fake_remove_node_p);

          // We have two nodes to delete here
          delete split_node_p;
          delete new_inner_node_p;

          return;
        } // if CAS fails
      } else if(node_size <= INNER_NODE_SIZE_LOWER_THRESHOLD) {
        if(context_p->current_level == 0) {
          bwt_printf("Root underflow - let it be\n");

          return;
        }

        // NOTE: If the parent node has changed (e.g. split on the current
        // node's low key) we will not be able to catch it. But we will
        // find it later by posting an InnerAbortNode on parent which would
        // result in CAS failing and aborting

        // We could not remove leftmost node
        if(IsOnLeftMostChild(context_p) == true) {
          bwt_printf("Left most inner node cannot be removed\n");

          return;
        }

        // After this point we decide to remove

        bwt_printf("Node size <= inner lower threshold. Remove\n");

        // Then we abort its parent node
        // These two are used to hold abort node and its previous child
        const BaseNode *abort_node_p;
        const BaseNode *abort_child_node_p;
        NodeID parent_node_id;

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

        const InnerRemoveNode *remove_node_p = \
          new InnerRemoveNode{node_id, node_p};

        bool ret = InstallNodeToReplace(node_id, remove_node_p, node_p);
        if(ret == true) {
          bwt_printf("LeafRemoveNode CAS succeeds. ABORT\n");

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
   * CheckSplitItem() - Given a parent snapshot, check whether the inner
   *                    insert could be posted
   *
   * This functions checks whether the split key is out of the range
   * of the current parent node. This is possible as long as the split
   * delta has been finished with InnerInsertNode posted, and then
   * parent node splited and the insert sibling is the left most child
   * in the parent split sibling. In this case the insert item is out of the
   * range of the current parent node which should be prevented since
   * we assume all delta nodes are within the valid range of the current node
   * observed from the topmost node of the delta chain
   *
   * Note: This function could abort
   */
  inline bool CheckSplitItem(Context *context_p,
                             NodeSnapshot *snapshot_p,
                             const KeyNodeIDPair &insert_item) {
    // Save some keystrokes
    const BaseNode *node_p = snapshot_p->node_p;

    const KeyType &search_key = insert_item.first;

    // If the split key is out of range then just ignore
    // we do not worry that through split sibling link
    // we would traverse to the child of a different parent node
    // than the current one, since we always guarantee that after
    // NavigateInnerNode() returns if it does not abort, then we
    // are on the correct node for the current key

    // This happens when the parent splits on the newly inserted index term
    // and the split delta has not been consolidated yet, so that a thread
    // sees the split delta and traverses to its right sibling which is on
    // another parent node (the right sibling of the current parent node)
    // In this case we do not have to insert since we know the index term has
    // already been inserted
    if((node_p->GetNextNodeID() != INVALID_NODE_ID) &&
       (KeyCmpGreaterEqual(search_key, node_p->GetHighKey()) == true)) {
      bwt_printf("Bounds check failed on parent node"
                 " - split key >= high key\n");
                 
      return false;
    }

    while(1) {
      switch(node_p->GetType()) {
      case NodeType::InnerInsertType: {
        const KeyNodeIDPair &item = \
          static_cast<const InnerInsertNode *>(node_p)->item;

        if(KeyCmpEqual(item.first, search_key) == true) {
          
          if(item.second != insert_item.second) {
            bwt_printf("NodeID does not match (InnerInsertNode)!\n");
            
            // We know the old item must be an obsolete one since
            // the node it points to has been removed and merged.
            // The split sibling points to a new node whose split
            // point is chosen to be the same as the old one
            const BaseNode *node_p = GetNode(item.second);
            // Also we know the NodeID must be valid, since
            //
            assert(node_p->GetType() == NodeType::LeafRemoveType ||
                   node_p->GetType() == NodeType::InnerRemoveType);
                   
            (void)node_p;

            // We cannot continue traversing the node without
            // finishing the split SMO. If we keep traversing down
            // then it is possible that the SMO is posted upon before
            // it is consolidated
            context_p->abort_flag = true;
            
            return false;
          }
          
          return false;
        }

        node_p = \
          static_cast<const InnerInsertNode *>(node_p)->child_node_p;

        break;
      } // InnerInsertNode
      case NodeType::InnerDeleteType: {
        const KeyNodeIDPair &item = \
          static_cast<const InnerDeleteNode *>(node_p)->item;

        if(KeyCmpEqual(item.first, search_key) == true) {
          return true;
        }

        node_p = \
          static_cast<const InnerDeleteNode *>(node_p)->child_node_p;

        break;
      } // InnerDeleteNode
      case NodeType::InnerType: {
        auto sep_list_p = &static_cast<const InnerNode *>(node_p)->sep_list;

        auto it = std::lower_bound(sep_list_p->begin() + 1,
                                   sep_list_p->end(),
                                   insert_item,
                                   [this](const KeyNodeIDPair &knp1, const KeyNodeIDPair &knp2) {
                                     return this->key_cmp_obj(knp1.first, knp2.first);
                                   });
        // This is special case since we could not compare the iterator
        if(it == sep_list_p->end()) {
          // If the key does not exist then return true and post node
          return true;
        } else if(KeyCmpEqual(it->first, insert_item.first) == false) {
          // If found the lower bound but keys are different
          // then also could post
          return true;
        } else {
          
          // If the parent node is out of date
          if(it->second != insert_item.second) {
            bwt_printf("NodeID does not match (InnerNode) !\n");

            const BaseNode *node_p = GetNode(it->second);
            assert(node_p->GetType() == NodeType::LeafRemoveType ||
                   node_p->GetType() == NodeType::InnerRemoveType);
            // To avoid compiler warning
            (void)node_p;
                   
            context_p->abort_flag = true;

            return false;
          }
          
          return false;
        }

        assert(false);
        return false;
      } // InnerNode
      case NodeType::InnerSplitType: {
        // Since we already did bounds checking at the beginning of this
        // function, the split key must be less than the split key
        // here in this node
        node_p = \
          static_cast<const InnerSplitNode *>(node_p)->child_node_p;

        break;
      } // InnerSplitType
      case NodeType::InnerMergeType: {
        const KeyNodeIDPair &item = \
          static_cast<const InnerMergeNode *>(node_p)->delete_item;

        // If the split key >= merge key then just go to the right branch
        if(KeyCmpGreaterEqual(insert_item.first, item.first) == true) {
          node_p = static_cast<const InnerMergeNode *>(node_p)->right_merge_p;
        } else {
          node_p = static_cast<const InnerMergeNode *>(node_p)->child_node_p;
        }

        break;
      } // InnerMergeNode
      default: {
        bwt_printf("Unknown InnerNode type: %d", (int)node_p->GetType());

        assert(false);
        return false;
      } // default
      } // switch
    } // while(1)

    assert(false);
    return false;
  }

  /*
   * FindMergePrevNextKey() - Find merge next and prev key and node ID
   *
   * This function loops through keys in the snapshot, and if there is a
   * match of the merge key, the iterator to the merge key item is returned
   * using the iterator pointer, and return valur is true. Otherwise
   * the iterator pointer is not modified and return value of the function
   * is false
   *
   * NOTE: There is possibility that the index term has already been
   * deleted in the parent node, in which case this function does not
   * fill in the given pointer but returns false instead.
   *
   * NOTE 2: This function does not require a context object being
   * passed, unlile its counterpart when spliting a node, since this function
   * is guaranteed not to abort
   *
   * NOTE 3: Deleted node ID is verrified against the NodeID of the merge key
   * item if found in the InnerNode
   * We are guaranteed by the SMO serialization order of BwTree that if
   * an item is found, the NodeID will match. See comments for more details
   *
   * NOTE 4: delete_item_p is a pointer to the merge item, the first element
   * being the merge key, and the second element being the removed node ID
   */
  inline bool FindMergePrevNextKey(NodeSnapshot *snapshot_p,
                                   const KeyNodeIDPair *delete_item_p,
                                   const KeyNodeIDPair **prev_item_p_p,
                                   const KeyNodeIDPair **next_item_p_p) {
    // We could only post merge key on merge node
    assert(snapshot_p->IsLeaf() == false);

    const InnerNode *inner_node_p = \
      static_cast<const InnerNode *>(snapshot_p->node_p);

    if(snapshot_p->node_p->IsInnerNode() == false) {
      inner_node_p = CollectAllSepsOnInner(snapshot_p,
                                           snapshot_p->node_p->GetDepth() + 1);

      // We are going to garbage collect this node
      const BaseNode *old_node_p = snapshot_p->node_p;

      bool ret = InstallNodeToReplace(snapshot_p->node_id,
                                      inner_node_p,
                                      old_node_p);

      if(ret == true) {
        snapshot_p->node_p = inner_node_p;

        epoch_manager.AddGarbageNode(old_node_p);
      } else {
        // Must delay deallocation since we return pointers pointing
        // into this node's data
        epoch_manager.AddGarbageNode(inner_node_p);
      }
    }

    // Find the merge key
    // NOTE: Merge key will not be the left most key in the inner node
    // since otherwise we are merging the leftmost child into other nodes
    // which is not allowed
    auto delete_item_it = \
      std::lower_bound(inner_node_p->sep_list.begin() + 1,
                       inner_node_p->sep_list.end(),
                       *delete_item_p,
                       [this](const KeyNodeIDPair &knp1, const KeyNodeIDPair &knp2) {
                         return this->key_cmp_obj(knp1.first, knp2.first);
                       });

    // If the lower bound of merge_key_p is not itself, then we
    // declare such key does not exist
    // Another corner case is that merge key is greater than the last item
    // then we know it does not exist
    if(delete_item_it == inner_node_p->sep_list.end() || \
       KeyCmpEqual(delete_item_it->first, delete_item_p->first) == false) {
      return false;
    }

    // If we have found the deleted entry then it must be associated
    // with the node id that has been deleted
    // NOTE: Unlike the case in split delta, here if we find the index term
    // on the parent node, then the NodeID must equal the ID of the node
    // being deleted
    // The reason is that, the only way that these two not being equal
    // is the child node was merged and splited again. However, if this
    // happens then on the top of child node's delta chain would be
    // a split delta rather than merge delta. This is possible with
    // a split delta, but impossible when we see a merge delta
    assert(delete_item_it->second == delete_item_p->second);

    // In the parent node merge key COULD NOT be the left most key
    // since the merge node itself has a low key, which < merge key
    // and the low key >= parent node low key
    // NOTE: Since we search from begin() + 1, this must be true
    //assert(merge_key_it != inner_node_p->sep_list.begin());

    // We know delete_item_it could not be the first element
    auto prev_item_it = delete_item_it - 1;

    // We know delete_item_it could not be end element so it is either end
    // element or a valid element
    auto next_item_it = delete_item_it + 1;

    // Previous item is always inside the sep list of the parent inner node
    *prev_item_p_p = &(*prev_item_it);

    // next item iterator could either be the high key, or
    // it could be the item inside sep list of the parent inner node
    if(next_item_it == inner_node_p->sep_list.end()) {
      *next_item_p_p = &inner_node_p->GetHighKeyPair();
    } else {
      *next_item_p_p = &(*next_item_it);
    }

    return true;
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
                     NodeID deleted_node_id,
                     const BaseNode **node_p_p) {
    // This is the child node of merge delta
    const BaseNode *node_p = snapshot_p->node_p;
    NodeID node_id = snapshot_p->node_id;

    // NOTE: DO NOT FORGET TO DELETE THIS IF CAS FAILS
    const MergeNodeType *merge_node_p = \
      new MergeNodeType{*merge_key_p,
                        merge_branch_p,
                        deleted_node_id,
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
   * Insert() - Insert a key-value pair
   *
   * This function returns false if value already exists
   * If CAS fails this function retries until it succeeds
   */
  bool Insert(const KeyType &key, const ValueType &value) {
    bwt_printf("Insert called\n");

    #ifdef BWTREE_DEBUG
    insert_op_count.fetch_add(1);
    #endif

    EpochNode *epoch_node_p = epoch_manager.JoinEpoch();

    while(1) {
      Context context{key};
      std::pair<int, bool> index_pair;

      // Check whether the key-value pair exists
      // Also if the key previously exists in the delta chain
      // then return the position of the node using next_key_p
      // if there is none then return nullptr
      const KeyValuePair *item_p = Traverse(&context, &value, &index_pair);

      // If the key-value pair already exists then return false
      if(item_p != nullptr) {
        epoch_manager.LeaveEpoch(epoch_node_p);

        return false;
      }

      NodeSnapshot *snapshot_p = GetLatestNodeSnapshot(&context);

      // We will CAS on top of this
      const BaseNode *node_p = snapshot_p->node_p;
      NodeID node_id = snapshot_p->node_id;

      const LeafInsertNode *insert_node_p = \
        new LeafInsertNode{key, value, node_p, index_pair};

      bool ret = InstallNodeToReplace(node_id,
                                      insert_node_p,
                                      node_p);
      if(ret == true) {
        bwt_printf("Leaf Insert delta CAS succeed\n");

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
   * NOTE: We first test the predicate, and then test for duplicated values
   * so predicate test result is always available
   */
  bool ConditionalInsert(const KeyType &key,
                         const ValueType &value,
                         std::function<bool(const ItemPointer &)> predicate,
                         bool *predicate_satisfied) {
    bwt_printf("Insert (cond.) called\n");

    #ifdef BWTREE_DEBUG
    insert_op_count.fetch_add(1);
    #endif

    EpochNode *epoch_node_p = epoch_manager.JoinEpoch();

    while(1) {
      Context context{key};

      // This will just stop on the correct leaf page
      // without traversing into it. Next we manually traverse
      Traverse(&context, nullptr, nullptr);

      *predicate_satisfied = false;
      
      // This is used to hold the index for which this delta will
      // be applied to
      std::pair<int, bool> index_pair;

      // Call navigate leaf node to test predicate and to test duplicates
      const KeyValuePair *item_p = \
        NavigateLeafNode(&context,
                         value,
                         &index_pair,
                         predicate,
                         predicate_satisfied);
      
      // We do not insert anything if predicate is satisfied
      if(*predicate_satisfied == true) {
        epoch_manager.LeaveEpoch(epoch_node_p);
        
        return false;
      } else if(item_p != nullptr) {
        epoch_manager.LeaveEpoch(epoch_node_p);
        
        return false;
      }

      // This retrieves the most up-to-date snapshot (which does not change)
      NodeSnapshot *snapshot_p = GetLatestNodeSnapshot(&context);

      // We will CAS on top of this
      const BaseNode *node_p = snapshot_p->node_p;
      NodeID node_id = snapshot_p->node_id;

      // Here since we could not know which is the next key node
      // just use child node as a cpnservative way of inserting
      const LeafInsertNode *insert_node_p = \
        new LeafInsertNode{key, value, node_p, index_pair};

      bool ret = InstallNodeToReplace(node_id,
                                      insert_node_p,
                                      node_p);
      if(ret == true) {
        bwt_printf("Leaf Insert (cond.) delta CAS succeed\n");

        // If install is a success then just break from the loop
        // and return
        break;
      } else {
        bwt_printf("Leaf insert (cond.) delta CAS failed\n");

        context.abort_counter++;

        delete insert_node_p;
      }

      insert_abort_count.fetch_add(context.abort_counter);

      bwt_printf("Retry installing leaf insert (cond.) delta from the root\n");
    }

    epoch_manager.LeaveEpoch(epoch_node_p);

    return true;
  }

#endif


  /*
   * Delete() - Remove a key-value pair from the tree
   *
   * This function returns false if the key and value pair does not
   * exist. Return true if delete succeeds
   *
   * This functions shares a same structure with the Insert() one
   */
  bool Delete(const KeyType &key, const ValueType &value) {
    bwt_printf("Delete called\n");

    #ifdef BWTREE_DEBUG
    delete_op_count.fetch_add(1);
    #endif

    EpochNode *epoch_node_p = epoch_manager.JoinEpoch();

    while(1) {
      Context context{key};
      std::pair<int, bool> index_pair;

      // Navigate leaf nodes to check whether the key-value
      // pair exists
      const KeyValuePair *item_p = Traverse(&context, &value, &index_pair);

      if(item_p == nullptr) {
        epoch_manager.LeaveEpoch(epoch_node_p);

        return false;
      }

      NodeSnapshot *snapshot_p = GetLatestNodeSnapshot(&context);

      // We will CAS on top of this
      const BaseNode *node_p = snapshot_p->node_p;
      NodeID node_id = snapshot_p->node_id;

      const LeafDeleteNode *delete_node_p = \
        new LeafDeleteNode{key, value, node_p, index_pair};

      bool ret = InstallNodeToReplace(node_id,
                                      delete_node_p,
                                      node_p);
      if(ret == true) {
        bwt_printf("Leaf Delete delta CAS succeed\n");

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

  /*
   * DeleteExchange() - Deletes an item pointer and copies the deleted
   *                    value to the input parameter
   *
   * If the key-value pair is found, then they will be deleted from the
   * index, and input parameter value would be overwritten
   * using the deleted value of value (key is unchanged; of course it
   * should not change since the key must be the same)
   */
  bool DeleteExchange(KeyType &key, ValueType *value_p) {
    bwt_printf("DeleteExchange called\n");

    #ifdef BWTREE_DEBUG
    delete_op_count.fetch_add(1);
    #endif

    const KeyValuePair *item_p;

    EpochNode *epoch_node_p = epoch_manager.JoinEpoch();

    while(1) {
      Context context{key};
      std::pair<int, bool> index_pair;

      // Navigate leaf nodes to check whether the key-value
      // pair exists
      item_p = Traverse(&context, value_p, &index_pair);

      // If value not found just return
      if(item_p == nullptr) {
        epoch_manager.LeaveEpoch(epoch_node_p);

        return false;
      }

      NodeSnapshot *snapshot_p = GetLatestNodeSnapshot(&context);

      // We will CAS on top of this
      const BaseNode *node_p = snapshot_p->node_p;
      NodeID node_id = snapshot_p->node_id;

      const LeafDeleteNode *delete_node_p = \
        new LeafDeleteNode{key, *value_p, node_p, index_pair};

      bool ret = InstallNodeToReplace(node_id,
                                      delete_node_p,
                                      node_p);
      if(ret == true) {
        bwt_printf("Leaf Delete delta CAS succeed\n");

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

    // Assign the old deleted value to input parameter value
    *value_p = item_p->second;

    epoch_manager.LeaveEpoch(epoch_node_p);

    return true;
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
  void GetValue(const KeyType &search_key,
                std::vector<ValueType> &value_list) {
    bwt_printf("GetValue()\n");

    EpochNode *epoch_node_p = epoch_manager.JoinEpoch();

    Context context{search_key};

    TraverseReadOptimized(&context, &value_list);

    epoch_manager.LeaveEpoch(epoch_node_p);

    return;
  }

  /*
   * GetValue() - Return value in a ValueSet object
   *
   * This is used for verification in the benchmark test suite. Do not
   * remove this method
   */
  ValueSet GetValue(const KeyType &search_key) {
    bwt_printf("GetValue()\n");

    EpochNode *epoch_node_p = epoch_manager.JoinEpoch();

    Context context{search_key};

    std::vector<ValueType> value_list{};
    TraverseReadOptimized(&context, &value_list);

    epoch_manager.LeaveEpoch(epoch_node_p);

    ValueSet value_set{value_list.begin(),
                       value_list.end(),
                       10,
                       value_hash_obj,
                       value_eq_obj};

    return value_set;
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
  // Key comparator
  const KeyComparator key_cmp_obj;

  // Raw key eq checker
  const KeyEqualityChecker key_eq_obj;

  // Raw key hasher
  const KeyHashFunc key_hash_obj;

  // Check whether values are equivalent
  const ValueEqualityChecker value_eq_obj;

  // Hash ValueType into a size_t
  const ValueHashFunc value_hash_obj;

  // The following three are used for std::pair<KeyType, NodeID>
  const KeyNodeIDPairEqualityChecker key_node_id_pair_eq_obj;
  const KeyNodeIDPairHashFunc key_node_id_pair_hash_obj;

  // The following two are used for hashing KeyValuePair
  const KeyValuePairEqualityChecker key_value_pair_eq_obj;
  const KeyValuePairHashFunc key_value_pair_hash_obj;

  // This is used to preallocate space for vector to avoid reallocation
  // for NodeSnapshot
  std::atomic<size_t> tree_height;

  // This value is atomic and will change
  std::atomic<NodeID> root_id;

  // This value is non-atomic, but it remains constant after constructor
  NodeID first_leaf_id;

  std::atomic<NodeID> next_unused_node_id;
  std::array<std::atomic<const BaseNode *>, MAPPING_TABLE_SIZE> mapping_table;

  // This list holds free NodeID which was removed by remove delta
  // We recycle NodeID in epoch manager
  AtomicStack<NodeID, MAPPING_TABLE_SIZE> free_node_id_list;

  std::atomic<uint64_t> insert_op_count;
  std::atomic<uint64_t> insert_abort_count;

  std::atomic<uint64_t> delete_op_count;
  std::atomic<uint64_t> delete_abort_count;

  std::atomic<uint64_t> update_op_count;
  std::atomic<uint64_t> update_abort_count;

  //InteractiveDebugger idb;

  EpochManager epoch_manager;

 public:

  /*
   * class EpochManager - Maintains a linked list of deleted nodes
   *                      for threads to access until all threads
   *                      entering epochs before the deletion of
   *                      nodes have exited
   */
  class EpochManager {
   public:
    BwTree *tree_p;

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
      // count the number of threads
      std::atomic<int> active_thread_count;

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

    // The counter that counts how many free is called
    // inside the epoch manager
    // NOTE: We cannot precisely count the size of memory freed
    // since sizeof(Node) does not reflect the true size, since
    // some nodes are embedded with complicated data structure that
    // maintains its own memory
    #ifdef BWTREE_DEBUG
    // Number of nodes we have freed
    size_t freed_count;

    // Number of NodeID we have freed
    size_t freed_id_count;

    // These two are used for debugging
    size_t epoch_created;
    size_t epoch_freed;

    std::atomic<size_t> epoch_join;
    std::atomic<size_t> epoch_leave;
    #endif

    /*
     * Constructor - Initialize the epoch list to be a single node
     *
     * NOTE: We do not start thread here since the init of bw-tree itself
     * might take a long time
     */
    EpochManager(BwTree *p_tree_p) :
      tree_p{p_tree_p} {
      current_epoch_p = new EpochNode{};

      // These two are atomic variables but we could
      // simply assign to them
      current_epoch_p->active_thread_count = 0;
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
      freed_id_count = 0UL;

      // It is not 0UL since we create an initial epoch on initialization
      epoch_created = 1UL;
      epoch_freed = 0UL;

      epoch_join = 0UL;
      epoch_leave = 0UL;
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

      // If we have a bug (currently there is one) then as a temporary
      // measure just force cleaning all epoches no matter whether they
      // are cleared or not
      if(head_epoch_p != nullptr) {
        printf("ERROR: After cleanup there is still epoch left\n");
        printf("==============================================\n");
        printf("DUMP\n");

        for(EpochNode *epoch_node_p = head_epoch_p;
            epoch_node_p != nullptr;
            epoch_node_p = epoch_node_p->next_p) {
          printf("Active thread count: %d\n",
                 epoch_node_p->active_thread_count.load());
          epoch_node_p->active_thread_count = 0;
        }

        printf("RETRY CLEANING...\n");
        ClearEpoch();
      }

      assert(head_epoch_p == nullptr);
      bwt_printf("Clean up for garbage collector\n");

      #ifdef BWTREE_DEBUG
      bwt_printf("Stat: Freed %lu nodes and %lu NodeID by epoch manager\n",
                 freed_count,
                 freed_id_count);

      bwt_printf("      Epoch created = %lu; epoch freed = %lu\n",
                 epoch_created,
                 epoch_freed);

      bwt_printf("      Epoch join = %lu; epoch leave = %lu\n",
                 epoch_join.load(),
                 epoch_leave.load());
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

      EpochNode *epoch_node_p = new EpochNode{};

      epoch_node_p->active_thread_count = 0;
      epoch_node_p->garbage_list_p = nullptr;

      // We always append to the tail of the linked list
      // so this field for new node is always nullptr
      epoch_node_p->next_p = nullptr;

      // Update its previous node (current tail)
      current_epoch_p->next_p = epoch_node_p;

      // And then switch current epoch pointer
      current_epoch_p = epoch_node_p;

      #ifdef BWTREE_DEBUG
      epoch_created++;
      #endif

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
      GarbageNode *garbage_node_p = new GarbageNode;
      garbage_node_p->node_p = node_p;

      garbage_node_p->next_p = epoch_p->garbage_list_p.load();

      while(1) {
        // Then CAS previous node with new garbage node
        // If this fails, then garbage_node_p->next_p is the actual value
        // of garbage_list_p, in which case we do not need to load it again
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
     *
     * NOTE: It is possible that prev_count < 0, because in ClearEpoch()
     * the cleaner thread will decrease the epoch counter by a large amount
     * to prevent this function using an epoch currently being recycled
     */
    inline EpochNode *JoinEpoch() {
try_join_again:
      // We must make sure the epoch we join and the epoch we
      // return are the same one because the current point
      // could change in the middle of this function
      EpochNode *epoch_p = current_epoch_p;

      int64_t prev_count = epoch_p->active_thread_count.fetch_add(1);

      // We know epoch_p is now being cleaned, so need to read the
      // current epoch again because it must have been moved
      if(prev_count < 0) {
        goto try_join_again;
      }

      #ifdef BWTREE_DEBUG
      epoch_join.fetch_add(1);
      #endif

      return epoch_p;
    }

    /*
     * LeaveEpoch() - Leave epoch a thread has once joined
     *
     * After an epoch has been cleared all memories allocated on
     * and before that epoch could safely be deallocated
     */
    inline void LeaveEpoch(EpochNode *epoch_p) {
      // This might return a negative value if the current epoch
      // is being cleaned
      epoch_p->active_thread_count.fetch_sub(1);

      #ifdef BWTREE_DEBUG
      epoch_leave.fetch_add(1);
      #endif

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
     * NOTE: For leaf remove node and inner remove, the removed node id should
     * also be freed inside this function. This is because the Node ID might
     * be accessed by some threads after the time the remove node was sent
     * here. So we need to make sure all accessing threads have exited before
     * recycling NodeID
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
            freed_count++;
            #endif
            break;
          case NodeType::LeafDeleteType:
            next_node_p = ((LeafDeleteNode *)node_p)->child_node_p;

            delete (LeafDeleteNode *)node_p;

            #ifdef BWTREE_DEBUG
            freed_count++;
            #endif

            break;
          case NodeType::LeafSplitType:
            next_node_p = ((LeafSplitNode *)node_p)->child_node_p;

            delete (LeafSplitNode *)node_p;

            #ifdef BWTREE_DEBUG
            freed_count++;
            #endif

            break;
          case NodeType::LeafMergeType:
            FreeEpochDeltaChain(((LeafMergeNode *)node_p)->child_node_p);
            FreeEpochDeltaChain(((LeafMergeNode *)node_p)->right_merge_p);

            delete (LeafMergeNode *)node_p;

            #ifdef BWTREE_DEBUG
            freed_count++;
            #endif

            // Leaf merge node is an ending node
            return;
          case NodeType::LeafRemoveType:
            // This recycles node ID
            tree_p->InvalidateNodeID(((LeafRemoveNode *)node_p)->removed_id);

            delete (LeafRemoveNode *)node_p;

            #ifdef BWTREE_DEBUG
            freed_count++;
            freed_id_count++;
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
            freed_count++;
            #endif

            // We have reached the end of delta chain
            return;
          case NodeType::InnerInsertType:
            next_node_p = ((InnerInsertNode *)node_p)->child_node_p;

            delete (InnerInsertNode *)node_p;

            #ifdef BWTREE_DEBUG
            freed_count++;
            #endif

            break;
          case NodeType::InnerDeleteType:
            next_node_p = ((InnerDeleteNode *)node_p)->child_node_p;

            delete (InnerDeleteNode *)node_p;
            #ifdef BWTREE_DEBUG
            freed_count++;
            #endif

            break;
          case NodeType::InnerSplitType:
            next_node_p = ((InnerSplitNode *)node_p)->child_node_p;

            delete (InnerSplitNode *)node_p;

            #ifdef BWTREE_DEBUG
            freed_count++;
            #endif

            break;
          case NodeType::InnerMergeType:
            FreeEpochDeltaChain(((InnerMergeNode *)node_p)->child_node_p);
            FreeEpochDeltaChain(((InnerMergeNode *)node_p)->right_merge_p);

            delete (InnerMergeNode *)node_p;

            #ifdef BWTREE_DEBUG
            freed_count++;
            #endif

            // Merge node is also an ending node
            return;
          case NodeType::InnerRemoveType:
            // Recycle NodeID here together with RemoveNode
            // Since we need to guatantee all threads that could potentially
            // see the remove node exit before cleaning the NodeID
            tree_p->InvalidateNodeID(((InnerRemoveNode *)node_p)->removed_id);

            delete (InnerRemoveNode *)node_p;

            #ifdef BWTREE_DEBUG
            freed_count++;
            freed_id_count++;
            #endif

            // We never free nodes under remove node
            return;
          case NodeType::InnerType:
            delete (InnerNode *)node_p;

            #ifdef BWTREE_DEBUG
            freed_count++;
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
            freed_count++;
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
        if(head_epoch_p == current_epoch_p) {
          bwt_printf("Current epoch is head epoch. Do not clean\n");

          break;
        }

        // Since it could only be acquired and released by worker thread
        // the value must be >= 0
        int active_thread_count = head_epoch_p->active_thread_count.load();
        assert(active_thread_count >= 0);

        // If we have seen an epoch whose count is not zero then all
        // epochs after that are protected and we stop
        if(active_thread_count != 0) {
          bwt_printf("Head epoch is not empty. Return\n");

          break;
        }

        // If some thread joins the epoch between the previous branch
        // and the following fetch_sub(), then fetch_sub() returns a positive
        // number, which is the number of threads that have joined the epoch
        // since last epoch counter testing.

        if(head_epoch_p->active_thread_count.fetch_sub(max_thread_count) > 0) {
          bwt_printf("Some thread sneaks in after we have decided"
                     " to clean. Return\n");

          // Must add it back to let the next round of cleaning correctly
          // identify empty epoch
          head_epoch_p->active_thread_count.fetch_add(max_thread_count);

          break;
        }

        // After this point all fetch_add() on the epoch counter would return
        // a negative value which will cause re-read of current_epoch_p
        // to prevent joining an epoch that is being deleted

        // If the epoch has cleared we just loop through its garbage chain
        // and then free each delta chain

        const GarbageNode *next_garbage_node_p = nullptr;

        // Walk through its garbage chain
        for(const GarbageNode *garbage_node_p = head_epoch_p->garbage_list_p.load();
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

        #ifdef BWTREE_DEBUG
        epoch_freed++;
        #endif

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
        //printf("Start new epoch cycle\n");
        ClearEpoch();
        CreateNewEpoch();

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
   * NullIterator() - Returns an empty iterator that cannot do anything
   *
   * This is useful as a placeholder when we are not sure when an iterator
   * is required but need to use a placeholder.
   *
   * NOTE: This iterator can be assigned and destructed (nullptr leaf node will
   * tell the story) but cannot be moved
   */
  ForwardIterator NullIterator() {
    return ForwardIterator{};
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
     * Default Constructor - This acts as a place holder for some functions
     *                       that require a type and an object but we do not
     *                       want to afford the overhead of loading a page into
     *                       the iterator
     *
     * Only leaf_node_p is initialized to avoid destructor destructing the
     * iterator, also to avoid assignment operator directly assign to it.
     */
    ForwardIterator() :
      leaf_node_p{nullptr}
    {}

    /*
     * Constructor
     *
     * NOTE: We try to load the first page using -Inf as the next key
     * during construction in order to correctly identify the case where
     * the tree is empty, and such that begin() iterator equals end() iterator
     */
    ForwardIterator(BwTree *p_tree_p) :
      tree_p{p_tree_p},
      is_end{false} {
      // Load the first leaf page
      const BaseNode *node_p = tree_p->GetNode(FIRST_LEAF_NODE_ID);

      assert(node_p != nullptr);
      assert(node_p->IsOnLeafDeltaChain() == true);

      // Use the high key of current node as the next key locking
      next_key_pair = node_p->GetHighKeyPair();

      NodeSnapshot snapshot{FIRST_LEAF_NODE_ID, node_p};

      // Consolidate the current node (this does not change high key)
      leaf_node_p = tree_p->CollectAllValuesOnLeaf(&snapshot);

      it = leaf_node_p->data_list.begin();

      // Corner case: if the vector is itself empty
      // then we should set the flag manually
      if(leaf_node_p->data_list.size() == 0UL) {
        is_end = true;
      }

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
                    const KeyType &start_key) :
      tree_p{p_tree_p},
      leaf_node_p{nullptr}, // This is used to singal LowerBound() that
                            // no memory should be freed
      is_end{false} {

      // This sets all members, and might return with is_end == true
      // Also this function
      LowerBound(&start_key);

      // After LowerBound(), either it points to KeyValuePair or
      // is_end is set to true

      return;
    }

    /*
     * Copy Constructor - Constructs a new iterator instance from existing one
     *
     * During copy construction we need to take care that the iterator is
     * invalidated after copy constructing LeafNode from the other iterator
     * to this. So we should move the iterator manually
     */
    ForwardIterator(const ForwardIterator &other) :
      tree_p{other.tree_p},
      // This copy constructs all members recursively by default
      leaf_node_p{new LeafNode{*other.leaf_node_p}},
      next_key_pair{other.next_key_pair},
      is_end{other.is_end} {

      // Move the iterator ahead
      it = leaf_node_p->data_list.begin() + \
           std::distance(((const LeafNode *)other.leaf_node_p)->data_list.begin(), other.it);

      return;
    }

    /*
     * operator= - Assigns one object to another
     *
     * DONE: As an optimization we could define an assignment operation
     * for logical leaf node, and direct assign the logical leaf node from
     * the source object to the current object
     */
    ForwardIterator &operator=(const ForwardIterator &other) {
      // It is crucial to prevent self assignment since we do pointer
      // operation here
      if(this == &other) {
        return *this;
      }

      // For an empty iterator this branch is necessary
      if(leaf_node_p == nullptr) {
        leaf_node_p = new LeafNode{*other.leaf_node_p};
      } else {
        // First copy the logical node into current instance
        // DO NOT NEED new and delete; JUST DO A VALUE COPY
        // since the storage has already been allocated during construction
        // and the old value with be automatically dealt with LeafNode
        // and vector's assignment operation
        *leaf_node_p = *other.leaf_node_p;
      }

      // Copy everything that could be copied
      tree_p = other.tree_p;
      next_key_pair = other.next_key_pair;

      is_end = other.is_end;

      // Move the iterator ahead
      it = leaf_node_p->data_list.begin() + \
           std::distance(((const LeafNode *)other.leaf_node_p)->data_list.begin(), other.it);

      return *this;
    }

    /*
     * Move Assignment - Assigns a temporary iterator object
     *
     * NOTE: In move assignment we do not need to move iterator; instead
     * iterator could be directly copied since the leaf node does not change
     * and such that the iterator is not invalidated
     */
    ForwardIterator &operator=(ForwardIterator &&other) {
      if(this == &other) {
        return *this;
      }

      // For an empty iterator this branch is necessary
      if(leaf_node_p != nullptr) {
        delete leaf_node_p;
      }

      // Direcrly moves the leaf node pointer without copying
      leaf_node_p = other.leaf_node_p;

      // Since the leaf node does not change, the constructor is not invalidated
      // we could just copy it here
      it = other.it;

      tree_p = other.tree_p;
      next_key_pair = other.next_key_pair;

      is_end = other.is_end;

      // This is necessary to avoid the leaf node pointer being
      // deleted when the other is destructed
      other.leaf_node_p = nullptr;

      return *this;
    }

    /*
     * operator*() - Return the value reference currently pointed to by this
     *               iterator
     *
     * NOTE: We need to return a constant reference to both save a value copy
     * and also to prevent caller modifying value using the reference
     */
    inline const KeyValuePair &operator*() {
      // This itself is a ValueType reference
      return (*it);
    }

    /*
     * operator->() - Returns the value pointer pointed to by this iterator
     *
     * Note that this function returns a contsnat pointer which can be used
     * to access members of the value, but cannot modify
     */
    inline const KeyValuePair *operator->() {
      return &(*it);
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
    inline bool operator<(const ForwardIterator &other) const {
      if(other.is_end == true) {
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
      // using the iterator
      return tree_p->KeyCmpLess(it->first, other.it->first);
    }

    /*
     * operator==() - Compares whether two iterators refer to the same key
     *
     * If both iterators are end iterator then we return true
     * If one of them is not then the result is false
     * Otherwise the result is the comparison result of current key
     */
    inline bool operator==(const ForwardIterator &other) const {
      if(other.is_end == true) {
        if(is_end == true) {
          // Two end iterators are equal to each other
          return true;
        } else {
          // Otherwise they are not equal
          return false;
        }
      }

      return tree_p->KeyCmpEqual(it->first, other.it->first);
    }

    /*
     * Destructor
     *
     * NOTE: Since we always make copies of logical node object when copy
     * constructing the iterator, we could always safely delete the logical
     * node, because its memory is not shared between iterators
     */
    ~ForwardIterator() {
      if(leaf_node_p != nullptr) {
        delete leaf_node_p;
      }

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

   private:
    // We need access to the tree in order to traverse down using
    // a low key to leaf node level
    BwTree *tree_p;

    // This points to a consolidated leaf node
    // The leaf node is not shared with any internal structure of
    // BwTree and also not between iterators. Each iterator
    // keeps an instance of LeafNode on which it iterates
    LeafNode *leaf_node_p;

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
    KeyNodeIDPair next_key_pair;

    // This is the actual iterator
    typename std::vector<KeyValuePair>::const_iterator it;

    // We use this flag to indicate whether we have reached the end of
    // iteration.
    // NOTE: We could not directly check for next_key being +Inf, since
    // there might still be keys not scanned yet even if next_key is +Inf
    // LoadNextKey() checks whether the current page has no key >= next_key
    // and the next key is +Inf for current page. If these two conditions hold
    // then we know we have reached the last key of the last page
    bool is_end;

    /*
     * LowerBound() - Load leaf page whose key > start_key
     *
     * NOTE: Consider the case where there are two nodes [1, 2, 3] [4, 5, 6]
     * after we have scanned [1, 2, 3] (i.e. got its logical leaf node object)
     * the node [4, 5, 6] is merged into [1, 2, 3], forming [1, 2, 3, 4, 5, 6]
     * then when we query again using the saved high key = 4, we would still
     * be reading [1, 2, 3] first, which is not consistent (duplicated values)
     *
     * To address this problem, after loading a logical node, we need to advance
     * the key iterator to locate the first key that >= next_key
     *
     * NOTE: If no argument is given then this function uses next_key_pair
     * and checks for INVALID_NODE_ID. Otherwise it uses the given key
     * as the starting point of iteration, and sets next_key_pair
     */
    void LowerBound(const KeyType *start_key_p = nullptr) {
      // Caller needs to guarantee this function not being called if
      // we have already reached the end
      assert(is_end == false);

      while(1) {
        // If we use the nexy_key_pair
        if(start_key_p == nullptr) {
          // If there is no next key, then just return
          if(next_key_pair.second == INVALID_NODE_ID) {
            // If there is no next key then we are at the end of the iteration
            is_end = true;

            return;
          }

          start_key_p = &next_key_pair.first;
        }

        // We need to save this since the start key pointer will be overwritten
        const KeyType start_key = *start_key_p;

        // First join the epoch to prevent physical nodes being deallocated
        // too early
        EpochNode *epoch_node_p = tree_p->epoch_manager.JoinEpoch();

        // Traverse down the tree to get to leaf node
        Context context{*start_key_p};
        
        // NOTE: Even if ValueType is a value we will not test it here
        // since Traverse() will not pass the pointer into NavigateLeafNode
        // but rather just stop on the correct leaf node
        tree_p->Traverse(&context, nullptr, nullptr);

        NodeSnapshot *snapshot_p = tree_p->GetLatestNodeSnapshot(&context);
        const BaseNode *node_p = snapshot_p->node_p;

        // The page must be on a leaf delta chain
        assert(node_p->IsOnLeafDeltaChain() == true);

        // Set high key pair for next call of this function
        next_key_pair = node_p->GetHighKeyPair();

        // If this is nullptr then we are calling it from the constructor
        if(leaf_node_p != nullptr) {
          // Only we call it from the constructor will the start key pointer
          // be a null pointer
          assert(start_key_p != nullptr);

          delete leaf_node_p;
        }

        // Consolidate the current node
        leaf_node_p = tree_p->CollectAllValuesOnLeaf(snapshot_p);

        // Leave the epoch, since we have already had all information
        tree_p->epoch_manager.LeaveEpoch(epoch_node_p);

        // Then we need to find the start key in the leaf node until we have seen
        // a larger key

        // Find the lower bound of the current start search key
        // NOTE: Do not use start_key_p since it has been changed by the
        // assignment to next_key_pair
        it = std::lower_bound(leaf_node_p->data_list.begin(),
                              leaf_node_p->data_list.end(),
                              std::make_pair(start_key, ValueType{}),
                              [this](const KeyValuePair &kvp1, const KeyValuePair &kvp2) {
                                return this->tree_p->key_cmp_obj(kvp1.first, kvp2.first);
                              });

        // All keys in the leaf page are < start key. Switch the next key until
        // we have found the key or until we have reached end of tree
        if(it != leaf_node_p->data_list.end()) {
          return;
        }

        // Need to set this to nullptr to let the function to use next key pair
        start_key_p = nullptr;
      } // while(1)

      assert(false);
      return;
    }

    /*
     * MoveAheadByOne() - Move the iterator ahead by one
     *
     * The caller is responsible for checking whether the iterator has reached
     * its end. If iterator has reached end then assertion fails.
     */
    inline void MoveAheadByOne() {
      // Could not do this on an empty iterator
      assert(leaf_node_p != nullptr);

      // Move the iterator on leaf node data list
      it++;

      // If we have reached the last element then either go to the next
      // then just try to load the next key. This has the possibility
      // that the is_end flag is set, but we do not care about it, and
      // directly returns after LowerBound() returns
      if(it == leaf_node_p->data_list.end()) {
        LowerBound();
      }

      return;
    }
  }; // ForwardIterator

}; // class BwTree

#ifdef BWTREE_PELOTON
}  // End index namespace
}  // End peloton namespace
#endif

#endif
