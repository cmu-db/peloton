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
#include <chrono>
#include <thread>
#include <unordered_set>
// offsetof() is defined here
#include <cstddef>
#include <vector>

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

// This must be declared before all include directives
#include <inttypes.h>
using NodeID = uint64_t;

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
 * USE_OLD_EPOCH - This flag switches between old epoch and new epoch mechanism
 */
#define USE_OLD_EPOCH

/*
 * BWTREE_TEMPLATE_ARGUMENTS - Save some key strokes
 */
#define BWTREE_TEMPLATE_ARGUMENTS                                         \
  template <typename KeyType, typename ValueType, typename KeyComparator, \
            typename KeyEqualityChecker, typename KeyHashFunc,            \
            typename ValueEqualityChecker, typename ValueHashFunc>

#ifdef BWTREE_PELOTON
namespace peloton {
namespace index {
#else
namespace wangziqi2013 {
namespace bwtree {
#endif

// This could not be set as a macro since we will change the flag inside
// the testing framework
extern bool print_flag;

// This constant represents INVALID_NODE_ID which is used as an indication
// that the node is actually the last node on that level
#define INVALID_NODE_ID ((NodeID)0UL)

// The NodeID for the first leaf is fixed, which is 2
#define FIRST_LEAF_NODE_ID ((NodeID)2UL)

// This is the value we use in epoch manager to make sure
// no thread sneaking in while GC decision is being made
#define MAX_THREAD_COUNT ((int)0x7FFFFFFF)

// The maximum number of nodes we could map in this index
#define MAPPING_TABLE_SIZE ((size_t)(1 << 20))

// If the length of delta chain exceeds ( >= ) this then we consolidate the node
#define INNER_DELTA_CHAIN_LENGTH_THRESHOLD ((int)8)
#define LEAF_DELTA_CHAIN_LENGTH_THRESHOLD ((int)8)

// If node size goes above this then we split it
#define INNER_NODE_SIZE_UPPER_THRESHOLD ((int)128)
#define INNER_NODE_SIZE_LOWER_THRESHOLD ((int)32)

#define LEAF_NODE_SIZE_UPPER_THRESHOLD ((int)128)
#define LEAF_NODE_SIZE_LOWER_THRESHOLD ((int)32)

#define PREALLOCATE_THREAD_NUM ((size_t)1024)

/*
 * InnerInlineAllocateOfType() - allocates a chunk of memory from base node and
 *                               initialize it using placement new and then
 *                               return its pointer
 *
 * This is used for InnerNode delta chains
 */
#define InnerInlineAllocateOfType(T, node_p, ...)                    \
  (static_cast<T *>(new (ElasticNode<KeyNodeIDPair>::InlineAllocate( \
      &node_p->GetLowKeyPair(), sizeof(T))) T{__VA_ARGS__}))

/*
 * LeafInlineAllocateOfType() - allocates a chunk of memory from base node and
 *                              initialize it using placement new and then
 *                              return its pointer
 *
 * This is used for LeafNode delta chains
 */
#define LeafInlineAllocateOfType(T, node_p, ...)                    \
  (static_cast<T *>(new (ElasticNode<KeyValuePair>::InlineAllocate( \
      &node_p->GetLowKeyPair(), sizeof(T))) T{__VA_ARGS__}))

/*
 * class BwTreeBase - Base class of BwTree that stores some common members
 */
class BwTreeBase {
 public:
  // This is the presumed size of cache line
  static constexpr size_t CACHE_LINE_SIZE = 64;

  // This is the mask we used for address alignment (AND with this)
  static constexpr size_t CACHE_LINE_MASK = ~(CACHE_LINE_SIZE - 1);

  // We invoke the GC procedure after this has been reached
  static constexpr size_t GC_NODE_COUNT_THREADHOLD = 1024;

  /*
   * class GarbageNode - Garbage node used to represent delayed allocation
   *
   * Note that since we could not know the actual definition of BaseNode here,
   * all garbage pointer to BaseNode should be represented as void *, and are
   * casted to appropriate type manually
   */
  class GarbageNode {
   public:
    // The epoch that this node is unlinked
    // This do not have to be exact - just make sure it is no earlier than the
    // actual epoch it is unlinked from the data structure
    uint64_t delete_epoch;
    void *node_p;
    GarbageNode *next_p;

    /*
     * Constructor
     */
    GarbageNode(uint64_t p_delete_epoch, void *p_node_p)
        : delete_epoch{p_delete_epoch}, node_p{p_node_p}, next_p{nullptr} {}

    GarbageNode() : delete_epoch{0UL}, node_p{nullptr}, next_p{nullptr} {}
  };

  /*
   * class GCMetaData - Metadata for performing GC on per-thread basis
   */
  class GCMetaData {
   public:
    // This is the last active epoch counter; all garbages before this counter
    // are guaranteed to be not being used by this thread
    // So if we take a global minimum of this value, that minimum could be
    // be used as the global epoch value to decide whether a garbage node could
    // be recycled
    uint64_t last_active_epoch;

    // We only need a pointer
    GarbageNode header;

    // This points to the last node in the garbage node linked list
    // We always append new nodes to this pointer, and thus inside one
    // node's context these garbage nodes are always sorted, from low
    // epoch to high epoch. This facilitates memory reclaimation since we
    // just start from the lowest epoch garbage and traverse the linked list
    // until we see an epoch >= GC epoch
    GarbageNode *last_p;

    // The number of nodes inside this GC context
    // We use this as a threshold to trigger GC
    uint64_t node_count;

    /*
     * Default constructor
     */
    GCMetaData()
        : last_active_epoch{0UL}, header{}, last_p{&header}, node_count{0UL} {}
  };

  // Make sure class Data does not exceed one cache line
  static_assert(sizeof(GCMetaData) < CACHE_LINE_SIZE,
                "class Data size exceeds cache line length!");

  /*
   * class PaddedData - Padded data to the length of a cache line
   */
  template <typename DataType, size_t Alignment>
  class PaddedData {
   public:
    // This is the alignment of padded data - we adjust its alignment
    // after malloc() a chunk of memory
    static constexpr size_t ALIGNMENT = Alignment;

    // This is where real data goes
    DataType data;

    /*
     * Default constructor - This is called if DataType could be initialized
     *                       without any constructor
     */
    PaddedData() : data{} {}

   private:
    char padding[ALIGNMENT - sizeof(DataType)];
  };

  using PaddedGCMetadata = PaddedData<GCMetaData, CACHE_LINE_SIZE>;

  static_assert(sizeof(PaddedGCMetadata) == PaddedGCMetadata::ALIGNMENT,
                "class PaddedGCMetadata size does"
                " not conform to the alignment!");

 public:
  // This is used as the garbage collection ID, and is maintained in a per
  // thread level
  // This is initialized to -1 in order to distinguish between registered
  // threads and unregistered threads
  static thread_local int gc_id;

 private:
  // This is used to count the number of threads participating GC process
  // We use this number to initialize GC data structure
  static std::atomic<size_t> total_thread_num;

  // This is the array being allocated for performing GC
  // The allocation aligns its address to cache line boundary
  PaddedGCMetadata *gc_metadata_p;

  // We use this to compute aligned memory address to be
  // used as the gc metadata array
  unsigned char *original_p;

  // This is the number of thread that this instance could support
  size_t thread_num;

  // This is current epoch
  // We need to make it atomic since multiple threads might try to modify it
  uint64_t epoch;

 public:
  /*
   * DestroyThreadLocal() - Destroies thread local
   *
   * This function calls destructor for each metadata element and then
   * frees the memory
   *
   * NOTE: We should also free all garbage nodes before this is called. However
   * since we do not know the type of garbage nodes yet, we should call the
   * function inside BwTree destructor
   *
   * This function must be called when the garbage pool is empty
   */
  void DestroyThreadLocal() {
    LOG_TRACE("Destroy %lu thread local slots", thread_num);

    // There must already be metadata allocated
    PL_ASSERT(original_p != nullptr);

    // Manually call destructor
    for (size_t i = 0; i < thread_num; i++) {
      PL_ASSERT((gc_metadata_p + i)->data.header.next_p == nullptr);

      (gc_metadata_p + i)->~PaddedGCMetadata();
    }

    // Free memory using original pointer rather than adjusted pointer
    free(original_p);

    return;
  }

  /*
   * PrepareThreadLocal() - Initialize thread local variables
   *
   * This function uses thread_num to initialize number of threads
   */
  void PrepareThreadLocal() {
    LOG_TRACE("Preparing %lu thread local slots", thread_num);

    // This is the unaligned base address
    // We allocate one more element than requested as the buffer
    // for doing alignment
    original_p = static_cast<unsigned char *>(
        malloc(CACHE_LINE_SIZE * (thread_num + 1)));
    PL_ASSERT(original_p != nullptr);

    // Align the address to cache line boundary
    gc_metadata_p = reinterpret_cast<PaddedGCMetadata *>(
        (reinterpret_cast<size_t>(original_p) + CACHE_LINE_SIZE - 1) &
        CACHE_LINE_MASK);

    // Make sure it is aligned
    PL_ASSERT(((size_t)gc_metadata_p % CACHE_LINE_SIZE) == 0);

    // Make sure we do not overflow the chunk of memory
    PL_ASSERT(((size_t)gc_metadata_p + thread_num * CACHE_LINE_SIZE) <=
              ((size_t)original_p + (thread_num + 1) * CACHE_LINE_SIZE));

    // At last call constructor of the class; we use placement new
    for (size_t i = 0; i < thread_num; i++) {
      new (gc_metadata_p + i) PaddedGCMetadata{};
    }

    return;
  }

  /*
   * SetThreadNum() - Sets number of threads manually
   */
  void SetThreadNum(size_t p_thread_num) {
    thread_num = p_thread_num;

    return;
  }

 public:
  /*
   * Constructor - Initialize GC data structure
   */
  BwTreeBase()
      : gc_metadata_p{nullptr},
        original_p{nullptr},
        thread_num{total_thread_num.load()},
        epoch{0UL} {
    // Allocate memory for thread local data structure
    PrepareThreadLocal();

    return;
  }

  /*
   * Destructor - Manually call destructor and then frees the memory
   */
  ~BwTreeBase() {
    // Frees all metadata
    DestroyThreadLocal();

    LOG_TRACE("Finished destroying class BwTreeBase");

    return;
  }

  /*
   * GetThreadNum() - Returns the number of thread currently this instance of
   *                  BwTree is serving
   */
  inline size_t GetThreadNum() { return thread_num; }

  /*
   * AssignGCID() - Assigns a gc_id manually
   *
   * This is mainly used for debugging
   */
  inline void AssignGCID(int p_gc_id) {
    gc_id = p_gc_id;

    return;
  }

  /*
   * RegisterThread() - Registers a thread for GC for all instances of BwTree
   *                    in the current process's address space
   *
   * This function assigns an ID to a thread starting from 0, which could be
   * used as thread ID for the garbage collection process.
   *
   * Also note that only threads registered before an instance is created will
   * be considered as being eligible for GC for that thread
   *
   * This function does not return any value, and instead it uses an atomic
   * counter to counter the number of threads currently in this system, and
   * assigns the thread ID to a thread local variable called gc_id decleared
   * inside this class
   *
   * Each thread being allocated a GC ID has a context for garbage collection
   * that is aligned to cache lines. The context will be allocated for every
   * thread being registered, even if it has already exited. Therefore, this
   * approach is only suitable for thread pools where the number of threads
   * is fixed at startup time.
   */
  static void RegisterThread() {
    gc_id = total_thread_num.fetch_add(1);

    return;
  }

  /*
   * IncreaseEpoch() - Go to the next epoch by increasing the counter
   *
   * Note that this should not be called by worker threads since
   * it will cause contention
   */
  inline void IncreaseEpoch() {
    epoch++;

    return;
  }

  /*
   * UpdateLastActiveEpoch() - Updates the last active epoch field of thread
   *                           local storage
   *
   * This is the core of GC algorithm. Its implication is that all garbage nodes
   * unlinked before this epoch could be safely collected since at the time
   * the thread local counter is updated, we know all references to shared
   * resources have been released
   */
  inline void UpdateLastActiveEpoch() {
    GetCurrentGCMetaData()->last_active_epoch = GetGlobalEpoch();

    return;
  }

  /*
   * UnregisterThread() - Unregisters a thread by setting its epoch to
   *                      0xFFFFFFFFFFFFFFFF such that it will not be considered
   *                      for GC
   */
  inline void UnregisterThread(int thread_id) {
    GetGCMetaData(thread_id)->last_active_epoch = static_cast<uint64_t>(-1);
  }

  /*
   * GetGlobalEpoch() - Returns the current global epoch counter
   *
   * Note that this function might return a stale value, which does not affect
   * correctness as long as unlinking the node form data structure is atomic
   * since all refreshing operations will read the same or smaller value
   * when it reads the counter
   */
  inline uint64_t GetGlobalEpoch() { return epoch; }

  /*
   * GetGCMetaData() - Returns the thread-local metadata for GC for a specified
   *                   thread
   */
  inline GCMetaData *GetGCMetaData(int thread_id) {
    // The thread ID must be within the range
    PL_ASSERT(thread_id >= 0 && thread_id < static_cast<int>(thread_num));

    return &(gc_metadata_p + thread_id)->data;
  }

  /*
   * GetCurrentGCMetaData() - Returns the metadata for the current thread
   */
  inline GCMetaData *GetCurrentGCMetaData() { return GetGCMetaData(gc_id); }

  /*
   * SummarizeGCEpoch() - Returns the minimum epochs among the current epoch
   *                      counters of all threads
   *
   * Note that if this is called then it must be true that there are at least
   * one thread participating into the GC process
   */
  uint64_t SummarizeGCEpoch() {
    PL_ASSERT(thread_num >= 1);

    // Use the first metadata's epoch as min and update it on the fly
    uint64_t min_epoch = GetGCMetaData(0)->last_active_epoch;

    // This might not be executed if there is only one thread
    for (int i = 1; i < static_cast<int>(thread_num); i++) {
      // This will be compiled into using CMOV which is more efficient
      // than CMP and JMP
      min_epoch = std::min(GetGCMetaData(i)->last_active_epoch, min_epoch);
    }

    return min_epoch;
  }
};

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
template <typename KeyType, typename ValueType,
          typename KeyComparator = std::less<KeyType>,
          typename KeyEqualityChecker = std::equal_to<KeyType>,
          typename KeyHashFunc = std::hash<KeyType>,
          typename ValueEqualityChecker = std::equal_to<ValueType>,
          typename ValueHashFunc = std::hash<ValueType>>
class BwTree : public BwTreeBase {
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

  class KeyNodeIDPairComparator;
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
  using KeyNodeIDPairSet =
      std::unordered_set<KeyNodeIDPair, KeyNodeIDPairHashFunc,
                         KeyNodeIDPairEqualityChecker>;
  using KeyNodeIDPairBloomFilter =
      BloomFilter<KeyNodeIDPair, KeyNodeIDPairEqualityChecker,
                  KeyNodeIDPairHashFunc>;

  // KeyType-ValueType pair
  using KeyValuePair = std::pair<KeyType, ValueType>;
  using KeyValuePairBloomFilter =
      BloomFilter<KeyValuePair, KeyValuePairEqualityChecker,
                  KeyValuePairHashFunc>;

  using ValueSet =
      std::unordered_set<ValueType, ValueHashFunc, ValueEqualityChecker>;

  using EpochNode = typename EpochManager::EpochNode;

  /*
   * enum class NodeType - Bw-Tree node type
   */
  enum class NodeType : short {
    // We separate leaf and inner into two different intervals
    // to make it possible for compiler to optimize

    InnerType = 0,

    // Only valid for inner
    InnerInsertType = 1,
    InnerDeleteType = 2,
    InnerSplitType = 3,
    InnerRemoveType = 4,
    InnerMergeType = 5,
    InnerAbortType = 6,  // Unconditional abort

    LeafStart = 7,

    // Data page type
    LeafType = 7,

    // Only valid for leaf
    LeafInsertType = 8,
    LeafSplitType = 9,
    LeafDeleteType = 10,
    LeafRemoveType = 11,
    LeafMergeType = 12,
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
    KeyNodeIDPairComparator(BwTree *p_tree_p)
        : key_cmp_obj_p{&p_tree_p->key_cmp_obj} {}

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
    KeyNodeIDPairEqualityChecker(BwTree *p_tree_p)
        : key_eq_obj_p{&p_tree_p->key_eq_obj} {}

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
    KeyNodeIDPairHashFunc(BwTree *p_tree_p)
        : key_hash_obj_p{&p_tree_p->key_hash_obj} {}

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
  // Comparator, equality checker and hasher for key-value pair
  ///////////////////////////////////////////////////////////////////

  /*
   * class KeyValuePairComparator - Comparator class for KeyValuePair
   */
  class KeyValuePairComparator {
   public:
    const KeyComparator *key_cmp_obj_p;

    /*
     * Default constructor - deleted
     */
    KeyValuePairComparator() = delete;

    /*
     * Constructor
     */
    KeyValuePairComparator(BwTree *p_tree_p)
        : key_cmp_obj_p{&p_tree_p->key_cmp_obj} {}

    /*
     * operator() - Compares key-value pair by comparing each component
     *              of them
     *
     * NOTE: This function only compares keys with KeyType. For +/-Inf
     * the wrapped raw key comparator will fail
     */
    inline bool operator()(const KeyValuePair &kvp1,
                           const KeyValuePair &kvp2) const {
      return (*key_cmp_obj_p)(kvp1.first, kvp2.first);
    }
  };

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
    KeyValuePairEqualityChecker(BwTree *p_tree_p)
        : key_eq_obj_p{&p_tree_p->key_eq_obj},
          value_eq_obj_p{&p_tree_p->value_eq_obj} {}

    /*
     * operator() - Compares key-value pair by comparing each component
     *              of them
     *
     * NOTE: This function only compares keys with KeyType. For +/-Inf
     * the wrapped raw key comparator will fail
     */
    inline bool operator()(const KeyValuePair &kvp1,
                           const KeyValuePair &kvp2) const {
      return ((*key_eq_obj_p)(kvp1.first, kvp2.first)) &&
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
    KeyValuePairHashFunc(BwTree *p_tree_p)
        : key_hash_obj_p{&p_tree_p->key_hash_obj},
          value_hash_obj_p{&p_tree_p->value_hash_obj} {}

    /*
     * operator() - Hashes a key-value pair by hashing each part and
     *              combine them into one size_t
     *
     * We use XOR to combine hashes of the key and value together into one
     * single hash value
     */
    inline size_t operator()(const KeyValuePair &kvp) const {
      return ((*key_hash_obj_p)(kvp.first)) ^ ((*value_hash_obj_p)(kvp.second));
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

#ifdef BWTREE_DEBUG

    // Counts abort in one traversal
    int abort_counter;

    // Represents current level we are on the tree
    // root is level 0
    // On initialization this is set to -1
    int current_level;

#endif

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
    inline Context(const KeyType &p_search_key)
        :
#ifdef BWTREE_PELOTON

          // Because earlier versions of g++ does not support
          // initializer list so must use () form
          search_key(p_search_key),

#else

          search_key{p_search_key},

#endif

#ifdef BWTREE_DEBUG

          abort_counter{0},
          current_level{-1},

#endif

          abort_flag{false} {
    }

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

#ifdef BWTREE_DEBUG

    /*
     * HasParentNode() - Returns whether the current node (top of path list)
     *                   has a parent node
     *
     * NOTE: This function is only called under debug mode, since we should
     * validate when there is a remove node on the delta chain. However, under
     * release mode, this function is unnecessary (also current_level is not
     * present) and is not compiled into the binary.
     */
    inline bool HasParentNode() const { return current_level >= 1; }

#endif

    /*
     * IsOnRootNode() - Returns true if the current node is root
     *
     * Though root node might change during traversal, but once it has been
     * fixed using LoadNodeID(), the identity of root node has also been fixed
     */
    inline bool IsOnRootNode() const {
      return parent_snapshot.node_id == INVALID_NODE_ID;
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
    // For all nodes including base node and data node and SMO nodes,
    // the low key pointer always points to a KeyNodeIDPair structure
    // inside the base node, which is either the first element of the
    // node sep list (InnerNode), or a class member (LeafNode)
    const KeyNodeIDPair *low_key_p;

    // high key points to the KeyNodeIDPair inside the LeafNode and InnerNode
    // if there is neither SplitNode nor MergeNode. Otherwise it
    // points to the item inside split node or merge right sibling branch
    const KeyNodeIDPair *high_key_p;

    // The type of the node; this is forced to be represented as a short type
    NodeType type;

    // This is the depth of current delta chain
    // For merge delta node, the depth of the delta chain is the
    // sum of its two children
    short depth;

    // This counts the number of items alive inside the Node
    // when consolidating nodes, we use this piece of information
    // to reserve space for the new node
    int item_count;

    /*
     * Constructor
     */
    NodeMetaData(const KeyNodeIDPair *p_low_key_p,
                 const KeyNodeIDPair *p_high_key_p, NodeType p_type,
                 int p_depth, int p_item_count)
        : low_key_p{p_low_key_p},
          high_key_p{p_high_key_p},
          type{p_type},
          depth{static_cast<short>(p_depth)},
          item_count{p_item_count} {}
  };

  /*
   * class BaseNode - Generic node class; inherited by leaf, inner
   *                  and delta node
   */
  class BaseNode {
    // We hold its data structure as private to force using member functions
    // for member access
   private:
    // This holds low key, high key, next node ID, type, depth and item count
    NodeMetaData metadata;

   public:
    /*
     * Constructor - Initialize type and metadata
     */
    BaseNode(NodeType p_type, const KeyNodeIDPair *p_low_key_p,
             const KeyNodeIDPair *p_high_key_p, int p_depth, int p_item_count)
        : metadata{p_low_key_p, p_high_key_p, p_type, p_depth, p_item_count} {}

    /*
     * GetType() - Return the type of node
     *
     * This method does not allow overridding
     */
    inline NodeType GetType() const { return metadata.type; }

    /*
     * GetNodeMetaData() - Returns a const reference to node metadata
     *
     * Please do not override this method
     */
    inline const NodeMetaData &GetNodeMetaData() const { return metadata; }

    /*
     * IsDeltaNode() - Return whether a node is delta node
     *
     * All nodes that are neither inner nor leaf type are of
     * delta node type
     */
    inline bool IsDeltaNode() const {
      if (GetType() == NodeType::InnerType || GetType() == NodeType::LeafType) {
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
    inline bool IsInnerNode() const { return GetType() == NodeType::InnerType; }

    /*
     * IsRemoveNode() - Returns true if the node is of inner/leaf remove type
     *
     * This is used in JumpToLeftSibling() as an assertion
     */
    inline bool IsRemoveNode() const {
      return (GetType() == NodeType::InnerRemoveType) ||
             (GetType() == NodeType::LeafRemoveType);
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
      return GetType() >= NodeType::LeafStart;
    }

    /*
     * GetLowKey() - Returns the low key of the current base node
     *
     * NOTE: Since it is defined that for LeafNode the low key is undefined
     * and pointers should be set to nullptr, accessing the low key of
     * a leaf node would result in Segmentation Fault
     */
    inline const KeyType &GetLowKey() const {
      return metadata.low_key_p->first;
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
      return *metadata.low_key_p;
    }

    /*
     * GetNextNodeID() - Returns the next NodeID of the current node
     */
    inline NodeID GetNextNodeID() const { return metadata.high_key_p->second; }

    /*
     * GetLowKeyNodeID() - Returns the NodeID for low key
     *
     * NOTE: This function should not be called for leaf nodes
     * since the low key node ID for leaf node is not defined
     */
    inline NodeID GetLowKeyNodeID() const {
      PL_ASSERT(IsOnLeafDeltaChain() == false);

      return metadata.low_key_p->second;
    }

    /*
     * GetDepth() - Returns the depth of the current node
     */
    inline int GetDepth() const { return metadata.depth; }

    /*
     * GetItemCount() - Returns the item count of the current node
     */
    inline int GetItemCount() const { return metadata.item_count; }

    /*
     * SetLowKeyPair() - Sets the low key pair of metadata
     */
    inline void SetLowKeyPair(const KeyNodeIDPair *p_low_key_p) {
      metadata.low_key_p = p_low_key_p;

      return;
    }

    /*
     * SetHighKeyPair() - Sets the high key pair of metdtata
     */
    inline void SetHighKeyPair(const KeyNodeIDPair *p_high_key_p) {
      metadata.high_key_p = p_high_key_p;

      return;
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
    DeltaNode(NodeType p_type, const BaseNode *p_child_node_p,
              const KeyNodeIDPair *p_low_key_p,
              const KeyNodeIDPair *p_high_key_p, int p_depth, int p_item_count)
        : BaseNode{p_type, p_low_key_p, p_high_key_p, p_depth, p_item_count},
          child_node_p{p_child_node_p} {}
  };

  /*
   * class LeafDataNode - Holds LeafInsertNode and LeafDeleteNode's data
   *
   * This class is used in node consolidation to provide a uniform
   * interface for the log-structured merge process
   */
  class LeafDataNode : public DeltaNode {
   public:
    // This is the item being deleted or inserted
    KeyValuePair item;

    // This is the index of the node when inserting/deleting
    // the item into the base leaf node
    std::pair<int, bool> index_pair;

    LeafDataNode(const KeyValuePair &p_item, NodeType p_type,
                 const BaseNode *p_child_node_p,
                 std::pair<int, bool> p_index_pair,
                 const KeyNodeIDPair *p_low_key_p,
                 const KeyNodeIDPair *p_high_key_p, int p_depth,
                 int p_item_count)
        : DeltaNode{p_type,       p_child_node_p, p_low_key_p,
                    p_high_key_p, p_depth,        p_item_count},
          item{p_item},
          index_pair{p_index_pair} {}

    /*
     * GetIndexPair() - Returns the index pair for LeafDataNode
     *
     * Note that this function does not return reference which means
     * that there is no way to modify the index pair
     */
    std::pair<int, bool> GetIndexPair() const { return index_pair; }
  };

  /*
   * class LeafInsertNode - Insert record into a leaf node
   */
  class LeafInsertNode : public LeafDataNode {
   public:
    /*
     * Constructor
     */
    LeafInsertNode(const KeyType &p_insert_key, const ValueType &p_value,
                   const BaseNode *p_child_node_p,
                   std::pair<int, bool> p_index_pair)
        : LeafDataNode{
              std::make_pair(p_insert_key, p_value), NodeType::LeafInsertType,
              p_child_node_p, p_index_pair, &p_child_node_p->GetLowKeyPair(),
              &p_child_node_p->GetHighKeyPair(), p_child_node_p->GetDepth() + 1,
              // For insert nodes, the item count is inheried from the child
              // node + 1 since it inserts new item
              p_child_node_p->GetItemCount() + 1} {}
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
    LeafDeleteNode(const KeyType &p_delete_key, const ValueType &p_value,
                   const BaseNode *p_child_node_p,
                   std::pair<int, bool> p_index_pair)
        : LeafDataNode{
              std::make_pair(p_delete_key, p_value), NodeType::LeafDeleteType,
              p_child_node_p, p_index_pair, &p_child_node_p->GetLowKeyPair(),
              &p_child_node_p->GetHighKeyPair(), p_child_node_p->GetDepth() + 1,
              // For delete node it inherits item count from its child
              // and - 1 from it since one element was deleted
              p_child_node_p->GetItemCount() - 1} {}
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
                  const BaseNode *p_split_node_p)
        : DeltaNode{NodeType::LeafSplitType, p_child_node_p,
                    &p_child_node_p->GetLowKeyPair(),
                    // High key is redirected to the split item inside the node
                    &insert_item,
                    // NOTE: split node is SMO and does not introduce
                    // new piece of data
                    // So we set its depth the same as its child
                    p_child_node_p->GetDepth(),
                    // For split node it is a little bit tricky - we must
                    // know the item count of its sibling to decide how many
                    // items were removed by the split delta
                    p_child_node_p->GetItemCount() -
                        p_split_node_p->GetItemCount()},
          insert_item{p_insert_item} {}
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
    LeafRemoveNode(NodeID p_removed_id, const BaseNode *p_child_node_p)
        : DeltaNode{NodeType::LeafRemoveType, p_child_node_p,
                    &p_child_node_p->GetLowKeyPair(),
                    &p_child_node_p->GetHighKeyPair(),
                    // REMOVE node is an SMO and does not introduce data
                    p_child_node_p->GetDepth(), p_child_node_p->GetItemCount()},
          removed_id{p_removed_id} {}
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
    LeafMergeNode(const KeyType &p_merge_key, const BaseNode *p_right_merge_p,
                  NodeID p_deleted_node_id, const BaseNode *p_child_node_p)
        : DeltaNode{NodeType::LeafMergeType, p_child_node_p,
                    &p_child_node_p->GetLowKeyPair(),
                    // The high key of the merge node is inherited
                    // from the right sibling
                    &p_right_merge_p->GetHighKeyPair(),
                    // std::max(p_child_node_p->metadata.depth,
                    //         p_right_merge_p->metadata.depth) + 1,
                    p_child_node_p->GetDepth() + p_right_merge_p->GetDepth(),
                    // For merge node the item count should be the
                    // sum of items inside both branches
                    p_child_node_p->GetItemCount() +
                        p_right_merge_p->GetItemCount()},
          delete_item{p_merge_key, p_deleted_node_id},
          right_merge_p{p_right_merge_p} {}
  };

  ///////////////////////////////////////////////////////////////////
  // Leaf Delta Chain Node Type End
  ///////////////////////////////////////////////////////////////////

  ///////////////////////////////////////////////////////////////////
  // Inner Delta Chain Node Type
  ///////////////////////////////////////////////////////////////////

  /*
   * class InnerDataNode - Base class for InnerInsertNode and InnerDeleteNode
   *
   * We need this node since we want to sort pointers to such nodes using
   * stable sorting algorithm
   */
  class InnerDataNode : public DeltaNode {
   public:
    KeyNodeIDPair item;

    // This pointer points to the underlying InnerNode to indicate if
    // the search key >= key recorded in this delta node then the binary
    // search could start at this pointer's location; Similarly, if the
    // search key is smaller than this key then binary search could end before
    // this pointer
    const KeyNodeIDPair *location;

    InnerDataNode(const KeyNodeIDPair &p_item, NodeType p_type,
                  const BaseNode *p_child_node_p,
                  const KeyNodeIDPair *p_location,
                  const KeyNodeIDPair *p_low_key_p,
                  const KeyNodeIDPair *p_high_key_p, int p_depth,
                  int p_item_count)
        : DeltaNode{p_type,       p_child_node_p, p_low_key_p,
                    p_high_key_p, p_depth,        p_item_count},
          item{p_item},
          location{p_location} {}
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
                    const BaseNode *p_child_node_p,
                    const KeyNodeIDPair *p_location)
        : InnerDataNode{p_insert_item, NodeType::InnerInsertType,
                        p_child_node_p, p_location,
                        &p_child_node_p->GetLowKeyPair(),
                        &p_child_node_p->GetHighKeyPair(),
                        p_child_node_p->GetDepth() + 1,
                        p_child_node_p->GetItemCount() + 1},
          next_item{p_next_item} {}
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
                    const BaseNode *p_child_node_p,
                    const KeyNodeIDPair *p_location)
        : InnerDataNode{p_delete_item, NodeType::InnerDeleteType,
                        p_child_node_p, p_location,
                        &p_child_node_p->GetLowKeyPair(),
                        &p_child_node_p->GetHighKeyPair(),
                        p_child_node_p->GetDepth() + 1,
                        p_child_node_p->GetItemCount() - 1},
          prev_item{p_prev_item},
          next_item{p_next_item} {}
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
                   const BaseNode *p_split_node_p)
        : DeltaNode{NodeType::InnerSplitType, p_child_node_p,
                    &p_child_node_p
                         ->GetLowKeyPair(),  // Low key does not change
                    &insert_item,            // High key are defined by this
                    // For split node depth does not change since it does not
                    // introduce new data
                    p_child_node_p->GetDepth(),
                    // For split node we need the physical pointer to the
                    // split sibling to compute item count
                    p_child_node_p->GetItemCount() -
                        p_split_node_p->GetItemCount()},
          insert_item{p_insert_item} {}
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
    InnerRemoveNode(NodeID p_removed_id, const BaseNode *p_child_node_p)
        : DeltaNode{NodeType::InnerRemoveType, p_child_node_p,
                    &p_child_node_p->GetLowKeyPair(),
                    &p_child_node_p->GetHighKeyPair(),
                    p_child_node_p->GetDepth(), p_child_node_p->GetItemCount()},
          removed_id{p_removed_id} {}
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
    InnerMergeNode(const KeyType &p_merge_key, const BaseNode *p_right_merge_p,
                   NodeID p_deleted_node_id, const BaseNode *p_child_node_p)
        : DeltaNode{NodeType::InnerMergeType, p_child_node_p,
                    &p_child_node_p->GetLowKeyPair(),
                    &p_right_merge_p->GetHighKeyPair(),
                    // Note: Since both children under merge node is considered
                    // as part of the same node, we use the larger one + 1
                    // as the depth of the merge node
                    // std::max(p_child_node_p->metadata.depth,
                    //         p_right_merge_p->metadata.depth) + 1,
                    p_child_node_p->GetDepth() + p_right_merge_p->GetDepth(),
                    // For merge node the item count is the sum of its two
                    // branches
                    p_child_node_p->GetItemCount() +
                        p_right_merge_p->GetItemCount()},
          delete_item{p_merge_key, p_deleted_node_id},
          right_merge_p{p_right_merge_p} {}
  };

  /*
   * class InnerAbortNode - Same as LeafAbortNode
   */
  class InnerAbortNode : public DeltaNode {
   public:
    /*
     * Constructor
     */
    InnerAbortNode(const BaseNode *p_child_node_p)
        : DeltaNode{NodeType::InnerAbortType, p_child_node_p,
                    &p_child_node_p->GetLowKeyPair(),
                    &p_child_node_p->GetHighKeyPair(),
                    p_child_node_p->GetDepth(),
                    p_child_node_p->GetItemCount()} {}
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
    NodeSnapshot(NodeID p_node_id, const BaseNode *p_node_p)
        : node_id{p_node_id}, node_p{p_node_p} {}

    /*
     * Default Constructor - Fast path
     */
    NodeSnapshot() {}

    /*
     * IsLeaf() - Test whether current snapshot is on leaf delta chain
     *
     * This function is just a wrapper of IsOnLeafDeltaChain() in BaseNode
     */
    inline bool IsLeaf() const { return node_p->IsOnLeafDeltaChain(); }
  };

  /*
   * union DeltaNodeUnion - The union of all delta nodes - we use this to
   *                        precllocate memory on the base node for delta nodes
   */
  union DeltaNodeUnion {
    InnerInsertNode inner_insert_node;
    InnerDeleteNode inner_delete_node;
    InnerSplitNode inner_split_node;
    InnerMergeNode inner_merge_node;
    InnerRemoveNode inner_remove_node;
    InnerAbortNode inner_abort_node;

    LeafInsertNode leaf_insert_node;
    LeafDeleteNode leaf_delete_node;
    LeafSplitNode leaf_split_node;
    LeafMergeNode leaf_merge_node;
    LeafRemoveNode leaf_remove_node;
  };

  /*
   * class AllocationMeta - Metadata for maintaining preallocated space
   */
  class AllocationMeta {
   public:
    // One reasonable amount of memory for each chunk is
    // delta chain len * struct len + sizeof this struct
    static constexpr size_t CHUNK_SIZE() {
      return sizeof(DeltaNodeUnion) * 8 + sizeof(AllocationMeta);
    }

   private:
    // This points to the higher address end of the chunk we are
    // allocating from
    std::atomic<char *> tail;
    // This points to the lower limit of the memory region we could use
    char *const limit;
    // This forms a linked list which needs to be traversed in order to
    // free chunks of memory
    std::atomic<AllocationMeta *> next;

   public:
    /*
     * Constructor
     */
    AllocationMeta(char *p_tail, char *p_limit)
        : tail{p_tail}, limit{p_limit}, next{nullptr} {}

    /*
     * TryAllocate() - Try to allocate from this chunk
     *
     * Returns the base address if size fits into this chunk
     * Returns nullptr if the chunk is full
     *
     * Note that this function retries internally if CAS fails. Therefore,
     * the number of CAS loop is upperbounded by the chunk size
     */
    void *TryAllocate(size_t size) {
      // This acts as a guard to prevent allocating from a chunk which has
      // already underflown to avoid integer (64 bit pointer) underflow
      if (tail.load() < limit) {
        return nullptr;
      }

      // This substracts size from tail, and assigns
      // the value to local variable in one atomic step
      char *new_tail = tail.fetch_sub(size) - size;

      // If the newly allocated memory is under the minimum then
      // the chunk could not be used anymore
      // Note that if threads keeps subtracting before the new pointer
      if (new_tail < limit) {
        return nullptr;
      }

      return new_tail;
    }

    /*
     * GrowChunk() - Adds one chunk after the current chunk
     *
     * This procedure is thread safe since we always CAS the next pointer
     * with expected value nullptr, such that it should never succeed twice
     * even under contention
     *
     * Whether or not this has succeded, always return the pointer to the next
     * chunk such that the caller could retry on next chunk
     */
    AllocationMeta *GrowChunk() {
      // If we know there is a next chunk just return it to avoid
      // having too many failed CAS instruction
      AllocationMeta *meta_p = next.load();
      if (meta_p != nullptr) {
        return meta_p;
      }

      char *new_chunk = new char[CHUNK_SIZE()];
      AllocationMeta *expected = nullptr;

      // Prepare the new chunk's metadata field
      AllocationMeta *new_meta_base =
          reinterpret_cast<AllocationMeta *>(new_chunk);

      // We initialize the allocation meta at lower end of the address
      // and let tail points to the first byte after this chunk, and the limit
      // is the first byte after AllocationMeta
      new (new_meta_base)
          AllocationMeta{new_chunk + CHUNK_SIZE(),             // tail
                         new_chunk + sizeof(AllocationMeta)};  // limit

      // Always CAS with nullptr such that we will never install/replace
      // a chunk that has already been installed here
      bool ret = next.compare_exchange_strong(expected, new_meta_base);
      if (ret == true) {
        return new_meta_base;
      }

      // Note that here we call destructor manually and then delete the char[]
      // to complete the entire sequence which should be done by the compiler
      new_meta_base->~AllocationMeta();
      delete[] new_chunk;

      // If CAS fails this will be loaded with the real value such that we have
      // free access to the next chunk
      return expected;
    }

    /*
     * Allocate() - Allocates a chunk of memory from the preallocated space
     *
     * This allocation is guaranteed to succeed as long as there is memory. It
     * tries to allocate from the first chunk pointed to by the current first
     * and then if it fails go to the next chunk and try to allocate there
     *
     * Note that this must be called at the header node of the chain, since it
     * takes "this" pointer and iterate using the "next" field
     */
    void *Allocate(size_t size) {
      AllocationMeta *meta_p = this;
      while (1) {
        // Allocate from the current chunk first
        // If this is nullptr then this chunk has been depleted
        void *p = meta_p->TryAllocate(size);
        if (p == nullptr) {
          // Then try to grow it - if there is already another next chunk
          // just return the pointer to that chunk
          // This will surely traverse the entire linked list
          // but since the linked list itself is supposed to be relatively short
          // even under contention, we do not worry about it right now
          meta_p = meta_p->GrowChunk();
          PL_ASSERT(meta_p != nullptr);
        } else {
          return p;
        }
      }

      PL_ASSERT(false);
      return nullptr;
    }

    /*
     * Destroy() - Frees all chunks in the linked list
     *
     * Note that this function must be called for every metadata object
     * in the linked list, and we could should use operator delete since it
     * is allocated through operator new
     *
     * This function is not thread-safe and should only be called in a single
     * thread environment such as GC
     */
    void Destroy() {
      AllocationMeta *meta_p = this;

      while (meta_p != nullptr) {
        // Save the next pointer to traverse to it later
        AllocationMeta *next_p = meta_p->next.load();

        // 1. Manually call destructor
        // 2. Delete it as a char[]
        // Note that we know the base of meta_p is always the address
        // returned by operator new[]
        meta_p->~AllocationMeta();
        delete[] reinterpret_cast<char *>(meta_p);

        meta_p = next_p;
      }

      return;
    }
  };

  /*
   * class ElasticNode - The base class for elastic node types, i.e. InnerNode
   *                     and LeafNode
   *
   * Since for InnerNode and LeafNode, the number of elements is not a compile
   * time known constant. However, for efficient tree traversal we must inline
   * all elements to reduce cache misses with workload that's less predictable
   */
  template <typename ElementType>
  class ElasticNode : public BaseNode {
   private:
    // These two are the low key and high key of the node respectively
    // since we could not add it in the inherited class (will clash with
    // the array which is invisible to the compiler) so they must be added here
    KeyNodeIDPair low_key;
    KeyNodeIDPair high_key;

    // This is the end of the elastic array
    // We explicitly store it here to avoid calculating the end of the array
    // everytime
    ElementType *end;

    // This is the starting point
    ElementType start[0];

   public:
    /*
     * Constructor
     *
     * Note that this constructor uses the low key and high key stored as
     * members to initialize the NodeMetadata object in class BaseNode
     */
    ElasticNode(NodeType p_type, int p_depth, int p_item_count,
                const KeyNodeIDPair &p_low_key, const KeyNodeIDPair &p_high_key)
        : BaseNode{p_type, &low_key, &high_key, p_depth, p_item_count},
          low_key{p_low_key},
          high_key{p_high_key},
          end{start} {}

    /*
     * Copy() - Copy constructs another instance
     */
    static ElasticNode *Copy(const ElasticNode &other) {
      ElasticNode *node_p = ElasticNode::Get(
          other.GetItemCount(), other.GetType(), other.GetDepth(),
          other.GetItemCount(), other.GetLowKeyPair(), other.GetHighKeyPair());

      node_p->PushBack(other.Begin(), other.End());

      return node_p;
    }

    /*
     * Destructor
     *
     * All element types are destroyed inside the destruction function. D'tor
     * is called by Destroy(), and therefore should not be called directly
     * by external functions.
     *
     * Note that this is not called by Destroy() and instead it should be
     * called by an external function that destroies a delta chain, since in one
     * instance of thie class there might be multiple nodes of different types
     * so destroying should be dont individually with each type.
     */
    ~ElasticNode() {
      // Use two iterators to iterate through all existing elements
      for (ElementType *element_p = Begin(); element_p != End(); element_p++) {
        // Manually calls destructor when the node is destroyed
        element_p->~ElementType();
      }

      return;
    }

    /*
     * Destroy() - Frees the memory by calling AllocationMeta::Destroy()
     *
     * Note that function does not call destructor, and instead the destructor
     * should be called first before this function is called
     *
     * Note that this function does not call destructor for the class since
     * it holds multiple instances of tree nodes, we should call destructor
     * for each individual type outside of this class, and only frees memory
     * when Destroy() is called.
     */
    void Destroy() const {
      // This finds the allocation header for this base node, and then
      // traverses the linked list
      ElasticNode::GetAllocationHeader(this)->Destroy();

      return;
    }

    /*
     * Begin() - Returns a begin iterator to its internal array
     */
    inline ElementType *Begin() { return start; }

    inline const ElementType *Begin() const { return start; }

    /*
     * End() - Returns an end iterator that is similar to the one for vector
     */
    inline ElementType *End() { return end; }

    inline const ElementType *End() const { return end; }

    /*
     * REnd() - Returns the element before the first element
     *
     * Note that since we returned an invalid pointer into the array, the
     * return value should not be modified and is therefore of const type
     */
    inline const ElementType *REnd() { return start - 1; }

    inline const ElementType *REnd() const { return start - 1; }

    /*
     * GetSize() - Returns the size of the embedded list
     *
     * Note that the return type is integer since we use integer to represent
     * the size of a node
     */
    inline int GetSize() const { return static_cast<int>(End() - Begin()); }

    /*
     * PushBack() - Push back an element
     *
     * This function takes an element type and copy-construct it on the array
     * which is invisible to the compiler. Therefore we must call placement
     * operator new to do the job
     */
    inline void PushBack(const ElementType &element) {
      // Placement new + copy constructor using end pointer
      new (end) ElementType{element};

      // Move it pointing to the enxt available slot, if not reached the end
      end++;

      return;
    }

    /*
     * PushBack() - Push back a series of elements
     *
     * The overloaded PushBack() could also push an array of elements
     */
    inline void PushBack(const ElementType *copy_start_p,
                         const ElementType *copy_end_p) {
      // Make sure the loop will come to an end
      PL_ASSERT(copy_start_p <= copy_end_p);

      while (copy_start_p != copy_end_p) {
        PushBack(*copy_start_p);
        copy_start_p++;
      }

      return;
    }

   public:
    /*
     * Get() - Static helper function that constructs a elastic node of
     *         a certain size
     *
     * Note that since operator new is only capable of allocating a fixed
     * sized structure, we need to call malloc() directly to deal with variable
     * lengthed node. However, after malloc() returns we use placement operator
     * new to initialize it, such that the node could be freed using operator
     * delete later on
     */
    inline static ElasticNode *Get(int size,  // Number of elements
                                   NodeType p_type, int p_depth,
                                   int p_item_count,  // Usually equal to size
                                   const KeyNodeIDPair &p_low_key,
                                   const KeyNodeIDPair &p_high_key) {
      // Currently this is always true - if we want a larger array then
      // just remove this line
      PL_ASSERT(size == p_item_count);

      // Allocte memory for
      //   1. AllocationMeta (chunk)
      //   2. node meta
      //   3. ElementType array
      // basic template + ElementType element size * (node size) + CHUNK_SIZE()
      // Note: do not make it constant since it is going to be modified
      // after being returned
      char *alloc_base =
          new char[sizeof(ElasticNode) + size * sizeof(ElementType) +
                   AllocationMeta::CHUNK_SIZE()];
      PL_ASSERT(alloc_base != nullptr);

      // Initialize the AllocationMeta - tail points to the first byte inside
      // class ElasticNode; limit points to the first byte after class
      // AllocationMeta
      new (reinterpret_cast<AllocationMeta *>(alloc_base))
          AllocationMeta{alloc_base + AllocationMeta::CHUNK_SIZE(),
                         alloc_base + sizeof(AllocationMeta)};

      // The first CHUNK_SIZE() byte is used by class AllocationMeta
      // and chunk data
      ElasticNode *node_p = reinterpret_cast<ElasticNode *>(
          alloc_base + AllocationMeta::CHUNK_SIZE());

      // Call placement new to initialize all that could be initialized
      new (node_p)
          ElasticNode{p_type, p_depth, p_item_count, p_low_key, p_high_key};

      return node_p;
    }

    /*
     * GetNodeHeader() - Given low key pointer, returns the node header
     *
     * This is useful since only the low key pointer is available from any
     * type of node
     */
    static ElasticNode *GetNodeHeader(const KeyNodeIDPair *low_key_p) {
      static constexpr size_t low_key_offset = offsetof(ElasticNode, low_key);

      return reinterpret_cast<ElasticNode *>(
          reinterpret_cast<uint64_t>(low_key_p) - low_key_offset);
    }

    /*
     * GetAllocationHeader() - Returns the address of class AllocationHeader
     *                         embedded inside the ElasticNode object
     */
    static AllocationMeta *GetAllocationHeader(const ElasticNode *node_p) {
      return reinterpret_cast<AllocationMeta *>(
          reinterpret_cast<uint64_t>(node_p) - AllocationMeta::CHUNK_SIZE());
    }

    /*
     * InlineAllocate() - Allocates a delta node in preallocated area preceeds
     *                    the data area of this ElasticNode
     *
     * Note that for any given NodeType, we always know its low key and the
     * low key always points to the struct inside base node. This way, we
     * compute the offset of the low key from the begining of the struct,
     * and then subtract it with CHUNK_SIZE() to derive the address of
     * class AllocationMeta
     *
     * Note that since this function is accessed when the header is unknown
     * so (1) it is static, and (2) it takes low key p which is universally
     * available for all node type (stored in NodeMetadata)
     */
    static void *InlineAllocate(const KeyNodeIDPair *low_key_p, size_t size) {
      const ElasticNode *node_p = GetNodeHeader(low_key_p);
      PL_ASSERT(&node_p->low_key == low_key_p);

      // Jump over chunk content
      AllocationMeta *meta_p = GetAllocationHeader(node_p);

      void *p = meta_p->Allocate(size);
      PL_ASSERT(p != nullptr);

      return p;
    }

    /*
     * At() - Access element with bounds checking under debug mode
     */
    inline ElementType &At(const int index) {
      // The index must be inside the valid range
      PL_ASSERT(index < GetSize());

      return *(Begin() + index);
    }

    inline const ElementType &At(const int index) const {
      // The index must be inside the valid range
      PL_ASSERT(index < GetSize());

      return *(Begin() + index);
    }
  };

  /*
   * class InnerNode - Inner node that holds separators
   */
  class InnerNode : public ElasticNode<KeyNodeIDPair> {
   public:
    /*
     * Constructor - Deleted
     *
     * All construction of InnerNode should be through ElasticNode interface
     */
    InnerNode() = delete;
    InnerNode(const InnerNode &) = delete;
    InnerNode(InnerNode &&) = delete;
    InnerNode &operator=(const InnerNode &) = delete;
    InnerNode &operator=(InnerNode &&) = delete;

    /*
     * Destructor - Calls destructor of ElasticNode
     */
    ~InnerNode() { this->~ElasticNode<KeyNodeIDPair>(); }

    /*
     * GetSplitSibling() - Split InnerNode into two halves.
     *
     * This function does not change the current node since all existing nodes
     * should be read-only to avoid data race. It copies half of the inner node
     * into the split sibling, and return the sibling node.
     */
    InnerNode *GetSplitSibling() const {
      // Call function in class ElasticNode to determine the size of the
      // inner node
      int key_num = this->GetSize();

      // Inner node size must be > 2 to avoid empty split node
      PL_ASSERT(key_num >= 2);

      // Same reason as in leaf node - since we only split inner node
      // without a delta chain on top of it, the sep list size must equal
      // the recorded item count
      PL_ASSERT(key_num == this->GetItemCount());

      int split_item_index = key_num / 2;

      // This is the split point of the inner node
      auto copy_start_it = this->Begin() + split_item_index;

      // We need this to allocate enough space for the embedded array
      int sibling_size =
          static_cast<int>(std::distance(copy_start_it, this->End()));

      // This sets metadata inside BaseNode by calling SetMetaData()
      // inside inner node constructor
      InnerNode *inner_node_p =
          reinterpret_cast<InnerNode *>(ElasticNode<KeyNodeIDPair>::Get(
              sibling_size, NodeType::InnerType, 0, sibling_size,
              this->At(split_item_index), this->GetHighKeyPair()));

      // Call overloaded PushBack() to insert an array of elements
      inner_node_p->PushBack(copy_start_it, this->End());

      // Since we copy exactly that many elements
      PL_ASSERT(inner_node_p->GetSize() == sibling_size);
      PL_ASSERT(inner_node_p->GetSize() == inner_node_p->GetItemCount());

      return inner_node_p;
    }
  };

  /*
   * class LeafNode - Leaf node that holds data
   *
   * There are 5 types of delta nodes that could be appended
   * to a leaf node. 3 of them are SMOs, and 2 of them are data operation
   */
  class LeafNode : public ElasticNode<KeyValuePair> {
   public:
    LeafNode() = delete;
    LeafNode(const LeafNode &) = delete;
    LeafNode(LeafNode &&) = delete;
    LeafNode &operator=(const LeafNode &) = delete;
    LeafNode &operator=(LeafNode &&) = delete;

    /*
     * Destructor - Calls underlying ElasticNode d'tor
     */
    ~LeafNode() { this->~ElasticNode<KeyValuePair>(); }

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
      int central_index = this->GetSize() / 2;
      PL_ASSERT(central_index > 1);

      // This will used as upper_bound and lower_bound key
      const KeyValuePair &central_kvp = this->At(central_index);

      // Move it to the element before data_list
      auto it = this->Begin() + central_index - 1;

      // If iterator has reached the begin then we know there could not
      // be any split points
      while ((it != this->Begin()) &&
             (t->KeyCmpEqual(it->first, central_kvp.first) == true)) {
        it--;
      }

      // This is the real split point
      it++;

      // This size is exactly the index of the split point
      int left_sibling_size = std::distance(this->Begin(), it);

      if (left_sibling_size >
          static_cast<int>(LEAF_NODE_SIZE_LOWER_THRESHOLD)) {
        return left_sibling_size;
      }

      // Move it to the element after data_list
      it = this->Begin() + central_index + 1;

      // If iterator has reached the end then we know there could not
      // be any split points
      while ((it != this->End()) &&
             (t->KeyCmpEqual(it->first, central_kvp.first) == true)) {
        it++;
      }

      int right_sibling_size = std::distance(it, this->End());

      if (right_sibling_size >
          static_cast<int>(LEAF_NODE_SIZE_LOWER_THRESHOLD)) {
        return std::distance(this->Begin(), it);
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
      PL_ASSERT(this->GetSize() == this->GetItemCount());

      // This is the index of the actual key-value pair in data_list
      // We need to substract this value from the prefix sum in the new
      // inner node
      int split_item_index = FindSplitPoint(t);

      // Could not split because we could not find a split point
      // and the caller is responsible for not spliting the node
      // This should happen relative infrequently, in a sense that
      // oversized page would affect performance
      if (split_item_index == -1) {
        return nullptr;
      }

      // This is an iterator pointing to the split point in the vector
      // note that std::advance() operates efficiently on std::vector's
      // RandomAccessIterator
      auto copy_start_it = this->Begin() + split_item_index;

      // This is the end point for later copy of data
      auto copy_end_it = this->End();

      // This is the key part of the key-value pair, also the low key
      // of the new node and new high key of the current node (will be
      // reflected in split delta later in its caller)
      const KeyType &split_key = copy_start_it->first;

      int sibling_size =
          static_cast<int>(std::distance(copy_start_it, copy_end_it));

      // This will call SetMetaData inside its constructor
      LeafNode *leaf_node_p =
          reinterpret_cast<LeafNode *>(ElasticNode<KeyValuePair>::Get(
              sibling_size, NodeType::LeafType, 0, sibling_size,
              std::make_pair(split_key, ~INVALID_NODE_ID),
              this->GetHighKeyPair()));

      // Copy data item into the new node using PushBack()
      leaf_node_p->PushBack(copy_start_it, copy_end_it);

      PL_ASSERT(leaf_node_p->GetSize() == sibling_size);
      PL_ASSERT(leaf_node_p->GetSize() == leaf_node_p->GetItemCount());

      return leaf_node_p;
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
   *
   * Some properties of the tree should be specified in the argument.
   *
   *   start_gc_thread - If set to true then a separate gc thred will be
   *                     started. Otherwise GC must be done by the user
   *                     using PerformGarbageCollection() interface
   */
  BwTree(bool start_gc_thread = true,
         KeyComparator p_key_cmp_obj = KeyComparator{},
         KeyEqualityChecker p_key_eq_obj = KeyEqualityChecker{},
         KeyHashFunc p_key_hash_obj = KeyHashFunc{},
         ValueEqualityChecker p_value_eq_obj = ValueEqualityChecker{},
         ValueHashFunc p_value_hash_obj = ValueHashFunc{})
      : BwTreeBase(),
        // Key comparator, equality checker and hasher
        key_cmp_obj{p_key_cmp_obj},
        key_eq_obj{p_key_eq_obj},
        key_hash_obj{p_key_hash_obj},

        // Value equality checker and hasher
        value_eq_obj{p_value_eq_obj},
        value_hash_obj{p_value_hash_obj},

        // key-node ID pair cmp, equality checker and hasher
        key_node_id_pair_cmp_obj{this},
        key_node_id_pair_eq_obj{this},
        key_node_id_pair_hash_obj{this},

        // key-value pair cmp, equality checker and hasher
        key_value_pair_cmp_obj{this},
        key_value_pair_eq_obj{this},
        key_value_pair_hash_obj{this},

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
    LOG_TRACE(
        "Bw-Tree Constructor called. "
        "Setting up execution environment...");

    InitMappingTable();
    InitNodeLayout();

    LOG_TRACE("sizeof(NodeMetaData) = %lu is the overhead for each node",
              sizeof(NodeMetaData));
    LOG_TRACE("sizeof(KeyType) = %lu is the size of key", sizeof(KeyType));

    // We could choose not to start GC thread inside the BwTree
    // in that case GC must be done by calling the interface
    if (start_gc_thread == true) {
      LOG_TRACE("Starting epoch manager thread...");
      epoch_manager.StartThread();
    }

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
    LOG_TRACE("Next node ID at exit: %" PRIu64 "", next_unused_node_id.load());
    LOG_TRACE("Destructor: Free tree nodes");

    // Clear all garbage nodes awaiting cleaning
    // First of all it should set all last active epoch counter to -1
    ClearThreadLocalGarbage();

    // Free all nodes recursively
    size_t node_count = FreeNodeByNodeID(root_id.load());

    (void)node_count;
    LOG_TRACE("Freed %lu tree nodes", node_count);

    return;
  }

  /*
   * ClearThreadLocalGarbage() - Clears all thread local garbage
   *
   * This must be called under single threaded environment
   */
  void ClearThreadLocalGarbage() {
    // First of all we should set all last active counter to -1 to
    // guarantee progress to clear all epoches
    for (size_t i = 0; i < GetThreadNum(); i++) {
      UnregisterThread(i);
    }

    for (size_t i = 0; i < GetThreadNum(); i++) {
      // Here all epoch counters have been set to 0xFFFFFFFFFFFFFFFF
      // so GC should always succeed
      PerformGC(i);

      // This will collect all nodes since we have adjusted the currenr thread
      // GC ID
      PL_ASSERT(GetGCMetaData(i)->node_count == 0);
    }

    return;
  }

  /*
   * UpdateThreadLocal() - Frees all memorys currently existing and then
   *                       reallocate a chunk of memory to represent the
   *                       thread local variables
   *
   * This function is majorly used for debugging pruposes. The argument is
   * the new number of threads we want to support here for doing experiments
   */
  void UpdateThreadLocal(size_t p_thread_num) {
    LOG_TRACE("Updating thread-local array to length %lu......", p_thread_num);

    // 1. Frees all pending memory chunks
    // 2. Frees the thread local array
    ClearThreadLocalGarbage();
    DestroyThreadLocal();

    SetThreadNum(p_thread_num);

    // 3. Allocate a new array based on the new given size
    // Here all epoches are restored to 0
    PrepareThreadLocal();

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
    if (node_p == nullptr) {
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
    // free_node_id_list.SingleThreadPush(node_id);

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

    while (1) {
      node_p = next_node_p;
      PL_ASSERT(node_p != nullptr);

      NodeType type = node_p->GetType();

      switch (type) {
        case NodeType::LeafInsertType:
          next_node_p = ((LeafInsertNode *)node_p)->child_node_p;

          ((LeafInsertNode *)node_p)->~LeafInsertNode();
          freed_count++;

          break;
        case NodeType::LeafDeleteType:
          next_node_p = ((LeafDeleteNode *)node_p)->child_node_p;

          ((LeafDeleteNode *)node_p)->~LeafDeleteNode();

          break;
        case NodeType::LeafSplitType:
          next_node_p = ((LeafSplitNode *)node_p)->child_node_p;

          freed_count +=
              FreeNodeByNodeID(((LeafSplitNode *)node_p)->insert_item.second);

          ((LeafSplitNode *)node_p)->~LeafSplitNode();
          freed_count++;

          break;
        case NodeType::LeafMergeType:
          freed_count +=
              FreeNodeByPointer(((LeafMergeNode *)node_p)->child_node_p);
          freed_count +=
              FreeNodeByPointer(((LeafMergeNode *)node_p)->right_merge_p);

          ((LeafMergeNode *)node_p)->~LeafMergeNode();
          freed_count++;

          // Leaf merge node is an ending node
          return freed_count;
        case NodeType::LeafType:
          // Call destructor first, and then call Destroy() on its preallocated
          // linked list of chunks
          ((LeafNode *)node_p)->~LeafNode();

          // Free the memory
          ((LeafNode *)node_p)->Destroy();

          freed_count++;

          // We have reached the end of delta chain
          return freed_count;
        case NodeType::InnerInsertType:
          next_node_p = ((InnerInsertNode *)node_p)->child_node_p;

          freed_count +=
              FreeNodeByNodeID(((InnerInsertNode *)node_p)->item.second);

          ((InnerInsertNode *)node_p)->~InnerInsertNode();
          freed_count++;

          break;
        case NodeType::InnerDeleteType:
          next_node_p = ((InnerDeleteNode *)node_p)->child_node_p;

          // For those already deleted, the delta chain has been merged
          // and the remove node should be freed (either before this point
          // or will be freed) epoch manager
          // NOTE: No need to call InvalidateNodeID since this function is
          // only called on destruction of the tree
          mapping_table[((InnerDeleteNode *)node_p)->item.second] = nullptr;

          ((InnerDeleteNode *)node_p)->~InnerDeleteNode();
          freed_count++;

          break;
        case NodeType::InnerSplitType:
          next_node_p = ((InnerSplitNode *)node_p)->child_node_p;

          freed_count +=
              FreeNodeByNodeID(((LeafSplitNode *)node_p)->insert_item.second);

          ((InnerSplitNode *)node_p)->~InnerSplitNode();
          freed_count++;

          break;
        case NodeType::InnerMergeType:
          freed_count +=
              FreeNodeByPointer(((InnerMergeNode *)node_p)->child_node_p);
          freed_count +=
              FreeNodeByPointer(((InnerMergeNode *)node_p)->right_merge_p);

          ((InnerMergeNode *)node_p)->~InnerMergeNode();
          freed_count++;

          return freed_count;
        case NodeType::InnerType: {
          const InnerNode *inner_node_p =
              static_cast<const InnerNode *>(node_p);

          // Free NodeID one by one stored in its separator list
          // Even if they are already freed (e.g. a split delta has not
          // been consolidated would share a NodeID with its parent)
          for (auto it = inner_node_p->Begin(); it != inner_node_p->End();
               it++) {
            freed_count += FreeNodeByNodeID(it->second);
          }

          inner_node_p->~InnerNode();
          inner_node_p->Destroy();

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
          LOG_DEBUG("Unknown node type: %d", (int)type);

          PL_ASSERT(false);
          return 0;
      }  // switch
    }    // while 1

    PL_ASSERT(false);
    return 0;
  }

  /*
   * InitNodeLayout() - Initialize the nodes required to start BwTree
   *
   * We need at least a root inner node and a leaf node in order
   * to guide the first operation to the right place
   */
  void InitNodeLayout() {
    LOG_TRACE("Initializing node layout for root and first page...");

    root_id = GetNextNodeID();
    PL_ASSERT(root_id == 1UL);

    // This is important since in the iterator we will use NodeID = 2
    // as the starting point of the traversal
    first_leaf_id = GetNextNodeID();
    PL_ASSERT(first_leaf_id == FIRST_LEAF_NODE_ID);

#ifdef BWTREE_PELOTON

    // For the first inner node, it needs an empty low key
    // the search procedure will not look at it and only use it
    // if the search key could not be matched to anything after the first key
    KeyNodeIDPair first_sep{KeyType(), first_leaf_id};

    // Initially there is one element inside the root node
    // so we set item count to be 1
    // The high key is +Inf which is identified by INVALID_NODE_ID
    InnerNode *root_node_p =
        reinterpret_cast<InnerNode *>(ElasticNode<KeyNodeIDPair>::Get(
            1, NodeType::InnerType, 0, 1, first_sep,
            std::make_pair(KeyType(), INVALID_NODE_ID)));

#else

    // For the first inner node, it needs an empty low key
    // the search procedure will not look at it and only use it
    // if the search key could not be matched to anything after the first key
    KeyNodeIDPair first_sep{KeyType{}, first_leaf_id};

    // Initially there is one element inside the root node
    // so we set item count to be 1
    // The high key is +Inf which is identified by INVALID_NODE_ID
    InnerNode *root_node_p =
        reinterpret_cast<InnerNode *>(ElasticNode<KeyNodeIDPair>::Get(
            1, NodeType::InnerType, 0, 1,
            first_sep,  // Copy this as the first key
            std::make_pair(KeyType{}, INVALID_NODE_ID)));

#endif

    root_node_p->PushBack(first_sep);

    LOG_TRACE("root id = %" PRIu64 "; first leaf id = %" PRIu64 "", root_id.load(),
              first_leaf_id);

    InstallNewNode(root_id, root_node_p);

// Initially there is no element inside the leaf node so we set element
// count to be 0
#ifdef BWTREE_PELOTON

    LeafNode *left_most_leaf =
        reinterpret_cast<LeafNode *>(ElasticNode<KeyValuePair>::Get(
            0, NodeType::LeafType, 0, 0,
            std::make_pair(KeyType(), INVALID_NODE_ID),
            std::make_pair(KeyType(), INVALID_NODE_ID)));

#else

    LeafNode *left_most_leaf =
        reinterpret_cast<LeafNode *>(ElasticNode<KeyValuePair>::Get(
            0, NodeType::LeafType, 0, 0,
            std::make_pair(KeyType{}, INVALID_NODE_ID),
            std::make_pair(KeyType{}, INVALID_NODE_ID)));

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
    LOG_TRACE("Initializing mapping table.... size = %lu", MAPPING_TABLE_SIZE);
    LOG_TRACE("Fast initialization: Do not set to zero");

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
    if (ret_pair.first == false) {
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
  inline bool InstallNodeToReplace(NodeID node_id, const BaseNode *node_p,
                                   const BaseNode *prev_p) {
    // Make sure node id is valid and does not exceed maximum
    PL_ASSERT(node_id != INVALID_NODE_ID);
    PL_ASSERT(node_id < MAPPING_TABLE_SIZE);

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
    return root_id.compare_exchange_strong(old_root_node_id, new_root_node_id);
  }

  /*
   * InstallNewNode() - Install a new node into the mapping table
   *
   * This function does not return any value since we assume new node
   * installation would always succeed
   */
  inline void InstallNewNode(NodeID node_id, const BaseNode *node_p) {
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
    PL_ASSERT(node_id != INVALID_NODE_ID);
    PL_ASSERT(node_id < MAPPING_TABLE_SIZE);

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
  const KeyValuePair *Traverse(Context *context_p, const ValueType *value_p,
                               std::pair<int, bool> *index_pair_p) {
    // For value collection it always returns nullptr
    const KeyValuePair *found_pair_p = nullptr;

  retry_traverse:
    PL_ASSERT(context_p->abort_flag == false);
#ifdef BWTREE_DEBUG
    PL_ASSERT(context_p->current_level == -1);
#endif

    // This is the serialization point for reading/writing root node
    NodeID start_node_id = root_id.load();

    // This is used to identify root nodes
    // NOTE: We set current snapshot since in LoadNodeID() or read opt.
    // version the parent node snapshot will be overwritten with this child
    // node snapshot
    //
    // NOTE 2: We could not use GetLatestNodeSnashot() here since it checks
    // current_level, which is -1 at this point
    context_p->current_snapshot.node_id = INVALID_NODE_ID;

    // We need to call this even for root node since there could
    // be split delta posted on to root node
    LoadNodeID(start_node_id, context_p);

    // There could be an abort here, and we could not directly jump
    // to Init state since we would like to do some clean up or
    // statistics after aborting
    if (context_p->abort_flag == true) {
      goto abort_traverse;
    }

    LOG_TRACE("Successfully loading root node ID");

    while (1) {
      NodeID child_node_id = NavigateInnerNode(context_p);

      // Navigate could abort since it might go to another NodeID
      // if there is a split delta and the key is >= split key
      if (context_p->abort_flag == true) {
        LOG_TRACE("Navigate Inner Node abort. ABORT");

        // If NavigateInnerNode() aborts then it retrns INVALID_NODE_ID
        // as a double check
        // This is the only situation that this function returns
        // INVALID_NODE_ID
        PL_ASSERT(child_node_id == INVALID_NODE_ID);

        goto abort_traverse;
      }

      // This might load a leaf child
      // Also LoadNodeID() does not guarantee the node bound matches
      // seatch key. Since we could readjust using the split side link
      // during Navigate...Node()
      LoadNodeID(child_node_id, context_p);

      if (context_p->abort_flag == true) {
        LOG_TRACE("LoadNodeID aborted. ABORT");

        goto abort_traverse;
      }

      // This is the node we have just loaded
      NodeSnapshot *snapshot_p = GetLatestNodeSnapshot(context_p);

      if (snapshot_p->IsLeaf() == true) {
        LOG_TRACE("The next node is a leaf");

        break;
      }
    }  // while(1)

    if (value_p == nullptr) {
      // We are using an iterator just to get a leaf page
      PL_ASSERT(index_pair_p == nullptr);

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

    if (context_p->abort_flag == true) {
      LOG_TRACE(
          "NavigateLeafNode() or NavigateSiblingChain()"
          " aborts. ABORT");

      goto abort_traverse;
    }

#ifdef BWTREE_DEBUG

    LOG_TRACE("Found leaf node. Abort count = %d, level = %d",
              context_p->abort_counter, context_p->current_level);

#endif

    // If there is no abort then we could safely return
    return found_pair_p;

  abort_traverse:
#ifdef BWTREE_DEBUG

    PL_ASSERT(context_p->current_level >= 0);

    context_p->current_level = -1;

    context_p->abort_counter++;

#endif

    // This is used to identify root node
    context_p->current_snapshot.node_id = INVALID_NODE_ID;

    context_p->abort_flag = false;

    goto retry_traverse;

    PL_ASSERT(false);
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
      if ((node_p->GetNextNodeID() != INVALID_NODE_ID) &&
          (KeyCmpGreaterEqual(context_p->search_key, node_p->GetHighKey()))) {
        LOG_TRACE(
            "Bounds checking failed (id = %" PRIu64 ") - "
            "Go right.",
            snapshot_p->node_id);

        JumpToNodeID(node_p->GetNextNodeID(), context_p);

        if (context_p->abort_flag == true) {
          LOG_TRACE("JumpToNodeID aborts(). ABORT");

          return;
        }

      } else {
        break;
      }
    } while (1);

    return;
  }

  /*
   * NavigateSiblingChainBI() - Navigates sibling chain for backward iteration
   *
   * This function traverses to the right sibling of the current node only if
   * the search key > current high key. In the case of search key == high key
   * we stay on the same node to enable finding the left node of the node with
   * the search key as low key
   */
  void NavigateSiblingChainBI(Context *context_p) {
    while (1) {
      NodeSnapshot *snapshot_p = GetLatestNodeSnapshot(context_p);
      const BaseNode *node_p = snapshot_p->node_p;
      if ((node_p->GetNextNodeID() != INVALID_NODE_ID) &&
          (KeyCmpGreater(context_p->search_key, node_p->GetHighKey()))) {
        LOG_TRACE(
            "Bounds checking for BI failed (id = %" PRIu64 ") - "
            "Go right.",
            snapshot_p->node_id);

        JumpToNodeID(node_p->GetNextNodeID(), context_p);
        if (context_p->abort_flag == true) {
          LOG_TRACE("JumpToNodeID() aborts for BI. ABORT");
          return;
        }
      } else {
        break;
      }
    }  // while(1)

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
                                     const InnerNode *inner_node_p,
                                     const KeyNodeIDPair *start_p,
                                     const KeyNodeIDPair *end_p) {
    // Inner node could not be empty
    PL_ASSERT(inner_node_p->GetSize() != 0UL);
    (void)inner_node_p;

    // Hopefully std::upper_bound would use binary search here
    auto it = std::upper_bound(start_p, end_p,
                               std::make_pair(search_key, INVALID_NODE_ID),
                               key_node_id_pair_cmp_obj) -
              1;
#ifdef BWTREE_DEBUG
// auto it2 = std::upper_bound(inner_node_p->Begin() + 1,
//                           inner_node_p->End(),
//                           std::make_pair(search_key, INVALID_NODE_ID),
//                           key_node_id_pair_cmp_obj) - 1;

// PL_ASSERT(it == it2);
#endif

    // Since upper_bound returns the first element > given key
    // so we need to decrease it to find the last element <= given key
    // which is out separator key
    return it->second;
  }

  /*
   * LocateSeparatorByKeyBI() - Same as locate separator by key but it
   *                            goes left when we see the selected separator
   *                            being the search key
   *
   * This function guarantees to find a left key if we see the separator
   * being the search key. This is because if there is not a left key then we
   * we must have come down from a node where the search key is its separator
   * key (remember that the separator key is the low key of ots child node)
   */
  inline NodeID LocateSeparatorByKeyBI(const KeyType &search_key,
                                       const InnerNode *inner_node_p) {
    PL_ASSERT(inner_node_p->GetSize() != 0UL);
    auto it = std::upper_bound(inner_node_p->Begin() + 1, inner_node_p->End(),
                               std::make_pair(search_key, INVALID_NODE_ID),
                               key_node_id_pair_cmp_obj) -
              1;

    if (KeyCmpEqual(it->first, search_key) == true) {
      // If search key is the low key then we know we should have already
      // gone left on the parent node
      PL_ASSERT(it != inner_node_p->Begin());

      // Go to the left separator to find the left node with range < search key
      // After decreament it might or might not be the low key
      it--;
    }

    return it->second;
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
  NodeID NavigateInnerNode(Context *context_p) {
    // This will go to the right sibling until we have seen
    // a node whose range match the search key
    NavigateSiblingChain(context_p);

    // If navigating sibling chain aborts then abort here
    if (context_p->abort_flag) {
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
    PL_ASSERT(snapshot_p->IsLeaf() == false);
    PL_ASSERT(snapshot_p->node_p != nullptr);

    // For read only workload this is always true since we do not need
    // to remember the node ID for read - read is always stateless until
    // it has reached a leaf node
    PL_ASSERT(snapshot_p->node_id != INVALID_NODE_ID);

    LOG_TRACE("Navigating inner node delta chain...");

    // Always start with the first element
    const KeyNodeIDPair *start_p =
        InnerNode::GetNodeHeader(&node_p->GetLowKeyPair())->Begin() + 1;
    // Use low key pair to find base node and then use base node pointer to find
    // total number of elements in the array. We search in this array later
    const KeyNodeIDPair *end_p =
        InnerNode::GetNodeHeader(&node_p->GetLowKeyPair())->End();

    while (1) {
      NodeType type = node_p->GetType();

      switch (type) {
        case NodeType::InnerType: {
          const InnerNode *inner_node_p =
              static_cast<const InnerNode *>(node_p);

          // We always use the ubound recorded inside the top of the
          // delta chain
          NodeID target_id =
              LocateSeparatorByKey(search_key, inner_node_p, start_p, end_p);

          LOG_TRACE("Found child in inner node; child ID = %" PRIu64 "", target_id);

          return target_id;
        }  // case InnerType
        case NodeType::InnerInsertType: {
          const InnerInsertNode *insert_node_p =
              static_cast<const InnerInsertNode *>(node_p);

          const KeyNodeIDPair &insert_item = insert_node_p->item;
          const KeyNodeIDPair &next_item = insert_node_p->next_item;

          // This comparison servers two purposes:
          //   1. Check whether we could use it to do a quick jump
          //   2. Update start_index or end_index depending on the
          //      result of comparison
          if (KeyCmpGreaterEqual(search_key, insert_item.first)) {
            if ((next_item.second == INVALID_NODE_ID) ||
                (KeyCmpLess(search_key, next_item.first))) {
              LOG_TRACE("Find target ID = %" PRIu64 " in insert delta",
                        insert_item.second);

              return insert_item.second;
            }

            start_p = std::max(start_p, insert_node_p->location);
          } else {
            end_p = std::min(end_p, insert_node_p->location);
          }

          break;
        }  // InnerInsertType
        case NodeType::InnerDeleteType: {
          const InnerDeleteNode *delete_node_p =
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
          if ((delete_node_p->GetLowKeyNodeID() == prev_item.second) ||
              (KeyCmpGreaterEqual(search_key, prev_item.first))) {
            // If the next item is +Inf key then we also choose not to compare
            // keys directly since we know the search key is definitely smaller
            // then +Inf
            if ((next_item.second == INVALID_NODE_ID) ||
                (KeyCmpLess(search_key, next_item.first))) {
              LOG_TRACE("Find target ID = %" PRIu64 " in delete delta",
                        prev_item.second);

              return prev_item.second;
            }
          }

          // Use the deleted key to do a divide - all keys less than
          // it is on the left of the index recorded in this InnerInsertNode
          // Otherwise it is to the right of it
          if (KeyCmpGreaterEqual(search_key, delete_node_p->item.first) ==
              true) {
            start_p = std::max(delete_node_p->location, start_p);
          } else {
            end_p = std::min(delete_node_p->location, end_p);
          }

          break;
        }  // InnerDeleteType
        case NodeType::InnerSplitType: {
          break;
        }  // case InnerSplitType
        case NodeType::InnerMergeType: {
          const InnerMergeNode *merge_node_p =
              static_cast<const InnerMergeNode *>(node_p);

          const KeyType &merge_key = merge_node_p->delete_item.first;

          // Here since we will only take one branch, so
          // high key does not need to be updated
          // Since we still could not know the high key
          if (KeyCmpGreaterEqual(search_key, merge_key)) {
            LOG_TRACE("Take merge right branch (ID = %" PRIu64 ")",
                      snapshot_p->node_id);

            node_p = merge_node_p->right_merge_p;
          } else {
            LOG_TRACE("Take merge left branch (ID = %" PRIu64 ")", snapshot_p->node_id);

            node_p = merge_node_p->child_node_p;
          }

          // Since all indices are not invalidated, we do not know on which
          // branch it is referring to
          // After this point node_p has been updated as the newest branch we
          // are travelling on
          start_p =
              InnerNode::GetNodeHeader(&node_p->GetLowKeyPair())->Begin() + 1;
          end_p = InnerNode::GetNodeHeader(&node_p->GetLowKeyPair())->End();

          // Note that we should jump to the beginning of the loop without
          // going to child node any further
          continue;
        }  // InnerMergeType
        default: {
          LOG_TRACE("ERROR: Unknown node type = %d", static_cast<int>(type));

          PL_ASSERT(false);
        }
      }  // switch type

      node_p = static_cast<const DeltaNode *>(node_p)->child_node_p;
    }  // while 1

    // Should not reach here
    PL_ASSERT(false);
    return INVALID_NODE_ID;
  }

  /*
   * NavigateInnerNodeBI() - Traverses an InnerNode for backward iteration
   *
   * This function serves the same purpose as NavigateInnerNode() in a sense
   * that it also traverses down the delta chain of an InnerNode and returns
   * an next level NodeID for later traversal.
   *
   * The difference between this function and NavigateInnerNode() is that
   * this function will go left even if the search key is found. Similarly
   * if the key happens to be the merge key in InnerMergeNode, we just take
   * the left branch to avoid ending up in the node with low key == search key
   */
  NodeID NavigateInnerNodeBI(Context *context_p) {
    NavigateSiblingChainBI(context_p);
    if (context_p->abort_flag == true) {
      return INVALID_NODE_ID;
    }

    const KeyType &search_key = context_p->search_key;
    NodeSnapshot *snapshot_p = GetLatestNodeSnapshot(context_p);
    const BaseNode *node_p = snapshot_p->node_p;

    PL_ASSERT(snapshot_p->IsLeaf() == false);
    PL_ASSERT(snapshot_p->node_p != nullptr);
    LOG_TRACE("Navigating inner node delta chain for BI...");

    while (1) {
      NodeType type = node_p->GetType();

      switch (type) {
        case NodeType::InnerType: {
          NodeID target_id = LocateSeparatorByKeyBI(
              search_key, static_cast<const InnerNode *>(node_p));

          LOG_TRACE("Found child in inner node (BI); child ID = %" PRIu64 "",
                    target_id);

          return target_id;
        }
        case NodeType::InnerInsertType: {
          const InnerInsertNode *insert_node_p =
              static_cast<const InnerInsertNode *>(node_p);

          const KeyNodeIDPair &insert_item = insert_node_p->item;
          const KeyNodeIDPair &next_item = insert_node_p->next_item;
          if ((next_item.second == INVALID_NODE_ID) ||
              (KeyCmpLess(search_key, next_item.first))) {
            // *********************************************
            // * NOTE: DO NOT PROCEED IF IT IS "==" RELATION
            // *********************************************

            if (KeyCmpGreater(search_key, insert_item.first)) {
              LOG_TRACE("Find target ID = %" PRIu64 " in insert delta (BI)",
                        insert_item.second);

              return insert_item.second;
            }
          }

          node_p = insert_node_p->child_node_p;

          break;
        }  // InnerInsertType
        case NodeType::InnerDeleteType: {
          const InnerDeleteNode *delete_node_p =
              static_cast<const InnerDeleteNode *>(node_p);

          const KeyNodeIDPair &prev_item = delete_node_p->prev_item;
          const KeyNodeIDPair &next_item = delete_node_p->next_item;

          // *********************************************
          // * NOTE: DO NOT PROCEED IF IT IS "==" RELATION
          // *********************************************

          if ((delete_node_p->GetLowKeyNodeID() == prev_item.second) ||
              (KeyCmpGreater(search_key, prev_item.first))) {
            if ((next_item.second == INVALID_NODE_ID) ||
                (KeyCmpLess(search_key, next_item.first))) {
              LOG_TRACE("Find target ID = %" PRIu64 " in delete delta (BI)",
                        prev_item.second);

              return prev_item.second;
            }
          }

          node_p = delete_node_p->child_node_p;

          break;
        }  // InnerDeleteType
        case NodeType::InnerSplitType: {
          node_p = static_cast<const InnerSplitNode *>(node_p)->child_node_p;
          break;
        }  // case InnerSplitType
        case NodeType::InnerMergeType: {
          const InnerMergeNode *merge_node_p =
              static_cast<const InnerMergeNode *>(node_p);

          const KeyType &merge_key = merge_node_p->delete_item.first;

          // ************************************************
          // * NOTE: GO TO LEFT BRANCH IF IT IS "==" RELATION
          // ************************************************

          if (KeyCmpGreater(search_key, merge_key)) {
            LOG_TRACE("Take merge right branch (ID = %" PRIu64 ") for BI",
                      snapshot_p->node_id);

            node_p = merge_node_p->right_merge_p;
          } else {
            LOG_TRACE("Take merge left branch (ID = %" PRIu64 ") for BI",
                      snapshot_p->node_id);

            node_p = merge_node_p->child_node_p;
          }

          break;
        }  // InnerMergeType
        default: {
          LOG_ERROR("ERROR: Unknown or unsupported node type = %d",
                    static_cast<int>(type));

          PL_ASSERT(false);
        }
      }  // switch type
    }    // while 1

    // Should not reach here
    PL_ASSERT(false);
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
  InnerNode *CollectAllSepsOnInner(NodeSnapshot *snapshot_p, int p_depth = 0) {
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

    SortedSmallSet<const InnerDataNode *, decltype(f1), decltype(f2)> sss{
        data_node_list, f1, f2};

    // The effect of this function is a consolidation into inner node
    InnerNode *inner_node_p =
        reinterpret_cast<InnerNode *>(ElasticNode<KeyNodeIDPair>::Get(
            node_p->GetItemCount(), NodeType::InnerType, p_depth,
            node_p->GetItemCount(), node_p->GetLowKeyPair(),
            node_p->GetHighKeyPair()));

    // The first element is always the low key
    // since we know it will never be deleted
    // We do this because for an InnerNode, its first separator key is just
    // a placeholder and never used (reading its content causes undefined
    // behavior), because there is one more NodeID than separators
    inner_node_p->PushBack(node_p->GetLowKeyPair());

    // This will fill in two sets with values present in the inner node
    // and values deleted
    CollectAllSepsOnInnerRecursive(node_p, node_p->GetLowKeyNodeID(), sss,
                                   inner_node_p);

    // Since consolidation would not change item count they must be equal
    // Also allocated space should be used exactly as described in the
    // construction function
    PL_ASSERT(inner_node_p->GetSize() == node_p->GetItemCount());
    PL_ASSERT(inner_node_p->GetSize() == inner_node_p->GetItemCount());

    return inner_node_p;
  }

  /*
   * CollectAllSepsOnInnerRecursive() - This is the counterpart on inner node
   *
   * Please refer to the function on leaf node for details. These two have
   * almost the same logical flow
   */
  template <typename T>  // To make the f**king compiler
                         // to deduce SortedSmallSet template type
  void CollectAllSepsOnInnerRecursive(const BaseNode *node_p,
                                      NodeID low_key_node_id, T &sss,
                                      InnerNode *new_inner_node_p) const {
    // High key should be the high key of the branch (if there is a merge
    // then the high key of the branch may not always equal the high key
    // of the merged node)
    // Even if there is no merge, we still need the high key to rule out
    // keys that has already been splited
    const KeyNodeIDPair &high_key_pair = node_p->GetHighKeyPair();

    while (1) {
      NodeType type = node_p->GetType();

      switch (type) {
        case NodeType::InnerType: {
          const InnerNode *inner_node_p =
              static_cast<const InnerNode *>(node_p);

          // These two will be set according to the high key and
          // low key
          const KeyNodeIDPair *copy_end_it;
          const KeyNodeIDPair *copy_start_it;

          if (high_key_pair.second == INVALID_NODE_ID) {
            copy_end_it = inner_node_p->End();
          } else {
            // This search for the first key >= high key of the current node
            // being consolidated
            // This is exactly where we should stop copying
            // The return value might be end() iterator, but it is also
            // consistent
            copy_end_it =
                std::lower_bound(inner_node_p->Begin() + 1, inner_node_p->End(),
                                 high_key_pair,  // This contains the high key
                                 key_node_id_pair_cmp_obj);
          }

          // Since we want to access its first element
          PL_ASSERT(inner_node_p->GetSize() > 0);

          // If the first element in the sep list equals the low key pair's
          // NodeID then we are at the leftmost node of the merge tree
          // and we ignore the leftmost sep (since it could be -Inf)
          // For other nodes, the leftmost item inside sep list has a valid
          // key and could thus be pushed directly
          if (inner_node_p->At(0).second == low_key_node_id) {
            copy_start_it = inner_node_p->Begin() + 1;
          } else {
            copy_start_it = inner_node_p->Begin();
          }

          // Find the end of copying
          auto sss_end_it = sss.GetEnd() - 1;

          // If the high key is +Inf then sss_end_it is the real end of the
          // sorted array
          // Otherwise need to find the end iterator of sss for the current
          // InnerNode by using its high key
          if (high_key_pair.second != INVALID_NODE_ID) {
            // Corner case: If the first element is lower bound
            // then current_p will move outside
            // the valid range of the array but still we return
            // the first element in the array
            while (sss_end_it >= sss.GetBegin()) {
              if (key_cmp_obj((*sss_end_it)->item.first, high_key_pair.first) ==
                  true) {
                break;
              }

              sss_end_it--;
            }
          }

          // This points to the first element >= high key
          sss_end_it++;

          // printf("%ld", sss.GetSize());

          while (1) {
            bool sss_end_flag = (sss.GetBegin() == sss_end_it);
            bool array_end_flag = (copy_start_it == copy_end_it);

            // printf("sss_end_flag = %d; array_end_flag = %d", sss_end_flag,
            // array_end_flag);

            if (sss_end_flag == true && array_end_flag == true) {
              // Both are drained
              break;
            } else if (sss_end_flag == true) {
              // If the sss has drained we continue to drain the array
              // This version of PushBack() takes two iterators and
              // insert from start to end - 1
              new_inner_node_p->PushBack(copy_start_it, copy_end_it);

              break;
            } else if (array_end_flag == true) {
              // Then we drain all elements inside the delta chain
              while (sss.GetBegin() != sss_end_it) {
                // We could not pop here since we will use the value later
                NodeType data_node_type = (sss.GetFront())->GetType();

                // It is possible that there is an InnerDeleteNode:
                // InnerNode: [1, 2, 3, 4, 5]
                // Delta Chain:               Delete 6, Insert 6
                // Only the first "Delete 6" will appear in the set
                // and we just do not care
                if (data_node_type == NodeType::InnerInsertType) {
                  // Pop the value here
                  new_inner_node_p->PushBack(sss.PopFront()->item);
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

            if (key_cmp_obj(copy_start_it->first, sss.GetFront()->item.first) ==
                true) {
              // If array element is less than data node list element
              new_inner_node_p->PushBack(*copy_start_it);

              copy_start_it++;
            } else if (key_cmp_obj(sss.GetFront()->item.first,
                                   copy_start_it->first) == true) {
              NodeType data_node_type = (sss.GetFront())->GetType();

              // Delta Insert with array not having that element
              if (data_node_type == NodeType::InnerInsertType) {
                // Pop the value here
                new_inner_node_p->PushBack(sss.PopFront()->item);
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
              if (data_node_type == NodeType::InnerInsertType) {
                new_inner_node_p->PushBack(sss.PopFront()->item);
              } else {
                // There is a value in InnerNode that does not exist
                // in consolidated node. Just ignore
                sss.PopFront();
              }

              copy_start_it++;
            }  // Compare leading elements
          }    // while(1)

          return;
        }  // case InnerType
        case NodeType::InnerRemoveType: {
          LOG_ERROR("ERROR: InnerRemoveNode not allowed");

          PL_ASSERT(false);
          return;
        }  // case InnerRemoveType
        case NodeType::InnerInsertType: {
          const InnerInsertNode *insert_node_p =
              static_cast<const InnerInsertNode *>(node_p);

          // delta nodes must be consistent with the most up-to-date
          // node high key
          PL_ASSERT(
              (high_key_pair.second == INVALID_NODE_ID) ||
              (KeyCmpLess(insert_node_p->item.first, high_key_pair.first)));

          sss.Insert(static_cast<const InnerDataNode *>(node_p));

          // Go to next node
          node_p = insert_node_p->child_node_p;

          break;
        }  // case InnerInsertType
        case NodeType::InnerDeleteType: {
          const InnerDeleteNode *delete_node_p =
              static_cast<const InnerDeleteNode *>(node_p);

          // Since we do not allow any delta node under split node
          // this must be true
          // i.e. delta nodes must be consistent with the most up-to-date
          // node high key
          PL_ASSERT(
              (high_key_pair.second == INVALID_NODE_ID) ||
              (KeyCmpLess(delete_node_p->item.first, high_key_pair.first)));

          sss.Insert(static_cast<const InnerDataNode *>(node_p));

          node_p = delete_node_p->child_node_p;

          break;
        }  // case InnerDeleteType
        case NodeType::InnerSplitType: {
          node_p = (static_cast<const DeltaNode *>(node_p))->child_node_p;

          break;
        }  // case InnerSplitType
        case NodeType::InnerMergeType: {
          const InnerMergeNode *merge_node_p =
              static_cast<const InnerMergeNode *>(node_p);

          // NOTE: We alaways use the same metadata object which is the
          // one passed by the wrapper. Though node_p changes for each
          // recursive call, metadata should not change and should remain
          // constant

          CollectAllSepsOnInnerRecursive(merge_node_p->child_node_p,
                                         low_key_node_id, sss,
                                         new_inner_node_p);

          CollectAllSepsOnInnerRecursive(merge_node_p->right_merge_p,
                                         low_key_node_id, sss,
                                         new_inner_node_p);

          // There is no unvisited node
          return;
        }  // case InnerMergeType
        default: {
          LOG_ERROR("ERROR: Unknown inner node type = %d",
                    static_cast<int>(type));

          PL_ASSERT(false);
          return;
        }
      }  // switch type
    }    // while(1)

    // Should not get to here
    PL_ASSERT(false);
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
   * NOTE 2: After jumping to a new NodeID in this function there will be
   *similar
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
    if (context_p->abort_flag == true) {
      return;
    }

    /////////////////////////////////////////////////////////////////
    // Only after this point could we get snapshot and node_p
    /////////////////////////////////////////////////////////////////

    // This contains information for current node
    NodeSnapshot *snapshot_p = GetLatestNodeSnapshot(context_p);
    const BaseNode *node_p = snapshot_p->node_p;

    PL_ASSERT(snapshot_p->IsLeaf() == true);

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

    BloomFilter<ValueType, ValueEqualityChecker, ValueHashFunc> present_set{
        present_set_data_p, value_eq_obj, value_hash_obj};

    BloomFilter<ValueType, ValueEqualityChecker, ValueHashFunc> deleted_set{
        deleted_set_data_p, value_eq_obj, value_hash_obj};

    int start_index = 0;
    int end_index = -1;

    while (1) {
      NodeType type = node_p->GetType();

      switch (type) {
        case NodeType::LeafType: {
          const LeafNode *leaf_node_p = static_cast<const LeafNode *>(node_p);

          auto start_it = leaf_node_p->Begin() + start_index;

          // That is the end of searching
          auto end_it = ((end_index == -1) ? leaf_node_p->End()
                                           : leaf_node_p->Begin() + end_index);

          // Here we know the search key < high key of current node
          // NOTE: We only compare keys here, so it will get to the first
          // element >= search key
          auto copy_start_it = std::lower_bound(
              start_it, end_it, std::make_pair(search_key, ValueType{}),
              key_value_pair_cmp_obj);

          // If there is something to copy
          while ((copy_start_it != leaf_node_p->End()) &&
                 (KeyCmpEqual(search_key, copy_start_it->first))) {
            // If the value has not been deleted then just insert
            // Note that here we use ValueSet, so need to extract value from
            // the key value pair
            if (deleted_set.Exists(copy_start_it->second) == false) {
              if (present_set.Exists(copy_start_it->second) == false) {
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
          const LeafInsertNode *insert_node_p =
              static_cast<const LeafInsertNode *>(node_p);

          if (KeyCmpEqual(search_key, insert_node_p->item.first)) {
            if (deleted_set.Exists(insert_node_p->item.second) == false) {
              // We must do this, since inserted set does not detect for
              // duplication, and if the value has already been in present set
              // then we inserted the same value twice
              if (present_set.Exists(insert_node_p->item.second) == false) {
                present_set.Insert(insert_node_p->item.second);

                value_list.push_back(insert_node_p->item.second);
              }
            }
          } else if (KeyCmpGreater(search_key, insert_node_p->item.first)) {
            start_index = insert_node_p->GetIndexPair().first;
          } else {
            end_index = insert_node_p->GetIndexPair().first;
          }

          node_p = insert_node_p->child_node_p;

          break;
        }  // case LeafInsertType
        case NodeType::LeafDeleteType: {
          const LeafDeleteNode *delete_node_p =
              static_cast<const LeafDeleteNode *>(node_p);

          if (KeyCmpEqual(search_key, delete_node_p->item.first)) {
            if (present_set.Exists(delete_node_p->item.second) == false) {
              deleted_set.Insert(delete_node_p->item.second);
            }
          } else if (KeyCmpGreater(search_key, delete_node_p->item.first)) {
            start_index = delete_node_p->GetIndexPair().first;
          } else {
            end_index = delete_node_p->GetIndexPair().first;
          }

          node_p = delete_node_p->child_node_p;

          break;
        }  // case LeafDeleteType
        case NodeType::LeafRemoveType: {
          LOG_ERROR("ERROR: Observed LeafRemoveNode in delta chain");

          PL_ASSERT(false);
          PELOTON_FALLTHROUGH;
        }  // case LeafRemoveType
        case NodeType::LeafMergeType: {
          LOG_TRACE("Observed a merge node on leaf delta chain");

          const LeafMergeNode *merge_node_p =
              static_cast<const LeafMergeNode *>(node_p);

          // Decide which side we should choose
          // Using >= for separator key
          if (KeyCmpGreaterEqual(search_key, merge_node_p->delete_item.first)) {
            LOG_TRACE("Take leaf merge right branch");

            node_p = merge_node_p->right_merge_p;
          } else {
            LOG_TRACE("Take leaf merge left branch");

            node_p = merge_node_p->child_node_p;
          }

          break;
        }  // case LeafMergeType
        case NodeType::LeafSplitType: {
          LOG_TRACE("Observed a split node on leaf delta chain");

          const LeafSplitNode *split_node_p =
              static_cast<const LeafSplitNode *>(node_p);

          // Do not need to go right here since we have already
          // done that on the top level
          // and the high key has been observed on the top level high key
          node_p = split_node_p->child_node_p;

          break;
        }  // case LeafSplitType
        default: {
          LOG_ERROR("ERROR: Unknown leaf delta node type: %d",
                    static_cast<int>(node_p->GetType()));

          PL_ASSERT(false);
        }  // default
      }    // switch
    }      // while

    // We cannot reach here
    PL_ASSERT(false);
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
    if (context_p->abort_flag == true) {
      return nullptr;
    }

    /////////////////////////////////////////////////////////////////
    // Only after this point could we get snapshot and node_p
    /////////////////////////////////////////////////////////////////

    // Snapshot pointer, node pointer, and metadata reference all need
    // updating once LoadNodeID() returns with success
    NodeSnapshot *snapshot_p = GetLatestNodeSnapshot(context_p);
    PL_ASSERT(snapshot_p->IsLeaf() == true);

    const BaseNode *node_p = snapshot_p->node_p;

    // Save some typing
    const KeyType &search_key = context_p->search_key;

    while (1) {
      NodeType type = node_p->GetType();

      switch (type) {
        case NodeType::LeafType: {
          const LeafNode *leaf_node_p = static_cast<const LeafNode *>(node_p);

          // Here we know the search key < high key of current node
          // NOTE: We only compare keys here, so it will get to the first
          // element >= search key
          auto scan_start_it = std::lower_bound(
              leaf_node_p->Begin(), leaf_node_p->End(),
              std::make_pair(search_key, ValueType{}), key_value_pair_cmp_obj);

          // Search all values with the search key
          while ((scan_start_it != leaf_node_p->End()) &&
                 (KeyCmpEqual(scan_start_it->first, search_key))) {
            // If there is a value matching the search value then return true
            // We do not need to check any delete set here, since if the
            // value has been deleted earlier then this function would
            // already have returned
            if (ValueCmpEqual(scan_start_it->second, search_value)) {
              // Since only Delete() will use this piece of information
              // we set exist flag to false to indicate that the value
              // has been invalidated
              index_pair_p->first = scan_start_it - leaf_node_p->Begin();
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
          index_pair_p->first = scan_start_it - leaf_node_p->Begin();
          index_pair_p->second = false;

          return nullptr;
        }  // case LeafType
        case NodeType::LeafInsertType: {
          const LeafInsertNode *insert_node_p =
              static_cast<const LeafInsertNode *>(node_p);

          if (KeyCmpEqual(search_key, insert_node_p->item.first)) {
            if (ValueCmpEqual(insert_node_p->item.second, search_value)) {
              // Only Delete() will use this
              // We just simply inherit from the first node
              *index_pair_p = insert_node_p->GetIndexPair();

              return &insert_node_p->item;
            }
          }

          node_p = insert_node_p->child_node_p;

          break;
        }  // case LeafInsertType
        case NodeType::LeafDeleteType: {
          const LeafDeleteNode *delete_node_p =
              static_cast<const LeafDeleteNode *>(node_p);

          // If the value was deleted then return false
          if (KeyCmpEqual(search_key, delete_node_p->item.first)) {
            if (ValueCmpEqual(delete_node_p->item.second, search_value)) {
              // Only Insert() will use this
              // We just simply inherit from the first node
              *index_pair_p = delete_node_p->GetIndexPair();

              return nullptr;
            }
          }

          node_p = delete_node_p->child_node_p;

          break;
        }  // case LeafDeleteType
        case NodeType::LeafRemoveType: {
          LOG_ERROR("ERROR: Observed LeafRemoveNode in delta chain");

          PL_ASSERT(false);
          PELOTON_FALLTHROUGH;
        }  // case LeafRemoveType
        case NodeType::LeafMergeType: {
          LOG_TRACE("Observed a merge node on leaf delta chain");

          const LeafMergeNode *merge_node_p =
              static_cast<const LeafMergeNode *>(node_p);

          // Decide which side we should choose
          // Using >= for separator key
          if (KeyCmpGreaterEqual(search_key, merge_node_p->delete_item.first)) {
            LOG_TRACE("Take leaf merge right branch");

            node_p = merge_node_p->right_merge_p;
          } else {
            LOG_TRACE("Take leaf merge left branch");

            node_p = merge_node_p->child_node_p;
          }

          break;
        }  // case LeafMergeType
        case NodeType::LeafSplitType: {
          LOG_TRACE("Observed a split node on leaf delta chain");

          const LeafSplitNode *split_node_p =
              static_cast<const LeafSplitNode *>(node_p);

          node_p = split_node_p->child_node_p;

          break;
        }  // case LeafSplitType
        default: {
          LOG_ERROR("ERROR: Unknown leaf delta node type: %d",
                    static_cast<int>(node_p->GetType()));

          PL_ASSERT(false);
        }  // default
      }    // switch
    }      // while

    // We cannot reach here
    PL_ASSERT(false);
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
  const KeyValuePair *NavigateLeafNode(
      Context *context_p, const ValueType &value,
      std::pair<int, bool> *index_pair_p,
      std::function<bool(const void *)> predicate, bool *predicate_satisfied) {
    // NOTE: We do not have to traverse to the right sibling here
    // since Traverse() already traverses to the right sibling
    // if value pointer given to it is nullptr
    // So we are guaranteed that we are always on the correct leaf
    // page delta chain on entry of this function

    NodeSnapshot *snapshot_p = GetLatestNodeSnapshot(context_p);
    const BaseNode *node_p = snapshot_p->node_p;

    PL_ASSERT(snapshot_p->IsLeaf() == true);

    const KeyType &search_key = context_p->search_key;

    const int set_max_size = node_p->GetDepth();

    const ValueType *present_set_data_p[set_max_size];
    const ValueType *deleted_set_data_p[set_max_size];

    BloomFilter<ValueType, ValueEqualityChecker, ValueHashFunc> present_set{
        present_set_data_p, value_eq_obj, value_hash_obj};

    BloomFilter<ValueType, ValueEqualityChecker, ValueHashFunc> deleted_set{
        deleted_set_data_p, value_eq_obj, value_hash_obj};

    while (1) {
      NodeType type = node_p->GetType();

      switch (type) {
        case NodeType::LeafType: {
          const LeafNode *leaf_node_p = static_cast<const LeafNode *>(node_p);

          auto copy_start_it = std::lower_bound(
              leaf_node_p->Begin(), leaf_node_p->End(),
              std::make_pair(search_key, ValueType{}), key_value_pair_cmp_obj);

          while ((copy_start_it != leaf_node_p->End()) &&
                 (KeyCmpEqual(search_key, copy_start_it->first))) {
            if (deleted_set.Exists(copy_start_it->second) == false) {
              if (present_set.Exists(copy_start_it->second) == false) {
                // If the predicate is satified by the value
                // then return with predicate flag set to true
                // Otherwise test for duplication
                if (predicate(copy_start_it->second) == true) {
                  *predicate_satisfied = true;

                  return nullptr;
                } else if (value_eq_obj(value, copy_start_it->second) == true) {
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
          index_pair_p->first = copy_start_it - leaf_node_p->Begin();
          // Value does not exist
          index_pair_p->second = false;

          // Only if we return here could we insert
          return nullptr;
        }
        case NodeType::LeafInsertType: {
          const LeafInsertNode *insert_node_p =
              static_cast<const LeafInsertNode *>(node_p);

          if (KeyCmpEqual(search_key, insert_node_p->item.first)) {
            if (deleted_set.Exists(insert_node_p->item.second) == false) {
              if (present_set.Exists(insert_node_p->item.second) == false) {
                present_set.Insert(insert_node_p->item.second);

                // LeafInsertNode means this value exists
                if (predicate(insert_node_p->item.second) == true) {
                  *predicate_satisfied = true;

                  // Could return here since we know the predicate is satisfied
                  return nullptr;
                } else if (value_eq_obj(value, insert_node_p->item.second) ==
                           true) {
                  // Could also return here since we know the value exists
                  // and we could not insert anyway
                  return &insert_node_p->item;
                }  // test predicate and duplicates
              }    // if value not seen
            }      // if value not deleted
          }        // if key equal

          node_p = insert_node_p->child_node_p;

          break;
        }  // case LeafInsertType
        case NodeType::LeafDeleteType: {
          const LeafDeleteNode *delete_node_p =
              static_cast<const LeafDeleteNode *>(node_p);

          if (KeyCmpEqual(search_key, delete_node_p->item.first)) {
            if (present_set.Exists(delete_node_p->item.second) == false) {
              // Even if we know the value does not exist, we still need
              // to test all predicates to the leaf base node
              deleted_set.Insert(delete_node_p->item.second);
            }
          }

          node_p = delete_node_p->child_node_p;

          break;
        }  // case LeafDeleteType
        case NodeType::LeafRemoveType: {
          LOG_ERROR("ERROR: Observed LeafRemoveNode in delta chain");

          PL_ASSERT(false);
          PELOTON_FALLTHROUGH;
        }  // case LeafRemoveType
        case NodeType::LeafMergeType: {
          LOG_TRACE("Observed a merge node on leaf delta chain");

          const LeafMergeNode *merge_node_p =
              static_cast<const LeafMergeNode *>(node_p);

          if (KeyCmpGreaterEqual(search_key, merge_node_p->delete_item.first)) {
            LOG_TRACE("Take leaf merge right branch");

            node_p = merge_node_p->right_merge_p;
          } else {
            LOG_TRACE("Take leaf merge left branch");

            node_p = merge_node_p->child_node_p;
          }

          break;
        }  // case LeafMergeType
        case NodeType::LeafSplitType: {
          LOG_TRACE("Observed a split node on leaf delta chain");

          const LeafSplitNode *split_node_p =
              static_cast<const LeafSplitNode *>(node_p);

          node_p = split_node_p->child_node_p;

          break;
        }  // case LeafSplitType
        default: {
          LOG_ERROR("ERROR: Unknown leaf delta node type: %d",
                    static_cast<int>(node_p->GetType()));

          PL_ASSERT(false);
        }  // default
      }    // switch
    }      // while

    // We cannot reach here
    PL_ASSERT(false);
    return nullptr;
  }

  /*
   * CollectAllValuesOnLeaf() - Consolidate delta chain for a single logical
   *                            leaf node
   *
   * This function is the non-recursive wrapper of the resursive core function.
   * It calls the recursive version to collect all base leaf nodes, and then
   * it replays delta records on top of them.
   *
   * If leaf_node_p is nullptr (default) then a new leaf node instance is
   * created; Otherwise we simply use the existing pointer *WITHOUT* performing
   * any initialization. This implies that the caller should initialize
   * a valid LeafNode object before calling this function
   */
  LeafNode *CollectAllValuesOnLeaf(NodeSnapshot *snapshot_p,
                                   LeafNode *leaf_node_p = nullptr) {
    PL_ASSERT(snapshot_p->IsLeaf() == true);

    const BaseNode *node_p = snapshot_p->node_p;

    /////////////////////////////////////////////////////////////////
    // Prepare new node
    /////////////////////////////////////////////////////////////////

    if (leaf_node_p == nullptr) {
      leaf_node_p = reinterpret_cast<LeafNode *>(ElasticNode<KeyValuePair>::Get(
          node_p->GetItemCount(), NodeType::LeafType, 0, node_p->GetItemCount(),
          node_p->GetLowKeyPair(), node_p->GetHighKeyPair()));
    }

    PL_ASSERT(leaf_node_p != nullptr);

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
    KeyValuePairBloomFilter delta_set{delta_set_data_p, key_value_pair_eq_obj,
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
      if (this->key_cmp_obj(ldn1->item.first, ldn2->item.first)) {
        return true;
      } else if (this->key_eq_obj(ldn1->item.first, ldn2->item.first)) {
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

      PL_ASSERT(false);
      return false;
    };

    // Declare an sss object with the previously declared comparators and
    // null equality checkers (not used)
    SortedSmallSet<const LeafDataNode *, decltype(f1), decltype(f2)> sss{
        sss_data_p, f1, f2};

    /////////////////////////////////////////////////////////////////
    // Start collecting values!
    /////////////////////////////////////////////////////////////////

    // We collect all valid values in present_set
    // and deleted_set is just for bookkeeping
    CollectAllValuesOnLeafRecursive(node_p, sss, delta_set, leaf_node_p);

    // Item count would not change during consolidation
    PL_ASSERT(leaf_node_p->GetSize() == node_p->GetItemCount());

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
  void CollectAllValuesOnLeafRecursive(const BaseNode *node_p, T &sss,
                                       KeyValuePairBloomFilter &delta_set,
                                       LeafNode *new_leaf_node_p) const {
    // The top node is used to derive high key
    // NOTE: Low key for Leaf node and its delta chain is nullptr
    const KeyNodeIDPair &high_key_pair = node_p->GetHighKeyPair();

    while (1) {
      NodeType type = node_p->GetType();

      switch (type) {
        // When we see a leaf node, just copy all keys together with
        // all values into the value set
        case NodeType::LeafType: {
          const LeafNode *leaf_node_p = static_cast<const LeafNode *>(node_p);

          // We compute end iterator based on the high key
          const KeyValuePair *copy_end_it;

          // If the high key is +Inf then all items could be copied
          if ((high_key_pair.second == INVALID_NODE_ID)) {
            copy_end_it = leaf_node_p->End();
          } else {
            // This points copy_end_it to the first element >= current high key
            // If no such element exists then copy_end_it is end() iterator
            // which is also consistent behavior
            copy_end_it = std::lower_bound(
                leaf_node_p->Begin(), leaf_node_p->End(),
                // It only compares key so we
                // just use high key pair
                std::make_pair(high_key_pair.first, ValueType{}),
                key_value_pair_cmp_obj);
          }

          // This is the index of the copy end it
          int copy_end_index =
              static_cast<int>(copy_end_it - leaf_node_p->Begin());
          int copy_start_index = 0;

          ///////////////////////////////////////////////////////////
          // Find the end index for sss
          ///////////////////////////////////////////////////////////

          // Find the end of copying
          auto sss_end_it = sss.GetEnd() - 1;

          // If the next key is +Inf then sss_end_it is the real end of the
          // sorted array
          if (high_key_pair.second != INVALID_NODE_ID) {
            // Corner case: If the first element is lower bound
            // then current_p will move outside
            // the valid range of the array but still we return
            // the first element in the array
            while (sss_end_it >= sss.GetBegin()) {
              if (key_cmp_obj((*sss_end_it)->item.first, high_key_pair.first) ==
                  true) {
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
          while (sss.GetBegin() != sss_end_it) {
            int current_index = sss.GetFront()->GetIndexPair().first;

            // If we did not see any overwriting delta then
            // we also copy the old item in leaf node
            bool item_overwritten = false;

            PL_ASSERT(copy_start_index <= current_index);
            PL_ASSERT(current_index <= copy_end_index);

            // First copy all items before the current index
            new_leaf_node_p->PushBack(leaf_node_p->Begin() + copy_start_index,
                                      leaf_node_p->Begin() + current_index);

            // Update copy start index for next copy
            copy_start_index = current_index;

            // Drain delta records on the same index
            while (sss.GetFront()->GetIndexPair().first == current_index) {
              // Update current status of the item on leaf base node
              // IndexPair.second == true if the value has been overwritten
              item_overwritten =
                  item_overwritten || sss.GetFront()->GetIndexPair().second;

              // We only insert those in LeafInsertNode
              // and ignore all LeafDeleteNode
              if (sss.GetFront()->GetType() == NodeType::LeafInsertType) {
                // We remove the element from sss here
                new_leaf_node_p->PushBack(sss.PopFront()->item);
              } else {
                PL_ASSERT(sss.GetFront()->GetType() ==
                          NodeType::LeafDeleteType);

                // ... and here
                sss.PopFront();
              }

              // If we have drained sss
              if (sss.GetBegin() == sss_end_it) {
                break;
              }
            }

            // If the element has been overwritten by some of the deltas
            // just advance the pointer
            if (item_overwritten == true) {
              copy_start_index++;
            }
          }  // while sss has not reached the copy end

          // Also need to insert all other elements if there are some
          new_leaf_node_p->PushBack(leaf_node_p->Begin() + copy_start_index,
                                    leaf_node_p->Begin() + copy_end_index);

          return;
        }  // case LeafType
        case NodeType::LeafInsertType: {
          const LeafInsertNode *insert_node_p =
              static_cast<const LeafInsertNode *>(node_p);

          if (delta_set.Exists(insert_node_p->item) == false) {
            delta_set.Insert(insert_node_p->item);

            sss.InsertNoDedup(insert_node_p);
          }

          node_p = insert_node_p->child_node_p;

          break;
        }  // case LeafInsertType
        case NodeType::LeafDeleteType: {
          const LeafDeleteNode *delete_node_p =
              static_cast<const LeafDeleteNode *>(node_p);

          if (delta_set.Exists(delete_node_p->item) == false) {
            delta_set.Insert(delete_node_p->item);

            sss.InsertNoDedup(delete_node_p);
          }

          node_p = delete_node_p->child_node_p;

          break;
        }  // case LeafDeleteType
        case NodeType::LeafRemoveType: {
          LOG_ERROR("ERROR: LeafRemoveNode not allowed");

          PL_ASSERT(false);
          PELOTON_FALLTHROUGH;
        }  // case LeafRemoveType
        case NodeType::LeafSplitType: {
          const LeafSplitNode *split_node_p =
              static_cast<const LeafSplitNode *>(node_p);

          node_p = split_node_p->child_node_p;

          break;
        }  // case LeafSplitType
        case NodeType::LeafMergeType: {
          const LeafMergeNode *merge_node_p =
              static_cast<const LeafMergeNode *>(node_p);

          /**** RECURSIVE CALL ON LEFT AND RIGHT SUB-TREE ****/
          CollectAllValuesOnLeafRecursive(merge_node_p->child_node_p, sss,
                                          delta_set, new_leaf_node_p);

          CollectAllValuesOnLeafRecursive(merge_node_p->right_merge_p, sss,
                                          delta_set, new_leaf_node_p);

          return;
        }  // case LeafMergeType
        default: {
          LOG_ERROR("ERROR: Unknown node type: %d", static_cast<int>(type));

          PL_ASSERT(false);
        }  // default
      }    // switch
    }      // while(1)

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
  static inline NodeSnapshot *GetLatestNodeSnapshot(Context *context_p) {
#ifdef BWTREE_DEBUG
    PL_ASSERT(context_p->current_level >= 0);
#endif

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
#ifdef BWTREE_DEBUG
    // Make sure the current node has a parent
    PL_ASSERT(context_p->current_level >= 1);
#endif
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
#ifdef BWTREE_DEBUG
    PL_ASSERT(context_p->current_level >= 1);
#endif

    return GetLatestParentNodeSnapshot(context_p)->node_p->GetLowKeyNodeID() ==
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
    LOG_TRACE("Jumping to the left sibling");

#ifdef BWTREE_DEBUG
    PL_ASSERT(context_p->HasParentNode());
#endif

    // Get last record which is the current node's context
    // and we must make sure the current node is not left mode node
    NodeSnapshot *snapshot_p = GetLatestNodeSnapshot(context_p);

    // Check currently we are on a remove node
    PL_ASSERT(snapshot_p->node_p->IsRemoveNode());

    // This is not necessarily true. e.g. if the parent node was merged into
    // its left sibling before we take the snapshot of its previou left child
    // then this no longer holds
    if (IsOnLeftMostChild(context_p)) {
      LOG_TRACE(
          "Observed a remove node on left most child."
          "  Parent node must have been merged. ABORT");

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
    PL_ASSERT(parent_snapshot_p->IsLeaf() == false);

    // If the parent has changed then abort
    // This is to avoid missing InnerInsertNode which is fatal in our case
    // because we could not find the item that actually is on the parent node
    //
    // Or we might traverse right on the child (i.e. there was a split and
    // the split was consolidated), which means also that we
    // could not find a sep item on the parent node
    if (parent_snapshot_p->node_p != GetNode(parent_snapshot_p->node_id)) {
      LOG_TRACE(
          "Inconsistent parent node snapshot and "
          "current parent node. ABORT");

      context_p->abort_flag = true;

      return;
    }

    NodeID left_sibling_id =
        FindLeftSibling(snapshot_p->node_p->GetLowKey(), parent_snapshot_p);

    // This might incur recursive update
    // We need to pass in the low key of left sibling node
    JumpToNodeID(left_sibling_id, context_p);

    if (context_p->abort_flag == true) {
      LOG_TRACE("JumpToLeftSibling()'s call to JumpToNodeID() ABORT");

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
    if (removed_node_id != snapshot_p->node_p->GetNextNodeID()) {
      LOG_TRACE(
          "Left sibling's next node ID does not match removed NodeID."
          " ABORT");

      context_p->abort_flag = true;

      return;
    }

    return;
  }

  /*
   * TakeNodeSnapshot() - Take the snapshot of a node by pushing node
   *information
   *
   * This function simply copies NodeID, and physical pointer into
   * the snapshot object. Node that the snapshot object itself is directly
   * constructed on the path list which is a vector. This avoids copying the
   * NodeSnapshot object from the stack to vector
   */
  void TakeNodeSnapshot(NodeID node_id, Context *context_p) {
    const BaseNode *node_p = GetNode(node_id);

    LOG_TRACE("Is leaf node? - %d", node_p->IsOnLeafDeltaChain());

#ifdef BWTREE_DEBUG

    // This is used to record how many levels we have traversed
    context_p->current_level++;

#endif

    // Copy assignment
    // NOTE: For root node, the parent node contains garbage
    // but current node ID is INVALID_NODE_ID
    // So after this line parent node ID is INVALID_NODE_ID
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
  void UpdateNodeSnapshot(NodeID node_id, Context *context_p) {
    const BaseNode *node_p = GetNode(node_id);

    // We operate on the latest snapshot instead of creating a new one
    NodeSnapshot *snapshot_p = GetLatestNodeSnapshot(context_p);

    // Assume we always use this function to traverse on the same
    // level
    PL_ASSERT(node_p->IsOnLeafDeltaChain() == snapshot_p->IsLeaf());

    // Make sure we are not switching to itself
    PL_ASSERT(snapshot_p->node_id != node_id);

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
  void LoadNodeID(NodeID node_id, Context *context_p) {
    LOG_TRACE("Loading NodeID = %" PRIu64 "", node_id);

    // This pushes a new snapshot into stack
    TakeNodeSnapshot(node_id, context_p);

    // If there are SMO (split; merge) that require much computation to
    // deal with (e.g. go to its parent and comsolidate parent first) then
    // we should aggressively comsolidate the SMO away to avoid further
    // access
    FinishPartialSMO(context_p);

    if (context_p->abort_flag == true) {
      return;
    }

    // This does not abort
    TryConsolidateNode(context_p);

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
    LOG_TRACE("Jumping to node ID = %" PRIu64 "", node_id);

    // This updates the current snapshot in the stack
    UpdateNodeSnapshot(node_id, context_p);

    FinishPartialSMO(context_p);

    if (context_p->abort_flag == true) {
      return;
    }

    // This does not abort
    TryConsolidateNode(context_p);

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
  inline void FinishPartialSMOReadOptimized(Context *context_p) {
    // Note: If the top of the path list changes then this pointer
    // must also be updated
    NodeSnapshot *snapshot_p = GetLatestNodeSnapshot(context_p);

  before_switch:
    switch (snapshot_p->node_p->GetType()) {
      case NodeType::InnerAbortType: {
        LOG_TRACE("Observed Inner Abort Node; Continue");

        snapshot_p->node_p =
            (static_cast<const DeltaNode *>(snapshot_p->node_p))->child_node_p;

        goto before_switch;
      }
      case NodeType::LeafRemoveType:
      case NodeType::InnerRemoveType: {
        LOG_TRACE("Observed remove node; abort");

        // Since remove node is just a temporary measure, and if
        // a thread proceeds to do its own job it must finish the remove
        // SMO, posting InnerDeleteNode on the parent
        context_p->abort_flag = true;

        return;
      }  // case Inner/LeafRemoveType
      default: { return; }
    }  // switch

    PL_ASSERT(false);
    return;
  }

  /*
   * TakeNodeSnapshotReadOptimized() - Take node snapshot without saving
   *                                   parent node
   *
   * This implies that if there is remove delta on the path then
   * the read thread will have to spin and wait. But finally it will
   * complete the job since remove delta will not be present if the thread
   * posting the remove delta finally proceeds to finish its job
   */
  inline void TakeNodeSnapshotReadOptimized(NodeID node_id,
                                            Context *context_p) {
    const BaseNode *node_p = GetNode(node_id);

    LOG_TRACE("Is leaf node (RO)? - %d", node_p->IsOnLeafDeltaChain());

#ifdef BWTREE_DEBUG

    // This is used to record how many levels we have traversed
    context_p->current_level++;

#endif

    context_p->current_snapshot.node_p = node_p;

    // DO NOT REMOVE THIS!
    // REMOVING THIS WOULD CAUSE ASSERTION FAILURE WHEN JUMPING
    // TO RIGHT SIBLING SINCEI IT CHECKS NODE ID
    context_p->current_snapshot.node_id = node_id;

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
    LOG_TRACE("Loading NodeID (RO) = %" PRIu64 "", node_id);

    // This pushes a new snapshot into stack
    TakeNodeSnapshotReadOptimized(node_id, context_p);

    FinishPartialSMOReadOptimized(context_p);

    // Do not need to check for abort flag here

    return;
  }

  /*
   * TraverseBI() - Read optimized traversal for backward iteration
   *
   * This function calls NavigateInnerNodeBI() to find the node with a smaller
   * key than search key
   */
  void TraverseBI(Context *context_p) {
  retry_traverse:
    PL_ASSERT(context_p->abort_flag == false);
#ifdef BWTREE_DEBUG
    PL_ASSERT(context_p->current_level == -1);
#endif

    NodeID start_node_id = root_id.load();
    context_p->current_snapshot.node_id = INVALID_NODE_ID;
    LoadNodeID(start_node_id, context_p);
    if (context_p->abort_flag == true) {
      goto abort_traverse;
    }

    LOG_TRACE("Successfully loading root node ID for BI");

    while (1) {
      NodeID child_node_id = NavigateInnerNodeBI(context_p);
      if (context_p->abort_flag == true) {
        LOG_TRACE("Navigate Inner Node abort (BI). ABORT");
        PL_ASSERT(child_node_id == INVALID_NODE_ID);
        goto abort_traverse;
      }

      LoadNodeID(child_node_id, context_p);
      if (context_p->abort_flag == true) {
        LOG_TRACE("LoadNodeID aborted (BI). ABORT");
        goto abort_traverse;
      }

      NodeSnapshot *snapshot_p = GetLatestNodeSnapshot(context_p);
      if (snapshot_p->IsLeaf() == true) {
        LOG_TRACE("The next node is a leaf (BI)");

        // After reaching leaf level just traverse the sibling chain
        // and stop before the search key
        NavigateSiblingChainBI(context_p);
        if (context_p->abort_flag == true) {
          LOG_TRACE("NavigateSiblingChainBI() inside TraverseBI() aborts");

          goto abort_traverse;
        }

        return;
      }
    }  // while(1)

  abort_traverse:
#ifdef BWTREE_DEBUG
    PL_ASSERT(context_p->current_level >= 0);
    context_p->current_level = -1;
    context_p->abort_counter++;
#endif

    // This is used to identify root node
    context_p->current_snapshot.node_id = INVALID_NODE_ID;
    context_p->abort_flag = false;
    goto retry_traverse;

    PL_ASSERT(false);
    return;
  }

  void TraverseReadOptimized(Context *context_p,
                             std::vector<ValueType> *value_list_p) {
  retry_traverse:
    PL_ASSERT(context_p->abort_flag == false);
#ifdef BWTREE_DEBUG
    PL_ASSERT(context_p->current_level == -1);
#endif
    // This is the serialization point for reading/writing root node
    NodeID child_node_id = root_id.load();

    LoadNodeIDReadOptimized(child_node_id, context_p);

    if (context_p->abort_flag == true) {
      LOG_TRACE("LoadNodeID aborted on loading root (RO)");

      goto abort_traverse;
    }

    LOG_TRACE("Successfully loading root node ID (RO)");

    while (1) {
      child_node_id = NavigateInnerNode(context_p);

      // Navigate could abort since it might go to another NodeID
      // if there is a split delta and the key is >= split key
      if (context_p->abort_flag == true) {
        LOG_TRACE("Navigate Inner Node abort (RO)");

        // If NavigateInnerNode() aborts then it retrns INVALID_NODE_ID
        // as a double check
        // This is the only situation that this function returns
        // INVALID_NODE_ID
        PL_ASSERT(child_node_id == INVALID_NODE_ID);

        goto abort_traverse;
      }

      // This might load a leaf child
      // Also LoadNodeID() does not guarantee the node bound matches
      // search key. Since we could readjust using the split side link
      // during Navigate...Node(), or abort if we reach the bottom
      // while still observing an inconsistent high key
      // (low key is always consistent)
      LoadNodeIDReadOptimized(child_node_id, context_p);

      if (context_p->abort_flag == true) {
        LOG_TRACE("LoadNodeID aborted (RO). ABORT");

        goto abort_traverse;
      }

      // This is the node we have just loaded
      NodeSnapshot *snapshot_p = GetLatestNodeSnapshot(context_p);

      if (snapshot_p->IsLeaf() == true) {
        LOG_TRACE("The next node is a leaf (RO)");

        NavigateLeafNode(context_p, *value_list_p);

        if (context_p->abort_flag == true) {
          LOG_TRACE("NavigateLeafNode aborts (RO). ABORT");

          goto abort_traverse;
        }

#ifdef BWTREE_DEBUG

        LOG_TRACE("Found leaf node (RO). Abort count = %d, level = %d",
                  context_p->abort_counter, context_p->current_level);

#endif

        // If there is no abort then we could safely return
        return;
      }

      // This does not work
      //_mm_prefetch(&context_p->current_snapshot.node_p->GetLowKeyPair(),
      //_MM_HINT_T0);
    }  // while(1)

  abort_traverse:
#ifdef BWTREE_DEBUG

    PL_ASSERT(context_p->current_level >= 0);

    context_p->current_level = -1;

    context_p->abort_counter++;

#endif

    context_p->current_snapshot.node_id = INVALID_NODE_ID;

    context_p->abort_flag = false;

    goto retry_traverse;

    PL_ASSERT(false);
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
                                  const KeyNodeIDPair &next_item,
                                  const KeyNodeIDPair *location) {
    // We post on the parent node, after which we check for size and decide
    // whether
    // to consolidate and/or split the node
    NodeSnapshot *parent_snapshot_p = GetLatestParentNodeSnapshot(context_p);

    // Three arguments are: inserted key-node id pair
    //                      next key-node id pair
    //                      child node in delta chain
    const InnerInsertNode *insert_node_p = InnerInlineAllocateOfType(
        InnerInsertNode, parent_snapshot_p->node_p, insert_item, next_item,
        parent_snapshot_p->node_p, location);

    // CAS Index Term Insert Delta onto the parent node
    bool ret = InstallNodeToReplace(parent_snapshot_p->node_id, insert_node_p,
                                    parent_snapshot_p->node_p);

    if (ret == true) {
      LOG_TRACE("Index term insert (from %" PRIu64 " to %" PRIu64 ") delta CAS succeeds",
                GetLatestNodeSnapshot(context_p)->node_id, insert_item.second);

      // Update parent node pointer to reflect the fact that it has been
      // updated
      parent_snapshot_p->node_p = insert_node_p;

      ConsolidateNode(GetLatestNodeSnapshot(context_p));

      return true;
    } else {
      LOG_TRACE(
          "Index term insert (from %" PRIu64 " to %" PRIu64 ") delta CAS failed. "
          "ABORT",
          GetLatestNodeSnapshot(context_p)->node_id, insert_item.second);

      // Set abort, and remove the newly created node
      context_p->abort_flag = true;

      // Call destructor but do not free memory
      insert_node_p->~InnerInsertNode();

      return false;
    }  // if CAS succeeds/fails

    PL_ASSERT(false);
    return false;
  }

  /*
   * PostInnerDeleteNode() - Posts an InnerDeleteNode on the current parent node
   *
   * This function is called to complete node merges as part of the SMO
   *help-along
   * protocol. It posts an InnerDeleteNode on the parent node and returns the
   *result
   * of CAS instruction
   *
   * NOTE: This function may abort
   */
  inline bool PostInnerDeleteNode(Context *context_p,
                                  const KeyNodeIDPair &delete_item,
                                  const KeyNodeIDPair &prev_item,
                                  const KeyNodeIDPair &next_item,
                                  const KeyNodeIDPair *location) {
    NodeSnapshot *parent_snapshot_p = GetLatestParentNodeSnapshot(context_p);

    // Arguments are:
    // Deleted item; Prev item; next item (NodeID not used for next item)
    // and delta chail child node
    const InnerDeleteNode *delete_node_p = InnerInlineAllocateOfType(
        InnerDeleteNode, parent_snapshot_p->node_p, delete_item, prev_item,
        next_item, parent_snapshot_p->node_p, location);

    // Assume parent has not changed, and CAS the index term delete delta
    // If CAS failed then parent has changed, and we have no idea how it
    // could be modified. The safest way is just abort
    bool ret = InstallNodeToReplace(parent_snapshot_p->node_id, delete_node_p,
                                    parent_snapshot_p->node_p);

    // If we installed the IndexTermDeleteDelta successfully the next
    // step is to put the remove node into garbage chain
    // and also recycle the deleted NodeID since now no new thread
    // could access that NodeID until it is reused
    if (ret == true) {
      LOG_TRACE("Index term delete delta installed, ID = %" PRIu64 "; ABORT",
                parent_snapshot_p->node_id);

      // The deleted node ID must resolve to a RemoveNode of either
      // Inner or Leaf category
      const BaseNode *garbage_node_p = GetNode(delete_item.second);
      PL_ASSERT(garbage_node_p->IsRemoveNode());

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
      // mapping_table[deleted_node_id] = nullptr;

      parent_snapshot_p->node_p = delete_node_p;

      ConsolidateNode(GetLatestNodeSnapshot(context_p));

      return true;
    } else {
      LOG_TRACE("Index term delete delta install failed. ABORT");

      // DO NOT FORGET TO DELETE THIS
      delete_node_p->~InnerDeleteNode();

      // The caller just returns after this function, so do not have
      // to branch on abort_flag after this function returns
      context_p->abort_flag = true;

      return false;
    }

    PL_ASSERT(false);
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
    switch (snapshot_p->node_p->GetType()) {
      case NodeType::InnerAbortType: {
        LOG_TRACE("Observed Inner Abort Node; ABORT");

        // This is an optimization - when seeing an ABORT
        // node, we continue but set the physical pointer to be ABORT's
        // child, to make CAS always fail on this node to avoid
        // posting on ABORT, especially posting split node
        snapshot_p->node_p =
            (static_cast<const DeltaNode *>(snapshot_p->node_p))->child_node_p;

        goto before_switch;
      }
      case NodeType::LeafRemoveType:
      case NodeType::InnerRemoveType: {
        LOG_TRACE("Helping along remove node...");

        // The right branch for merging is the child node under remove node
        const BaseNode *merge_right_branch =
            (static_cast<const DeltaNode *>(snapshot_p->node_p))->child_node_p;

        // This will also be recorded in merge delta such that when
        // we finish merge delta we could recycle the node id as well
        // as the RemoveNode
        NodeID deleted_node_id = snapshot_p->node_id;

        JumpToLeftSibling(context_p);

        // If this aborts then we propagate this to the state machine driver
        if (context_p->abort_flag == true) {
          LOG_TRACE("Jump to left sibling in Remove help along ABORT");

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
        if (left_snapshot_p->IsLeaf() == true) {
          ret =
              PostLeafMergeNode(left_snapshot_p, &merge_key, merge_right_branch,
                                deleted_node_id, &merge_node_p);
        } else {
          ret = PostInnerMergeNode(left_snapshot_p, &merge_key,
                                   merge_right_branch, deleted_node_id,
                                   &merge_node_p);
        }

        // If CAS fails just abort and return
        if (ret == true) {
          LOG_TRACE(
              "Merge delta CAS succeeds. "
              "Continue to finish merge SMO");

          left_snapshot_p->node_p = merge_node_p;

          // merge_node_p is set as the newest merge node above
          // after this point
          // Also snapshot_p has been to left_snapshot_p above
        } else {
          LOG_TRACE("Merge delta CAS fails. ABORT");

          context_p->abort_flag = true;

          return;
        }  // if ret == true

        //
        // FALL THROUGH IF POSTING MERGE DELTA SUCCEEDS
        //
        PELOTON_FALLTHROUGH;
      }  // case Inner/LeafRemoveType
      case NodeType::InnerMergeType:
      case NodeType::LeafMergeType: {
        LOG_TRACE("Helping along merge delta");

        // First consolidate parent node and find the left/right
        // sep pair plus left node ID
        NodeSnapshot *parent_snapshot_p =
            GetLatestParentNodeSnapshot(context_p);

        // This is necessary - make sure the parent node snapshot
        // is always update to date such that we do not miss a
        // late InnerInsertNode that actually posts the deleted item
        // o.w. the thread will just continue and post on top of
        // an unfinished merge delta
        if (parent_snapshot_p->node_p != GetNode(parent_snapshot_p->node_id)) {
          context_p->abort_flag = true;

          return;
        }

        // This is the item being deleted inside parent node
        const KeyNodeIDPair *delete_item_p = nullptr;
        const BaseNode *right_merge_p = nullptr;

        // Type of the merge delta
        // This is important since we might fall through
        // from the LeafRemoveNode/InnerRemoveNode branch
        NodeType type = snapshot_p->node_p->GetType();

        if (type == NodeType::InnerMergeType) {
          const InnerMergeNode *merge_node_p =
              static_cast<const InnerMergeNode *>(snapshot_p->node_p);

          delete_item_p = &merge_node_p->delete_item;
          right_merge_p = merge_node_p->right_merge_p;
        } else if (type == NodeType::LeafMergeType) {
          const LeafMergeNode *merge_node_p =
              static_cast<const LeafMergeNode *>(snapshot_p->node_p);

          delete_item_p = &merge_node_p->delete_item;
          right_merge_p = merge_node_p->right_merge_p;
        } else {
          LOG_ERROR("ERROR: Illegal node type: %d", static_cast<int>(type));

          PL_ASSERT(false);
        }  // If on type of merge node

        const KeyNodeIDPair *location;

        // Find the deleted item
        const KeyNodeIDPair *found_pair_p = NavigateInnerNode(
            parent_snapshot_p, delete_item_p->first, &location);

        // If the item is found then next we post InnerDeleteNode
        if (found_pair_p != nullptr) {
          PL_ASSERT(found_pair_p->second == delete_item_p->second);
        } else {
          return;
        }

        // It will post an InnerDeleteNode on the parent node
        // and the return value is the result of CAS
        // Note: Even if this function aborts, since we return immediately
        // so do not have to test abort_flag here
        //
        // There is a trick: The prev key is the low key of the node
        // being merged into
        // and the next key is the high key of the node being merged from
        PostInnerDeleteNode(
            context_p, *delete_item_p,
            // Note that for leaf node the low key is not complete
            std::make_pair(snapshot_p->node_p->GetLowKey(),
                           snapshot_p->node_id),
            // Also note that high key pair is valid for both leaf and inner
            right_merge_p->GetHighKeyPair(),
            // This is location on InnerNode
            location);

        return;
      }  // case Inner/LeafMergeNode
      case NodeType::InnerSplitType:
      case NodeType::LeafSplitType: {
        LOG_TRACE("Helping along split node");

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
        if (type == NodeType::InnerSplitType) {
          const InnerSplitNode *split_node_p =
              static_cast<const InnerSplitNode *>(snapshot_p->node_p);

          insert_item_p = &split_node_p->insert_item;
          next_item_p = &split_node_p->child_node_p->GetHighKeyPair();
        } else {
          const LeafSplitNode *split_node_p =
              static_cast<const LeafSplitNode *>(snapshot_p->node_p);

          insert_item_p = &split_node_p->insert_item;
          next_item_p = &split_node_p->child_node_p->GetHighKeyPair();
        }

#ifdef BWTREE_DEBUG
        PL_ASSERT(context_p->current_level >= 0);
#endif

        // If the parent snapshot has an invalid node ID then it must be the
        // root node.
        if (context_p->IsOnRootNode() == true) {
          /***********************************************************
           * Root splits (don't have to consolidate parent node)
           ***********************************************************/
          LOG_TRACE("Root splits!");

          // Allocate a new node ID for the newly created node
          // If CAS fails we need to free the root ID
          NodeID new_root_id = GetNextNodeID();

// InnerNode requires high key pair which is +Inf, INVALID NODE ID
// low key pair will be set inside the constructor to be pointing
// to the first element in the sep list
// NOTE: Storage will be automatically reserved inside the
// constructor
#ifdef BWTREE_PELOTON

          // This is the first item of the newly created InnerNode
          // and also the low key for the newly created InnerNode
          const KeyNodeIDPair first_item =
              std::make_pair(KeyType(), snapshot_p->node_id);

          // Allocate an InnerNode with KeyNodeIDPair embedded
          InnerNode *inner_node_p =
              reinterpret_cast<InnerNode *>(ElasticNode<KeyNodeIDPair>::Get(
                  2, NodeType::InnerType, 0, 2, first_item,
                  std::make_pair(KeyType(), INVALID_NODE_ID)));

#else

          // This is the first item of the newly created InnerNode
          // and also the low key for the InnerNode
          const KeyNodeIDPair first_item =
              std::make_pair(KeyType{}, snapshot_p->node_id);

          // Allocate an InnerNode with KeyNodeIDPair embedded
          InnerNode *inner_node_p =
              reinterpret_cast<InnerNode *>(ElasticNode<KeyNodeIDPair>::Get(
                  2, NodeType::InnerType, 0, 2, first_item,
                  std::make_pair(KeyType{}, INVALID_NODE_ID)));

#endif

          // Add new element - one points to the current node (new second level
          // left most inner node), another points to its split sibling
          inner_node_p->PushBack(first_item);
          inner_node_p->PushBack(*insert_item_p);

          // First we need to install the new node with NodeID
          // This makes it visible
          InstallNewNode(new_root_id, inner_node_p);
          bool ret = InstallRootNode(snapshot_p->node_id, new_root_id);

          if (ret == true) {
            LOG_TRACE("Install root CAS succeeds");

            // After installing new root we abort in order to load
            // the new root
            context_p->abort_flag = true;

            return;
          } else {
            LOG_TRACE("Install root CAS failed. ABORT");

            // We need to make a new remove node and send it into EpochManager
            // for recycling the NodeID
            // Note that the remove node must not be created on inner_node_p
            // since inner_node_p might be destroyed before remove node is
            // destroyed since they are both put into the GC chain
            const InnerRemoveNode *fake_remove_node_p =
                new InnerRemoveNode{new_root_id, inner_node_p};

            // Put the remove node into garbage chain, because
            // we cannot call InvalidateNodeID() here
            epoch_manager.AddGarbageNode(fake_remove_node_p);
            epoch_manager.AddGarbageNode(inner_node_p);

            context_p->abort_flag = true;

            return;
          }  // if CAS succeeds/fails
        } else {
          /***********************************************************
           * Index term insert for non-root nodes
           ***********************************************************/

          // First consolidate parent node and find the right sep
          NodeSnapshot *parent_snapshot_p =
              GetLatestParentNodeSnapshot(context_p);

          // If the split key is out of range then just ignore
          // we do not worry that through split sibling link
          // we would traverse to the child of a different parent node
          // than the current one, since we always guarantee that after
          // NavigateInnerNode() returns if it does not abort, then we
          // are on the correct node for the current key

          // This happens when the parent splits on the newly inserted index
          // term
          // and the split delta has not been consolidated yet, so that a thread
          // sees the split delta and traverses to its right sibling which is on
          // another parent node (the right sibling of the current parent node)
          // In this case we do not have to insert since we know the index term
          // has
          // already been inserted
          if ((parent_snapshot_p->node_p->GetNextNodeID() != INVALID_NODE_ID) &&
              (KeyCmpGreaterEqual(insert_item_p->first,
                                  parent_snapshot_p->node_p->GetHighKey()))) {
            LOG_TRACE(
                "Bounds check failed on parent node"
                " - item key >= high key");

            return;
          }

          // This is used to hold index information for InnerInsertNode
          const KeyNodeIDPair *location;

          // Find the split item that we intend to insert in the parent node
          // This function returns a pointer to the item if found, or
          // nullptr if not found
          const KeyNodeIDPair *found_item_p = NavigateInnerNode(
              parent_snapshot_p, insert_item_p->first, &location);

          // If the item has been found then we do not post
          // InnerInsertNode onto the parent
          if (found_item_p != nullptr) {
            // Check whether there is an item in the parent
            // node that has the same key but different NodeID
            // This is totally legal
            if (found_item_p->second != insert_item_p->second) {
#ifdef BWTREE_DEBUG

              // If this happens then it must be the case that the node with
              // the same split key but different node ID was removed and then
              // merged into the current node and then it is splited again
              //
              // We are now on the way of completing the second split SMO
              // but since the parent has changed (we must have missed an
              // InnerInsertNode) we need to abort and restart traversing
              const BaseNode *node_p = GetNode(found_item_p->second);

              PL_ASSERT(node_p->GetType() == NodeType::InnerRemoveType ||
                        node_p->GetType() == NodeType::LeafRemoveType);

#endif

              context_p->abort_flag = true;
            }

            return;
          }

          // Post InnerInsertNode on the parent node.
          // Also if this returns true then we have successfully completed split
          // SMO
          // which means the SMO could be removed by a consolidation to avoid
          // consolidating the parent node again and again
          // Note: Even if this function aborts, since we return immediately
          // so do not have to test abort_flag here
          PostInnerInsertNode(context_p, *insert_item_p, *next_item_p,
                              location);

          return;
        }  // if split root / else not split root
      }    // case split node
      default: {
        return;
        // By default we do not do anything special
        break;
      }
    }  // switch

    PL_ASSERT(false);
    return;
  }

  /*
   * ConsolidateLeafNode() - Consolidates a leaf delta chian unconditionally
   *
   * This function does not check delta chain size
   */
  inline void ConsolidateLeafNode(NodeSnapshot *snapshot_p) {
    PL_ASSERT(snapshot_p->node_p->IsOnLeafDeltaChain() == true);

    LeafNode *leaf_node_p = CollectAllValuesOnLeaf(snapshot_p);

    bool ret = InstallNodeToReplace(snapshot_p->node_id, leaf_node_p,
                                    snapshot_p->node_p);

    if (ret == true) {
      epoch_manager.AddGarbageNode(snapshot_p->node_p);

      snapshot_p->node_p = leaf_node_p;
    } else {
      epoch_manager.AddGarbageNode(leaf_node_p);
    }

    return;
  }

  /*
   * ConsolidateInnerNode() - Consolidates inner node unconditionally
   *
   * This function does not check for inner delta chain length
   */
  inline void ConsolidateInnerNode(NodeSnapshot *snapshot_p) {
    PL_ASSERT(snapshot_p->node_p->IsOnLeafDeltaChain() == false);

    InnerNode *inner_node_p = CollectAllSepsOnInner(snapshot_p);

    bool ret = InstallNodeToReplace(snapshot_p->node_id, inner_node_p,
                                    snapshot_p->node_p);

    if (ret == true) {
      epoch_manager.AddGarbageNode(snapshot_p->node_p);

      snapshot_p->node_p = inner_node_p;
    } else {
      epoch_manager.AddGarbageNode(inner_node_p);
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
    if (snapshot_p->node_p->IsOnLeafDeltaChain() == true) {
      ConsolidateLeafNode(snapshot_p);
    } else {
      ConsolidateInnerNode(snapshot_p);
    }  // if on leaf/inner node

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
    if (node_p->IsDeltaNode() == false) {
      // The depth of base node may not be 0
      // since if we consolidate parent node to finish the partial SMO,
      // then parent node will have non-0 depth in order to avoid being too
      // large (see FindSplitNextKey() and FindMergePrevNextKey() and
      // JumpToLeftSibling())
      // PL_ASSERT(node_p->metadata.depth == 0);

      return;
    }

    // If depth does not exceed threshold then we check recommendation flag
    int depth = node_p->GetDepth();

    if (snapshot_p->IsLeaf() == true) {
      if (depth < LEAF_DELTA_CHAIN_LENGTH_THRESHOLD) {
        return;
      }
    } else {
      if (depth < INNER_DELTA_CHAIN_LENGTH_THRESHOLD) {
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
    if (node_p->IsDeltaNode() == true) {
      // TODO: If we want to strictly enforce the size of any node
      // then we should aggressively consolidate here and always
      // Though it brings huge overhead because now we are consolidating
      // every node on the path

      return;
    }

    NodeID node_id = snapshot_p->node_id;

    if (snapshot_p->IsLeaf() == true) {
      const LeafNode *leaf_node_p = static_cast<const LeafNode *>(node_p);

      // NOTE: We use key number as the size of the node
      // because using item count might result in a very unstable
      // split, in a sense that we could not always split the node
      // evenly, and in the worst case if there is one key in the
      // node the node could not be splited while having a large
      // item count
      size_t node_size = leaf_node_p->GetItemCount();

      // Perform corresponding action based on node size
      if (node_size >= LEAF_NODE_SIZE_UPPER_THRESHOLD) {
        LOG_TRACE("Node size >= leaf upper threshold. Split");

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
        if (new_leaf_node_p == nullptr) {
          LOG_TRACE(
              "LeafNode size exceeds overhead, "
              "but could not find split point");

          return;
        }

        // Since we would like to access its first element to get the low key
        PL_ASSERT(new_leaf_node_p->GetSize() > 0);

        // The split key must be a valid key
        // Note that the lowkey for leaf node is not defined, so in the
        // case that it is required we must manually goto its data list
        // and find the low key in its leftmost element
        const KeyType &split_key = new_leaf_node_p->At(0).first;

        // If leaf split fails this should be recyced using a fake remove node
        NodeID new_node_id = GetNextNodeID();

        // Note that although split node only stores the new node ID
        // we still need its pointer to compute item_count
        const LeafSplitNode *split_node_p = LeafInlineAllocateOfType(
            LeafSplitNode, node_p, std::make_pair(split_key, new_node_id),
            node_p, new_leaf_node_p);

        //  First install the NodeID -> split sibling mapping
        // If CAS fails we also need to recycle the node ID allocated here
        InstallNewNode(new_node_id, new_leaf_node_p);

        // Then CAS split delta into current node's NodeID
        bool ret = InstallNodeToReplace(node_id, split_node_p, node_p);

        if (ret == true) {
          LOG_TRACE("Leaf split delta (from %" PRIu64 " to %" PRIu64 ") CAS succeeds. ABORT",
                    node_id, new_node_id);

          // TODO: WE ABORT HERE TO AVOID THIS THREAD POSTING ANYTHING
          // ON TOP OF IT WITHOUT HELPING ALONG AND ALSO BLOCKING OTHER
          // THREAD TO HELP ALONG
          context_p->abort_flag = true;

          return;
        } else {
          LOG_TRACE("Leaf split delta CAS fails");

          // Need to use the epoch manager to recycle NodeID
          // Note that this node must not be created on new_leaf_node_p
          // since they are both put into the GC chain, it is possible
          // for new_leaf_node_p to be deleted first and then remove node
          // is deleted
          const LeafRemoveNode *fake_remove_node_p =
              new LeafRemoveNode{new_node_id, new_leaf_node_p};

          // Must put both of them into GC chain since RemoveNode
          // will not be followed by GC thread
          epoch_manager.AddGarbageNode(fake_remove_node_p);
          epoch_manager.AddGarbageNode(new_leaf_node_p);

          // We have two nodes to delete here
          split_node_p->~LeafSplitNode();

          return;
        }

      } else if (node_size <= LEAF_NODE_SIZE_LOWER_THRESHOLD) {
        // This might yield a false positive of left child
        // but correctness is not affected - sometimes the merge is delayed
        if (IsOnLeftMostChild(context_p) == true) {
          LOG_TRACE("Left most leaf node cannot be removed");

          return;
        }

        // After this point we decide to remove leaf node

        LOG_TRACE("Node size <= leaf lower threshold. Remove");

        // Install an abort node on parent
        const BaseNode *abort_node_p;
        const BaseNode *abort_child_node_p;
        NodeID parent_node_id;

        bool abort_node_ret = PostAbortOnParent(
            context_p, &parent_node_id, &abort_node_p, &abort_child_node_p);

        // If we could not block the parent then the parent has changed
        // (splitted, etc.)
        if (abort_node_ret == true) {
          LOG_TRACE("Blocked parent node (current node is leaf)");
        } else {
          LOG_TRACE(
              "Unable to block parent node "
              "(current node is leaf). ABORT");

          // ABORT and return
          context_p->abort_flag = true;

          return;
        }

        const LeafRemoveNode *remove_node_p =
            new LeafRemoveNode{node_id, node_p};

        bool ret = InstallNodeToReplace(node_id, remove_node_p, node_p);
        if (ret == true) {
          LOG_TRACE("LeafRemoveNode CAS succeeds. ABORT.");

          context_p->abort_flag = true;

          RemoveAbortOnParent(parent_node_id, abort_node_p, abort_child_node_p);

          return;
        } else {
          LOG_TRACE("LeafRemoveNode CAS failed");

          delete remove_node_p;

          context_p->abort_flag = true;

          RemoveAbortOnParent(parent_node_id, abort_node_p, abort_child_node_p);

          return;
        }
      }
    } else {  // If this is an inner node
      const InnerNode *inner_node_p = static_cast<const InnerNode *>(node_p);

      size_t node_size = inner_node_p->GetSize();

      if (node_size >= INNER_NODE_SIZE_UPPER_THRESHOLD) {
        LOG_TRACE("Node size >= inner upper threshold. Split");

        const InnerNode *new_inner_node_p = inner_node_p->GetSplitSibling();

        // Since this is a split sibling, the low key must be a valid key
        // NOTE: Only for InnerNodes could we call GetLowKey()
        const KeyType &split_key = new_inner_node_p->GetLowKey();

        // New node has at least one item (this is the basic requirement)
        PL_ASSERT(new_inner_node_p->GetSize() > 0);

        const KeyNodeIDPair &first_item = new_inner_node_p->At(0);

        // This points to the left most node on the right split sibling
        // If this node is being removed then we abort
        NodeID split_key_child_node_id = first_item.second;

        // This must be the split key
        PL_ASSERT(KeyCmpEqual(first_item.first, split_key));

        // NOTE: WE FETCH THE POINTER WITHOUT HELP ALONG SINCE WE ARE
        // NOW ON ITS PARENT
        const BaseNode *split_key_child_node_p =
            GetNode(split_key_child_node_id);

        // If the type is remove then we just continue without abort
        // If we abort then it might introduce deadlock
        if (split_key_child_node_p->IsRemoveNode()) {
          LOG_TRACE("Found a removed node on split key child. CONTINUE ");

          // Put the new inner node into GC chain
          // Although it is not entirely necessary it is good for us to
          // make is so to avoid redundant code here
          epoch_manager.AddGarbageNode(new_inner_node_p);

          return;
        }

        NodeID new_node_id = GetNextNodeID();

        const InnerSplitNode *split_node_p = InnerInlineAllocateOfType(
            InnerSplitNode, node_p, std::make_pair(split_key, new_node_id),
            node_p, new_inner_node_p);

        //  First install the NodeID -> split sibling mapping
        InstallNewNode(new_node_id, new_inner_node_p);

        // Then CAS split delta into current node's NodeID
        bool ret = InstallNodeToReplace(node_id, split_node_p, node_p);

        if (ret == true) {
          LOG_TRACE(
              "Inner split delta (from %" PRIu64 " to %" PRIu64 ") CAS succeeds."
              " ABORT",
              node_id, new_node_id);

          // Same reason as in leaf node
          context_p->abort_flag = true;

          return;
        } else {
          LOG_TRACE("Inner split delta CAS fails");

          // Use the epoch manager to recycle NodeID in single threaded
          // environment
          // Note that this remove node should be created on existing node
          // rather than on new_inner_node_p, since new_inner_node_p may
          // be destroyed before fake_remove_node_p is destroyed
          const InnerRemoveNode *fake_remove_node_p =
              new InnerRemoveNode{new_node_id, new_inner_node_p};

          epoch_manager.AddGarbageNode(fake_remove_node_p);
          epoch_manager.AddGarbageNode(new_inner_node_p);

          // Call destructor since it is allocated from the base InnerNode
          split_node_p->~InnerSplitNode();

          return;
        }  // if CAS fails
      } else if (node_size <= INNER_NODE_SIZE_LOWER_THRESHOLD) {
        if (context_p->IsOnRootNode() == true) {
          LOG_TRACE("Root underflow - let it be");

          return;
        }

        // NOTE: If the parent node has changed (e.g. split on the current
        // node's low key) we will not be able to catch it. But we will
        // find it later by posting an InnerAbortNode on parent which would
        // result in CAS failing and aborting

        // We could not remove leftmost node
        if (IsOnLeftMostChild(context_p) == true) {
          LOG_TRACE("Left most inner node cannot be removed");

          return;
        }

        // After this point we decide to remove

        LOG_TRACE("Node size <= inner lower threshold. Remove");

        // Then we abort its parent node
        // These two are used to hold abort node and its previous child
        const BaseNode *abort_node_p;
        const BaseNode *abort_child_node_p;
        NodeID parent_node_id;

        bool abort_node_ret = PostAbortOnParent(
            context_p, &parent_node_id, &abort_node_p, &abort_child_node_p);

        // If we could not block the parent then the parent has changed
        // (splitted, etc.)
        if (abort_node_ret == true) {
          LOG_TRACE("Blocked parent node (current node is inner)");
        } else {
          LOG_TRACE(
              "Unable to block parent node "
              "(current node is inner). ABORT");

          // ABORT and return
          context_p->abort_flag = true;

          return;
        }

        const InnerRemoveNode *remove_node_p =
            new InnerRemoveNode{node_id, node_p};

        bool ret = InstallNodeToReplace(node_id, remove_node_p, node_p);
        if (ret == true) {
          LOG_TRACE("InnerRemoveNode CAS succeeds. ABORT");

          // We abort after installing a node remove delta
          context_p->abort_flag = true;

          // Even if we success we need to remove the abort
          // on the parent, and let parent split thread to detect the remove
          // delta on child
          RemoveAbortOnParent(parent_node_id, abort_node_p, abort_child_node_p);

          return;
        } else {
          LOG_TRACE("InnerRemoveNode CAS failed");

          delete remove_node_p;

          // We must abort here since otherwise it might cause
          // merge nodes to underflow
          context_p->abort_flag = true;

          // Same as above
          RemoveAbortOnParent(parent_node_id, abort_node_p, abort_child_node_p);

          return;
        }
      }  // if split/remove
    }

    return;
  }

  /*
   * RemoveAbortOnParent() - Removes the abort node on the parent
   *
   * This operation must succeeds since only the thread that installed
   * the abort node could remove it
   */
  void RemoveAbortOnParent(NodeID parent_node_id, const BaseNode *abort_node_p,
                           const BaseNode *abort_child_node_p) {
    LOG_TRACE("Remove abort on parent node");

    // We switch back to the child node (so it is the target)
    bool ret =
        InstallNodeToReplace(parent_node_id, abort_child_node_p, abort_node_p);

    // This CAS must succeed since nobody except this thread could remove
    // the ABORT delta on parent node
    PL_ASSERT(ret == true);
    (void)ret;

    // NOTE: DO NOT FORGET TO REMOVE THE ABORT AFTER
    // UNINSTALLING IT FROM THE PARENT NODE
    // NOTE 2: WE COULD NOT DIRECTLY DELETE THIS NODE
    // SINCE SOME OTHER NODES MIGHT HAVE TAKEN A SNAPSHOT
    // AND IF ABORT NODE WAS REMOVED, THE TYPE INFORMATION
    // CANNOT BE PRESERVED AND IT DOES NOT ABORT; INSTEAD
    // IT WILL TRY TO CALL CONSOLIDATE ON ABORT
    // NODE, CAUSING TYPE ERROR
    // delete (InnerAbortNode *)abort_node_p;

    // This delays the deletion of abort node until all threads has exited
    // so that existing pointers to ABORT node remain valid
    // GC does not follow the delta chain of ABORT node
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
  bool PostAbortOnParent(Context *context_p, NodeID *parent_node_id_p,
                         const BaseNode **abort_node_p_p,
                         const BaseNode **abort_child_node_p_p) {
    // This will make sure the path list has length >= 2
    NodeSnapshot *parent_snapshot_p = GetLatestParentNodeSnapshot(context_p);

    const BaseNode *parent_node_p = parent_snapshot_p->node_p;
    NodeID parent_node_id = parent_snapshot_p->node_id;

    // Save original node pointer
    *abort_child_node_p_p = parent_node_p;
    *parent_node_id_p = parent_node_id;

    InnerAbortNode *abort_node_p = new InnerAbortNode{parent_node_p};

    bool ret =
        InstallNodeToReplace(parent_node_id, abort_node_p, parent_node_p);

    if (ret == true) {
      LOG_TRACE("Inner Abort node CAS succeeds");

      // Copy the new node to caller since after posting remove delta we will
      // remove this abort node to enable accessing again
      *abort_node_p_p = abort_node_p;
    } else {
      LOG_TRACE("Inner Abort node CAS failed");

      delete abort_node_p;
    }

    return ret;
  }

  /*
   * NavigateInnerNode() - Given a parent snapshot and a key, return the item
   *                       with the same key if that item exists
   *
   * This function checks whether a given key exists in the current
   * inner node delta chain. If it exists then return a pointer to the
   * item, otherwise return nullptr.
   *
   * This function is called when completing both split SMO and merge SMO
   * For split SMO we need to check, key range and key existance, and
   * NodeID value, to avoid mssing a few InnerDeleteNode on the parent node.
   * However, for merge SMO, we only check key existence.
   *
   * Note: This function does not abort. Any extra checking (e.g. whether
   * NodeIDs match, whether key is inside range) should be done by the caller
   *
   * Note 2: index_pair_p always reflects the relative position of the search
   * key inside this InnerNode, no matter nullptr or non-null pointer is
   * returned, the integer inside the pair is always the index for all keys
   * >= the search key. Currently the secong component is not set and not
   * used
   */
  const KeyNodeIDPair *NavigateInnerNode(NodeSnapshot *snapshot_p,
                                         const KeyType &search_key,
                                         const KeyNodeIDPair **location) {
    // Save some keystrokes
    const BaseNode *node_p = snapshot_p->node_p;

    // This is used to recognize the leftmost branch if there is
    // a merge node
    const KeyNodeIDPair &low_key_pair = node_p->GetLowKeyPair();

    // The caller must make sure this is true
    PL_ASSERT(node_p->GetNextNodeID() == INVALID_NODE_ID ||
              KeyCmpLess(search_key, node_p->GetHighKey()));

    while (1) {
      switch (node_p->GetType()) {
        case NodeType::InnerInsertType: {
          const KeyNodeIDPair &insert_item =
              static_cast<const InnerInsertNode *>(node_p)->item;

          if (KeyCmpEqual(insert_item.first, search_key) == true) {
            // Same key, same index
            *location = static_cast<const InnerInsertNode *>(node_p)->location;

            return &insert_item;
          }

          node_p = static_cast<const InnerInsertNode *>(node_p)->child_node_p;

          break;
        }  // InnerInsertNode
        case NodeType::InnerDeleteType: {
          const KeyNodeIDPair &delete_item =
              static_cast<const InnerDeleteNode *>(node_p)->item;

          if (KeyCmpEqual(delete_item.first, search_key) == true) {
            *location = static_cast<const InnerDeleteNode *>(node_p)->location;

            return nullptr;
          }

          node_p = static_cast<const InnerDeleteNode *>(node_p)->child_node_p;

          break;
        }  // InnerDeleteNode
        case NodeType::InnerType: {
          const InnerNode *inner_node_p =
              static_cast<const InnerNode *>(node_p);

          // DO NOT REMOVE THIS!!!!!!
          // Unlike a NavigateInnerNode(Context *) which searches for child
          // node ID, this function needs to cover all possible separators
          // in the merged InnerNode (right branch)
          const KeyNodeIDPair *start_it = inner_node_p->Begin();

          // If we are on the leftmost branch of the inner node delta chain
          // if there is a merge delta, then we should start searching from
          // the second element. Otherwise always start search from the first
          // element
          if (low_key_pair.second == inner_node_p->At(0).second) {
            start_it++;
          }

          const KeyNodeIDPair *it =
              std::lower_bound(start_it, inner_node_p->End(),
                               std::make_pair(search_key, INVALID_NODE_ID),
                               key_node_id_pair_cmp_obj);

          // Just give the location information by assigning to location
          *location = it;

          if (it == inner_node_p->End()) {
            // This is special case since we could not compare the iterator
            // If the key does not exist then return nullptr
            return nullptr;
          } else if (KeyCmpEqual(it->first, search_key) == false) {
            // If found the lower bound but keys are different
            // then also return nullptr
            return nullptr;
          } else {
            // If found the lower bound and the key matches
            // return the key
            return it;
          }

          PL_ASSERT(false);
          return nullptr;
        }  // InnerNode
        case NodeType::InnerSplitType: {
          // We must guarantee that the key must be inside the range
          // of this function
          // For split SMO this is true by extra checking
          // For merge SMO this is implicitly true sunce the merge key
          // should not be in another node, o.w. the high key has changed
          node_p = static_cast<const InnerSplitNode *>(node_p)->child_node_p;

          break;
        }  // InnerSplitType
        case NodeType::InnerMergeType: {
          const KeyNodeIDPair &delete_item =
              static_cast<const InnerMergeNode *>(node_p)->delete_item;

          // If the split key >= merge key then just go to the right branch
          if (KeyCmpGreaterEqual(search_key, delete_item.first) == true) {
            node_p = static_cast<const InnerMergeNode *>(node_p)->right_merge_p;
          } else {
            node_p = static_cast<const InnerMergeNode *>(node_p)->child_node_p;
          }

          break;
        }  // InnerMergeNode
        default: {
          LOG_DEBUG("Unknown InnerNode type: %d", (int)node_p->GetType());

          PL_ASSERT(false);
          return nullptr;
        }  // default
      }    // switch
    }      // while(1)

    PL_ASSERT(false);
    return nullptr;
  }

  /*
   * FindLeftSibling() - Finds the left sibling of the current child node
   *                     specified by a search key
   *
   * This function works in a way similar to how inner node delta chains
   * are consolidated: We replay the log using sorted small set on all keys
   * less than or equal to the search key, and then choose the second key
   * in key order from high to low, after combing the content of InnerNode
   * and the delta chain nodes
   *
   * NOTE: search_key is the low key of the current node for which we are
   * find the left sibling, not the search key of the current operation.
   * These two might differ in the case of cascading remove node delta
   *
   * NOTE 2: This function could not deal with InnerMergeNode since the merge
   * node imposes a non-consecutive storage for adjacent keys. As an alternative
   * way of finding left sibling, whenever an InnerMergeNode is observed,
   * the node will be consolidated and we use the plain InnerNode to
   * continue searching
   */
  inline NodeID FindLeftSibling(const KeyType &search_key,
                                NodeSnapshot *snapshot_p) {
    // This will be changed during the traversal
    const BaseNode *node_p = snapshot_p->node_p;

    // First check that the node is always in the range of the inner node
    PL_ASSERT(node_p->GetNextNodeID() == INVALID_NODE_ID ||
              KeyCmpLess(search_key, node_p->GetHighKey()));

    // We can only search for left sibling on inner delta chain
    PL_ASSERT(node_p->IsOnLeafDeltaChain() == false);

    const InnerDataNode *data_node_list[node_p->GetDepth()];

    // These two are used to compare InnerDataNode for < and == relation

    // NOTE: For this function we reverse the direction of comparison,
    // i.e. the order is big-to-small to make iteration a little bit easier
    auto f1 = [this](const InnerDataNode *idn_1, const InnerDataNode *idn_2) {
      return this->KeyCmpLess(idn_2->item.first, idn_1->item.first);
      //                          ^ <-------------> ^
    };

    auto f2 = [this](const InnerDataNode *idn_1, const InnerDataNode *idn_2) {
      return this->KeyCmpEqual(idn_1->item.first, idn_2->item.first);
    };

    SortedSmallSet<const InnerDataNode *, decltype(f1), decltype(f2)> sss{
        data_node_list, f1, f2};

    while (1) {
      NodeType type = node_p->GetType();

      switch (type) {
        case NodeType::InnerType: {
          const InnerNode *inner_node_p =
              static_cast<const InnerNode *>(node_p);

          ///////////////////////////////////////////////////////////
          // First find the nearest sep key <= search key on InnerNode
          ///////////////////////////////////////////////////////////

          // This is the logical end of the array
          const KeyNodeIDPair *end_it = inner_node_p->End();

          // Since we know the search key must be one of the key inside
          // the inner node, lower bound is sufficient
          auto it1 =
              std::upper_bound(inner_node_p->Begin() + 1, end_it,
                               std::make_pair(search_key, INVALID_NODE_ID),
                               key_node_id_pair_cmp_obj) -
              1;

          // Note that it is possible for it1 to be begin()
          // since it is not the real current node if the node id
          // is actually found on the delta chain

          ///////////////////////////////////////////////////////////
          // After this point, it is guaranteed:
          //   1. it1 points to an element <= removed node's low key
          //   2. sss.GetBegin() points to an element <= removed node's low key
          //      * OR *
          //      sss.GetBegin() == sss.GetEnd()
          ///////////////////////////////////////////////////////////

          // We need to pop out 2 items
          const KeyNodeIDPair *left_item_p = nullptr;

          int counter = 0;

          // Loop twice. Hope compiler expands this loop
          while (counter < 2) {
            if (sss.GetBegin() == sss.GetEnd()) {
              left_item_p = &(*it1);

              it1--;
              counter++;

              continue;
            } else if (it1 == inner_node_p->Begin()) {
              // We know the sss is not empty, so could always pop from it
              if (sss.GetFront()->GetType() == NodeType::InnerInsertType) {
                left_item_p = &(sss.GetFront()->item);

                counter++;
              }

              // This has to be done anyway
              sss.PopFront();

              continue;
            }

            // After this point it1-- is always valid since it is not begin()

            // If the two items are same
            if (KeyCmpEqual(sss.GetFront()->item.first, it1->first)) {
              // If a delete node and a sep item has the same key
              if (sss.GetFront()->GetType() == NodeType::InnerDeleteType) {
                it1--;
              } else {
                // Otherwise an insert node overrides existing key
                left_item_p = &(sss.GetFront()->item);

                it1--;

                counter++;
              }

              // This is common
              sss.PopFront();
            } else if (KeyCmpLess(sss.GetFront()->item.first, it1->first)) {
              // If the inner node has larger sep item
              // Otherwise an insert node overrides existing key
              left_item_p = &(*it1);

              it1--;

              counter++;
            } else {
              if (sss.GetFront()->GetType() == NodeType::InnerInsertType) {
                // If the delta node has larger item then set left item p
                // to delta item
                left_item_p = &(sss.GetFront()->item);

                counter++;
              }

              // This is common
              sss.PopFront();
            }
          }  // while counter < 2

          // This is the NodeID of the left sibling
          return left_item_p->second;
        }
        case NodeType::InnerInsertType: {
          const InnerInsertNode *insert_node_p =
              static_cast<const InnerInsertNode *>(node_p);

          const KeyNodeIDPair &insert_item = insert_node_p->item;

          if (KeyCmpLessEqual(insert_item.first, search_key)) {
            sss.Insert(insert_node_p);
          }

          node_p = insert_node_p->child_node_p;

          break;
        }  // InnerInsertType
        case NodeType::InnerDeleteType: {
          const InnerDeleteNode *delete_node_p =
              static_cast<const InnerDeleteNode *>(node_p);

          const KeyNodeIDPair &delete_item = delete_node_p->item;

          if (KeyCmpLessEqual(delete_item.first, search_key)) {
            sss.Insert(delete_node_p);
          }

          node_p = delete_node_p->child_node_p;

          break;
        }  // InnerDeleteType
        case NodeType::InnerSplitType: {
          const InnerSplitNode *split_node_p =
              static_cast<const InnerSplitNode *>(node_p);

          node_p = split_node_p->child_node_p;

          break;
        }  // case InnerSplitType
        case NodeType::InnerMergeType: {
          LOG_TRACE(
              "Found merge node. "
              "Need consolidation to find left sibling");

          // Since we could not deal with merge delta, if there is
          // a merge delta on the path, the only solution is to
          // consolidate the inner node, and try to install it
          //
          // Even if installation fails, we need to prevserve the content
          // of the consolidated node, so need to add it to the GC instead
          // of directly releasing its memory

          InnerNode *inner_node_p = CollectAllSepsOnInner(
              snapshot_p,
              // Must +1 to avoid looping on the same depth
              // without any consolidation
              snapshot_p->node_p->GetDepth() + 1);

          bool ret = InstallNodeToReplace(snapshot_p->node_id, inner_node_p,
                                          snapshot_p->node_p);

          if (ret == true) {
            epoch_manager.AddGarbageNode(snapshot_p->node_p);

            snapshot_p->node_p = inner_node_p;
          } else {
            // This is necessary to preserve the content of the inner node
            // while avoid memory leaks
            epoch_manager.AddGarbageNode(inner_node_p);
          }

          // Next iteration will go directly into inner node we have
          // just consolidated
          node_p = inner_node_p;

          // This is important - since we just changed the entire node,
          // the sss should be empty
          sss.Invalidate();

          break;
        }  // InnerMergeType
        default: {
          LOG_ERROR("ERROR: Unknown node type = %d", static_cast<int>(type));

          PL_ASSERT(false);
        }
      }  // switch type
    }    // while 1

    // Should not reach here
    PL_ASSERT(false);
    return INVALID_NODE_ID;
  }

  /*
   * PostInnerMergeNode() - Post an inner merge node
   */
  bool PostInnerMergeNode(const NodeSnapshot *snapshot_p,
                          const KeyType *merge_key_p,
                          const BaseNode *merge_branch_p,
                          NodeID deleted_node_id, const BaseNode **node_p_p) {
    // This is the child node of merge delta
    const BaseNode *node_p = snapshot_p->node_p;
    NodeID node_id = snapshot_p->node_id;

    // Note that we allocate from merge_banch_p since node_p is deallocated
    // first and then merge_branch_p, so if we allocate the merge node
    // on node_p's base node it will be invalid for the second recursive call
    const InnerMergeNode *merge_node_p =
        InnerInlineAllocateOfType(InnerMergeNode, merge_branch_p, *merge_key_p,
                                  merge_branch_p, deleted_node_id, node_p);

    // Compare and Swap!
    bool ret = InstallNodeToReplace(node_id, merge_node_p, node_p);

    // If CAS fails we delete the node and return false
    if (ret == false) {
      merge_node_p->~InnerMergeNode();
    } else {
      *node_p_p = merge_node_p;
    }

    return ret;
  }

  /*
   * PostLeafMergeNode() - Post an inner merge node
   */
  bool PostLeafMergeNode(const NodeSnapshot *snapshot_p,
                         const KeyType *merge_key_p,
                         const BaseNode *merge_branch_p, NodeID deleted_node_id,
                         const BaseNode **node_p_p) {
    // This is the child node of merge delta
    const BaseNode *node_p = snapshot_p->node_p;
    NodeID node_id = snapshot_p->node_id;

    // Must allocate on merge_branch_p, otherwise when recycle this
    // delta chain, node_p is reclaimed first and then merge_branch_p,
    // so if we allocate it on node_p, we will get an invalid reference
    // for the second recursive call
    const LeafMergeNode *merge_node_p =
        InnerInlineAllocateOfType(LeafMergeNode, merge_branch_p, *merge_key_p,
                                  merge_branch_p, deleted_node_id, node_p);

    // Compare and Swap!
    bool ret = InstallNodeToReplace(node_id, merge_node_p, node_p);

    // If CAS fails we delete the node and return false
    if (ret == false) {
      merge_node_p->~LeafMergeNode();
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
    LOG_TRACE("Insert called");

#ifdef BWTREE_DEBUG
    insert_op_count.fetch_add(1);
#endif

    EpochNode *epoch_node_p = epoch_manager.JoinEpoch();

    while (1) {
      Context context{key};
      std::pair<int, bool> index_pair;

      // Check whether the key-value pair exists
      // Also if the key previously exists in the delta chain
      // then return the position of the node using next_key_p
      // if there is none then return nullptr
      const KeyValuePair *item_p = Traverse(&context, &value, &index_pair);

      // If the key-value pair already exists then return false
      if (item_p != nullptr) {
        epoch_manager.LeaveEpoch(epoch_node_p);

        return false;
      }

      NodeSnapshot *snapshot_p = GetLatestNodeSnapshot(&context);

      // We will CAS on top of this
      const BaseNode *node_p = snapshot_p->node_p;
      NodeID node_id = snapshot_p->node_id;

      const LeafInsertNode *insert_node_p = LeafInlineAllocateOfType(
          LeafInsertNode, node_p, key, value, node_p, index_pair);

      bool ret = InstallNodeToReplace(node_id, insert_node_p, node_p);
      if (ret == true) {
        LOG_TRACE("Leaf Insert delta CAS succeed");

        // If install is a success then just break from the loop
        // and return
        break;
      } else {
        LOG_TRACE("Leaf insert delta CAS failed");

#ifdef BWTREE_DEBUG

        context.abort_counter++;

#endif

        insert_node_p->~LeafInsertNode();
      }

#ifdef BWTREE_DEBUG

      // Update abort counter
      // NOTE 1: We could not do this before return since the context
      // object is cleared at the end of loop
      // NOTE 2: Since Traverse() might abort due to other CAS failures
      // context.abort_counter might be larger than 1 when
      // LeafInsertNode installation fails
      insert_abort_count.fetch_add(context.abort_counter);

#endif

      // We reach here only because CAS failed
      LOG_TRACE("Retry installing leaf insert delta from the root");
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
  bool ConditionalInsert(const KeyType &key, const ValueType &value,
                         std::function<bool(const void *)> predicate,
                         bool *predicate_satisfied) {
    LOG_TRACE("Insert (cond.) called");

#ifdef BWTREE_DEBUG
    insert_op_count.fetch_add(1);
#endif

    EpochNode *epoch_node_p = epoch_manager.JoinEpoch();

    while (1) {
      Context context{key};

      // This will just stop on the correct leaf page
      // without traversing into it. Next we manually traverse
      Traverse(&context, nullptr, nullptr);

      *predicate_satisfied = false;

      // This is used to hold the index for which this delta will
      // be applied to
      std::pair<int, bool> index_pair;

      // Call navigate leaf node to test predicate and to test duplicates
      const KeyValuePair *item_p = NavigateLeafNode(
          &context, value, &index_pair, predicate, predicate_satisfied);

      // We do not insert anything if predicate is satisfied
      if (*predicate_satisfied == true) {
        epoch_manager.LeaveEpoch(epoch_node_p);

        return false;
      } else if (item_p != nullptr) {
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
      const LeafInsertNode *insert_node_p = LeafInlineAllocateOfType(
          LeafInsertNode, node_p, key, value, node_p, index_pair);

      bool ret = InstallNodeToReplace(node_id, insert_node_p, node_p);
      if (ret == true) {
        LOG_TRACE("Leaf Insert (cond.) delta CAS succeed");

        // If install is a success then just break from the loop
        // and return
        break;
      } else {
        LOG_TRACE("Leaf insert (cond.) delta CAS failed");

#ifdef BWTREE_DEBUG

        context.abort_counter++;

#endif

        insert_node_p->~LeafInsertNode();
      }

#ifdef BWTREE_DEBUG

      insert_abort_count.fetch_add(context.abort_counter);

#endif

      LOG_TRACE("Retry installing leaf insert (cond.) delta from the root");
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
    LOG_TRACE("Delete called");

#ifdef BWTREE_DEBUG
    delete_op_count.fetch_add(1);
#endif

    EpochNode *epoch_node_p = epoch_manager.JoinEpoch();

    while (1) {
      Context context{key};
      std::pair<int, bool> index_pair;

      // Navigate leaf nodes to check whether the key-value
      // pair exists
      const KeyValuePair *item_p = Traverse(&context, &value, &index_pair);

      if (item_p == nullptr) {
        epoch_manager.LeaveEpoch(epoch_node_p);

        return false;
      }

      NodeSnapshot *snapshot_p = GetLatestNodeSnapshot(&context);

      // We will CAS on top of this
      const BaseNode *node_p = snapshot_p->node_p;
      NodeID node_id = snapshot_p->node_id;

      const LeafDeleteNode *delete_node_p = LeafInlineAllocateOfType(
          LeafDeleteNode, node_p, key, value, node_p, index_pair);

      bool ret = InstallNodeToReplace(node_id, delete_node_p, node_p);
      if (ret == true) {
        LOG_TRACE("Leaf Delete delta CAS succeed");

        // If install is a success then just break from the loop
        // and return
        break;
      } else {
        LOG_TRACE("Leaf Delete delta CAS failed");

        delete_node_p->~LeafDeleteNode();

#ifdef BWTREE_DEBUG

        context.abort_counter++;

#endif
      }

#ifdef BWTREE_DEBUG

      delete_abort_count.fetch_add(context.abort_counter);

#endif

      // We reach here only because CAS failed
      LOG_TRACE("Retry installing leaf delete delta from the root");
    }

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
  void GetValue(const KeyType &search_key, std::vector<ValueType> &value_list) {
    LOG_TRACE("GetValue()");

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
    LOG_TRACE("GetValue()");

    EpochNode *epoch_node_p = epoch_manager.JoinEpoch();

    Context context{search_key};

    std::vector<ValueType> value_list{};
    TraverseReadOptimized(&context, &value_list);

    epoch_manager.LeaveEpoch(epoch_node_p);

    ValueSet value_set{value_list.begin(), value_list.end(), 10, value_hash_obj,
                       value_eq_obj};

    return value_set;
  }

  ///////////////////////////////////////////////////////////////////
  // Garbage Collection Interface
  ///////////////////////////////////////////////////////////////////

  /*
   * NeedGarbageCollection() - Whether the tree needs garbage collection
   *
   * Ideally, if there is no modification workload on BwTree from the last
   * time GC was called, then GC is unnecessary since read operation does not
   * modify any data structure
   *
   * Currently we just leave the interface as a placeholder, which returns true
   * everytime to force GC thred to at least take a look into the epoch
   * counter.
   */
  bool NeedGarbageCollection() { return true; }

  /*
   * PerformGarbageCollection() - Interface function for external users to
   *                              force a garbage collection
   *
   * This function is a wrapper to the internal GC thread function. Since
   * users could choose whether to let BwTree handle GC by itself or to
   * control GC using external threads. This function is left as a convenient
   * interface for external threads to do garbage collection.
   */
  void PerformGarbageCollection() {
    // This function creates a new epoch node, and then checks
    // epoch counter for exiatsing nodes.
    epoch_manager.PerformGarbageCollection();

    return;
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
  const KeyNodeIDPairComparator key_node_id_pair_cmp_obj;
  const KeyNodeIDPairEqualityChecker key_node_id_pair_eq_obj;
  const KeyNodeIDPairHashFunc key_node_id_pair_hash_obj;

  // The following two are used for hashing KeyValuePair
  const KeyValuePairComparator key_value_pair_cmp_obj;
  const KeyValuePairEqualityChecker key_value_pair_eq_obj;
  const KeyValuePairHashFunc key_value_pair_hash_obj;

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

  // InteractiveDebugger idb;

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

    // This flag indicates whether the destructor is running
    // If it is true then GC thread should not clean
    // Therefore, strict ordering is required
    std::atomic<bool> exited_flag;

    // If GC is done with external thread then this should be set
    // to nullptr
    // Otherwise it points to a thread created by EpochManager internally
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
    EpochManager(BwTree *p_tree_p) : tree_p{p_tree_p} {
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
      exited_flag.store(false);

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
     *
     * NOTE: If no internal GC is started then thread_p would be a nullptr
     * and we neither wait nor free the pointer.
     */
    ~EpochManager() {
      // Set stop flag and let thread terminate
      // Also if there is an external GC thread then it should
      // check this flag everytime it does cleaning since otherwise
      // the un-thread-safe function ClearEpoch() would be ran
      // by more than 1 threads
      exited_flag.store(true);

      // If thread pointer is nullptr then we know the GC thread
      // is not started. In this case do not wait for the thread, and just
      // call destructor
      //
      // NOTE: The destructor routine is not thread-safe, so if an external
      // GC thread is being used then that thread should check for
      // exited_flag everytime it wants to do GC
      //
      // If the external thread calls ThreadFunc() then it is safe
      if (thread_p != nullptr) {
        LOG_TRACE("Waiting for thread");

        thread_p->join();

        // Free memory
        delete thread_p;

        LOG_TRACE("Thread stops");
      }

      // So that in the following function the comparison
      // would always fail, until we have cleaned all epoch nodes
      current_epoch_p = nullptr;

      // If all threads has exited then all thread counts are
      // 0, and therefore this should proceed way to the end
      ClearEpoch();

      // If we have a bug (currently there is one) then as a temporary
      // measure just force cleaning all epoches no matter whether they
      // are cleared or not
      if (head_epoch_p != nullptr) {
        LOG_DEBUG("ERROR: After cleanup there is still epoch left");
        LOG_DEBUG("%s", peloton::GETINFO_THICK_LINE.c_str());
        LOG_DEBUG("DUMP");

        for (EpochNode *epoch_node_p = head_epoch_p; epoch_node_p != nullptr;
             epoch_node_p = epoch_node_p->next_p) {
          LOG_DEBUG("Active thread count: %d",
                    epoch_node_p->active_thread_count.load());
          epoch_node_p->active_thread_count = 0;
        }

        LOG_DEBUG("RETRY CLEANING...");
        ClearEpoch();
      }

      PL_ASSERT(head_epoch_p == nullptr);
      LOG_TRACE("Garbage Collector has finished freeing all garbage nodes");

#ifdef BWTREE_DEBUG
      LOG_TRACE("Stat: Freed %" PRIu64 " nodes and %" PRIu64 " NodeID by epoch manager",
                freed_count, freed_id_count);

      LOG_TRACE("      Epoch created = %" PRIu64 "; epoch freed = %" PRIu64 "", epoch_created,
                epoch_freed);

      LOG_TRACE("      Epoch join = %" PRIu64 "; epoch leave = %" PRIu64 "", epoch_join.load(),
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
      LOG_TRACE("Creating new epoch...");

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

#ifdef USE_OLD_EPOCH

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

      while (1) {
        // Then CAS previous node with new garbage node
        // If this fails, then garbage_node_p->next_p is the actual value
        // of garbage_list_p, in which case we do not need to load it again
        bool ret = epoch_p->garbage_list_p.compare_exchange_strong(
            garbage_node_p->next_p, garbage_node_p);

        // If CAS succeeds then just return
        if (ret == true) {
          break;
        } else {
          LOG_TRACE("Add garbage node CAS failed. Retry");
        }
      }  // while 1

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
      if (prev_count < 0) {
        // Consider the following sequence:
        //   0. Start with counter = 0
        //   1. Worker thread 1 fetch_add() -> return 0, OK
        //   2. GC thread fetch_sub() -> return positive, abort!
        //   3. Worker thread 2 fetch_add() -> return negative, retry!
        //   4. GC thread fetch_add() and aborts
        //   5. Worker thread 2 retries, and fetch_add() -> return 1, OK
        // This way the second worker thread actually incremented the epoch
        // counter twice
        epoch_p->active_thread_count.fetch_sub(1);

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
     * PerformGarbageCollection() - Actual job of GC is done here
     *
     * We need to separate the GC loop and actual GC routine to enable
     * external threads calling the function while also allows BwTree maintains
     * its own GC thread using the loop
     */
    void PerformGarbageCollection() {
      ClearEpoch();
      CreateNewEpoch();

      return;
    }

#else  // #ifdef USE_OLD_EPOCH

    /*
     * AddGarbageNode() - This encapsulates BwTree::AddGarbageNode()
     */
    inline void AddGarbageNode(const BaseNode *node_p) {
      tree_p->AddGarbageNode(node_p);

      return;
    }

    inline EpochNode *JoinEpoch() {
      tree_p->UpdateLastActiveEpoch();

      return nullptr;
    }

    inline void LeaveEpoch(EpochNode *epoch_p) {
      tree_p->UpdateLastActiveEpoch();

      (void)epoch_p;
      return;
    }

    inline void PerformGarbageCollection() {
      tree_p->IncreaseEpoch();

      return;
    }

#endif  // #ifdef USE_OLD_EPOCH

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

      while (1) {
        node_p = next_node_p;
        PL_ASSERT(node_p != nullptr);

        NodeType type = node_p->GetType();

        switch (type) {
          case NodeType::LeafInsertType:
            next_node_p = ((LeafInsertNode *)node_p)->child_node_p;

            ((LeafInsertNode *)node_p)->~LeafInsertNode();

#ifdef BWTREE_DEBUG
            freed_count++;
#endif
            break;
          case NodeType::LeafDeleteType:
            next_node_p = ((LeafDeleteNode *)node_p)->child_node_p;

            ((LeafDeleteNode *)node_p)->~LeafDeleteNode();

#ifdef BWTREE_DEBUG
            freed_count++;
#endif

            break;
          case NodeType::LeafSplitType:
            next_node_p = ((LeafSplitNode *)node_p)->child_node_p;

            ((LeafSplitNode *)node_p)->~LeafSplitNode();

#ifdef BWTREE_DEBUG
            freed_count++;
#endif

            break;
          case NodeType::LeafMergeType:
            FreeEpochDeltaChain(((LeafMergeNode *)node_p)->child_node_p);
            FreeEpochDeltaChain(((LeafMergeNode *)node_p)->right_merge_p);

            ((LeafMergeNode *)node_p)->~LeafMergeNode();

#ifdef BWTREE_DEBUG
            freed_count++;
#endif

            // Leaf merge node is an ending node
            return;
          case NodeType::LeafRemoveType:
            // This recycles node ID
            tree_p->InvalidateNodeID(((LeafRemoveNode *)node_p)->removed_id);

            delete ((LeafRemoveNode *)node_p);

#ifdef BWTREE_DEBUG
            freed_count++;
            freed_id_count++;
#endif

            // We never try to free those under remove node
            // since they will be freed by recursive call from
            // merge node
            return;
          case NodeType::LeafType:
            ((LeafNode *)node_p)->~LeafNode();
            ((LeafNode *)node_p)->Destroy();

#ifdef BWTREE_DEBUG
            freed_count++;
#endif

            // We have reached the end of delta chain
            return;
          case NodeType::InnerInsertType:
            next_node_p = ((InnerInsertNode *)node_p)->child_node_p;

            ((InnerInsertNode *)node_p)->~InnerInsertNode();

#ifdef BWTREE_DEBUG
            freed_count++;
#endif

            break;
          case NodeType::InnerDeleteType:
            next_node_p = ((InnerDeleteNode *)node_p)->child_node_p;

            ((InnerDeleteNode *)node_p)->~InnerDeleteNode();

#ifdef BWTREE_DEBUG
            freed_count++;
#endif

            break;
          case NodeType::InnerSplitType:
            next_node_p = ((InnerSplitNode *)node_p)->child_node_p;

            ((InnerSplitNode *)node_p)->~InnerSplitNode();

#ifdef BWTREE_DEBUG
            freed_count++;
#endif

            break;
          case NodeType::InnerMergeType:
            FreeEpochDeltaChain(((InnerMergeNode *)node_p)->child_node_p);
            FreeEpochDeltaChain(((InnerMergeNode *)node_p)->right_merge_p);

            ((InnerMergeNode *)node_p)->~InnerMergeNode();

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

            delete ((InnerRemoveNode *)node_p);

#ifdef BWTREE_DEBUG
            freed_count++;
            freed_id_count++;
#endif

            // We never free nodes under remove node
            return;
          case NodeType::InnerType:
            ((InnerNode *)node_p)->~InnerNode();
            ((InnerNode *)node_p)->Destroy();

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
            delete ((InnerAbortNode *)node_p);

#ifdef BWTREE_DEBUG
            freed_count++;
#endif

            // Inner abort node is also a terminating node
            // so we do not delete the beneath nodes, but just return
            return;
          default:
            // This does not include INNER ABORT node
            LOG_ERROR("Unknown node type: %d", (int)type);

            PL_ASSERT(false);
            return;
        }  // switch
      }    // while 1

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
      LOG_TRACE("Start to clear epoch");

      while (1) {
        // Even if current_epoch_p is nullptr, this should work
        if (head_epoch_p == current_epoch_p) {
          LOG_TRACE("Current epoch is head epoch. Do not clean");

          break;
        }

        // Since it could only be acquired and released by worker thread
        // the value must be >= 0
        int active_thread_count = head_epoch_p->active_thread_count.load();
        PL_ASSERT(active_thread_count >= 0);

        // If we have seen an epoch whose count is not zero then all
        // epochs after that are protected and we stop
        if (active_thread_count != 0) {
          LOG_TRACE("Head epoch is not empty. Return");

          break;
        }

        // If some thread joins the epoch between the previous branch
        // and the following fetch_sub(), then fetch_sub() returns a positive
        // number, which is the number of threads that have joined the epoch
        // since last epoch counter testing.

        if (head_epoch_p->active_thread_count.fetch_sub(MAX_THREAD_COUNT) > 0) {
          LOG_TRACE(
              "Some thread sneaks in after we have decided"
              " to clean. Return");

          // Must add it back to let the next round of cleaning correctly
          // identify empty epoch
          head_epoch_p->active_thread_count.fetch_add(MAX_THREAD_COUNT);

          break;
        }

        // After this point all fetch_add() on the epoch counter would return
        // a negative value which will cause re-read of current_epoch_p
        // to prevent joining an epoch that is being deleted

        // If the epoch has cleared we just loop through its garbage chain
        // and then free each delta chain

        const GarbageNode *next_garbage_node_p = nullptr;

        // Walk through its garbage chain
        for (const GarbageNode *garbage_node_p =
                 head_epoch_p->garbage_list_p.load();
             garbage_node_p != nullptr; garbage_node_p = next_garbage_node_p) {
          FreeEpochDeltaChain(garbage_node_p->node_p);

          // Save the next pointer so that we could
          // delete current node directly
          next_garbage_node_p = garbage_node_p->next_p;

          // This invalidates any further reference to its
          // members (so we saved next pointer above)
          delete garbage_node_p;
        }  // for

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
      }  // while(1) through epoch nodes

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
      while (exited_flag.load() == false) {
        // printf("Start new epoch cycle");
        PerformGarbageCollection();

        // Sleep for 50 ms
        std::chrono::milliseconds duration(GC_INTERVAL);
        std::this_thread::sleep_for(duration);
      }

      LOG_TRACE("exit flag is true; thread return");

      return;
    }

    /*
     * StartThread() - Start cleaner thread for garbage collection
     *
     * NOTE: This is not called in the constructor, and needs to be
     * called manually
     */
    void StartThread() {
      thread_p = new std::thread{[this]() { this->ThreadFunc(); }};

      return;
    }

  };  // Epoch manager

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
  ForwardIterator Begin() { return ForwardIterator{this}; }

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
  ForwardIterator NullIterator() { return ForwardIterator{}; }

  /*
   * Iterators
   */

  /*
   * class IteratorContext - Buffers leaf page information for iterating on
   *                         that page
   *
   * This page buffers the content of a leaf page in the tree. We do not
   * directly refer to a page because there is no protection from the page being
   * recycled by SMR.
   *
   * Please note that this IteratorContext could only be used under single
   * threaded environment. This is a valid assumption since different threads
   * could always start their own iterators
   *
   * Since the instance of this class is created as char[], with a class
   * ElasticNode<KeyValuePair> embedded, when destroy the instance we must
   * manually call destructor first, and then call member Destroy() to free
   * the chunk of memory as char[] rather than IteratorContext instance.
   */
  class IteratorContext {
   private:
    // We need this reference to traverse and also to call GC
    BwTree *tree_p;

    // This is a reference counter used for single threaded environment
    // Note that if multiple threads modifies the reference counter concurrentl
    // then we could not recycle it even if the ref count has droped to 0
    size_t ref_count;

    // This is a stub that points to class LeafNode which is used to
    // receive consolidated key value pairs from a leaf delta chain
    LeafNode leaf_node_p[0];

    /*
     * Constructor - Initialize class IteratorContext part
     *
     * Note that the LeafNode instance is initialized outside of this class
     */
    IteratorContext(BwTree *p_tree_p) : tree_p{p_tree_p}, ref_count{0UL} {}

    /*
     * Destructor
     *
     * Note that this could not be directly deleted by calling operator delete[]
     * since we allocate it as char[], so the destructor must be called
     * manually and then free the chunk of memory as char[] rather than as
     * class IteratorContext instance
     */
    ~IteratorContext() {
      // Call destructor to destruct all KeyValuePairs stored in its array
      GetLeafNode()->~ElasticNode<KeyValuePair>();

      return;
    }

   public:
    /*
     * GetLeafNode() - Returns a pointer to the leaf node object embedded inside
     *                 class IteratorContext object
     */
    inline LeafNode *GetLeafNode() { return &leaf_node_p[0]; }

    /*
     * GetTree() - Returns a tree instance
     */
    inline BwTree *GetTree() { return tree_p; }

    /*
     * InnRef() - Increase reference counter
     *
     * This must be called when an object is newly constructed or is referecne
     * copied to another iterator instance
     */
    inline void IncRef() {
      ref_count++;
      PL_ASSERT(ref_count != 0UL);

      return;
    }

    /*
     * DecRef() - Decrease the refreence counter by 1
     *
     * If ref_count drops to zero after the decreament then call the destructor
     * of the current object to destroy it. External functions do not need
     * to take care of the lifetime of the object
     *
     * Note that the reference count mechanism could only be used when there
     * is only one thread accessing this instance. If multiple threads are
     * using the reference counter then the following is possible:
     *   1. Thread 1 decreases ref count and it becomes 0
     *   2. Thread 1 decides to destroy the object
     *   3. Thread 2 refers to the object, and increases the ref count
     *   4. Thread 1 frees memory
     *   5. Thread 2 access invalid memory
     *
     * An alternative way for this naive ref count would be to lock it using
     * either implicit atomic variable or explicitly with a lock. Once ref
     * count drops to 0, we lock it to avoid threads further accessing.
     */
    inline void DecRef() {
      // for unsigned long the only thing we could ensure
      // is that it does not cross 0 boundary
      PL_ASSERT(ref_count != 0UL);

      ref_count--;
      if (ref_count == 0UL) {
        // 1. calls d'tor of class IteratorContext which calls d'tor
        //    for class ElasticNode
        this->~IteratorContext();
        // 2. Frees memory as char[]
        this->Destroy();
      }

      return;
    }

    /*
     * GetRefCount() - Returns the current reference counter
     */
    inline size_t GetRefCount() { return ref_count; }

    /*
     * Get() - Static function that constructs an iterator context object
     *
     * Note that node_p is passed as the head node of a delta chain, and we
     * only need its high key and item count field. Low key, depth and node type
     * will be used to initialize the LeafNode instance embedded inside
     * this object but they will not be used as part of the iteration
     *
     * Both class IteratorContext and class LeafNode is initialized when
     * this function returns. CollectAllValuesOnLeaf() does not einitialize
     * the leaf node if it is provided in the argument list.
     */
    inline static IteratorContext *Get(BwTree *p_tree_p,
                                       const BaseNode *node_p) {
      // This is the size of the memory chunk we allocate for the leaf node
      size_t size = sizeof(IteratorContext) + sizeof(LeafNode) +
                    sizeof(KeyValuePair) * node_p->GetItemCount();

      // This is the size of memory we wish to initialize for IteratorContext
      // plus data
      IteratorContext *ic_p =
          reinterpret_cast<IteratorContext *>(new char[size]);
      PL_ASSERT(ic_p != nullptr);

      // Initialize class IteratorContext part
      new (ic_p) IteratorContext{p_tree_p};

      // Then initialize class LeafNode
      // i.e. class ElasticNode<KeyValuePair> part
      new (ic_p->GetLeafNode()) ElasticNode<KeyValuePair>{
          node_p->GetType(), node_p->GetDepth(), node_p->GetItemCount(),
          node_p->GetLowKeyPair(), node_p->GetHighKeyPair()};

      // So after this function returns the ref count should be exactly 1
      ic_p->IncRef();
      PL_ASSERT(ic_p->GetRefCount() == 1UL);

      return ic_p;
    }

    /*
     * Destroy() - Manually frees memory as char[]
     *
     * This function is necessary to ensure well defined bahavior of the
     * class since the memory of "this" pointer is allocated through operator
     * new[] of type char[], so we must reclaim memory using the same
     * operator delete[] of type char[]
     *
     * Note that class ElasticNode<KeyValuePair> d'tor needs to be called
     * before this function is called then it is deleted in the d'tor
     */
    inline void Destroy() {
      // Note that we must cast it as char * before
      // delete[] otherwise C++ will try to find the size of the array
      // and call d'tor for each element
      delete[] reinterpret_cast<char *>(this);

      return;
    }
  };

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
   private:
    // This points to the iterator context that holds the LeafNode object
    IteratorContext *ic_p;
    KeyValuePair *kv_p;

   public:
    /*
     * Default Constructor - This acts as a place holder for some functions
     *                       that require a type and an object but we do not
     *                       want to afford the overhead of loading a page into
     *                       the iterator
     *
     * All pointers are initialized to nullptr to indicate that it does not
     * need any form of memory reclaim
     */
    ForwardIterator() : ic_p{nullptr}, kv_p{nullptr} {}

    /*
     * Constructor
     *
     * NOTE: We load the first leaf page using FIRST_LEAF_NODE_ID since we
     * know it is there
     */
    ForwardIterator(BwTree *p_tree_p) {
      // This also needs to be protected by epoch since we do access internal
      // node that is possible to be reclaimed
      EpochNode *epoch_node_p = p_tree_p->epoch_manager.JoinEpoch();

      // Load the first leaf page
      const BaseNode *node_p = p_tree_p->GetNode(FIRST_LEAF_NODE_ID);
      PL_ASSERT(node_p != nullptr);
      PL_ASSERT(node_p->IsOnLeafDeltaChain() == true);

      // Allocate space for IteratorContext + LeafNode Metadata + LeafNode data
      ic_p = IteratorContext::Get(p_tree_p, node_p);
      // This does not change after CollectAllValuesOnLeaf() is called
      kv_p = ic_p->GetLeafNode()->Begin();
      PL_ASSERT(ic_p->GetRefCount() == 1UL);

      // Use this to collect all values
      NodeSnapshot snapshot{FIRST_LEAF_NODE_ID, node_p};

      // Consolidate the current node. Note that we pass in the leaf node
      // object embedded inside the IteratorContext object
      p_tree_p->CollectAllValuesOnLeaf(&snapshot, ic_p->GetLeafNode());

      // Leave epoch
      p_tree_p->epoch_manager.LeaveEpoch(epoch_node_p);

      return;
    }

    /*
     * Constructor - Construct an iterator given a key
     *
     * The given key would locate the iterator on an data item whose
     * key is >= the given key. This is useful in range query if
     * a starting key could be derived according to conditions
     */
    ForwardIterator(BwTree *p_tree_p, const KeyType &start_key)
        : ic_p{nullptr}, kv_p{nullptr} {
      // Load the corresponding page using the given key and store all its
      // data into the iterator's embedded leaf page
      LowerBound(p_tree_p, &start_key);

      return;
    }

    /*
     * Copy Constructor - Constructs a new iterator instance from existing one
     *
     * During copy construction we need to take care that the iterator is
     * invalidated after copy constructing LeafNode from the other iterator
     * to this. So we should move the iterator manually
     */
    ForwardIterator(const ForwardIterator &other)
        : ic_p{other.ic_p}, kv_p{other.kv_p} {
      // Increase its reference count since now two iterators
      // share one IteratorContext object
      other.ic_p->IncRef();

      return;
    }

    /*
     * operator= - Assigns one object to another
     *
     * Note: For self-assignment special care must be taken to avoid
     * operating on an object itself
     */
    ForwardIterator &operator=(const ForwardIterator &other) {
      // It is crucial to prevent self assignment since we do pointer
      // operation here
      if (this == &other) {
        return *this;
      }

      // If this is an empty iterator then do nothing; otherwise need to
      // release a reference to the current ic_p first
      if (ic_p == nullptr) {
        PL_ASSERT(kv_p == nullptr);
      } else {
        PL_ASSERT(kv_p != nullptr);
        ic_p->DecRef();
      }

      // Add a reference to the IteratorContext
      ic_p = other.ic_p;
      kv_p = other.kv_p;
      other.ic_p->IncRef();

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
      // Note that this is necessary since even if other has an address
      // it could be transformed into a x-value using std::move()
      if (this == &other) {
        return *this;
      }

      // For move assignment we do not touch the ref count
      // and just nullify the other object
      if (ic_p == nullptr) {
        PL_ASSERT(kv_p == nullptr);
      } else {
        PL_ASSERT(kv_p != nullptr);
      }

      // Add a reference to the IteratorContext
      ic_p = other.ic_p;
      kv_p = other.kv_p;
      // Nullify it to avoid from being used
      other.ic_p = nullptr;
      other.kv_p = nullptr;

      return *this;
    }

    /*
     * IsEnd() - Whether the current iterator caches the last page and
     *           the iterator points to the last element
     *
     * Note that since in a lock-free data structure it is impossible to
     * derive a universal coordinate to denote where an iterator points to
     * we only make use of the current cached page to determine whether this
     * page is the last page (by looking at the next node ID stored in leaf
     * page metadata) and whether the current kv_p points to the End() iterator
     * of the currently cached page. If both are met then we claim it is an end
     * iterator.
     *
     * Comparing between two End() iterators are meaningless since the last
     * page might be different. Therefore, please always call IsEnd() to
     * detect end of iteration.
     *
     * Note also that for empty iterators we always declare them as end iterator
     * because this simplifies the construction of an End() iterator without
     * actually traversing the tree
     */
    bool IsEnd() const {
      // Empty iterator is naturally end iterator
      if (ic_p == nullptr) {
        PL_ASSERT(kv_p == nullptr);

        return true;
      }

      // 1. Next node ID is INVALID_NODE_ID
      // 2. Current iterator pointer equals end_p stored in leaf node
      return (ic_p->GetLeafNode()->GetNextNodeID() == INVALID_NODE_ID) &&
             (ic_p->GetLeafNode()->End() == kv_p);
    }

    /*
     * IsBegin() - Returns whether the iterator is Begin() iterator
     *
     * We define Begin() iterator as follows:
     *   (1) kv_p and ic_p are empty
     *   (2) Otherwise either low key node ID is invalid node ID * and *
     *       kv_p points to Begin() of the underlying leaf node
     */
    bool IsBegin() const {
      // This is both Begin() and End()
      if (ic_p == nullptr) {
        PL_ASSERT(kv_p == nullptr);

        return true;
      }

      return (ic_p->GetLeafNode()->GetLowKeyPair().second == INVALID_NODE_ID) &&
             (ic_p->GetLeafNode()->Begin() == kv_p);
    }

    /*
     * IsREnd() - Whether the pointer is one slot before the Begin() pointer
     *
     * We define REnd() as follows:
     *   (1) kv_p and ic_p are both empty
     *   (2) Otherwise the low key ID is invalid node ID to indicate it is the
     *       first leaf page of the tree, and also the kv_p pointer should
     *       point to the REnd() of the underlying leaf page
     */
    bool IsREnd() const {
      if (ic_p == nullptr) {
        PL_ASSERT(kv_p == nullptr);

        return true;
      }

      // Note that it is leaf node's Begin() - 1
      return (ic_p->GetLeafNode()->GetLowKeyPair().second == INVALID_NODE_ID) &&
             ((ic_p->GetLeafNode()->Begin() - 1) == kv_p);
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
      return *kv_p;
    }

    /*
     * operator->() - Returns the value pointer pointed to by this iterator
     *
     * Note that this function returns a contsnat pointer which can be used
     * to access members of the value, but cannot modify
     */
    inline const KeyValuePair *operator->() { return &*kv_p; }

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
     * in this function we check end flag first.
     *
     * Comparison rules:
     *   1. end iterator is no less than all other iterators
     *     1.5. end iterator is not less than end iterator
     *     1.75. end iterator is greater than all other non-end iterators
     *   2. If both are not end iterator then simply compare their keys
     *      currently pointed to by kv_p
     *   3. Values are never compared
     */
    inline bool operator<(const ForwardIterator &other) const {
      if (other.IsEnd() == true) {
        if (IsEnd() == true) {
          return false;
        } else {
          return true;
        }
      } else if (IsEnd() == true) {
        return false;
      }

      // After this point we know none of them is end iterator

      // If none of them is end iterator, we simply do a key comparison
      // using the iterator
      // Note: We should check whether these two iterators are from
      // the same tree
      return ic_p->GetTree()->KeyCmpLess(kv_p->first, other.kv_p->first);
    }

    /*
     * operator==() - Compares whether two iterators refer to the same key
     *
     * Comparison rules:
     *   1. end iterator equals end iterator
     *   2. end iterator does not equal all non-end iterators
     *   3. For all other cases, compare their key being currently pointed
     *      to by kv_p
     */
    inline bool operator==(const ForwardIterator &other) const {
      if (other.IsEnd() == true) {
        if (IsEnd() == true) {
          // Two end iterators are equal to each other
          return true;
        } else {
          // Otherwise they are not equal
          return false;
        }
      } else if (IsEnd() == true) {
        return false;
      }

      // After this we know none of them are end iterators

      return ic_p->GetTree()->KeyCmpEqual(kv_p->first, other.kv_p->first);
    }

    /*
     * Destructor
     *
     * If the iterator is not null iterator then we decrease the reference
     * count of the IteratorContext object which could possibly leads
     * to the destruction of ic_p. Otherwise just return
     */
    ~ForwardIterator() {
      if (ic_p != nullptr) {
        PL_ASSERT(kv_p != nullptr);
        // If ic_p is not nullptr we know it is a valid reference and
        // just decrease reference counter for it. This might also call
        // destructor for ic_p instance if we release the last reference
        ic_p->DecRef();
      } else {
        PL_ASSERT(kv_p == nullptr);
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
      if (IsEnd() == true) {
        return *this;
      }

      MoveAheadByOne();

      return *this;
    }

    /*
     * Prefix operator-- - Move one element back relative to the current key
     */
    inline ForwardIterator &operator--() {
      // This filters out:
      //   (1) Pointers being nullptr
      //   (2) Real begin iterator
      if (IsREnd() == true) {
        return *this;
      }

      MoveBackByOne();

      return *this;
    }

    /*
     * Postfix operator++ - Move the iterator ahead, and return the old one
     *
     * For end() iterator we do not do anything but return the same iterator
     */
    inline ForwardIterator operator++(int) {
      if (IsEnd() == true) {
        return *this;
      }

      // Make a copy of the current one before advancing
      // This will increase ref count temporarily, but it is always consistent
      ForwardIterator temp = *this;

      MoveAheadByOne();

      return temp;
    }

    /*
     * PostFix operator-- - Move the iterator backward by one element
     */
    inline ForwardIterator operator--(int) {
      if (IsREnd() == true) {
        return *this;
      }

      // Make a copy of the current one before advancing
      // This will increase ref count temporarily, but it is always consistent
      ForwardIterator temp = *this;

      MoveBackByOne();

      return temp;
    }

    /*
     * LowerBound() - Load leaf page whose key >= start_key
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
     * Note that the argument p_tree_p is required since this function might be
     * called with ic_p being nullptr, such that we need a reference to the tree
     * instance
     */
    void LowerBound(BwTree *p_tree_p, const KeyType *start_key_p) {
      PL_ASSERT(start_key_p != nullptr);
      // This is required since start_key_p might be pointing inside the
      // currently buffered IteratorContext which will be destroyed
      // after new IteratorContext is created
      KeyType start_key = *start_key_p;

      while (1) {
        // First join the epoch to prevent physical nodes being deallocated
        // too early
        EpochNode *epoch_node_p = p_tree_p->epoch_manager.JoinEpoch();

        // This traversal has the following characteristics:
        //   1. It stops at the leaf level without traversing leaf with the key
        //   2. It DOES finish partial SMO, consolidate overlengthed chain, etc.
        //   3. It DOES traverse horizontally using sibling pointer
        Context context{start_key};
        p_tree_p->Traverse(&context, nullptr, nullptr);

        NodeSnapshot *snapshot_p = BwTree::GetLatestNodeSnapshot(&context);
        const BaseNode *node_p = snapshot_p->node_p;
        PL_ASSERT(node_p->IsOnLeafDeltaChain() == true);

        // After this point, start_key_p from the last page becomes invalid

        // We are releasing the IteratorContext object currently held
        // because we are now going to the next page after it
        if (ic_p != nullptr) {
          ic_p->DecRef();
        }

        // Refresh the IteratorContext object and also refresh kv_p
        ic_p = IteratorContext::Get(p_tree_p, node_p);
        PL_ASSERT(ic_p->GetRefCount() == 1UL);

        // Consolidate the current node and store all key value pairs
        // to the embedded leaf node
        p_tree_p->CollectAllValuesOnLeaf(snapshot_p, ic_p->GetLeafNode());

        // Leave the epoch, since we have already had all information
        p_tree_p->epoch_manager.LeaveEpoch(epoch_node_p);

        // Find the lower bound of the current start search key
        // NOTE: Do not use start_key_p since the target it points to
        // might have been destroyed because we already released the reference
        //
        // There are three possible outcomes:
        //   1. kv_p is in the middle of the leaf page:
        //      Everything is OK, normal case
        //   2. kv_p points to End() of the leaf node and next node ID
        //      is INVALID_NODE_ID: Return because we have reached End()
        //   3. kv_p points to End() of the leaf node but next node ID
        //      is a valid one: Try next page since the current page might have
        //      been merged
        kv_p = std::lower_bound(ic_p->GetLeafNode()->Begin(),
                                ic_p->GetLeafNode()->End(),
                                std::make_pair(start_key, ValueType{}),
                                p_tree_p->key_value_pair_cmp_obj);

        // All keys in the leaf page are < start key. Switch the next key until
        // we have found the key or until we have reached end of tree
        if (kv_p != ic_p->GetLeafNode()->End()) {
          break;
        } else if (IsEnd() == true) {
          break;
        } else {
          // Must do a value copy since the current ic_p will be
          // destroyed before this variable is used
          start_key = ic_p->GetLeafNode()->GetHighKeyPair().first;
        }
      }  // while(1)

      return;
    }

    /*
     * MoveBackByOne() - Moves to the left key if there is one
     *
     * This function works by querying the tree using low key of the current
     * node (which must be nonempty), keeping going left until we have seen
     * a node whose low key is higher than or equal to the current low key,
     * thus locating the left node of the current node
     *
     * Note that when this function is called, the following must be satisfied:
     *   (1) There must be a valid IteratorContext
     *   (2) The current status must not be Begin() status
     */
    void MoveBackByOne() {
      PL_ASSERT(kv_p != nullptr);
      PL_ASSERT(ic_p != nullptr);
      PL_ASSERT(IsREnd() == false);

      // This is an invalid state
      PL_ASSERT(kv_p != ic_p->GetLeafNode()->REnd());

      // This will be used to call BwTree functions
      BwTree *tree_p = ic_p->GetTree();

      kv_p--;
      // If there is no nodes to the left of the current node
      if (IsREnd() == true) {
        return;
      } else if (kv_p != ic_p->GetLeafNode()->REnd()) {
        return;
      }

      while (1) {
        // Saves the low key such that even if we release the reference to
        // the IteratorContext object, it is still valid key
        KeyType low_key = ic_p->GetLeafNode()->GetLowKey();

        // Traverse backward using the low key. This function will
        // try its best to reach the exact left page whose high key
        // <= current low key
        Context context{low_key};

        EpochNode *epoch_node_p = tree_p->epoch_manager.JoinEpoch();

        // This function stops and does not traverse LeafNode after adjusting
        // itself by traversing sibling chain
        tree_p->TraverseBI(&context);
        NodeSnapshot *snapshot_p = tree_p->GetLatestNodeSnapshot(&context);
        const BaseNode *node_p = snapshot_p->node_p;

        // We must have reached a node whose low key is less than the
        // low key we used as the search key
        // Either it has a -Inf low key, or the low key could be compared
        PL_ASSERT((node_p->GetLowKeyPair().second == INVALID_NODE_ID) ||
                  (tree_p->KeyCmpLess(node_p->GetLowKey(), low_key) == true));

        // Release the current leaf page, and
        ic_p->DecRef();
        ic_p = IteratorContext::Get(tree_p, node_p);
        PL_ASSERT(ic_p->GetRefCount() == 1UL);
        tree_p->CollectAllValuesOnLeaf(snapshot_p, ic_p->GetLeafNode());

        // Now we could safely release the reference
        tree_p->epoch_manager.LeaveEpoch(epoch_node_p);

        // There are several possibilities:
        //    (1) kv_p stops at a key == low_key; kv_p--
        //    (2) kv_p stops at a key > low_key; kv_p--
        //        This implies the node has been merged into current node
        //        and the low key element is deleted
        //    (3) kv_p stops at a key < low key; this is impossible
        //    (4) kv_p stops at End(); this is the usual case; kv_p--
        //    (5) kv_p stops at Begin(); This is a special case of (1) or (2)
        //        kv_p-- will make it invalid, so if it is not Begin() then we
        //        need to take the current low key and retry
        //    (6) If the leaf node itself is empty then kv_p == End() == Begin()
        //        and kv_p-- is REnd()
        kv_p = std::lower_bound(ic_p->GetLeafNode()->Begin(),
                                ic_p->GetLeafNode()->End(),
                                std::make_pair(low_key, ValueType{}),
                                tree_p->key_value_pair_cmp_obj) -
               1;

        // If after decreament the kv_p points to the element before Begin()
        // then we know we should try again
        if (kv_p == ic_p->GetLeafNode()->REnd()) {
          // If there is no low key (-Inf) then that's it
          if (node_p->GetLowKeyPair().second == INVALID_NODE_ID) {
            return;
          } else {
            low_key = ic_p->GetLeafNode()->GetLowKey();
          }
        } else {
          return;
        }
      }  // while(1)

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
      PL_ASSERT(ic_p != nullptr);
      PL_ASSERT(kv_p != nullptr);

      // We could not be on the last page, since before calling this
      // function whether we are on the last page should be
      // checked
      PL_ASSERT(IsEnd() == false);

      kv_p++;

      // If we have drained the current page, just use its high key to
      // go to the next page that contains the high key
      if (kv_p == ic_p->GetLeafNode()->End()) {
        // If the current status after increment is End() then just exit and
        // does not go to the next page
        if (IsEnd() == true) {
          return;
        }

        // This will replace the current ic_p with a new one
        // all references to the ic_p will be invalidated
        LowerBound(ic_p->GetTree(),
                   &ic_p->GetLeafNode()->GetHighKeyPair().first);
      }

      return;
    }
  };  // ForwardIterator

  /*
   * AddGarbageNode() - Adds a garbage node into the thread-local GC context
   *
   * Since the thread local GC context is only accessed by this thread, this
   * process does not require any atomicity
   *
   * This is always called by the thread owning thread local data, so we
   * do not have to worry about thread identity issues
   */
  void AddGarbageNode(const BaseNode *node_p) {
    GarbageNode *garbage_node_p =
        new GarbageNode{GetGlobalEpoch(), (void *)(node_p)};
    PL_ASSERT(garbage_node_p != nullptr);

    // Link this new node to the end of the linked list
    // and then update last_p
    GetCurrentGCMetaData()->last_p->next_p = garbage_node_p;
    GetCurrentGCMetaData()->last_p = garbage_node_p;

    // Update the counter
    GetCurrentGCMetaData()->node_count++;

    // It is possible that we could not free enough number of nodes to
    // make it less than this threshold
    // So it is important to let the epoch counter be constantly increased
    // to guarantee progress
    if (GetCurrentGCMetaData()->node_count > GC_NODE_COUNT_THREADHOLD) {
      // Use current thread's gc id to perform GC
      PerformGC(gc_id);
    }

    return;
  }

  /*
   * PerformGC() - This function performs GC on the current thread's garbage
   *               chain using the call back function
   *
   * Note that this function only collects for the current thread. Therefore
   * this function does not have to be atomic since its
   *
   * Note that this function should be used with thread_id, since it will
   * also be called inside the destructor - so we could not rely on
   * GetCurrentGCMetaData()
   */
  void PerformGC(int thread_id) {
    // First of all get the minimum epoch of all active threads
    // This is the upper bound for deleted epoch in garbage node
    uint64_t min_epoch = SummarizeGCEpoch();

    // This is the pointer we use to perform GC
    // Note that we only fetch the metadata using the current thread-local id
    GarbageNode *header_p = &GetGCMetaData(thread_id)->header;
    GarbageNode *first_p = header_p->next_p;

    // Then traverse the linked list
    // Only reclaim memory when the deleted epoch < min epoch
    while (first_p != nullptr && first_p->delete_epoch < min_epoch) {
      // First unlink the current node from the linked list
      // This could set it to nullptr
      header_p->next_p = first_p->next_p;

      // Then free memory
      epoch_manager.FreeEpochDeltaChain((const BaseNode *)first_p->node_p);

      delete first_p;
      PL_ASSERT(GetGCMetaData(thread_id)->node_count != 0UL);
      GetGCMetaData(thread_id)->node_count--;

      first_p = header_p->next_p;
    }

    // If we have freed all nodes in the linked list we should
    // reset last_p to the header
    if (first_p == nullptr) {
      GetGCMetaData(thread_id)->last_p = header_p;
    }

    return;
  }

};  // class BwTree

}  // End index/bwtree namespace
}  // End peloton/wangziqi2013 namespace

#endif
