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

#pragma once
#include <cstring>
#include <functional>

#define BLOOM_FILTER_ENABLED

/*
 * class BloomFilter
 */
template <typename ValueType,
          typename ValueEqualityChecker = std::equal_to<ValueType>,
          typename ValueHashFunc = std::hash<ValueType>>
class BloomFilter {
  // The size of the bit field
  static constexpr int ARRAY_SIZE = 256;

  // The number of individual arrays inside the filter
  static constexpr int FILTER_NUM = 8;

  static constexpr int RIGHT_SHIFT_BIT = 8;

  // The size of a single array
  static constexpr int FILTER_SIZE = ARRAY_SIZE / FILTER_NUM;

  // 0000 0000 0000 0000 0000 0000 0000 0111
  // This is the mask for extracting offset inside a byte
  static constexpr size_t BIT_OFFSET_MASK = 0x0000000000000007;

  // 0000 0000 0000 0000 0000 0000 1111 1000
  // This is the mask for extracting byte offset inside the array
  static constexpr size_t BYTE_OFFSET_MASK = 0x00000000000000F8;

 private:
  #ifdef BLOOM_FILTER_ENABLED
  unsigned char bit_array_0[FILTER_SIZE];
  #endif

  const ValueType **data_p;
  int value_count;

  // This is used to compare values
  ValueEqualityChecker value_eq_obj;

  // This is the object used to hash values into a
  // size_t
  ValueHashFunc value_hash_obj;

 public:
  /*
   * Default Constructor - deleted
   * Copy Constructor - deleted
   * Copy assignment - deleted
   */
  BloomFilter() = delete;
  BloomFilter(const BloomFilter &) = delete;
  BloomFilter &operator=(const BloomFilter &) = delete;

  /*
   * Constructor - Initialize value hash function
   *
   * This function zeros out memory by calling memset(). Holefully the compiler
   * would understand the meaning of this and optimize them into one function
   * call
   *
   * Seems that GCC unrolls it into movq instructions which is good
   */
  inline BloomFilter(const ValueType **p_data_p,
                     const ValueEqualityChecker &p_value_eq_obj = ValueEqualityChecker{},
                     const ValueHashFunc &p_value_hash_obj = ValueHashFunc{}) :
    data_p{p_data_p},
    value_count{0},
    value_eq_obj{p_value_eq_obj},
    value_hash_obj{p_value_hash_obj} {
    #ifdef BLOOM_FILTER_ENABLED
    memset(bit_array_0, 0, sizeof(bit_array_0));
    #endif

    return;
  }

  /*
   * GetSize() - Returns the number of elements stored inside the BloomFilter
   */
  inline int GetSize() const {
    return value_count;
  }

  /*
   * Begin() - Returns a pointer to its internal array
   */
  inline const ValueType **Begin() {
    return data_p;
  }

  /*
   * End() - Returns a pointer to the end of the array
   */
  inline const ValueType **End() {
    return data_p + value_count;
  }

  /*
   * __InsertScalar() - Insert into the bloom filter using scalar instructions
   *
   * We have unrolled the loop inside this function. This might not be a good
   * habit, but this part is highly sensitive to the actual generated code,
   * so we took extreme care on this
   *
   * NOTE: The value reference should be stable - it must have a valid memory
   * address, and the address must remain valid and unchanged during the
   * existence of the bloom filter. This is trivially true if the value comes
   * from any page inside the bwtree, which is protected by the epoch manager
   * but delicate situations might occur and we need to take care.
   */
  inline void __InsertScalar(const ValueType &value) {
    register size_t hash_value = value_hash_obj(value);

    // Copy the pointer into the array
    // We do not do any bounds checking here, and assume there is always
    // sufficient space holding all values
    data_p[value_count] = &value;
    value_count++;

    #ifdef BLOOM_FILTER_ENABLED
    bit_array_0[(hash_value & BYTE_OFFSET_MASK) >> 3] |= \
      (0x1 << (hash_value & BIT_OFFSET_MASK));
    #endif

    return;
  }

  inline void Insert(const ValueType &value) {
    __InsertScalar(value);

    return;
  }

  /*
   * __ExistsScalar() - Test whether a value exists with scalar instructions
   *
   * Note that this function might fail with a low probablity, but only false
   * positive is possible. There is never false negatives
   *
   * If exact answers are desired then on returning true, we should check
   * whether the value really exists or not by checking into a simple
   * data structure (e.g. simple linear array)
   */
  inline bool __ExistsScalar(const ValueType &value) {

    #ifdef BLOOM_FILTER_ENABLED
    register size_t hash_value = value_hash_obj(value);

    if((bit_array_0[(hash_value & BYTE_OFFSET_MASK) >> 3] & \
      (0x1 << (hash_value & BIT_OFFSET_MASK))) == 0x00) {
      return false;
    }
    #endif

    // We are still not sure whether the element exists or not
    // even if we reach here - though it should be a rare event
    // Maybe try searching in a probablistic data structure

    for(int i = 0;i < value_count;i++) {
      if(value_eq_obj(value, *(data_p[i])) == true) {
        return true;
      }
    }

    // Bloom filter says the value exists but actually it does not
    return false;
  }

  inline bool Exists(const ValueType &value) {
    return __ExistsScalar(value);
  }
};
