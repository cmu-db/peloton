
#include <cassert>
#include <algorithm>

/*
 * class SortedSmallSet - An implementation of small sorted set with known
 *                        element count upper bound
 *
 * This functions uses insertion sort in anticipation that the number
 * of elements would be small. Under such condition, array based
 * implementation exploits locality and is therefore is cache friendly
 *
 * If it is not the case then we should use a more lever container
 *
 * NOTE: The data array must be large enough to hold the values. There
 * is no bounds checking and/or safeguard
 */
template<typename ValueType,
         typename ValueComparator = std::less<ValueType>,
         typename ValueEqualityChecker = std::equal_to<ValueType>>
class SortedSmallSet {
 private:
  ValueType *data;

  // These two are only valid if the item count is not 0
  ValueType *start_p;
  ValueType *end_p;

  ValueComparator value_cmp_obj;
  ValueEqualityChecker value_eq_obj;

 public:
  SortedSmallSet(ValueType *p_data,
                 const ValueComparator &p_value_cmp_obj = ValueComparator{},
                 const ValueEqualityChecker &p_value_eq_obj = ValueEqualityChecker{}) :
    data{p_data},
    start_p{p_data},
    end_p{p_data},
    value_cmp_obj{p_value_cmp_obj},
    value_eq_obj{p_value_eq_obj}
  {}

  /*
   * Insert() - Inserts a value into the set
   *
   * If the value already exists do nothing. Otherwise
   * it will be inserted before the first element that is larger
   */
  inline void Insert(const ValueType &value) {
    // Could not use upper bound here since if there are duplications
    // we want to catch the duplication by lower bound
    // But if we are OK with duplications then use upper bound here
    //auto it = std::lower_bound(start_p, end_p, value, value_cmp_obj);
    
    // Since the assumption is a small array
    // we choose to use linear search to increase performance
    for(auto it = start_p;it < end_p;it++) {
      // If it is the first element that is >= the search value
      if(value_cmp_obj(*it, value) == false) {
        // If we have seen a duplication then just return
        // since duplications are not allowed
        // Otherwise just break out of the loop
        // and continue with inserting & shifting
        if(value_eq_obj(*it, value) == false) {
          // It is like backward shift operation
          std::copy_backward(it, end_p, end_p + 1);
          *it = value;

          end_p++;
        }
        
        return;
      }
    }
    
    *end_p++ = value;

    return;
  }
  
  /*
   * InsertNoDedup() - Insert a value without removing duplications
   *
   * This function tries to find the upper bound of the inserted value,
   * and shifts them backward by 1 to make space for the current inserted
   * item
   */
  inline void InsertNoDedup(const ValueType &value) {
    auto it = std::upper_bound(start_p, end_p, value, value_cmp_obj);

    // We do not need fast path here since even if it == end_p then
    // copy_backward will still take care of this

    // It is like backward shift operation
    std::copy_backward(it, end_p, end_p + 1);
    *it = value;

    end_p++;

    return;
  }

  /*
   * GetBegin() - Returns the begin pointer to the current begin
   *
   * Note that the begin pointer could advance
   */
  inline ValueType *GetBegin() {
    return start_p;
  }

  inline ValueType *GetEnd() {
    return end_p;
  }

  /*
   * IsEmpty() - Returns true if the container is empty
   *
   * Empty is defined as start pointer being equal to the end pointer
   */
  inline bool IsEmpty() {
    return start_p == end_p;
  }

  /*
   * PopFront() - Removes a value from the head and shrinks
   *              the size of the container by 1
   */
  inline ValueType &PopFront() {
    assert(start_p < end_p);;

    return *(start_p++);
  }

  /*
   * GetFront() - Return a reference of the first element without
   *              moving the start pointer
   */
  inline ValueType &GetFront() {
    return *start_p;
  }

  /*
   * GetSize() - Returns the size of the container, in element count
   */
  inline std::ptrdiff_t GetSize() const {
    return end_p - start_p;
  }
  
  /*
   * Invaidate() - Clears all contents between start_p and end_p
   *
   * This is called if we want to set the size to 0 to avoid
   * fetching any element from the sss
   */
  inline void Invalidate() {
    end_p = start_p;
  }
};
