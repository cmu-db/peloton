/***************************************************************************
*   Copyright (C) 2008 by H-Store Project                                 *
*   Brown University                                                      *
*   Massachusetts Institute of Technology                                 *
*   Yale University                                                       *
*                                                                         *
* This software may be modified and distributed under the terms           *
* of the MIT license.  See the LICENSE file for details.                  *
*                                                                         *
***************************************************************************/
#ifndef HSTOREBYTEARRAY_H
#define HSTOREBYTEARRAY_H

#include <cstring>
#include <memory>
#include <assert.h>

#include "common/macros.h"

namespace peloton {
/**
* A safe and handy char* container.
* std::string is a good container, but we have to be very careful
* for a binary string that might include '\0' in arbitrary position
* because of its implicit construction from const char*.
* std::vector<char> or boost::array<char, N> works, but we can't pass
* the objects without copying the elements, which has significant overhead.
*
* I made this class to provide same semantics as Java's "byte[]"
* so that this class guarantees following properties.
*
* 1. ByteArray is always safe against '\0'.
*  This class has no method that implicitly accepts std::string which can
*  be automatically constructed from NULL-terminated const char*. Be careless!
* 2. ByteArray has explicit "length" property.
*  This is what boost::shared_array<char> can't provide.
* 3. Passing ByteArray (not ByteArray* nor ByteArray&) has almost no cost.
*  This is what boost::array<char, N> and std::vector<char> can't provide,
*  which copies elements everytime.
*  Copy constructor of this class just copies an internal smart pointer.
*  You can pass around instances of this class just like smart pointer/iterator.
* 4. No memory leaks.
* 5. All methods are exception-safe. Nothing dangerouns happens even if Outofmemory happens.
* @author Hideaki Kimura <hkimura@cs.brown.edu>
*/
template <typename T>
class GenericArray {
 public:
  // corresponds to "byte[] bar = null;" in Java
  GenericArray() { reset(); };

  // corresponds to "byte[] bar = new byte[len];" in Java
  // explicit because ByteArray bar = 10; sounds really weird in the semantics.
  explicit GenericArray(int length) { resetAndExpand(length); };

  // corresponds to "byte[] bar = new byte[] {1,2,...,10};" in Java
  // this constructor is safe because it explicitly receives length.
  GenericArray(const T* data, int length) {
    resetAndExpand(length);
    assign(data, 0, length);
  };
  
  // IMPORTANT : NEVER make a constructor that accepts std::string! It
  // demolishes all the significance of this class.

  // corresponds to "byte[] bar = bar2;" in Java. Note that this has no cost.
  GenericArray(const GenericArray<T> &rhs) {
    data_ = rhs.data_;
    length_ = rhs.length_;
  };
  
  inline GenericArray<T>& operator=(const GenericArray<T> &rhs) {
    data_ = rhs.data_;
    length_ = rhs.length_;
    return *this;
  }

  ~GenericArray() {};

  // corresponds to "(bar == null)" in Java
  bool isNull() const { return data_ == NULL; };

  // corresponds to "bar = null;" in Java
  void reset() {
    data_.reset();
    length_ = -1;
  };

  // corresponds to "bar = new byte[len];" in Java
  void resetAndExpand(int newLength) {
    PELOTON_ASSERT(newLength >= 0);
    //data_ = boost::shared_array<T>(new T[newLength]);
    data_ = std::shared_ptr<T>(new T[newLength], std::default_delete<T[]>{});
    PELOTON_MEMSET(data_.get(), 0, newLength * sizeof(T));
    length_ = newLength;
  };

  // corresponds to "tmp = new byte[newlen]; System.arraycopy(bar to tmp); bar = tmp;" in Java
  void copyAndExpand(int newLength) {
    PELOTON_ASSERT(newLength >= 0);
    PELOTON_ASSERT(newLength > length_);
    //boost::shared_array<T> newData(new T[newLength]);
    std::shared_ptr<T> newData(new T[newLength], std::default_delete<T[]>{});
    PELOTON_MEMSET(newData.get(), 0, newLength * sizeof(T)); // makes valgrind happy.
    PELOTON_MEMCPY(newData.get(), data_.get(), length_ * sizeof(T));
    data_ = newData;
    length_ = newLength;
  };

  // corresponds to "(bar.length)" in Java
  int length() const { return length_; };
  const T* data() const { return data_.get(); };
  T* data() { return data_.get(); };

  // helper functions for convenience.
  void assign(const T* assignedData, int offset, int assignedLength) {
    PELOTON_ASSERT(!isNull());
    PELOTON_ASSERT(length_ >= offset + assignedLength);
    PELOTON_ASSERT(offset >= 0);
    PELOTON_MEMCPY(data_.get() + offset, assignedData, assignedLength * sizeof(T));
  };

  GenericArray<T> operator+(const GenericArray<T> &tail) const {
    PELOTON_ASSERT(!isNull());
    PELOTON_ASSERT(!tail.isNull());
    GenericArray<T> concated(this->length_ + tail.length_);
    concated.assign(this->data_.get(), 0, this->length_);
    concated.assign(tail.data_.get(), this->length_, tail.length_);
    return concated;
  };

  const T& operator[](int index) const {
    PELOTON_ASSERT(!isNull());
    PELOTON_ASSERT(length_ > index);
    return data_.get()[index];
  };

  T& operator[](int index) {
    PELOTON_ASSERT(!isNull());
    PELOTON_ASSERT(length_ > index);
    return data_.get()[index];
  };

 private:
  //boost::shared_array<T> data_;
  std::shared_ptr<T> data_;
  int length_;
};

typedef GenericArray<char> ByteArray;

}
#endif
