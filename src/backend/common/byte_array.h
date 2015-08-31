//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// byte_array.h
//
// Identification: src/backend/common/byte_array.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstring>
#include <boost/shared_array.hpp>

namespace peloton {

//===--------------------------------------------------------------------===//
// A safe and handy char* container
//===--------------------------------------------------------------------===//

/**
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
 * This class has no method that implicitly accepts std::string which can
 * be automatically constructed from NULL-terminated const char*. Be careless!
 *
 * 2. ByteArray has explicit "Length" property.
 * This is what boost::shared_array<char> can't provide.
 *
 * 3. Passing ByteArray (not ByteArray* nor ByteArray&) has almost no cost.
 * This is what boost::array<char, N> and std::vector<char> can't provide,
 * which copies elements everytime.
 * Copy constructor of this class just copies an internal smart pointer.
 * You can pass around instances of this class just like smart pointer/iterator.
 *
 * 4. No memory leaks.
 *
 * 5. All methods are exception-safe. Nothing dangerouns happens even
 * if Outofmemory happens.
 */

template <typename T>
class GenericArray {
 public:
  /// corresponds to "byte[] bar = null;" in Java
  GenericArray() { Reset(); };

  /// corresponds to "byte[] bar = new byte[len];" in Java
  /// explicit because ByteArray bar = 10; sounds really weird in the semantics.
  explicit GenericArray(int Length) { ResetAndExpand(Length); };

  /// corresponds to "byte[] bar = new byte[] {1,2,...,10};" in Java
  /// this constructor is safe because it explicitly receives Length.
  GenericArray(const T *Data, int Length) {
    ResetAndExpand(Length);
    Assign(Data, 0, Length);
  };

  /// IMPORTANT : NEVER make a constructor that accepts std::string! It
  /// demolishes all the significance of this class.

  /// corresponds to "byte[] bar = bar2;" in Java. Note that this has no cost.
  GenericArray(const GenericArray<T> &rhs) {
    array_data = rhs.array_data;
    array_length = rhs.array_length;
  };
  inline GenericArray<T> &operator=(const GenericArray<T> &rhs) {
    array_data = rhs.array_data;
    array_length = rhs.array_length;
    return *this;
  }

  ~GenericArray(){};

  /// corresponds to "(bar == null)" in Java
  bool IsNull() const { return array_data == NULL; };

  /// corresponds to "bar = null;" in Java
  void Reset() {
    array_data.Reset();
    array_length = -1;
  };

  /// corresponds to "bar = new byte[len];" in Java
  void ResetAndExpand(int newLength) {
    assert(newLength >= 0);
    array_data = boost::shared_array<T>(new T[newLength]);
    ::memset(array_data.get(), 0, newLength * sizeof(T));
    array_length = newLength;
  };

  /// corresponds to "tmp = new byte[newlen]; System.arraycopy(bar to tmp); bar
  /// = tmp;" in Java
  void CopyAndExpand(int newLength) {
    assert(newLength >= 0);
    assert(newLength > array_length);
    boost::shared_array<T> newData(new T[newLength]);
    ::memset(newData.get(), 0,
             newLength * sizeof(T));  /// makes valgrind happy.
    ::memcpy(newData.get(), array_data.get(), array_length * sizeof(T));
    array_data = newData;
    array_length = newLength;
  };

  /// corresponds to "(bar.Length)" in Java
  int Length() const { return array_length; };
  const T *Data() const { return array_data.get(); };
  T *Data() { return array_data.get(); };

  /// helper functions for convenience.
  void Assign(const T *assignedData, int offset, int assignedLength) {
    assert(!IsNull());
    assert(array_length >= offset + assignedLength);
    assert(offset >= 0);
    ::memcpy(array_data.get() + offset, assignedData,
             assignedLength * sizeof(T));
  };

  GenericArray<T> operator+(const GenericArray<T> &tail) const {
    assert(!IsNull());
    assert(!tail.IsNull());
    GenericArray<T> concated(this->array_length + tail.array_length);
    concated.Assign(this->array_data.get(), 0, this->array_length);
    concated.Assign(tail.array_data.get(), this->array_length,
                    tail.array_length);
    return concated;
  };

  const T &operator[](int index) const {
    assert(!IsNull());
    assert(array_length > index);
    return array_data.get()[index];
  };

  T &operator[](int index) {
    assert(!IsNull());
    assert(array_length > index);
    return array_data.get()[index];
  };

 private:
  boost::shared_array<T> array_data;
  int array_length;
};

typedef GenericArray<char> ByteArray;

}  /// End peloton namespace
