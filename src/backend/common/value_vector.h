//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// value_vector.h
//
// Identification: src/backend/common/value_vector.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <vector>
#include <cstring>
#include <array>
#include <iostream>
#include <sstream>

#include "backend/common/types.h"
#include "backend/common/value.h"

#include "boost/unordered_map.hpp"
#include "boost/array.hpp"

namespace peloton {

//===--------------------------------------------------------------------===//
// Value Vector
//===--------------------------------------------------------------------===//
/**
 * Fixed size Array of Values. Less flexible but faster than std::vector of Value.
 * The constructor initializes all data with zeros, resulting in type==INVALID.
 * Destructors of the Value objects inside of this class will not be called.
 * Just the data area is revoked, which
 * means Value should not rely on destructor.
 */
template <typename V> class GenericValueArray {
public:
    inline GenericValueArray() : size_(0),
        data_(reinterpret_cast<V*>(new char[sizeof(V) * size_])) {
        ::memset(data_, 0, sizeof(V) * size_);
    }
    inline GenericValueArray(int size) : size_(size),
        data_(reinterpret_cast<V*>(new char[sizeof(V) * size_])) {
        ::memset(data_, 0, sizeof(V) * size_);
    }
    inline GenericValueArray(const GenericValueArray &rhs) : size_(rhs.size_),
        data_(reinterpret_cast<V*>(new char[sizeof(V) * size_])) {
        //::memset(data_, 0, sizeof(V) * size_);
        /*for (int i = 0; i < size_; ++i) {
            data_[i] = rhs.data_[i];
        }*/
        ::memcpy(data_, rhs.data_, sizeof(V) * size_);
    }
    inline ~GenericValueArray() {
        delete[] reinterpret_cast<char*>(data_);
    }

    inline void reset(int size) {
        delete[] reinterpret_cast<char*>(data_);
        size_ = size;
        data_ = reinterpret_cast<V*>(new char[sizeof(V) * size_]);
        ::memset(data_, 0, sizeof(V) * size_);
    }

    GenericValueArray<V>& operator=(const GenericValueArray<V> &rhs);

    inline const V* getRawPointer() const { return data_; }
    inline V& operator[](int index) {
        assert (index >= 0);
        assert (index < size_);
        return data_[index];
    }
    inline const V& operator[](int index) const {
        assert (index >= 0);
        assert (index < size_);
        return data_[index];
    }
    inline int GetSize() const { return size_;}
    inline int CompareValue (const GenericValueArray &rhs) const {
        assert (size_ == rhs.size_);
        for (int i = 0; i < size_; ++i) {
            int ret = data_[i].Compare(rhs.data_[i]);
            if (ret != 0) return ret;
        }
        return 0;
    }

    std::string Debug() const;
    std::string Debug(int columnCount) const;

private:
    int size_;
    V* data_;
};

template <typename V> inline bool operator == (const GenericValueArray<V> &lhs, const GenericValueArray<V> &rhs) {
    return lhs.CompareValue(rhs) == 0;
}
template <typename V> inline bool operator != (const GenericValueArray<V> &lhs, const GenericValueArray<V> &rhs) {
    return lhs.CompareValue(rhs) != 0;
}
template <typename V> inline bool operator < (const GenericValueArray<V> &lhs, const GenericValueArray<V> &rhs) {
    return lhs.CompareValue(rhs) < 0;
}
template <typename V> inline bool operator > (const GenericValueArray<V> &lhs, const GenericValueArray<V> &rhs) {
    return lhs.CompareValue(rhs) > 0;
}
template <typename V> inline bool operator <= (const GenericValueArray<V> &lhs, const GenericValueArray<V> &rhs) {
    return !(lhs > rhs);
}
template <typename V> inline bool operator >= (const GenericValueArray<V> &lhs, const GenericValueArray<V> &rhs) {
    return !(lhs < rhs);
}
template<> inline GenericValueArray<Value>::~GenericValueArray() {
    delete[] reinterpret_cast<char*>(data_);
}

template<> inline GenericValueArray<Value>&
GenericValueArray<Value>::operator=(const GenericValueArray<Value> &rhs) {
    delete[] data_;
    size_ = rhs.size_;
    data_ = reinterpret_cast<Value*>(new char[sizeof(Value) * size_]);
    ::memcpy(data_, rhs.data_, sizeof(Value) * size_);
    return *this;
}

template<> inline std::string GenericValueArray<Value>::Debug() const {
    std::string out("[ ");
    for (int i = 0; i < size_; i++) {
        out += data_[i].Debug() + " "; // how to do with this...
    }
    return out + "]";
}

template<> inline std::string GenericValueArray<Value>::Debug(int columnCount) const {
    std::string out("[ ");
    for (int i = 0; i < columnCount; i++) {
        out += data_[i].Debug() + " "; // how to do with this...
    }
    return out + "]";
}

typedef GenericValueArray<Value> ValueArray;

/** comparator for ValueArray. */
class ValueArrayComparator {
  public:

    /** copy constructor is needed as std::map takes instance of comparator, not a pointer.*/
    ValueArrayComparator(const ValueArrayComparator &rhs)
    : colCount_(rhs.colCount_), column_types_(new ValueType[colCount_]) {
        ::memcpy(column_types_, rhs.column_types_, sizeof(ValueType) * colCount_);
    }

    ValueArrayComparator(const std::vector<ValueType> &column_types)
    : colCount_(column_types.size()), column_types_(new ValueType[colCount_]) {
        for (int i = (int)colCount_ - 1; i >= 0; --i) {
            column_types_[i] = column_types[i];
        }
    }

    ValueArrayComparator(int colCount, const ValueType* column_types)
    : colCount_(colCount), column_types_(new ValueType[colCount_]) {
        ::memcpy(column_types_, column_types, sizeof(ValueType) * colCount_);
    }

    ~ValueArrayComparator() {
        delete[] column_types_;
    }

    inline bool operator()(const ValueArray& lhs, const ValueArray& rhs) const {
        assert (lhs.GetSize() == rhs.GetSize());
        return lhs.CompareValue(rhs) < 0;
    }

  private:
    size_t colCount_;
    ValueType* column_types_;
};

/** comparator for ValueArray. */
template <std::size_t N>
class ValueArrayComparator2 {
  public:
    /** copy constructor is needed as std::map takes instance of comparator, not a pointer.*/
    ValueArrayComparator2(const ValueArrayComparator2 &rhs)
    : colCount_(rhs.colCount_), column_types_(new ValueType[colCount_]) {
        ::memcpy(column_types_, rhs.column_types_, sizeof(ValueType) * colCount_);
    }

    ValueArrayComparator2(const std::vector<ValueType> &column_types)
    : colCount_(column_types.size()), column_types_(new ValueType[colCount_]) {
        for (int i = (int)colCount_ - 1; i >= 0; --i) {
            column_types_[i] = column_types[i];
        }
    }

    ValueArrayComparator2(int colCount, const ValueType* column_types)
    : colCount_(colCount), column_types_(new ValueType[colCount_]) {
        ::memcpy(column_types_, column_types, sizeof(ValueType) * colCount_);
    }

    ~ValueArrayComparator2() {
        delete[] column_types_;
    }

    inline bool operator()(const boost::array<Value, N> &lhs,
                           const boost::array<Value, N> &rhs) const
    {
        int cmp = 0;
        for (int i = 0; i < N; ++i) {
            cmp = lhs[i].CompareValue(rhs[i], column_types_[i]);
            if (cmp != 0) return (cmp < 0);
        }
        return (cmp < 0);
    }

  private:
    size_t colCount_;
    ValueType* column_types_;
};

/** comparator for ValueArray. */
class ValueArrayEqualityTester {
public:
    /** copy constructor is needed as std::map takes instance of comparator, not a pointer.*/
    ValueArrayEqualityTester(const ValueArrayEqualityTester &rhs)
    : colCount_(rhs.colCount_), column_types_(new ValueType[colCount_]) {
        ::memcpy(column_types_, rhs.column_types_, sizeof(ValueType) * colCount_);
    }
    ValueArrayEqualityTester(const std::vector<ValueType> &column_types)
    : colCount_(column_types.size()), column_types_(new ValueType[colCount_]) {
        for (int i = (int)colCount_ - 1; i >= 0; --i) {
            column_types_[i] = column_types[i];
        }
    }
    ValueArrayEqualityTester(int colCount, const ValueType* column_types)
    : colCount_(colCount), column_types_(new ValueType[colCount_]) {
        ::memcpy(column_types_, column_types, sizeof(ValueType) * colCount_);
    }
    ~ValueArrayEqualityTester() {
        delete[] column_types_;
    }

    inline bool operator()(const ValueArray& lhs, const ValueArray& rhs) const {
        assert (lhs.GetSize() == rhs.GetSize());
        assert (lhs.GetSize() == static_cast<int>(colCount_));
        return lhs.CompareValue(rhs) == 0;
    }
private:
    size_t colCount_;
    ValueType* column_types_;
};

}  // End peloton namespace
