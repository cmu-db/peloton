//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// varlen.cpp
//
// Identification: src/backend/common/varlen.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/common/varlen.h"
#include "backend/common/pool.h"

namespace peloton {

Varlen *Varlen::Create(size_t size, VarlenPool *data_pool) {
  Varlen *retval;

  if (data_pool != NULL) {
    retval = new (data_pool->Allocate(sizeof(Varlen))) Varlen(size, data_pool);
  } else {
    retval = new Varlen(size);
  }

  return retval;
}

Varlen *Varlen::Clone(const Varlen &src, VarlenPool *data_pool) {
  // Create a new instance, back pointer is set inside
  Varlen *rv = Create(src.varlen_size - sizeof(Varlen *), data_pool);

  // copy the meat (excluding back pointer)
  ::memcpy(rv->Get(), src.Get(), (rv->varlen_size - sizeof(Varlen *)));

  return rv;
}

// Construct varlen in heap
Varlen::Varlen(size_t size) {
  varlen_size = size + sizeof(Varlen *);
  varlen_temp_pool = true;
  varlen_string_ptr = new char[varlen_size];
  SetBackPtr();
}

// Construct varlen in given data pool
Varlen::Varlen(std::size_t size, VarlenPool *data_pool) {
  varlen_size = size + sizeof(Varlen *);
  varlen_temp_pool = false;
  varlen_string_ptr =
      reinterpret_cast<char *>(data_pool->Allocate(varlen_size));
  SetBackPtr();
}

Varlen::~Varlen() {

  if (varlen_temp_pool == true) {
    delete[] varlen_string_ptr;
  }

}

char *Varlen::Get() { return varlen_string_ptr + sizeof(Varlen *); }

const char *Varlen::Get() const { return varlen_string_ptr + sizeof(Varlen *); }

void Varlen::UpdateStringLocation(void *location) {
  varlen_string_ptr = reinterpret_cast<char *>(location);
}

void Varlen::SetBackPtr() {
  Varlen **backptr = reinterpret_cast<Varlen **>(varlen_string_ptr);
  *backptr = this;
}

}  // End peloton namespace
