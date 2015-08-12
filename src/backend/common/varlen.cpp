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

Varlen *Varlen::Create(size_t size, Pool *dataPool) {
  Varlen *retval;

  if (dataPool != NULL) {
    retval = new (dataPool->Allocate(sizeof(Varlen))) Varlen(size, dataPool);
  } else {
    retval = new Varlen(size);
  }

  return retval;
}

void Varlen::Destroy(Varlen *varlen) { delete varlen; }

Varlen *Varlen::Clone(const Varlen &src, Pool *dataPool) {
  // Create a new instance, back pointer is set inside
  Varlen *rv = Create(src.varlen_size - sizeof(Varlen *), dataPool);

  // copy the meat (excluding back pointer)
  ::memcpy(rv->Get(), src.Get(), (rv->varlen_size - sizeof(Varlen *)));

  return rv;
}

Varlen::Varlen(size_t size) {
  varlen_size = size + sizeof(Varlen *);
  varlen_temp_pool = false;
  varlen_string_ptr = new char[varlen_size];
  SetBackPtr();
}

Varlen::Varlen(std::size_t size, Pool *dataPool) {
  varlen_size = size + sizeof(Varlen *);
  varlen_temp_pool = true;
  varlen_string_ptr =
      reinterpret_cast<char *>(dataPool->Allocate(varlen_size));
  SetBackPtr();
}

Varlen::~Varlen() {
  if (!varlen_temp_pool) {
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
