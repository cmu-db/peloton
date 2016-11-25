//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// indirection_array.h
//
// Identification: src/include/storage/indirection_array.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <array>

namespace peloton {
namespace storage {

const size_t INDIRECTION_ARRAY_MAX_SIZE = 1000;
const size_t INVALID_INDIRECTION_OFFSET = std::numeric_limits<size_t>::max();

class IndirectionArray {
 public:
  IndirectionArray(oid_t oid) : oid_(oid) {
    indirections_.reset(new indirection_array_t());
  }

  ~IndirectionArray() {}

  size_t AllocateIndirection() {
    if (indirection_counter_ >= INDIRECTION_ARRAY_MAX_SIZE) {
      return INVALID_INDIRECTION_OFFSET;
    }

    size_t indirection_id =
        indirection_counter_.fetch_add(1, std::memory_order_relaxed);

    if (indirection_id >= INDIRECTION_ARRAY_MAX_SIZE) {
      return INVALID_INDIRECTION_OFFSET;
    }
    return indirection_id;
  }

  ItemPointer *GetIndirectionByOffset(const size_t &offset) {
    return &(indirections_->at(offset));
  }

  inline oid_t GetOid() { return oid_; }

 private:
  typedef std::array<ItemPointer, INDIRECTION_ARRAY_MAX_SIZE>
      indirection_array_t;

  std::unique_ptr<indirection_array_t> indirections_;

  std::atomic<size_t> indirection_counter_ = ATOMIC_VAR_INIT(0);

  oid_t oid_;
};
}
}
