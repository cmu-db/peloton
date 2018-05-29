//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// hash_table_proxy.cpp
//
// Identification: src/codegen/proxy/hash_table_proxy.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/proxy/hash_table_proxy.h"

#include "codegen/proxy/executor_context_proxy.h"

namespace peloton {
namespace codegen {

// We need to manually define the type because it is recursive
llvm::Type *EntryProxy::GetType(CodeGen &codegen) {
  static const std::string kHashEntryTypeName = "peloton::Entry";

  // Check if the hash entry is already defined in the module
  auto *llvm_type = codegen.LookupType(kHashEntryTypeName);
  if (llvm_type != nullptr) {
    return llvm_type;
  }

  // Define the thing (the first field is the 64bit hash, the second is the
  // next HashEntry* pointer)
  auto *entry_type =
      llvm::StructType::create(codegen.GetContext(), kHashEntryTypeName);
  std::vector<llvm::Type *> elements = {
      codegen.Int64Type(),        // The hash value
      entry_type->getPointerTo()  // The next HashEntry* pointer
  };
  entry_type->setBody(elements, /*is_packed*/ false);
  return entry_type;
}
DEFINE_MEMBER(dummy, Entry, hash);
DEFINE_MEMBER(dummy, Entry, next);

DEFINE_TYPE(HashTable, "peloton::HashTable", memory, directory, size, mask,
            entry_buffer, num_elems, capacity, stats);

DEFINE_METHOD(peloton::codegen::util, HashTable, Init);
DEFINE_METHOD(peloton::codegen::util, HashTable, Insert);
DEFINE_METHOD(peloton::codegen::util, HashTable, InsertLazy);
DEFINE_METHOD(peloton::codegen::util, HashTable, BuildLazy);
DEFINE_METHOD(peloton::codegen::util, HashTable, ReserveLazy);
DEFINE_METHOD(peloton::codegen::util, HashTable, MergeLazyUnfinished);
DEFINE_METHOD(peloton::codegen::util, HashTable, Destroy);

}  // namespace codegen
}  // namespace peloton