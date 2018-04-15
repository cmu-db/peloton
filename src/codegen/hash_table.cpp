//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// hash_table.cpp
//
// Identification: src/codegen/hash_table.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/hash_table.h"

#include "codegen/proxy/hash_table_proxy.h"
#include "codegen/hash.h"
#include "codegen/lang/if.h"
#include "codegen/lang/loop.h"
#include "common/exception.h"

namespace peloton {
namespace codegen {

HashTable::HashTable() {
  // This constructor shouldn't generally be used at all, but there are
  // cases when the key-type is not known at construction time.
}

HashTable::HashTable(CodeGen &codegen, const std::vector<type::Type> &key_type,
                     uint32_t value_size)
    : value_size_(value_size) {
  key_storage_.Setup(codegen, key_type);
}

void HashTable::Init(UNUSED_ATTRIBUTE CodeGen &codegen,
                     UNUSED_ATTRIBUTE llvm::Value *ht_ptr) const {
  throw NotImplementedException{
      "Init with no ExecutorContext not supported in HashTable"};
}

void HashTable::Init(CodeGen &codegen, llvm::Value *exec_ctx,
                     llvm::Value *ht_ptr) const {
  auto *key_size = codegen.Const32(key_storage_.MaxStorageSize());
  auto *value_size = codegen.Const32(value_size_);
  codegen.Call(HashTableProxy::Init, {ht_ptr, exec_ctx, key_size, value_size});
}

void HashTable::ProbeOrInsert(CodeGen &codegen, llvm::Value *ht_ptr,
                              llvm::Value *hash,
                              const std::vector<codegen::Value> &key,
                              ProbeCallback &probe_callback,
                              InsertCallback &insert_callback) const {
  llvm::BasicBlock *cont_bb =
      llvm::BasicBlock::Create(codegen.GetContext(), "cont");

  // Compute hash value
  llvm::Value *hash_val =
      hash != nullptr ? hash : Hash::HashValues(codegen, key);

  // Compute bucket position
  llvm::Value *mask = codegen.Load(HashTableProxy::mask, ht_ptr);
  llvm::Value *bucket_idx = codegen->CreateAnd(hash_val, mask);
  llvm::Value *directory = codegen.Load(HashTableProxy::directory, ht_ptr);
  llvm::Value *bucket =
      codegen->CreateLoad(codegen->CreateInBoundsGEP(directory, {bucket_idx}));

  // Iterate the bucket chain until we get a NULL entry
  llvm::Value *null =
      codegen.NullPtr(llvm::cast<llvm::PointerType>(bucket->getType()));
  llvm::Value *end_condition = codegen->CreateICmpNE(bucket, null);
  lang::Loop chain_loop{codegen, end_condition, {{"iter", bucket}}};
  {
    // The current entry
    llvm::Type *ht_entry_type = EntryProxy::GetType(codegen);
    llvm::Value *entry = chain_loop.GetLoopVar(0);

    // Does the hash of the current entry match?
    llvm::Value *entry_hash = codegen.Load(EntryProxy::hash, entry);
    lang::If hash_match{codegen, codegen->CreateICmpEQ(entry_hash, hash_val),
                        "hashMatch"};
    {
      // The hashes match, what about the keys?
      llvm::Value *keys_ptr =
          codegen->CreateConstInBoundsGEP2_32(ht_entry_type, entry, 1, 0);

      // Pull out the keys
      std::vector<codegen::Value> hash_entry_keys;
      llvm::Value *values_area =
          key_storage_.LoadValues(codegen, keys_ptr, hash_entry_keys);

      // Check keys for equality
      auto keys_are_equal = Value::TestEquality(codegen, key, hash_entry_keys);
      lang::If key_match{codegen, keys_are_equal.GetValue(), "keyMatch"};
      {
        // We found a duplicate key, issue the probe callback
        probe_callback.ProcessEntry(codegen, values_area);
        key_match.EndIf(cont_bb);
      }
      hash_match.EndIf();
    }

    // No match found, move along
    entry = codegen.Load(EntryProxy::next, entry);
    chain_loop.LoopEnd(codegen->CreateICmpNE(entry, null), {entry});
  }

  // No entry found, insert a new one
  llvm::Value *value_size = insert_callback.GetValueSize(codegen);
  llvm::Value *keys_size = codegen.Const32(key_storage_.MaxStorageSize());
  llvm::Value *needed_bytes = codegen->CreateAdd(keys_size, value_size);
  llvm::Value *ptr =
      codegen.Call(HashTableProxy::Insert, {ht_ptr, hash_val, needed_bytes});

  // (5)
  llvm::Value *data_space_ptr = key_storage_.StoreValues(codegen, ptr, key);
  insert_callback.StoreValue(codegen, data_space_ptr);

  // Ending block
  codegen->CreateBr(cont_bb);
  codegen->GetInsertBlock()->getParent()->getBasicBlockList().push_back(
      cont_bb);
  codegen->SetInsertPoint(cont_bb);
}

HashTable::ProbeResult HashTable::ProbeOrInsert(
    UNUSED_ATTRIBUTE CodeGen &codegen, UNUSED_ATTRIBUTE llvm::Value *ht_ptr,
    UNUSED_ATTRIBUTE llvm::Value *hash,
    UNUSED_ATTRIBUTE const std::vector<codegen::Value> &key) const {
  throw NotImplementedException{
      "ProbeOrInsert returning probe result not support in HashTable"};
}

void HashTable::Insert(CodeGen &codegen, llvm::Value *ht_ptr, llvm::Value *hash,
                       const std::vector<codegen::Value> &key,
                       InsertCallback &insert_callback) const {
  // Calculate the hash
  llvm::Value *hash_val =
      hash != nullptr ? hash : Hash::HashValues(codegen, key);

  // Get the total size we need
  //  llvm::Value *value_size = insert_callback.GetValueSize(codegen);
  //  llvm::Value *keys_size = codegen.Const32(key_storage_.MaxStorageSize());
  //  llvm::Value *needed_bytes = codegen->CreateAdd(keys_size, value_size);

  // Invoke HashTable::InsertLazy(...)
  llvm::Value *ptr =
      codegen.Call(HashTableProxy::InsertLazy, {ht_ptr, hash_val});

  // Invoke the callback to let her store the payload
  llvm::Value *data_space_ptr = key_storage_.StoreValues(codegen, ptr, key);
  insert_callback.StoreValue(codegen, data_space_ptr);
}

void HashTable::InsertLazy(CodeGen &codegen, llvm::Value *ht_ptr,
                           llvm::Value *hash,
                           const std::vector<codegen::Value> &keys,
                           HashTable::InsertCallback &callback) const {
  Insert(codegen, ht_ptr, hash, keys, callback);
}

void HashTable::BuildLazy(CodeGen &codegen, llvm::Value *ht_ptr) const {
  codegen.Call(HashTableProxy::BuildLazy, {ht_ptr});
}

void HashTable::ReserveLazy(CodeGen &codegen, llvm::Value *ht_ptr,
                            llvm::Value *thread_states) const {
  codegen.Call(HashTableProxy::BuildLazy, {ht_ptr, thread_states});
}

void HashTable::MergeLazyUnfinished(CodeGen &codegen, llvm::Value *global_ht,
                                    llvm::Value *local_ht) const {
  codegen.Call(HashTableProxy::MergeLazyUnfinished, {global_ht, local_ht});
}

void HashTable::Iterate(CodeGen &codegen, llvm::Value *ht_ptr,
                        IterateCallback &callback) const {
  llvm::Value *buckets_ptr = codegen.Load(HashTableProxy::directory, ht_ptr);
  llvm::Value *num_buckets = codegen.Load(HashTableProxy::size, ht_ptr);
  llvm::Value *bucket_num = codegen.Const64(0);
  llvm::Value *bucket_cond = codegen->CreateICmpULT(bucket_num, num_buckets);

  lang::Loop bucket_loop{codegen, bucket_cond, {{"bucketNum", bucket_num}}};
  {
    bucket_num = bucket_loop.GetLoopVar(0);
    llvm::Value *bucket =
        codegen->CreateLoad(codegen->CreateGEP(buckets_ptr, bucket_num));
    llvm::Value *null_bucket =
        codegen.NullPtr(llvm::cast<llvm::PointerType>(bucket->getType()));

    lang::Loop chain_loop{codegen,
                          codegen->CreateICmpNE(bucket, null_bucket),
                          {{"entry", bucket}}};
    {
      llvm::Type *ht_entry_type = EntryProxy::GetType(codegen);
      llvm::Value *entry = chain_loop.GetLoopVar(0);
      llvm::Value *entry_data =
          codegen->CreateConstInBoundsGEP2_32(ht_entry_type, entry, 1, 0);

      // Pull out keys and invoke callback
      std::vector<codegen::Value> keys;
      auto *data_area_ptr = key_storage_.LoadValues(codegen, entry_data, keys);
      callback.ProcessEntry(codegen, keys, data_area_ptr);

      entry = codegen.Load(EntryProxy::next, entry);
      chain_loop.LoopEnd(codegen->CreateICmpNE(entry, null_bucket), {entry});
    }
    // Move to next bucket
    bucket_num = codegen->CreateAdd(bucket_num, codegen.Const64(1));
    bucket_loop.LoopEnd(codegen->CreateICmpULT(bucket_num, num_buckets),
                        {bucket_num});
  }
}

void HashTable::FindAll(CodeGen &codegen, llvm::Value *ht_ptr,
                        const std::vector<codegen::Value> &key,
                        IterateCallback &callback) const {
  llvm::Value *hash = Hash::HashValues(codegen, key);

  llvm::Value *mask = codegen.Load(HashTableProxy::mask, ht_ptr);
  llvm::Value *bucket_idx = codegen->CreateAnd(hash, mask);
  llvm::Value *directory = codegen.Load(HashTableProxy::directory, ht_ptr);
  llvm::Value *bucket =
      codegen->CreateLoad(codegen->CreateInBoundsGEP(directory, {bucket_idx}));

  llvm::Type *entry_type = EntryProxy::GetType(codegen);
  llvm::Value *null = codegen.NullPtr(entry_type->getPointerTo());

  // Loop chain
  llvm::Value *end_condition = codegen->CreateICmpNE(bucket, null);
  lang::Loop chain_loop{codegen, end_condition, {{"iter", bucket}}};
  {
    llvm::Value *entry = chain_loop.GetLoopVar(0);

    llvm::Value *entry_hash = codegen.Load(EntryProxy::hash, entry);
    lang::If hash_match{codegen, codegen->CreateICmpEQ(entry_hash, hash),
                        "hashMatch"};
    {
      llvm::Value *iter_keys =
          codegen->CreateConstInBoundsGEP1_32(entry_type, entry, 1);
      std::vector<codegen::Value> entry_keys;
      llvm::Value *data_area =
          key_storage_.LoadValues(codegen, iter_keys, entry_keys);

      auto keys_are_equal = Value::TestEquality(codegen, key, entry_keys);
      lang::If key_match{codegen, keys_are_equal.GetValue(), "keyMatch"};
      {
        callback.ProcessEntry(codegen, key, data_area);
        key_match.EndIf();
      }
      hash_match.EndIf();
    }
    entry = codegen.Load(EntryProxy::next, entry);
    chain_loop.LoopEnd(codegen->CreateICmpNE(entry, null), {entry});
  }
}

void HashTable::Destroy(CodeGen &codegen, llvm::Value *ht_ptr) const {
  codegen.Call(HashTableProxy::Destroy, {ht_ptr});
}

void HashTable::VectorizedIterate(
    UNUSED_ATTRIBUTE CodeGen &codegen, UNUSED_ATTRIBUTE llvm::Value *ht_ptr,
    UNUSED_ATTRIBUTE Vector &selection_vector,
    UNUSED_ATTRIBUTE HashTable::VectorizedIterateCallback &callback) const {
  throw NotImplementedException{
      "Vectorized iteration over hash-tables not supported yet"};
}

}  // namespace codegen
}  // namespace peloton