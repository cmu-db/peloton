//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// cc_hash_table.cpp
//
// Identification: src/codegen/cc_hash_table.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/cc_hash_table.h"

#include "include/codegen/proxy/cc_hash_table_proxy.h"
#include "codegen/hash.h"
#include "include/codegen/util/if.h"
#include "include/codegen/util/loop.h"
#include "common/exception.h"

namespace peloton {
namespace codegen {

// TODO: worry about growing the table

//===----------------------------------------------------------------------===//
// Constructor
//===----------------------------------------------------------------------===//
CCHashTable::CCHashTable() {
  // This constructor shouldn't generally be used at all, but there are
  // cases when the key-type is not known at construction time.
}

//===----------------------------------------------------------------------===//
// Constructor
//===----------------------------------------------------------------------===//
CCHashTable::CCHashTable(CodeGen &codegen,
                         const std::vector<type::Type::TypeId> &key_type) {
  key_storage_.Setup(codegen, key_type);
}

void CCHashTable::Init(CodeGen &codegen, llvm::Value *ht_ptr) const {
  auto *ht_init_fn = CCHashTableProxy::_Init::GetFunction(codegen);
  codegen.CallFunc(ht_init_fn, {ht_ptr});
}

//===----------------------------------------------------------------------===//
// Generate code to handle the insertion of a new tuple
//
// This is fairly simple, but verbose:
// (1) Generate the hash of the keys we're using to probe the hash table
// (2) Use the hash to find the bucket chain we'll follow
// (3) For every HashEntry in the chain:
// (3.1) Check if the HashEntry's hash is equal to what we calculated in (1)
// (3.2) If hash's are equal:
// (3.2.1) Load the keys from the hash entry and check raw key equality
// (3.2.2) If the keys are equal, successful probe, invoke the probe's callback
// (4) The key doesn't exist, invoke HashTable.storeTuple(...) to allocate space
//     for new entry
// (5) Invoke the insertion's callback with the allocated space so she can store
//     the payload
//===----------------------------------------------------------------------===//
void CCHashTable::ProbeOrInsert(CodeGen &codegen, llvm::Value *ht_ptr,
                                llvm::Value *hash,
                                const std::vector<codegen::Value> &key,
                                ProbeCallback &probe_callback,
                                InsertCallback &insert_callback) const {
  llvm::BasicBlock *cont_bb =
      llvm::BasicBlock::Create(codegen.GetContext(), "cont");
  llvm::Type *ht_type = CCHashTableProxy::GetType(codegen);
  llvm::Value *buckets_ptr = codegen->CreateLoad(
      codegen->CreateConstInBoundsGEP2_32(ht_type, ht_ptr, 0, 0));

  // (1)
  llvm::Value *hash_val =
      hash != nullptr ? hash : Hash::HashValues(codegen, key);

  // (2) bucket = hash & (num_buckets-1)
  llvm::Value *bucket_mask = codegen->CreateLoad(
      codegen->CreateConstInBoundsGEP2_32(ht_type, ht_ptr, 0, 2));
  llvm::Value *bucket_num =
      codegen->CreateAnd(hash_val, bucket_mask, "bucketNum");
  llvm::Value *bucket =
      codegen->CreateLoad(codegen->CreateGEP(buckets_ptr, bucket_num));

  // (3) Go to bucket and iterate through hash chain
  llvm::Value *null =
      codegen.NullPtr(llvm::cast<llvm::PointerType>(bucket->getType()));
  llvm::Value *end_condition = codegen->CreateICmpNE(bucket, null);
  Loop chain_loop{codegen, end_condition, {{"iter", bucket}}};
  {
    llvm::Type *ht_entry_type = HashEntryProxy::GetType(codegen);
    llvm::Value *entry = chain_loop.GetLoopVar(0);

    // (3.1) Check if the current iter's hash matches what we calculated earlier
    llvm::Value *entry_hash = codegen->CreateLoad(
        codegen->CreateConstInBoundsGEP2_32(ht_entry_type, entry, 0, 0));
    If hash_match{codegen, codegen->CreateICmpEQ(entry_hash, hash_val),
                  "hashMatch"};
    {
      // (3.2.1) Load the keys from the hash entry
      llvm::Value *keys_ptr =
          codegen->CreateConstInBoundsGEP2_32(ht_entry_type, entry, 1, 0);
      std::vector<codegen::Value> hash_entry_keys;
      llvm::Value *values_area =
          key_storage_.LoadValues(codegen, keys_ptr, hash_entry_keys);

      // (3.2.1)
      auto keys_are_equal = Value::TestEquality(codegen, key, hash_entry_keys);
      If key_match{codegen, keys_are_equal.GetValue(), "keyMatch"};
      {
        // (3.2.2) Call the probe callback since we found a matching key
        probe_callback.ProcessEntry(codegen, values_area);
        key_match.EndIf(cont_bb);
      }
      hash_match.EndIf();
    }

    // Move alone the hash chain
    entry = codegen->CreateLoad(
        codegen->CreateConstInBoundsGEP2_32(ht_entry_type, entry, 0, 1));
    chain_loop.LoopEnd(codegen->CreateICmpNE(entry, null), {entry});
  }

  // (4)
  llvm::Value *value_size = insert_callback.GetValueSize(codegen);
  llvm::Value *keys_size = codegen.Const32(key_storage_.MaxStorageSize());
  llvm::Value *needed_bytes = codegen->CreateAdd(keys_size, value_size);
  llvm::Function *store_func =
      CCHashTableProxy::_StoreTuple::GetFunction(codegen);
  llvm::Value *ptr =
      codegen.CallFunc(store_func, {ht_ptr, hash_val, needed_bytes});

  // (5)
  llvm::Value *data_space_ptr = key_storage_.StoreValues(codegen, ptr, key);
  insert_callback.StoreValue(codegen, data_space_ptr);

  // Ending block
  codegen->CreateBr(cont_bb);
  codegen->GetInsertBlock()->getParent()->getBasicBlockList().push_back(
      cont_bb);
  codegen->SetInsertPoint(cont_bb);
}

//===----------------------------------------------------------------------===//
// Insert lazily into the hash table an entry with the given keys.  Invoke the
// insertion callback when space has been allocated so it can dump its payload.
//===----------------------------------------------------------------------===//
void CCHashTable::Insert(CodeGen &codegen, llvm::Value *ht_ptr,
                         llvm::Value *hash,
                         const std::vector<codegen::Value> &key,
                         InsertCallback &insert_callback) const {
  // Calculate the hash
  llvm::Value *hash_val =
      hash != nullptr ? hash : Hash::HashValues(codegen, key);
  // Get the total size we need
  llvm::Value *value_size = insert_callback.GetValueSize(codegen);
  llvm::Value *keys_size = codegen.Const32(key_storage_.MaxStorageSize());
  llvm::Value *needed_bytes = codegen->CreateAdd(keys_size, value_size);
  // Invoke HashTable::StoreTuple(...)
  llvm::Function *store_func =
      CCHashTableProxy::_StoreTuple::GetFunction(codegen);
  llvm::Value *ptr =
      codegen.CallFunc(store_func, {ht_ptr, hash_val, needed_bytes});
  // 'ptr' points to the space in the hash table.  StoreValue keys first,
  // calculate
  // where the
  // data goes, then invoke the callback to store the payload
  llvm::Value *data_space_ptr = key_storage_.StoreValues(codegen, ptr, key);
  insert_callback.StoreValue(codegen, data_space_ptr);
}

//===----------------------------------------------------------------------===//
// Generate code to iterate over the entire hash table
//
// We follow this procedure to generate hash table iteration code:
// (1) For each bucket in the contiguous array of buckets:
// (1.1) Grab the HashEntry that is the head of the chain for the current bucket
// (1.2) For each HashEntry in the chain:
// (1.2.1) Grab a pointer to the data section of HashEntry
// (1.2.2) Load all the data
// (1.2.3) Invoke callback
//===----------------------------------------------------------------------===//
void CCHashTable::Iterate(CodeGen &codegen, llvm::Value *ht_ptr,
                          IterateCallback &callback) const {
  // Setup, grab points to the bucket array, number of buckets
  llvm::Type *ht_type = CCHashTableProxy::GetType(codegen);
  llvm::Value *buckets_ptr = codegen->CreateLoad(
      codegen->CreateConstInBoundsGEP2_32(ht_type, ht_ptr, 0, 0));
  llvm::Value *num_buckets = codegen->CreateLoad(
      codegen->CreateConstInBoundsGEP2_32(ht_type, ht_ptr, 0, 1));
  llvm::Value *bucket_num = codegen.Const64(0);
  llvm::Value *bucket_cond = codegen->CreateICmpULT(bucket_num, num_buckets);
  // (1)
  Loop bucket_loop{codegen, bucket_cond, {{"bucketNum", bucket_num}}};
  {
    // (1.1)
    bucket_num = bucket_loop.GetLoopVar(0);
    llvm::Value *bucket =
        codegen->CreateLoad(codegen->CreateGEP(buckets_ptr, bucket_num));
    llvm::Value *null_bucket =
        codegen.NullPtr(llvm::cast<llvm::PointerType>(bucket->getType()));
    // (1.2)
    Loop chain_loop{codegen,
                    codegen->CreateICmpNE(bucket, null_bucket),
                    {{"entry", bucket}}};
    {
      // (1.2.1)
      llvm::Type *ht_entry_type = HashEntryProxy::GetType(codegen);
      llvm::Value *entry = chain_loop.GetLoopVar(0);
      llvm::Value *entry_data =
          codegen->CreateConstInBoundsGEP2_32(ht_entry_type, entry, 1, 0);
      // (1.2.2)
      std::vector<codegen::Value> keys;
      auto *data_area_ptr = key_storage_.LoadValues(codegen, entry_data, keys);
      callback.ProcessEntry(codegen, keys, data_area_ptr);
      // Move to next entry in chain
      entry = codegen->CreateLoad(
          codegen->CreateConstInBoundsGEP2_32(ht_entry_type, entry, 0, 1));
      chain_loop.LoopEnd(codegen->CreateICmpNE(entry, null_bucket), {entry});
    }
    // Move to next bucket
    bucket_num = codegen->CreateAdd(bucket_num, codegen.Const64(1));
    bucket_loop.LoopEnd(codegen->CreateICmpULT(bucket_num, num_buckets),
                        {bucket_num});
  }
}

//===----------------------------------------------------------------------===//
//
//===----------------------------------------------------------------------===//
void CCHashTable::FindAll(CodeGen &codegen, llvm::Value *ht_ptr,
                          const std::vector<codegen::Value> &key,
                          IterateCallback &callback) const {
  llvm::Type *ht_type = CCHashTableProxy::GetType(codegen);
  llvm::Value *buckets_ptr = codegen->CreateLoad(
      codegen->CreateConstInBoundsGEP2_32(ht_type, ht_ptr, 0, 0));

  // (1)
  llvm::Value *hash = Hash::HashValues(codegen, key);

  // (2) bucket = hash & (num_buckets-1)
  llvm::Value *bucket_mask = codegen->CreateLoad(
      codegen->CreateConstInBoundsGEP2_32(ht_type, ht_ptr, 0, 2));
  llvm::Value *bucket_num = codegen->CreateAnd(hash, bucket_mask, "bucketNum");
  llvm::Value *bucket =
      codegen->CreateLoad(codegen->CreateGEP(buckets_ptr, bucket_num));

  // (3) Go to bucket and iterate through hash chain
  llvm::Value *null =
      codegen.NullPtr(llvm::cast<llvm::PointerType>(bucket->getType()));
  llvm::Value *end_condition = codegen->CreateICmpNE(bucket, null);
  Loop chain_loop{codegen, end_condition, {{"iter", bucket}}};
  {
    llvm::Type *ht_entry_type = HashEntryProxy::GetType(codegen);
    llvm::Value *entry = chain_loop.GetLoopVar(0);

    // (3.1) Check if the current iter's hash matches what we calculated earlier
    llvm::Value *entry_hash = codegen->CreateLoad(
        codegen->CreateConstInBoundsGEP2_32(ht_entry_type, entry, 0, 0));
    If hash_match{codegen, codegen->CreateICmpEQ(entry_hash, hash),
                  "hashMatch"};
    {
      // (3.2.1) Load the keys from the hash entry
      llvm::Value *iter_keys =
          codegen->CreateConstInBoundsGEP1_32(ht_entry_type, entry, 1);
      std::vector<codegen::Value> entry_keys;
      llvm::Value *data_area =
          key_storage_.LoadValues(codegen, iter_keys, entry_keys);

      auto keys_are_equal = Value::TestEquality(codegen, key, entry_keys);
      If key_match{codegen, keys_are_equal.GetValue(), "keyMatch"};
      {
        // (3.2.2) Call the probe callback since we found a matching key
        callback.ProcessEntry(codegen, key, data_area);
        key_match.EndIf();
      }
      hash_match.EndIf();
    }
    entry = codegen->CreateLoad(
        codegen->CreateConstInBoundsGEP2_32(ht_entry_type, entry, 0, 1));
    chain_loop.LoopEnd(codegen->CreateICmpNE(entry, null), {entry});
  }
}

//===----------------------------------------------------------------------===//
// Destroy/cleanup the hash table whose address is stored in the given LLVM
// register/value
//===----------------------------------------------------------------------===//
void CCHashTable::Destroy(CodeGen &codegen, llvm::Value *ht_ptr) const {
  auto *ht_destroy_func = CCHashTableProxy::_Destroy::GetFunction(codegen);
  codegen.CallFunc(ht_destroy_func, {ht_ptr});
}

void CCHashTable::VectorizedIterate(
    CodeGen &codegen, llvm::Value *ht_ptr, Vector &selection_vector,
    HashTable::VectorizedIterateCallback &callback) const {
  (void)codegen;
  (void)ht_ptr;
  (void)selection_vector;
  (void)callback;
  throw Exception{"Vectorized iteration over CC hash-tables not supported yet"};
}

}  // namespace codegen
}  // namespace peloton