//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// oa_hash_table.cpp
//
// Identification: src/codegen/oa_hash_table.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/oa_hash_table.h"

#include <llvm/IR/Intrinsics.h>
#include <llvm/IR/CFG.h>

#include "codegen/hash.h"
#include "codegen/lang/if.h"
#include "codegen/lang/loop.h"
#include "codegen/proxy/oa_hash_table_proxy.h"
#include "codegen/lang/vectorized_loop.h"
#include "codegen/type/integer_type.h"
#include "codegen/util/oa_hash_table.h"

namespace peloton {
namespace codegen {

// The global default prefetch distance
uint32_t OAHashTable::kDefaultGroupPrefetchSize = 10;

// The global attribute information instance used to populate a row's hash value
const planner::AttributeInfo OAHashTable::kHashAI{type::Integer::Instance(), 0,
                                                  "hash"};

//===----------------------------------------------------------------------===//
// CONSTRUCTORS
//===----------------------------------------------------------------------===//

OAHashTable::OAHashTable() {
  // This constructor shouldn't generally be used at all, but there are
  // cases when the key-type is not known at construction time.
}

OAHashTable::OAHashTable(CodeGen &codegen,
                         const std::vector<type::Type> &key_type,
                         uint64_t value_size)
    : value_size_(value_size) {
  key_storage_.Setup(codegen, key_type);

  // Configure the size of each HashEntry
  hash_entry_size_ = sizeof(codegen::util::OAHashTable::HashEntry) +
                     key_storage_.MaxStorageSize() + value_size_;
}

llvm::Value *OAHashTable::HashKey(
    CodeGen &codegen, const std::vector<codegen::Value> &key) const {
  return Hash::HashValues(codegen, key);
}

// Load the value of a field from the given hash table instance.
// NOTE: the type returned is the actual type registered in HashTable type
llvm::Value *OAHashTable::LoadHashTableField(CodeGen &codegen,
                                             llvm::Value *hash_table,
                                             uint32_t field_id) const {
  llvm::Type *hash_table_type = OAHashTableProxy::GetType(codegen);
  return codegen->CreateLoad(codegen->CreateConstInBoundsGEP2_32(
      hash_table_type, hash_table, 0, field_id));
}

// Return the element stored in a specified field of a HashEntry struct. Since
// we also need to access data field, the offset is an extra argument
llvm::Value *OAHashTable::LoadHashEntryField(CodeGen &codegen,
                                             llvm::Value *entry_ptr,
                                             uint32_t offset,
                                             uint32_t field_id) const {
  llvm::Type *entry_type = OAHashEntryProxy::GetType(codegen);
  return codegen->CreateLoad(codegen->CreateConstInBoundsGEP2_32(
      entry_type, entry_ptr, offset, field_id));
}

llvm::Value *OAHashTable::PtrToInt(CodeGen &codegen, llvm::Value *ptr) const {
  // Casts the pointer to uint64_t which is no-op on x86-64
  return codegen->CreatePtrToInt(ptr, codegen.Int64Type());
}

// Advance a given pointer by some number of bytes. This function is designed to
// manipulate pointer as byte addressable. The return value is a pointer of the
// same type with the value advanced by delta bytes.
llvm::Value *OAHashTable::AdvancePointer(CodeGen &codegen, llvm::Value *ptr,
                                         llvm::Value *delta) const {
  // Cast it to uint64_t
  llvm::Value *ptr_int = PtrToInt(codegen, ptr);

  // Create new value for the pointer
  llvm::Value *new_ptr_int = codegen->CreateAdd(ptr_int, delta);

  return codegen->CreateIntToPtr(new_ptr_int, ptr->getType());
}

llvm::Value *OAHashTable::AdvancePointer(CodeGen &codegen, llvm::Value *ptr,
                                         uint64_t delta) const {
  return AdvancePointer(codegen, ptr, codegen.Const64(delta));
}

//===----------------------------------------------------------------------===//
// Given the hash table, current entry and current index, return a pair
// representing next entry pointer and next index value
// This function takes care of possible wrapping back of the index
// and has one IF branch
// Note that entry_p is a pointer of HashEntry type
//===----------------------------------------------------------------------===//
OAHashTable::HashTablePos OAHashTable::GetNextEntry(CodeGen &codegen,
                                                    llvm::Value *hash_table,
                                                    llvm::Value *entry_ptr,
                                                    llvm::Value *index) const {
  // hash_table_size = hash_table->num_buckets_
  llvm::Value *hash_table_size = LoadHashTableField(codegen, hash_table, 1);
  // next_index = index + 1
  llvm::Value *next_index = codegen->CreateAdd(index, codegen.Const64(1));
  // next_entry_p = entry_p + HashEntrySize()
  llvm::Value *next_entry_ptr =
      AdvancePointer(codegen, entry_ptr, HashEntrySize());

  // Do wrap-around if we need to
  llvm::Value *wrap_back_index = nullptr, *wrap_back_entry_ptr = nullptr;
  lang::If wrap_back{codegen,
                     codegen->CreateICmpEQ(next_index, hash_table_size)};
  {
    wrap_back_index = codegen.Const64(0);
    wrap_back_entry_ptr = LoadHashTableField(codegen, hash_table, 0);
  }
  wrap_back.EndIf();

  auto final_index = wrap_back.BuildPHI(wrap_back_index, next_index);
  auto final_entry_ptr =
      wrap_back.BuildPHI(wrap_back_entry_ptr, next_entry_ptr);

  return HashTablePos{final_index, final_entry_ptr};
}

llvm::Value *OAHashTable::GetEntry(CodeGen &codegen, llvm::Value *hash_table,
                                   llvm::Value *index) const {
  llvm::Value *byte_offset = codegen->CreateMul(
      codegen.Const64(hash_entry_size_),
      codegen->CreateZExtOrBitCast(index, codegen.Int64Type()));
  llvm::Value *base_ptr = LoadHashTableField(codegen, hash_table, 0);
  return AdvancePointer(codegen, base_ptr, byte_offset);
}

OAHashTable::HashTablePos OAHashTable::GetEntryByHash(
    CodeGen &codegen, llvm::Value *hash_table, llvm::Value *hash_value) const {
  // Given the hash value, return a HashEntry * and the index
  // We need both to judge whether to wrap back

  // Load bucket mask from the hash table field #2 (3rd)
  llvm::Value *bucket_mask = LoadHashTableField(codegen, hash_table, 2);
  llvm::Value *index = codegen->CreateAnd(bucket_mask, hash_value);
  llvm::Value *entry_ptr = GetEntry(codegen, hash_table, index);
  return HashTablePos{index, entry_ptr};
}

// Return KeyValueList * inside the provided HashEntry *. Note that this
// function is quite redundant, but we would like to emphasize the fact that
// this function returns a pointer to KeyValueList rather than the status code.
llvm::Value *OAHashTable::GetKeyValueList(CodeGen &codegen,
                                          llvm::Value *entry_ptr) const {
  return LoadHashEntryField(codegen, entry_ptr, 0, 0);
}

// Get the pointer to the key in the provided HashEntry *
llvm::Value *OAHashTable::GetKeyPtr(CodeGen &codegen,
                                    llvm::Value *entry_ptr) const {
  llvm::Type *entry_type = OAHashEntryProxy::GetType(codegen);
  return codegen->CreateConstInBoundsGEP2_32(entry_type, entry_ptr, 1, 0);
}

// Return boolean result on whther the pointer is equal to a given
// integer that is LLVM compile time constant
llvm::Value *OAHashTable::IsPtrEqualTo(CodeGen &codegen, llvm::Value *ptr,
                                       uint64_t value) const {
  return codegen->CreateICmpEQ(PtrToInt(codegen, ptr), codegen.Const64(value));
}

// Reverse of IsPtrEqualTo()
llvm::Value *OAHashTable::IsPtrUnEqualTo(CodeGen &codegen, llvm::Value *ptr,
                                         uint64_t value) const {
  return codegen->CreateICmpNE(PtrToInt(codegen, ptr), codegen.Const64(value));
}

// Given the entry pointer and kv_p, determine the number of data items
// to process and determine the location of data items which is a consecutive
// chunk of storage of size (data item count) * (value size). We return a
// {data_count, data_array*} pair.
std::pair<llvm::Value *, llvm::Value *> OAHashTable::GetDataCountAndPointer(
    CodeGen &codegen, llvm::Value *kv_p, llvm::Value *after_key_p) const {
  llvm::Value *data_count_inline = nullptr, *data_count_noninline = nullptr;
  llvm::Value *data_ptr_inline = nullptr, *data_ptr_noninline = nullptr;

  // Whether kv_p equals 0x0000000000000001
  lang::If is_entry_single_value{codegen, IsPtrEqualTo(codegen, kv_p, 1UL),
                                 "singleValue"};
  {
    data_count_inline = codegen.Const64(1);
    data_ptr_inline = AdvancePointer(codegen, after_key_p, (uint64_t)0UL);
  }
  is_entry_single_value.ElseBlock("multipleValue");
  {
    llvm::Type *kv_list_type = KeyValueListProxy::GetType(codegen);
    data_count_noninline = codegen->CreateIntCast(
        codegen->CreateLoad(codegen->CreateConstInBoundsGEP2_32(
            kv_list_type,
            kv_p,  // KeyValueList *
            0,
            1)),              // size field of KeyValueList
        codegen.Int64Type(),  // uint64_t
        false);               // unsigned integer
    data_ptr_noninline = codegen->CreateBitCast(
        codegen->CreateConstInBoundsGEP2_32(kv_list_type, kv_p, 1, 0),
        codegen.CharPtrType());
  }
  is_entry_single_value.EndIf();

  auto final_data_count =
      is_entry_single_value.BuildPHI(data_count_inline, data_count_noninline);
  auto final_data_ptr =
      is_entry_single_value.BuildPHI(data_ptr_inline, data_ptr_noninline);

  return std::make_pair(final_data_count, final_data_ptr);
}

//===----------------------------------------------------------------------===//
// Translate the looping and probing framework for probing
//
// The invariance is that we do not manipulate uint64_t type pointer in this
// function - they should all be handled inside helper routines and abstracted
// away from this function
//
// Beware that returned struct will contain nullptr's if return_probe_result
// is set to false!
//===----------------------------------------------------------------------===//
OAHashTable::ProbeResult OAHashTable::TranslateProbing(
    CodeGen &codegen, llvm::Value *hash_table, llvm::Value *hash,
    const std::vector<codegen::Value> &key,
    std::function<void(llvm::Value *)> key_found,
    std::function<void(llvm::Value *)> key_not_found, bool process_value,
    bool process_only_one_value, bool create_key_if_missing,
    bool return_probe_result) const {
  // This is the basic block that we go through if the key is found
  // or jumped when after the key is missing and the call back is invoked
  llvm::BasicBlock *key_found_or_inserted_bb =
      llvm::BasicBlock::Create(codegen.GetContext(), "cont");

  // We later have to remember the basic block where the loop jumps out
  llvm::BasicBlock *before_jump_out_bb;

  // Create struct that will be returned
  ProbeResult probe_result;

  // Create the data_ptr values for both paths (for later use in the result
  // struct)
  llvm::Value *data_ptr;
  llvm::Value *dump_data_ptr;

  // Compute the hash value for the key
  llvm::Value *hash_value = hash != nullptr ? hash : HashKey(codegen, key);

  // Use the hash value to determine a position in the hash table
  auto pos = GetEntryByHash(codegen, hash_table, hash_value);
  llvm::Value *entry_ptr = pos.entry_ptr;
  llvm::Value *index = pos.entry_index;

  // Check if the status of the entry
  llvm::Value *kv_p = GetKeyValueList(codegen, entry_ptr);

  // Return the result of comparison (if ptr is 0 then the slot is free)
  llvm::Value *status_neq_zero = IsPtrUnEqualTo(codegen, kv_p, 0UL);

  lang::Loop probe_loop{
      codegen,
      status_neq_zero,
      {{"Probe.entry", entry_ptr}, {"Probe.index", index}, {"Probe.kvl", kv_p}}};
  {
    entry_ptr = probe_loop.GetLoopVar(0);
    index = probe_loop.GetLoopVar(1);
    kv_p = probe_loop.GetLoopVar(2);

    // Load hash entry from the HashEntry *
    llvm::Value *entry_hash_value =
        LoadHashEntryField(codegen, entry_ptr, 0, 1);
    llvm::Value *is_hash_match =
        codegen->CreateICmpEQ(entry_hash_value, hash_value);
    lang::If hash_match_branch{codegen, is_hash_match, "hashMatch"};
    {
      // Load the key from the HashEntry *
      std::vector<codegen::Value> entry_key{};
      llvm::Value *key_ptr = GetKeyPtr(codegen, entry_ptr);
      data_ptr = key_storage_.LoadValues(codegen, key_ptr, entry_key);

      // Check if the provided key matches what's in the HashEntry
      llvm::Value *is_key_match =
          Value::TestEquality(codegen, key, entry_key).GetValue();
      lang::If key_match_branch{codegen, is_key_match, "keyMatch"};
      {
        // Set result value to true if key was found
        probe_result.key_exists = codegen.ConstBool(true);

        if (process_value) {
          if (process_only_one_value) {
            // The client wants to ignore the value list and only use the key
            // and value stored inlined in the HashEntry. This branch is
            // prepared for ProbeOrInsert().

            // Process data
            if (key_found != nullptr) {
              key_found(data_ptr);
            }
          } else {
            // The client wants to process all values in the hash entry. We
            // iterate through the value list one-by-one. This branch is
            // prepared for FindAll()

            // Return count and pointer
            auto data_count_ptr_pair =
                GetDataCountAndPointer(codegen, kv_p, data_ptr);

            llvm::Value *data_count = data_count_ptr_pair.first;
            data_ptr = data_count_ptr_pair.second;
            llvm::Value *loop_counter = codegen.Const64(0);

            // Start a loop. Since we know at least one value exits, we build
            // a do-while loop.
            lang::Loop value_loop{
                codegen,
                codegen.ConstBool(true),
                {{"Probe.counter", loop_counter}, {"Probe.dataPtr", data_ptr}}};
            {
              // Loop variables
              loop_counter = value_loop.GetLoopVar(0);
              data_ptr = value_loop.GetLoopVar(1);

              // Process the data using its pointer
              if (key_found != nullptr) {
                key_found(data_ptr);
              }

              // Bump loop counter by one, and data pointer by value size
              loop_counter =
                  codegen->CreateAdd(loop_counter, codegen.Const64(1));
              data_ptr = AdvancePointer(codegen, data_ptr, value_size_);

              auto *cond = codegen->CreateICmpULT(loop_counter, data_count);
              value_loop.LoopEnd(cond, {loop_counter, data_ptr});
            }
          }
        } else {
          // The client doesn't care about the existing value, which exists
          // since we found the key. Call StoreTuple() to potentially create
          // AND/OR resize the KeyValueList. This branch is prepared for
          // Insert().

          // Return value is the pointer to value region
          llvm::Value *dump_value_ptr =
              codegen.Call(OAHashTableProxy::StoreTuple,
                           {hash_table, entry_ptr, hash_value});

          // Call the callback with the pointer to dump data
          if (key_not_found != nullptr) {
            key_not_found(dump_value_ptr);
          }
        }
      }

      // Save the current basic block, because we need to identify it later
      // (see generation of probe_result)
      before_jump_out_bb = codegen->GetInsertBlock();

      // If a key is found, we jump out of the loop since probing is complete
      key_match_branch.EndIf(key_found_or_inserted_bb);
    }
    hash_match_branch.EndIf();

    // Get next entry pointer and index (handling wrap-around)
    auto next_entry_pos = GetNextEntry(codegen, hash_table, entry_ptr, index);
    entry_ptr = next_entry_pos.entry_ptr;
    index = next_entry_pos.entry_index;

    // Copied from the above
    kv_p = GetKeyValueList(codegen, entry_ptr);

    // End probe loop
    probe_loop.LoopEnd(IsPtrUnEqualTo(codegen, kv_p, 0UL),
                       {entry_ptr, index, kv_p});
  }

  // Next deal with key not found

  // This is set to true if a missing key should be created
  // This is for Insert() and ProbeOrInsert()
  if (create_key_if_missing) {
    std::vector<llvm::Value *> final_probe_loop_vars;
    probe_loop.CollectFinalLoopVariables(final_probe_loop_vars);
    entry_ptr = final_probe_loop_vars[0];
    index = final_probe_loop_vars[1];

    // Return value is the pointer to key-value region
    llvm::Value *dump_key_ptr = codegen.Call(
        OAHashTableProxy::StoreTuple, {hash_table, entry_ptr, hash_value});

    // First store the keys into the region in hash entry. The pointer returned
    // is where the value goes.
    dump_data_ptr = key_storage_.StoreValues(codegen, dump_key_ptr, key);

    // Call the call back with the pointer to dump data
    if (key_not_found != nullptr) {
      key_not_found(dump_data_ptr);
    }
  }

  codegen->CreateBr(key_found_or_inserted_bb);

  // Finished dealing with key not found; going to the end of the procedure
  // (merge point where key found and key not found get merged into one BB)

  codegen->GetInsertBlock()->getParent()->getBasicBlockList().push_back(
      key_found_or_inserted_bb);
  codegen->SetInsertPoint(key_found_or_inserted_bb);

  // Check if probing result is requested
  if (return_probe_result) {
    // Create Phi for the boolean whether the key existed already

    // TODO: find better way to get number of predecessors of basic block
    // note: the iterator here can't be used for the for loop a few lines later
    auto bb_pred = llvm::predecessors(key_found_or_inserted_bb);
    auto bb_pred_num = std::distance(bb_pred.begin(), bb_pred.end());

    // Create Phis
    llvm::PHINode *key_exists_phi =
        codegen->CreatePHI(codegen.BoolType(), bb_pred_num);
    llvm::PHINode *data_ptr_phi = nullptr;

    if (create_key_if_missing) {
      data_ptr_phi = codegen->CreatePHI(data_ptr->getType(), bb_pred_num);
    }

    // This value needs special treatment, because one path is from the early
    // loop drop out,
    // and there can be numerous other paths that have been created from the
    // given functions,
    // that all end at the key_found_or_inserted_bb basic block
    for (llvm::BasicBlock *pred :
         llvm::predecessors(key_found_or_inserted_bb)) {
      if (pred == before_jump_out_bb) {
        key_exists_phi->addIncoming(codegen.ConstBool(true), pred);

        if (create_key_if_missing) {
          data_ptr_phi->addIncoming(data_ptr, pred);
        }
      } else {
        key_exists_phi->addIncoming(codegen.ConstBool(false), pred);

        if (create_key_if_missing) {
          data_ptr_phi->addIncoming(dump_data_ptr, pred);
        }
      }
    }

    probe_result.key_exists = key_exists_phi;

    if (create_key_if_missing) {
      probe_result.data_ptr = data_ptr_phi;
    } else {
      probe_result.data_ptr = data_ptr;
    }
  }

  return probe_result;
}

void OAHashTable::Init(CodeGen &codegen, llvm::Value *ht_ptr) const {
  auto *key_size = codegen.Const64(key_storage_.MaxStorageSize());
  auto *value_size = codegen.Const64(value_size_);
  auto *initial_size =
      codegen.Const64(codegen::util::OAHashTable::kDefaultInitialSize);
  codegen.Call(OAHashTableProxy::Init,
               {ht_ptr, key_size, value_size, initial_size});
}

void OAHashTable::ProbeOrInsert(CodeGen &codegen, llvm::Value *ht_ptr,
                                llvm::Value *hash,
                                const std::vector<codegen::Value> &key,
                                ProbeCallback &probe_callback,
                                InsertCallback &insert_callback) const {
  auto key_found = [&codegen, &probe_callback](llvm::Value *data_ptr) {
    probe_callback.ProcessEntry(codegen, data_ptr);
  };

  auto key_not_found = [&codegen, &insert_callback](llvm::Value *data_ptr) {
    insert_callback.StoreValue(codegen, data_ptr);
  };

  TranslateProbing(codegen, ht_ptr, hash, key, key_found, key_not_found, true,
                   true,
                   true,  // If key is missing create it in empty slot
                   false);
}

// Probe the hash table and insert a new slot if needed, returning both the
// result and the data pointer
OAHashTable::ProbeResult OAHashTable::ProbeOrInsert(
    CodeGen &codegen, llvm::Value *ht_ptr, llvm::Value *hash,
    const std::vector<codegen::Value> &key) const {
  ProbeResult probe_result;

  probe_result =
      TranslateProbing(codegen, ht_ptr, hash, key, nullptr, nullptr, true, true,
                       true,  // If key is missing create it in empty slot
                       true);

  return probe_result;
}

void OAHashTable::Insert(CodeGen &codegen, llvm::Value *ht_ptr,
                         llvm::Value *hash,
                         const std::vector<codegen::Value> &key,
                         InsertCallback &insert_callback) const {
  auto key_found = [&codegen, &insert_callback](llvm::Value *data_ptr) {
    insert_callback.StoreValue(codegen, data_ptr);
  };

  auto key_not_found = [&codegen, &insert_callback](llvm::Value *data_ptr) {
    insert_callback.StoreValue(codegen, data_ptr);
  };

  TranslateProbing(codegen, ht_ptr, hash, key, key_found, key_not_found, false,
                   false,
                   true,  // If key is missing create it in empty slot
                   false);
}

void OAHashTable::Iterate(CodeGen &codegen, llvm::Value *hash_table,
                          IterateCallback &callback) const {
  // Load the size of the array
  llvm::Value *num_buckets = LoadHashTableField(codegen, hash_table, 1);

  // Create a constant number from entry size
  llvm::Value *entry_size = codegen.Const64(HashEntrySize());

  // This is the first entry in the hash table
  // The type of this pointer is HashEntry without key and value field
  llvm::Value *entry_ptr = LoadHashTableField(codegen, hash_table, 0);

  // Create an uint64_t variable which is the current index in the array
  // and initialize it with cosntant number 0
  llvm::Value *entry_index = codegen.Const64(0);

  // Compare current index with number of buckets
  // If < then this is true
  llvm::Value *bucket_cond = codegen->CreateICmpULT(entry_index, num_buckets);

  // (1) loop var = bucket_index; loop cond = bucket_cond
  lang::Loop bucket_loop{
      codegen,
      bucket_cond,
      {{"Iterate.entryIndex", entry_index}, {"Iterate.entryPtr", entry_ptr}}};
  {
    entry_index = bucket_loop.GetLoopVar(0);
    entry_ptr = bucket_loop.GetLoopVar(1);

    // Get the key value list
    llvm::Value *kv_p = GetKeyValueList(codegen, entry_ptr);

    // Return the result of comparison (if ptr is 0 then the slot is free)
    llvm::Value *status_neq_zero = IsPtrUnEqualTo(codegen, kv_p, 0UL);

    // If the bucket is not free
    lang::If bucket_occupied{codegen, status_neq_zero, "bucketIsOccupied"};
    {
      // Read keys and return the pointer to value
      std::vector<codegen::Value> entry_key{};
      llvm::Value *key_ptr = GetKeyPtr(codegen, entry_ptr);
      llvm::Value *data_ptr =
          key_storage_.LoadValues(codegen, key_ptr, entry_key);

      // Return count and pointer
      auto data_count_ptr_pair =
          GetDataCountAndPointer(codegen, kv_p, data_ptr);

      llvm::Value *data_count = data_count_ptr_pair.first;
      data_ptr = data_count_ptr_pair.second;
      llvm::Value *val_index = codegen.Const64(0);

      lang::Loop read_value_loop{
          codegen,
          codegen.ConstBool(true),  // Always pass
          {{"Iterate.counter", val_index}, {"Iterate.dataPtr", data_ptr}}};
      {
        val_index = read_value_loop.GetLoopVar(0);
        data_ptr = read_value_loop.GetLoopVar(1);

        callback.ProcessEntry(codegen, entry_key, data_ptr);
        data_ptr = AdvancePointer(codegen, data_ptr, value_size_);

        val_index = codegen->CreateAdd(val_index, codegen.Const64(1));
        llvm::Value *is_end_of_loop =
            codegen->CreateICmpULT(val_index, data_count);
        read_value_loop.LoopEnd(is_end_of_loop, {val_index, data_ptr});
      }
    }
    bucket_occupied.EndIf();

    entry_index = codegen->CreateAdd(entry_index, codegen.Const64(1));
    entry_ptr = AdvancePointer(codegen, entry_ptr, entry_size);

    bucket_loop.LoopEnd(codegen->CreateICmpULT(entry_index, num_buckets),
                        {entry_index, entry_ptr});
  }
}

void OAHashTable::VectorizedIterate(
    CodeGen &codegen, llvm::Value *hash_table, Vector &selection_vector,
    OAHashTable::VectorizedIterateCallback &callback) const {
  // A vectorized iteration is done in two passes:
  // 1. In initial first-pass finds valid/occupied buckets and puts their
  //    addresses into the provided selection vector
  // 2. In the next-pass, only the valid buckets are read, invoking the callback

  uint32_t size = selection_vector.GetCapacity();
  PL_ASSERT((size & (size - 1)) == 0);

  // The start of the buckets array
  llvm::Value *entry_ptr = LoadHashTableField(codegen, hash_table, 0);

  // Load the size of the array
  llvm::Value *num_buckets = LoadHashTableField(codegen, hash_table, 1);
  num_buckets = codegen->CreateTruncOrBitCast(num_buckets, codegen.Int32Type());

  lang::VectorizedLoop vector_loop{
      codegen, num_buckets, size, {{"currEntryPtr", entry_ptr}}};
  {
    auto curr_range = vector_loop.GetCurrentRange();
    llvm::Value *start = curr_range.start;
    llvm::Value *end = curr_range.end;
    entry_ptr = vector_loop.GetLoopVar(0);

    // Initial filter loop
    std::vector<lang::Loop::LoopVariable> loop_vars = {
        {"VectorizedIterate.pos", start},
        {"VectorizedIterate.selPos", codegen.Const32(0)},
        {"VectorizedIterate.currEntryPtr", entry_ptr}};
    lang::Loop filter_loop{codegen, codegen.ConstBool(true), loop_vars};
    {
      llvm::Value *pos = filter_loop.GetLoopVar(0);
      llvm::Value *sel_pos = filter_loop.GetLoopVar(1);
      entry_ptr = filter_loop.GetLoopVar(2);

      // sel[sel_pos] = pos
      selection_vector.SetValue(codegen, sel_pos, pos);

      // sel_pos += curr_entry->IsFree()
      llvm::Value *status = GetKeyValueList(codegen, entry_ptr);
      llvm::Value *is_free = IsPtrUnEqualTo(codegen, status, 0UL);
      sel_pos = codegen->CreateAdd(
          sel_pos, codegen->CreateZExtOrBitCast(is_free, codegen.Int32Type()));

      // curr_entry += hash_entry_size
      pos = codegen->CreateAdd(pos, codegen.Const32(1));
      entry_ptr = AdvancePointer(codegen, entry_ptr, HashEntrySize());

      filter_loop.LoopEnd(codegen->CreateICmpULT(pos, end),
                          {pos, sel_pos, entry_ptr});
    }

    // Figure out where the filter loop ended to pull put the final loop vars
    // Set selection vector position and current entry position
    std::vector<llvm::Value *> final_vars;
    filter_loop.CollectFinalLoopVariables(final_vars);
    selection_vector.SetNumElements(final_vars[1]);
    entry_ptr = final_vars[2];

    // Selection vector is filled, deliver vector to callback
    OAHashTableAccess hash_table_accessor{*this, hash_table};
    callback.ProcessEntries(codegen, start, end, selection_vector,
                            hash_table_accessor);

    vector_loop.LoopEnd(codegen, {entry_ptr});
  }
}

void OAHashTable::FindAll(CodeGen &codegen, llvm::Value *ht_ptr,
                          const std::vector<codegen::Value> &key,
                          IterateCallback &callback) const {
  auto key_found = [&codegen, &callback, &key](llvm::Value *data_ptr) {
    callback.ProcessEntry(codegen, key, data_ptr);
  };

  // It does not do anything for a key that is not found
  auto key_not_found = [](llvm::Value *data_ptr) { (void)data_ptr; };

  TranslateProbing(codegen, ht_ptr, nullptr, key, key_found, key_not_found,
                   true, false,
                   false,  // If key is missing create it in empty slot
                   false);
}

void OAHashTable::Destroy(CodeGen &codegen, llvm::Value *ht_ptr) const {
  codegen.Call(OAHashTableProxy::Destroy, {ht_ptr});
}

void OAHashTable::PrefetchBucket(CodeGen &codegen, llvm::Value *ht_ptr,
                                 llvm::Value *hash,
                                 OAHashTable::PrefetchType pf_type,
                                 OAHashTable::Locality locality) const {
  static constexpr uint32_t kDataCache = 1;

  auto pos = GetEntryByHash(codegen, ht_ptr, hash);
  llvm::Value *entry_ptr =
      codegen->CreateBitCast(pos.entry_ptr, codegen.CharPtrType());

  // LLVM's prefetch intrinsic signature is:
  //
  //   void prefetch(i8* addr, i32 rw, i32 locality, i32 cache-type)
  //
  // The arguments are as follows:
  //       addr - the address we want to prefetch
  //         rw - is this prefetch for a read (0) or a write (1)
  //   locality - temporal locality specifier between 0 and 3 inclusive
  // cache type - prefetching from instruction cache (0) or data cache (1)
  llvm::Function *prefetch_func = llvm::Intrinsic::getDeclaration(
      &codegen.GetModule(), llvm::Intrinsic::prefetch);
  std::vector<llvm::Value *> fn_args = {
      // The address we're prefetching
      entry_ptr,
      // The type of prefetch (i.e., fetch for read or write)
      codegen.Const32(static_cast<uint32_t>(pf_type)),
      // The degree of temporal locality
      codegen.Const32(static_cast<uint32_t>(locality)),
      // The type of prefetch (i.e., instruction or data cache)
      codegen.Const32(kDataCache)};
  codegen.CallFunc(prefetch_func, fn_args);
}

void OAHashTable::OAHashTableAccess::ExtractBucketKeys(
    CodeGen &codegen, llvm::Value *index,
    std::vector<codegen::Value> &key) const {
  auto *entry_ptr = hash_table_.GetEntry(codegen, ht_ptr_, index);
  llvm::Value *key_ptr = hash_table_.GetKeyPtr(codegen, entry_ptr);
  hash_table_.key_storage_.LoadValues(codegen, key_ptr, key);
}

llvm::Value *OAHashTable::OAHashTableAccess::BucketValue(
    CodeGen &codegen, llvm::Value *index) const {
  llvm::Value *entry_ptr = hash_table_.GetEntry(codegen, ht_ptr_, index);
  llvm::Value *key_ptr = hash_table_.GetKeyPtr(codegen, entry_ptr);
  uint32_t key_size = hash_table_.key_storage_.MaxStorageSize();
  return hash_table_.AdvancePointer(codegen, key_ptr, key_size);
}

}  // namespace codegen
}  // namespace peloton