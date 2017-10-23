//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// bloom_filter_accessor.cpp
//
// Identification: src/codegen/bloom_filter_accessor.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/bloom_filter_accessor.h"
#include "codegen/lang/if.h"
#include "codegen/lang/loop.h"
#include "codegen/proxy/bloom_filter_proxy.h"
#include "codegen/util/bloom_filter.h"

#include <cmath>
#include <vector>

namespace peloton {
namespace codegen {

void BloomFilterAccessor::Init(CodeGen &codegen, llvm::Value *bloom_filter,
                               uint64_t estimated_num_tuples) const {
  codegen.Call(BloomFilterProxy::Init,
               {bloom_filter, codegen.Const64(estimated_num_tuples)});
}

void BloomFilterAccessor::Destroy(CodeGen &codegen,
                                  llvm::Value *bloom_filter) const {
  codegen.Call(BloomFilterProxy::Destroy, {bloom_filter});
}
void BloomFilterAccessor::Add(CodeGen &codegen, llvm::Value *bloom_filter,
                              const std::vector<codegen::Value> &key) const {
  // Index of current hash being calculated
  llvm::Value *index = codegen.Const64(0);
  llvm::Value *num_hashes = LoadBloomFilterField(codegen, bloom_filter, 0);
  llvm::Value *seed_hash1 =
      Hash::HashValues(codegen, key, util::BloomFilter::kSeedHashFuncs[0]);
  llvm::Value *seed_hash2 =
      Hash::HashValues(codegen, key, util::BloomFilter::kSeedHashFuncs[1]);

  llvm::Value *end_cond = codegen->CreateICmpULT(index, num_hashes);
  lang::Loop add_loop{codegen, end_cond, {{"i", index}}};
  {
    index = add_loop.GetLoopVar(0);

    // Calculate the ith hash
    llvm::Value *hash = CalculateHash(codegen, index, seed_hash1, seed_hash2);

    // Locate the byte that contains the corresponding bit.
    llvm::Value *bit_offset_in_byte;
    llvm::Value *byte_ptr;
    LocateBit(codegen, bloom_filter, hash, bit_offset_in_byte, byte_ptr);

    // Mark the corresponding bit
    llvm::Value *existing_byte = codegen->CreateLoad(byte_ptr);
    llvm::Value *new_byte = codegen->CreateOr(
        existing_byte,
        codegen->CreateShl(codegen.Const8(1), bit_offset_in_byte));
    codegen->CreateStore(new_byte, byte_ptr);

    index = codegen->CreateAdd(index, codegen.Const64(1));
    add_loop.LoopEnd(codegen->CreateICmpULT(index, num_hashes), {index});
  }
}

llvm::Value *BloomFilterAccessor::Contains(
    CodeGen &codegen, llvm::Value *bloom_filter,
    const std::vector<codegen::Value> &key) const {
  // Index of current hash being calculated
  llvm::Value *index = codegen.Const64(0);
  llvm::Value *num_hashes = LoadBloomFilterField(codegen, bloom_filter, 0);
  llvm::Value *end_cond = codegen->CreateICmpULT(index, num_hashes);
  llvm::Value *seed_hash1 =
      Hash::HashValues(codegen, key, util::BloomFilter::kSeedHashFuncs[0]);
  llvm::Value *seed_hash2 =
      Hash::HashValues(codegen, key, util::BloomFilter::kSeedHashFuncs[1]);
  // Update statistic. Increase number of probing
  llvm::Value *num_probe = LoadBloomFilterField(codegen, bloom_filter, 4);
  StoreBloomFilterField(codegen, bloom_filter, 4,
                        codegen->CreateAdd(num_probe, codegen.Const64(1)));

  lang::Loop add_loop{codegen, end_cond, {{"i", index}}};
  {
    index = add_loop.GetLoopVar(0);

    // Calculate the ith hash
    llvm::Value *hash = CalculateHash(codegen, index, seed_hash1, seed_hash2);

    // Locate the byte that contains the corresponding bit.
    llvm::Value *bit_offset_in_byte;
    llvm::Value *byte_ptr;
    LocateBit(codegen, bloom_filter, hash, bit_offset_in_byte, byte_ptr);

    // Check if the corresopnding bit is set
    llvm::Value *existing_byte = codegen->CreateLoad(byte_ptr);
    llvm::Value *and_val = codegen->CreateAnd(
        existing_byte,
        codegen->CreateShl(codegen.Const8(1), bit_offset_in_byte));
    llvm::Value *equal_zero = codegen->CreateICmpEQ(and_val, codegen.Const8(0));
    lang::If bit_not_set{codegen, equal_zero, "BitNotSet"};
    {
      // Bit is not set. It means object not in bloom filter. break
      llvm::Value *num_misses = LoadBloomFilterField(codegen, bloom_filter, 3);
      StoreBloomFilterField(codegen, bloom_filter, 3,
                            codegen->CreateAdd(num_misses, codegen.Const64(1)));
      add_loop.Break();
    }
    bit_not_set.EndIf();

    index = codegen->CreateAdd(index, codegen.Const64(1));
    add_loop.LoopEnd(codegen->CreateICmpULT(index, num_hashes), {index});
  }
  std::vector<llvm::Value *> vals;
  add_loop.CollectFinalLoopVariables(vals);

  // contains = (i == num_hashes ? true : false);
  llvm::Value *loop_to_end = codegen->CreateICmpEQ(vals[0], num_hashes);
  llvm::Value *contains = codegen->CreateSelect(
      loop_to_end, codegen.ConstBool(true), codegen.ConstBool(false));

  return contains;
}

llvm::Value *BloomFilterAccessor::CalculateHash(CodeGen &codegen,
                                                llvm::Value *index,
                                                llvm::Value *seed_hash1,
                                                llvm::Value *seed_hash2) const {
  // Calculate hashes[index]
  llvm::Value *combined_hash =
      codegen->CreateAdd(seed_hash1, codegen->CreateMul(index, seed_hash2));

  // return i == 0 ? seed_hash1 : (i == 1 ? seed_hash2 : combined_hash);
  llvm::Value *is_first = codegen->CreateICmpEQ(index, codegen.Const64(0));
  llvm::Value *is_second = codegen->CreateICmpEQ(index, codegen.Const64(1));
  llvm::Value *hash = codegen->CreateSelect(
      is_first, seed_hash1,
      codegen->CreateSelect(is_second, seed_hash2, combined_hash));
  return hash;
}

// Given the hash, find the corresponding byte that contains the bit
void BloomFilterAccessor::LocateBit(CodeGen &codegen, llvm::Value *bloom_filter,
                                    llvm::Value *hash,
                                    llvm::Value *&bit_offset_in_byte,
                                    llvm::Value *&byte_ptr) const {
  llvm::Value *byte_array = LoadBloomFilterField(codegen, bloom_filter, 1);
  llvm::Value *num_bits = LoadBloomFilterField(codegen, bloom_filter, 2);

  llvm::Value *bits_per_byte = codegen.Const64(8);
  llvm::Value *bit_offset = codegen->CreateURem(hash, num_bits);
  llvm::Value *byte_offset = codegen->CreateUDiv(bit_offset, bits_per_byte);
  bit_offset_in_byte = codegen->CreateTrunc(
      codegen->CreateURem(bit_offset, bits_per_byte), codegen.Int8Type());
  byte_ptr =
      codegen->CreateInBoundsGEP(codegen.ByteType(), byte_array, byte_offset);
}

void BloomFilterAccessor::StoreBloomFilterField(
    CodeGen &codegen, llvm::Value *bloom_filter, uint32_t field_id,
    llvm::Value *new_field_val) const {
  llvm::Type *bloom_filter_type = BloomFilterProxy::GetType(codegen);
  codegen->CreateStore(new_field_val,
                       codegen->CreateConstInBoundsGEP2_32(
                           bloom_filter_type, bloom_filter, 0, field_id));
}

llvm::Value *BloomFilterAccessor::LoadBloomFilterField(
    CodeGen &codegen, llvm::Value *bloom_filter, uint32_t field_id) const {
  llvm::Type *bloom_filter_type = BloomFilterProxy::GetType(codegen);
  return codegen->CreateLoad(codegen->CreateConstInBoundsGEP2_32(
      bloom_filter_type, bloom_filter, 0, field_id));
}

}  // namespace codegen
}  // namespace peloton
