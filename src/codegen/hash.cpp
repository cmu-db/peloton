//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// hash.cpp
//
// Identification: src/codegen/hash.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/hash.h"

#include "llvm/IR/Intrinsics.h"

#include "codegen/proxy/runtime_functions_proxy.h"
#include "common/exception.h"

namespace peloton {
namespace codegen {

const std::string Hash::kHashMethodStrings[4] = {"Crc32", "Murmur3", "CityHash",
                                                 "Multiplicative"};

//===----------------------------------------------------------------------===//
// Generate the code to compute the hash of all the given values
//===----------------------------------------------------------------------===//
llvm::Value *Hash::HashValues(CodeGen &codegen,
                              const std::vector<codegen::Value> &vals,
                              Hash::HashMethod method) {
  // The first thing we do is separate all input values into different lists
  // that each hold inputs of one type. For our purposes, we split inputs into
  // lists holding byte-types, short-types, integer-types, long-types and
  // variable-length strings.
  std::vector<llvm::Value *> bytes, shorts, ints, longs;
  std::vector<Hash::Varlen> varlens;

  for (const auto &value : vals) {
    llvm::Value *val = nullptr;
    llvm::Value *len = nullptr;
    value.ValuesForHash(val, len);
    PELOTON_ASSERT(val != nullptr);

    llvm::Type *val_type = val->getType();

    // Put the value into the appropriate bucket
    if (val_type == codegen.BoolType() || val_type == codegen.Int8Type()) {
      bytes.push_back(val);
    } else if (val_type == codegen.Int16Type()) {
      shorts.push_back(val);
    } else if (val_type == codegen.Int32Type()) {
      ints.push_back(val);
    } else if (val_type == codegen.Int64Type()) {
      longs.push_back(val);
    } else if (val_type == codegen.CharPtrType()) {
      varlens.emplace_back(val, value.GetLength());
    }
  }

  ///
  llvm::Type *int64 = codegen.Int64Type();
  std::vector<llvm::Value *> computed;

  // Collect all single-byte values into 4-byte values
  uint32_t index;
  for (index = 0; index + 4 < bytes.size(); index += 4) {
    llvm::Value *first = codegen->CreateShl(
        codegen->CreateSExt(bytes[index], int64), codegen.Const32(24));
    llvm::Value *second = codegen->CreateShl(
        codegen->CreateSExt(bytes[index + 1], int64), codegen.Const32(16));
    llvm::Value *third = codegen->CreateShl(
        codegen->CreateSExt(bytes[index + 2], int64), codegen.Const32(8));
    llvm::Value *four = codegen->CreateSExt(bytes[index + 3], int64);
    llvm::Value *or1 = codegen->CreateOr(first, second);
    llvm::Value *or2 = codegen->CreateOr(or1, third);
    computed.push_back(codegen->CreateOr(or2, four));
  }
  for (; index + 3 < bytes.size(); index += 3) {
    llvm::Value *first = codegen->CreateShl(
        codegen->CreateSExt(bytes[index], int64), codegen.Const32(16));
    llvm::Value *second = codegen->CreateShl(
        codegen->CreateSExt(bytes[index + 1], int64), codegen.Const32(8));
    llvm::Value *third = codegen->CreateSExt(bytes[index + 2], int64);
    llvm::Value *or1 = codegen->CreateOr(first, second);
    computed.push_back(codegen->CreateOr(or1, third));
  }
  for (; index + 2 < bytes.size(); index += 2) {
    llvm::Value *first = codegen->CreateShl(
        codegen->CreateSExt(bytes[index], int64), codegen.Const32(8));
    computed.push_back(
        codegen->CreateOr(first, codegen->CreateSExt(bytes[index + 1], int64)));
  }
  for (; index < bytes.size(); index++) {
    computed.push_back(codegen->CreateSExt(bytes[index], int64));
  }

  // Collect all the 2-byte values into 4-byte values
  for (index = 0; index + 2 < shorts.size(); index += 2) {
    llvm::Value *first = codegen->CreateShl(
        codegen->CreateSExt(shorts[index], int64), codegen.Const32(16));
    llvm::Value *second = codegen->CreateSExt(shorts[index + 1], int64);
    computed.push_back(codegen->CreateOr(first, second));
  }
  for (; index < shorts.size(); index++) {
    computed.push_back(codegen->CreateSExt(shorts[index], int64));
  }

  // Collect all 4-byte values
  for (index = 0; index + 2 < ints.size(); index += 2) {
    llvm::Value *first = codegen->CreateShl(
        codegen->CreateSExt(ints[index], int64), codegen.Const64(32));
    longs.push_back(
        codegen->CreateOr(first, codegen->CreateSExt(ints[index + 1], int64)));
  }
  for (; index < ints.size(); index++) {
    longs.push_back(codegen->CreateSExt(ints[index], int64));
  }

  // Collect the residuals
  for (index = 0; index + 2 < computed.size(); index += 2) {
    llvm::Value *first =
        codegen->CreateShl(computed[index], codegen.Const64(32));
    longs.push_back(codegen->CreateOr(
        first, codegen->CreateSExt(computed[index + 1], int64)));
  }
  for (; index < computed.size(); index++) {
    longs.push_back(computed[index]);
  }

  // Compute the hash of the 64-bit values in 'long' and the buffers in
  // 'buffers' based on the chosen hash method.
  switch (method) {
    case Hash::HashMethod::Crc32:
      return ComputeCRC32Hash(codegen, longs, varlens);
    case Hash::HashMethod::Murmur3:
      return ComputeMurmur3Hash(codegen, longs, varlens);
    default:
      throw Exception{"We currently don't support hash method: " +
                      kHashMethodStrings[static_cast<uint32_t>(method)]};
  }
}

//===----------------------------------------------------------------------===//
// Generate the calculation of a CRC64 hash for the given values
//===----------------------------------------------------------------------===//
llvm::Value *Hash::ComputeCRC32Hash(CodeGen &codegen,
                                    const std::vector<llvm::Value *> &numerics,
                                    const std::vector<Hash::Varlen> &varlens) {
  // The CRC32 generator polynomial
  static constexpr uint32_t kCrc32Generator = 0x04C11DB7;

  llvm::Value *crc_low = codegen.Const64(0);
  llvm::Value *crc_high = codegen.Const64(kCrc32Generator);

  // Hash the numerics
  llvm::Function *crc32_func = llvm::Intrinsic::getDeclaration(
      &codegen.GetModule(), llvm::Intrinsic::x86_sse42_crc32_64_64);
  PELOTON_ASSERT(crc32_func != nullptr);
  for (auto *val : numerics) {
    crc_low = codegen.CallFunc(crc32_func, {crc_low, val});
    crc_high = codegen.CallFunc(crc32_func, {crc_high, val});
  }

  // Let crc64 = (crc_high << 32) | crc_low
  crc_high = codegen->CreateShl(crc_high, 8 * sizeof(uint32_t));
  llvm::Value *crc = codegen->CreateOr(crc_high, crc_low);

  // Now hash the strings
  for (auto &varlen : varlens) {
    llvm::Value *len = codegen->CreateZExt(varlen.len, codegen.Int64Type());
    crc = codegen.Call(RuntimeFunctionsProxy::HashCrc64,
                       {varlen.val, len, crc});
  }

  ///
  return crc;
}

// Here we compute the hash of all the numerics and the varlen buffers into a
// single value
llvm::Value *Hash::ComputeMurmur3Hash(
    CodeGen &codegen, const std::vector<llvm::Value *> &numerics,
    const std::vector<Hash::Varlen> &varlen_buffers) {
  // The magic constants used in Murmur3's final 64-bit avalanche mix
  static constexpr uint64_t kMurmur3C1 = 0xff51afd7ed558ccdLLU;
  static constexpr uint64_t kMurmur3C2 = 0xc4ceb9fe1a85ec53LLU;
  // The magic constant used in Boost's hash_combine()
  static constexpr uint64_t kHashCombine = 0x9e3779b9LLU;

  llvm::Value *magic_const1 = codegen.Const64(kMurmur3C1);
  llvm::Value *magic_const2 = codegen.Const64(kMurmur3C2);
  llvm::Value *combine_const = codegen.Const64(kHashCombine);

  llvm::Value *hash = nullptr;

  // Taken from Murmur3 fmix64(...):
  // uint64_t fmix64(uint64_t k) {
  //   k ^= k >> 33;
  //   k *= 0xff51afd7ed558ccdLLU;
  //   k ^= k >> 33;
  //   k *= 0xc4ceb9fe1a85ec53LLU;
  //   k ^= k >> 33;
  //   return k;
  // }
  for (auto *val : numerics) {
    llvm::Value *k = codegen->CreateXor(val, codegen->CreateLShr(val, 33));
    k = codegen->CreateMul(k, magic_const1);
    k = codegen->CreateXor(k, codegen->CreateLShr(k, 33));
    k = codegen->CreateMul(k, magic_const2);
    k = codegen->CreateXor(k, codegen->CreateLShr(k, 33));

    if (hash == nullptr) {
      hash = k;
    } else {
      // Combine the computed hash 'k' with the overall hash 'hash'
      // Lifted from Boost's hash_combine(...):
      // hash ^= hashed_val + 0x9e3779b9 + (hash << 6) + (hash >> 2);
      llvm::Value *sum = codegen->CreateAdd(k, combine_const);
      sum = codegen->CreateAdd(sum, codegen->CreateShl(hash, 6));
      sum = codegen->CreateAdd(sum, codegen->CreateLShr(hash, 2));
      hash = codegen->CreateXor(hash, sum);
    }
  }

  if (!varlen_buffers.empty()) {
    throw Exception{"Cannot perform a vectorized Murmur3 hash on strings"};
  }

  return hash;
}

}  // namespace codegen
}  // namespace peloton