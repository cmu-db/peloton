//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// bloom_filter_test.cpp
//
// Identification: test/codegen/bloom_filter_test.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <unordered_set>
#include <vector>

#include "codegen/codegen.h"
#include "codegen/function_builder.h"
#include "codegen/lang/if.h"
#include "codegen/lang/loop.h"
#include "codegen/proxy/bloom_filter_proxy.h"
#include "codegen/testing_codegen_util.h"

namespace peloton {
namespace test {

class BloomFilterCodegenTest : public PelotonTest {};

TEST_F(BloomFilterCodegenTest, FalsePositiveRateTest) {
  codegen::CodeContext code_context;
  codegen::CodeGen codegen(code_context);

  // Generate an array of distinct random numbers.
  // Insert the first half into the bloom filter and
  // use the second half to test the false positive rate
  const int size = 100000;
  std::unordered_set<int> number_set;
  while (number_set.size() != size) {
    number_set.insert(rand());
  }
  std::vector<int> numbers(number_set.begin(), number_set.end());

  // Build the test function that has the following logic:
  // define @TestBloomFilter(BloomFilter* bloom_filter, i32* numbers, i32 size,
  //                         i32* false_positive_cnt) {
  //   // Insert the first half into the bloom filter
  //   for (i32 i = 0; i < size / 2; i++) {
  //      bloom_filter.Add(numbers[i]);
  //   }
  //   // Test the second half and measure false positive cnt
  //   for (i32 i = size / 2; i < size; i++) {
  //      if (bloom_filter.Contains) {
  //         *false_positive_cnt ++;
  //      }
  //   }
  // }
  codegen::FunctionBuilder func{
      code_context,
      "TestBloomFilter",
      codegen.VoidType(),
      {{"bloom_filter",
        codegen::BloomFilterProxy::GetType(codegen)->getPointerTo()},
       {"numbers", codegen.Int32Type()->getPointerTo()},
       {"size", codegen.Int32Type()},
       {"false_positive_cnt", codegen.Int32Type()->getPointerTo()}}};
  {
    llvm::Value *bloom_filter = func.GetArgumentByPosition(0);
    llvm::Value *number_array = func.GetArgumentByPosition(1);
    llvm::Value *size_val = func.GetArgumentByPosition(2);
    llvm::Value *false_positive_cnt = func.GetArgumentByPosition(3);
    llvm::Value *index = codegen.Const32(0);
    llvm::Value *half_size = codegen->CreateUDiv(size_val, codegen.Const32(2));
    llvm::Value *finish_cond = codegen->CreateICmpULT(index, half_size);
    
    // Loop that inserts the first half of array into the bloom filter
    codegen::lang::Loop insert_loop{codegen, finish_cond, {{"i", index}}};
    {
      index = insert_loop.GetLoopVar(0);
      
      // Get numbers[i]
      llvm::Value *number = codegen->CreateLoad(
          codegen->CreateInBoundsGEP(codegen.Int32Type(), number_array, index));
      codegen::Value number_val{codegen::type::Type(type::INTEGER, false),
                                number};
      // Insert numbers[i] into bloom filter
      codegen::BloomFilter::Add(codegen, bloom_filter, {number_val});

      index = codegen->CreateAdd(index, codegen.Const32(1));
      insert_loop.LoopEnd(codegen->CreateICmpULT(index, half_size), {index});
    }

    // Loop that test the false positive rate
    finish_cond = codegen->CreateICmpULT(half_size, size_val);
    codegen::lang::Loop test_loop{codegen, finish_cond, {{"i", half_size}}};
    {
      index = test_loop.GetLoopVar(0);

      // Get numbers[i]
      llvm::Value *number = codegen->CreateLoad(
          codegen->CreateInBoundsGEP(codegen.Int32Type(), number_array, index));
      codegen::Value number_val{codegen::type::Type(type::INTEGER, false),
                                number};
      
      // Test if numbers[i] is contained in bloom filter
      llvm::Value *contains =
          codegen::BloomFilter::Contains(codegen, bloom_filter, {number_val});
      codegen::lang::If if_contains{codegen, contains};
      {
        codegen->CreateStore(
            codegen->CreateAdd(codegen->CreateLoad(false_positive_cnt),
                               codegen.Const32(1)), false_positive_cnt);
      }
      if_contains.EndIf();

      index = codegen->CreateAdd(index, codegen.Const32(1));
      test_loop.LoopEnd(codegen->CreateICmpULT(index, size_val), {index});
    }

    func.ReturnAndFinish();
  }

  ASSERT_TRUE(code_context.Compile());

  typedef void (*ftype)(codegen::BloomFilter * bloom_filter, int *, int, int *);
  ftype f = (ftype)code_context.GetRawFunctionPointer(func.GetFunction());

  codegen::BloomFilter bloom_filter;
  bloom_filter.Init(size / 2);
  int num_false_positive = 0;

  // Run the function
  f(&bloom_filter, &numbers[0], size, &num_false_positive);
  double actual_FPR = (double)num_false_positive / (size / 2);
  double expected_FPR = codegen::BloomFilter::kFalsePositiveRate;
  LOG_DEBUG("Expected FPR %f, Actula FPR: %f", expected_FPR, actual_FPR);

  // Difference should be within 10%
  EXPECT_LT(expected_FPR * 0.9, actual_FPR);
  EXPECT_LT(actual_FPR, expected_FPR * 1.1);

  bloom_filter.Destroy();
}

}  // namespace test
}  // namespace peloton
