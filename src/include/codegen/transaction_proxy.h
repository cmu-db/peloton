//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// transaction_proxy.h
//
// Identification: src/include/codegen/transaction_proxy.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/codegen.h"
#include "concurrency/transaction.h"

namespace peloton {
namespace codegen {

class TransactionProxy {
 public:
  // Get the LLVM type for a transaction
  static llvm::Type *GetType(CodeGen &codegen) {
    static const std::string kTransactionName =
        "peloton::concurrency::Transaction";
    // Check if the data table type has already been registered in the current
    // codegen context
    auto transaction_type = codegen.LookupTypeByName(kTransactionName);
    if (transaction_type != nullptr) {
      return transaction_type;
    }

    // Type isn't cached, create a new one
    auto *opaque_byte_array = llvm::ArrayType::get(
        codegen.Int8Type(), sizeof(concurrency::Transaction));
    return llvm::StructType::create(codegen.GetContext(), {opaque_byte_array},
                                    kTransactionName);
  }
};

}  // namespace codegen
}  // namespace peloton
