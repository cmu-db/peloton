//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// transaction_record.h
//
// Identification: src/backend/logging/records/transaction_record.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/logging/log_record.h"
#include "backend/common/serializer.h"
#include "backend/common/printable.h"

namespace peloton {
namespace logging {

//===--------------------------------------------------------------------===//
// TransactionRecord
//===--------------------------------------------------------------------===//

class TransactionRecord : public LogRecord, Printable  {
 public:
  TransactionRecord(LogRecordType log_record_type,
                    const cid_t cid = INVALID_CID)
      : LogRecord(log_record_type, cid) {}

  ~TransactionRecord() {
    // Clean up the message
    delete[] message;
  }

  //===--------------------------------------------------------------------===//
  // Serial/Deserialization
  //===--------------------------------------------------------------------===//

  bool Serialize(CopySerializeOutput &output);

  void Deserialize(CopySerializeInputBE &input);

  static size_t GetTransactionRecordSize(void);

  //===--------------------------------------------------------------------===//
  // Accessors
  //===--------------------------------------------------------------------===//

  // Get a string representation for debugging
  const std::string GetInfo() const;

};

}  // namespace logging
}  // namespace peloton
