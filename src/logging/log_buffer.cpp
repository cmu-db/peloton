
#include "logging/wal_logger.h"
#include "catalog/manager.h"
#include "common/container_tuple.h"
#include "storage/tile_group.h"
#include "../include/type/value.h"
#include "../include/logging/log_buffer.h"
#include "../include/common/internal_types.h"
#include "type/value_peeker.h"


namespace peloton{
namespace logging{



void LogBuffer::WriteRecord(LogRecord &record) {

  // Reserve for the frame length
  size_t start = log_buffer_.Position();
  log_buffer_.WriteInt(0);


  LogRecordType type = record.GetType();
  log_buffer_.WriteEnumInSingleByte(
          static_cast<int>(type));

  log_buffer_.WriteLong(record.GetTransactionId());
  log_buffer_.WriteLong(record.GetEpochId());
  log_buffer_.WriteLong(record.GetCommitId());

  switch (type) {
    case LogRecordType::TUPLE_INSERT: {
      auto &manager = catalog::Manager::GetInstance();
      auto tuple_pos = record.GetItemPointer();
      auto tg = manager.GetTileGroup(tuple_pos.block).get();
      std::vector<catalog::Column> columns;

      // Write down the database id and the table id
      log_buffer_.WriteLong(tg->GetDatabaseId());
      log_buffer_.WriteLong(record.GetSchemaId());
      log_buffer_.WriteLong(tg->GetTableId());

      log_buffer_.WriteLong(tuple_pos.block);
      log_buffer_.WriteLong(tuple_pos.offset);

      peloton::type::Value *values_array = reinterpret_cast<peloton::type::Value *>(record.GetValuesArray());
      for (uint32_t i = 0; i < record.GetNumValues(); i++) {
        values_array[i].SerializeTo(log_buffer_);
      }

      break;
    }
    case LogRecordType::TUPLE_DELETE: {
      auto &manager = catalog::Manager::GetInstance();
      auto tuple_pos = record.GetItemPointer();
      auto tg = manager.GetTileGroup(tuple_pos.block).get();

      // Write down the database id and the table id
      log_buffer_.WriteLong(tg->GetDatabaseId());
      log_buffer_.WriteLong(record.GetSchemaId());
      log_buffer_.WriteLong(tg->GetTableId());

      log_buffer_.WriteLong(tuple_pos.block);
      log_buffer_.WriteLong(tuple_pos.offset);

      break;
    }
    case LogRecordType::TUPLE_UPDATE: {
      auto &manager = catalog::Manager::GetInstance();
      auto tuple_pos = record.GetItemPointer();
      auto old_tuple_pos = record.GetOldItemPointer();
      auto tg = manager.GetTileGroup(tuple_pos.block).get();

      // Write down the database id and the table id
      log_buffer_.WriteLong(tg->GetDatabaseId());
      log_buffer_.WriteLong(record.GetSchemaId());
      log_buffer_.WriteLong(tg->GetTableId());

      log_buffer_.WriteLong(old_tuple_pos.block);
      log_buffer_.WriteLong(old_tuple_pos.offset);

      log_buffer_.WriteLong(tuple_pos.block);
      log_buffer_.WriteLong(tuple_pos.offset);

      log_buffer_.WriteLong(record.GetNumValues());

      peloton::type::Value *values_array = reinterpret_cast<peloton::type::Value *>(record.GetValuesArray());
      TargetList *offsets = record.GetOffsets();
      for (uint32_t i = 0; i < record.GetNumValues(); i++) {
        log_buffer_.WriteInt(((*offsets)[i]).first);
        values_array[i].SerializeTo(log_buffer_);
      }

      break;
    }

    case LogRecordType::TRANSACTION_BEGIN:
    case LogRecordType::TRANSACTION_COMMIT:
    case LogRecordType::TRANSACTION_ABORT: {
      break;
    }

    default: {
      PELOTON_ASSERT(false);
    }
  }

  // Add the frame length
  // XXX: We rely on the fact that the serializer treat a int32_t as 4 bytes
  int32_t length = log_buffer_.Position() - start - sizeof(int32_t);
  log_buffer_.WriteIntAt(start, length);
}

}
}
