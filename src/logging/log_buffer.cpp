

#include "logging/log_record.h"
#include "logging/log_buffer.h"
#include "logging/wal_logger.h"
#include "catalog/manager.h"
#include "common/container_tuple.h"
#include "storage/tile_group.h"


namespace peloton{
namespace logging{

void LogBuffer::WriteRecord(LogRecord &record) {

  // Reserve for the frame length
  size_t start = log_buffer_.Position();
  log_buffer_.WriteInt(0);

  LogRecordType type = record.GetType();
  log_buffer_.WriteEnumInSingleByte(
          static_cast<int>(type));

  log_buffer_.WriteLong(record.GetEpochId());
  log_buffer_.WriteLong(record.GetTransactionId());
  log_buffer_.WriteLong(record.GetCommitId());


  switch (type) {
    case LogRecordType::TUPLE_INSERT: {
      auto &manager = catalog::Manager::GetInstance();
      auto tuple_pos = record.GetItemPointer();
      auto tg = manager.GetTileGroup(tuple_pos.block).get();
      std::vector<catalog::Column> columns;

      // Write down the database id and the table id
      log_buffer_.WriteLong(tg->GetDatabaseId());
      log_buffer_.WriteLong(tg->GetTableId());

      log_buffer_.WriteLong(tuple_pos.block);
      log_buffer_.WriteLong(tuple_pos.offset);

      // Write the full tuple into the buffer
      for (auto schema : tg->GetTileSchemas()) {
        for (auto column : schema.GetColumns()) {
          columns.push_back(column);
        }
      }

      ContainerTuple<storage::TileGroup> container_tuple(tg, tuple_pos.offset);
      for (oid_t oid = 0; oid < columns.size(); oid++) {

        // TODO: check if GetValue() returns variable length fields appropriately
        auto val = container_tuple.GetValue(oid);
        val.SerializeTo(log_buffer_);
      }
      break;
    }
    case LogRecordType::TUPLE_DELETE: {
      LOG_ERROR("Delete logging not supported");
      PL_ASSERT(false);
    }
    case LogRecordType::TUPLE_UPDATE: {
      LOG_ERROR("Update logging not supported");
      PL_ASSERT(false);
    }
    default: {
      LOG_ERROR("Unsupported log record type");
      PL_ASSERT(false);

    }
  }

  // Add the frame length
  // XXX: We rely on the fact that the serializer treat a int32_t as 4 bytes
  int32_t length = log_buffer_.Position() - start - sizeof(int32_t);
  log_buffer_.WriteIntAt(start, length);
}

}
}