

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
  size_t start = output_buffer.Position();
  output_buffer.WriteInt(0);

  LogRecordType type = record.GetType();
  output_buffer.WriteEnumInSingleByte(
          static_cast<int>(type));

  output_buffer.WriteLong(record.GetEpochId());
  output_buffer.WriteLong(record.GetTransactionId());
  output_buffer.WriteLong(record.GetCommitId());


  switch (type) {
    case LogRecordType::TUPLE_INSERT: {
      auto &manager = catalog::Manager::GetInstance();
      auto tuple_pos = record.GetItemPointer();
      auto tg = manager.GetTileGroup(tuple_pos.block).get();
      std::vector<catalog::Column> columns;

      // Write down the database id and the table id
      output_buffer.WriteLong(tg->GetDatabaseId());
      output_buffer.WriteLong(tg->GetTableId());

      output_buffer.WriteLong(tuple_pos.block);
      output_buffer.WriteLong(tuple_pos.offset);

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
        val.SerializeTo(output_buffer);
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
  int32_t length = output_buffer.Position() - start - sizeof(int32_t);
  output_buffer.WriteIntAt(start, length);
}

}
}