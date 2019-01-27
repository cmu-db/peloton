//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// postgres_network_commands.h
//
// Identification: src/include/network/postgres_network_commands.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once
#include <utility>
#include "type/value_factory.h"
#include "common/internal_types.h"
#include "common/logger.h"
#include "common/macros.h"
#include "network/network_types.h"
#include "network/marshal.h"
#include "network/postgres_protocol_utils.h"

#define DEFINE_COMMAND(name, flush)                                        \
class name : public PostgresNetworkCommand {                               \
 public:                                                                   \
  explicit name(PostgresInputPacket &in)                                   \
     : PostgresNetworkCommand(in, flush) {}                                \
  virtual Transition Exec(PostgresProtocolInterpreter &,                   \
                          PostgresPacketWriter &,                          \
                          CallbackFunc) override;                          \
}

namespace peloton {
namespace network {

class PostgresProtocolInterpreter;

class PostgresNetworkCommand {
 public:
  virtual Transition Exec(PostgresProtocolInterpreter &interpreter,
                          PostgresPacketWriter &out,
                          CallbackFunc callback) = 0;

  inline bool FlushOnComplete() { return flush_on_complete_; }

 protected:
  explicit PostgresNetworkCommand(PostgresInputPacket &in, bool flush)
      : in_(in.buf_->ReadIntoView(in.len_)), flush_on_complete_(flush) {}

  std::vector<PostgresValueType> ReadParamTypes();

  std::vector<PostgresDataFormat> ReadParamFormats();

  // Why are bind parameter and param values different?
  void ReadParamValues(std::vector<BindParameter> &bind_parameters,
                       std::vector<type::Value> &param_values,
                       const std::vector<PostgresValueType> &param_types,
                       const std::vector<PostgresDataFormat> &formats);

  void ProcessTextParamValue(std::vector<BindParameter> &bind_parameters,
                             std::vector<type::Value> &param_values,
                             PostgresValueType type,
                             int32_t len);

  void ProcessBinaryParamValue(std::vector<BindParameter> &bind_parameters,
                               std::vector<type::Value> &param_values,
                               PostgresValueType type,
                               int32_t len);

  std::vector<PostgresDataFormat> ReadResultFormats(size_t tuple_size);

  ReadBufferView in_;
 private:
  bool flush_on_complete_;
};

DEFINE_COMMAND(SimpleQueryCommand, true);
DEFINE_COMMAND(ParseCommand, false);
DEFINE_COMMAND(BindCommand, false);
DEFINE_COMMAND(DescribeCommand, false);
DEFINE_COMMAND(ExecuteCommand, false);
DEFINE_COMMAND(SyncCommand, true);
DEFINE_COMMAND(CloseCommand, false);
DEFINE_COMMAND(TerminateCommand, true);

} // namespace network
} // namespace peloton
