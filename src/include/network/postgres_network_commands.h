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
#include "common/internal_types.h"
#include "common/logger.h"
#include "common/macros.h"
#include "network/network_types.h"
#include "traffic_cop/traffic_cop.h"
#include "network/marshal.h"
#include "network/postgres_protocol_utils.h"

#define DEFINE_COMMAND(name, flush)                                        \
class name : public PostgresNetworkCommand {                               \
 public:                                                                   \
  explicit name(std::shared_ptr<ReadBuffer> in)                            \
     : PostgresNetworkCommand(std::move(in), flush) {}                     \
  virtual Transition Exec(PostgresProtocolInterpreter &,                   \
                          PostgresPacketWriter &,                          \
                          size_t) override;                                \
}

namespace peloton {
namespace network {

class PostgresProtocolInterpreter;

class PostgresNetworkCommand {
 public:
  virtual Transition Exec(PostgresProtocolInterpreter &protocol_obj,
                          PostgresPacketWriter &out,
                          size_t thread_id) = 0;

  inline bool FlushOnComplete() { return flush_on_complete_; }
 protected:
  explicit PostgresNetworkCommand(std::shared_ptr<ReadBuffer> in, bool flush)
      : in_(std::move(in)), flush_on_complete_(flush) {}

  std::shared_ptr<ReadBuffer> in_;
 private:
  bool flush_on_complete_;
};

DEFINE_COMMAND(StartupCommand, true);
DEFINE_COMMAND(SimpleQueryCommand, true);
DEFINE_COMMAND(ParseCommand, false);
DEFINE_COMMAND(BindCommand, false);
DEFINE_COMMAND(DescribeCommand, false);
DEFINE_COMMAND(ExecuteCommand, false);
DEFINE_COMMAND(SyncCommand, true);
DEFINE_COMMAND(CloseCommand, false);
DEFINE_COMMAND(TerminateCommand, true);
DEFINE_COMMAND(NullCommand, true);

} // namespace network
} // namespace peloton
