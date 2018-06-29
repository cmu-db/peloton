//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// postgres_wire_protocol.h
//
// Identification: src/include/network/postgres_wire_protocol.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "planner/plan_util.h"
#include "network/postgres_protocol_interpreter.h"

#define MAKE_COMMAND(type)                                 \
  std::static_pointer_cast<PostgresNetworkCommand, type>(  \
      std::make_shared<type>(std::move(curr_input_packet_.buf_)))

namespace peloton {
namespace network {
Transition PostgresProtocolInterpreter::Process(std::shared_ptr<ReadBuffer> in,
                                                std::shared_ptr<WriteQueue> out,
                                                CallbackFunc callback) {
  if (!TryBuildPacket(in)) return Transition::NEED_READ;
  std::shared_ptr<PostgresNetworkCommand> command = PacketToCommand();
  curr_input_packet_.Clear();
  PostgresPacketWriter writer(*out);
  if (command->FlushOnComplete()) out->ForceFlush();
  return command->Exec(*this, writer, callback);
}

bool PostgresProtocolInterpreter::TryBuildPacket(std::shared_ptr<ReadBuffer> &in) {
  if (!TryReadPacketHeader(in)) return false;

  size_t size_needed = curr_input_packet_.extended_
                       ? curr_input_packet_.len_
                           - curr_input_packet_.buf_->BytesAvailable()
                       : curr_input_packet_.len_;
  if (!in->HasMore(size_needed)) return false;

  // copy bytes only if the packet is longer than the read buffer,
  // otherwise we can use the read buffer to save space
  if (curr_input_packet_.extended_)
    curr_input_packet_.buf_->FillBufferFrom(*in, size_needed);
  return true;
}

bool PostgresProtocolInterpreter::TryReadPacketHeader(std::shared_ptr<ReadBuffer> &in) {
  if (curr_input_packet_.header_parsed_) return true;

  // Header format: 1 byte message type (only if non-startup)
  //              + 4 byte message size (inclusive of these 4 bytes)
  size_t header_size = startup_ ? sizeof(int32_t) : 1 + sizeof(int32_t);
  // Make sure the entire header is readable
  if (!in->HasMore(header_size)) return false;

  // The header is ready to be read, fill in fields accordingly
  if (!startup_)
    curr_input_packet_.msg_type_ = in->ReadRawValue<NetworkMessageType>();
  curr_input_packet_.len_ = in->ReadValue<uint32_t>() - sizeof(uint32_t);

  // Extend the buffer as needed
  if (curr_input_packet_.len_ > in->Capacity()) {
    LOG_INFO("Extended Buffer size required for packet of size %ld",
             curr_input_packet_.len_);
    // Allocate a larger buffer and copy bytes off from the I/O layer's buffer
    curr_input_packet_.buf_ =
        std::make_shared<ReadBuffer>(curr_input_packet_.len_);
    curr_input_packet_.extended_ = true;
  } else {
    curr_input_packet_.buf_ = in;
  }

  curr_input_packet_.header_parsed_ = true;
  return true;
}

std::shared_ptr<PostgresNetworkCommand> PostgresProtocolInterpreter::PacketToCommand() {
  if (startup_) return MAKE_COMMAND(StartupCommand);
  switch (curr_input_packet_.msg_type_) {
    case NetworkMessageType::SIMPLE_QUERY_COMMAND:
      return MAKE_COMMAND(SimpleQueryCommand);
    case NetworkMessageType::PARSE_COMMAND:
      return MAKE_COMMAND(ParseCommand);
    case NetworkMessageType::BIND_COMMAND
      :return MAKE_COMMAND(BindCommand);
    case NetworkMessageType::DESCRIBE_COMMAND:
      return MAKE_COMMAND(DescribeCommand);
    case NetworkMessageType::EXECUTE_COMMAND:
      return MAKE_COMMAND(ExecuteCommand);
    case NetworkMessageType::SYNC_COMMAND
      :return MAKE_COMMAND(SyncCommand);
    case NetworkMessageType::CLOSE_COMMAND:
      return MAKE_COMMAND(CloseCommand);
    case NetworkMessageType::TERMINATE_COMMAND:
      return MAKE_COMMAND(TerminateCommand);
    default:
      throw NetworkProcessException("Unexpected Packet Type: " +
          std::to_string(static_cast<int>(curr_input_packet_.msg_type_)));
  }
}

void PostgresProtocolInterpreter::CompleteCommand(PostgresPacketWriter &out,
                                                  const QueryType &query_type,
                                                  int rows) {

  std::string tag = QueryTypeToString(query_type);
  switch (query_type) {
    /* After Begin, we enter a txn block */
    case QueryType::QUERY_BEGIN:
      state_.txn_state_ = NetworkTransactionStateType::BLOCK;
      break;
      /* After commit, we end the txn block */
    case QueryType::QUERY_COMMIT:
      /* After rollback, the txn block is ended */
    case QueryType::QUERY_ROLLBACK:
      state_.txn_state_ = NetworkTransactionStateType::IDLE;
      break;
    case QueryType::QUERY_INSERT:
      tag += " 0 " + std::to_string(rows);
      break;
    case QueryType::QUERY_CREATE_TABLE:
    case QueryType::QUERY_CREATE_DB:
    case QueryType::QUERY_CREATE_INDEX:
    case QueryType::QUERY_CREATE_TRIGGER:
    case QueryType::QUERY_PREPARE:
      break;
    default:
      tag += " " + std::to_string(rows);
  }
  out.BeginPacket(NetworkMessageType::COMMAND_COMPLETE)
      .AppendString(tag);
}

void PostgresProtocolInterpreter::ExecQueryMessageGetResult(PostgresPacketWriter &out,
                                                            ResultType status) {
  std::vector<FieldInfo> tuple_descriptor;
  if (status == ResultType::SUCCESS) {
    tuple_descriptor = state_.statement_->GetTupleDescriptor();
  } else if (status == ResultType::FAILURE) {  // check status
    out.WriteErrorResponse({{NetworkMessageType::HUMAN_READABLE_ERROR,
                             state_.error_message_}});
    out.WriteReadyForQuery(NetworkTransactionStateType::IDLE);
    return;
  } else if (status == ResultType::TO_ABORT) {
    std::string error_message =
        "current transaction is aborted, commands ignored until end of "
        "transaction block";
    out.WriteErrorResponse(
        {{NetworkMessageType::HUMAN_READABLE_ERROR, error_message}});
    out.WriteReadyForQuery(NetworkTransactionStateType::IDLE);
    return;
  }

  // send the attribute names
  out.WriteTupleDescriptor(tuple_descriptor);
  out.WriteDataRows(state_.result_, tuple_descriptor.size());
  // TODO(Tianyu): WTF?
  state_.rows_affected_ = state_.result_.size() / tuple_descriptor.size();

  CompleteCommand(out,
                  state_.statement_->GetQueryType(),
                  state_.rows_affected_);

  out.WriteReadyForQuery(NetworkTransactionStateType::IDLE);
}

void PostgresProtocolInterpreter::ExecExecuteMessageGetResult(PostgresPacketWriter &out, peloton::ResultType status) {
  const auto &query_type = state_.statement_->GetQueryType();
  switch (status) {
    case ResultType::FAILURE:
      LOG_ERROR("Failed to execute: %s",
              state_.error_message_.c_str());
      out.WriteErrorResponse({{NetworkMessageType::HUMAN_READABLE_ERROR,
                               state_.error_message_}});
      return;
    case ResultType::ABORTED: {
      // It's not an ABORT query but Peloton aborts the transaction
      if (query_type != QueryType::QUERY_ROLLBACK) {
        LOG_DEBUG("Failed to execute: Conflicting txn aborted");
        // Send an error response if the abort is not due to ROLLBACK query
        out.WriteErrorResponse({{NetworkMessageType::SQLSTATE_CODE_ERROR,
                            SqlStateErrorCodeToString(
                                SqlStateErrorCode::SERIALIZATION_ERROR)}});
      }
      return;
    }
    case ResultType::TO_ABORT: {
      // User keeps issuing queries in a transaction that should be aborted
      std::string error_message =
          "current transaction is aborted, commands ignored until end of "
          "transaction block";
      out.WriteErrorResponse(
          {{NetworkMessageType::HUMAN_READABLE_ERROR, error_message}});
      out.WriteReadyForQuery(NetworkTransactionStateType::IDLE);
      return;
    }
    default: {
      auto tuple_descriptor =
          state_.statement_->GetTupleDescriptor();
      out.WriteDataRows(state_.result_, tuple_descriptor.size());
      state_.rows_affected_ = state_.result_.size() / tuple_descriptor.size();
      CompleteCommand(out, query_type, state_.rows_affected_);
      return;
    }
  }
}

ResultType PostgresProtocolInterpreter::ExecQueryExplain(const std::string &query,
                                                         peloton::parser::ExplainStatement &explain_stmt) {
  std::unique_ptr<parser::SQLStatementList> unnamed_sql_stmt_list(
      new parser::SQLStatementList());
  unnamed_sql_stmt_list->PassInStatement(std::move(explain_stmt.real_sql_stmt));
  auto stmt = tcop::Tcop::GetInstance().PrepareStatement(state_, "explain", query,
                                                         std::move(unnamed_sql_stmt_list));
  ResultType status;
  if (stmt != nullptr) {
    state_.statement_ = stmt;
    std::vector<std::string> plan_info = StringUtil::Split(
        planner::PlanUtil::GetInfo(stmt->GetPlanTree().get()), '\n');
    const std::vector<FieldInfo> tuple_descriptor = {
        tcop::Tcop::GetInstance().GetColumnFieldForValueType("Query plan",
                                                 type::TypeId::VARCHAR)};
    stmt->SetTupleDescriptor(tuple_descriptor);
    state_.result_ = plan_info;
    status = ResultType::SUCCESS;
  } else {
    status = ResultType::FAILURE;
  }
  return status;
}

bool PostgresProtocolInterpreter::HardcodedExecuteFilter(peloton::QueryType query_type) {
  switch (query_type) {
    // Skip SET
    case QueryType::QUERY_SET:
    case QueryType::QUERY_SHOW:
      return false;
      // Skip duplicate BEGIN
    case QueryType::QUERY_BEGIN:
      if (state_.txn_state_ == NetworkTransactionStateType::BLOCK) {
        return false;
      }
      break;
      // Skip duplicate Commits and Rollbacks
    case QueryType::QUERY_COMMIT:
    case QueryType::QUERY_ROLLBACK:
      if (state_.txn_state_ == NetworkTransactionStateType::IDLE) {
        return false;
      }
    default:
      break;
  }
  return true;
}
} // namespace network
} // namespace peloton