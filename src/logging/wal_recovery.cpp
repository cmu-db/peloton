#include <fstream>
#include <assert.h>

#include "logging/wal_recovery.h"
#include "logging/log_util.h"
#include "common/logger.h"


namespace peloton{
namespace logging{

void WalRecovery::StartRecovery(){

  if(!LoggingUtil::OpenFile(logpath_.c_str(),
                            std::ifstream::in, fstream_)) {

    LOG_ERROR("Recovery failed");
    return;
  }

  ReplayLogFile();
}


//void WalRecovery::InstallInsertRecord(){
//
//}
//
//void WalRecovery::InstallDeleteRecord(){
//
//}
//
//void WalRecovery::InstallUpdateRecord(){
//
//}

//LogRecord WalRecovery::ReadRecord(CopySerializeInput record_decode){
//
//  LogRecordType record_type =
//          (LogRecordType)(record_decode.ReadEnumInSingleByte());
//
//  eid_t record_eid = record_decode.ReadLong();
//  txn_id_t record_txn_id = record_decode.ReadLong();
//  cid_t record_cid = record_decode.ReadLong();
//
//
//  switch(record_type){
//    case LogRecordType::TRANSACTION_BEGIN:
//    case LogRecordType::TRANSACTION_ABORT:
//    case LogRecordType::TRANSACTION_COMMIT: {
//      return logging::LogRecordFactory::CreateTupleRecord(
//              record_type, record_eid, record_txn_id,
//              record_cid);
//    }
//
//    case LogRecordType::TUPLE_INSERT:
//    case LogRecordType::TUPLE_DELETE: {
//      oid_t database_id = (oid_t)record_decode.ReadLong();
//      oid_t table_id = (oid_t)record_decode.ReadLong();
//
//      oid_t tg_block = (oid_t)record_decode.ReadLong();
//      oid_t tg_offset = (oid_t)record_decode.ReadLong();
//
//      return logging::LogRecordFactory::CreateTupleRecord(
//              record_type, ItemPointer(tg_block, tg_offset),
//              record_eid, record_txn_id,
//              record_cid);
//    }
//
//
//    case LogRecordType::TUPLE_UPDATE:
//      oid_t database_id = (oid_t)record_decode.ReadLong();
//      oid_t table_id = (oid_t)record_decode.ReadLong();
//
//      oid_t old_tg_block = (oid_t)record_decode.ReadLong();
//      oid_t old_tg_offset = (oid_t)record_decode.ReadLong();
//
//      oid_t tg_block = (oid_t)record_decode.ReadLong();
//      oid_t tg_offset = (oid_t)record_decode.ReadLong();
//
//      return logging::LogRecordFactory::CreateTupleRecord(
//              record_type, ItemPointer(old_tg_block, old_tg_offset),
//              ItemPointer(tg_block, tg_offset),
//              record_eid, record_txn_id,
//              record_cid);
//
//
//    default:
//      LOG_ERROR("Unknown LogRecordType");
//      PL_ASSERT(false);
//
//  }
//
//}

//void WalRecovery::ReplayLogRecord(LogRecord &log_record){
//
//}
//

// Pass 1:
void WalRecovery::FirstPass(std::map<txn_id_t, std::pair<LogRecordType, int>> &all_txns,
                           ReplayMap &commited_txns, uint32_t &mem_size){

  int total_length = 0;
  int  buf_size = 4096, buf_rem = 0, buf_curr = 0;
  char *buf = new char[buf_size];

  bool replay_complete = false;

  while(!replay_complete) {

    int len, nbytes = buf_size-buf_rem;
    int bytes_read = LoggingUtil::ReadNBytesFromFile(fstream_, buf+buf_curr, nbytes);

    LOG_INFO("Read %d bytes from the log", bytes_read);

    buf_rem += bytes_read;

    if(bytes_read != nbytes){
      if(fstream_.eof()){
        replay_complete = true;

        if(buf_rem==0)
          continue;
      }
    }

    while(buf_rem >= (int)sizeof(len)){
      CopySerializeInput length_decode((const void *)(buf+buf_curr), 4);
      len = length_decode.ReadInt();

      if(buf_rem >= (int)(len + sizeof(len))){
        buf_curr += sizeof(len);

        CopySerializeInput record_decode((const void *)(buf+buf_curr), len);
        LogRecordType record_type = (LogRecordType)(record_decode.ReadEnumInSingleByte());
        txn_id_t txn_id = record_decode.ReadLong();

        total_length += len;


        //uint64_t id = txn_id & 0xFFFFFFFF;

        switch(record_type){
          case LogRecordType::TRANSACTION_BEGIN:{
            if(all_txns.find(txn_id) != all_txns.end()){
              LOG_ERROR("Duplicate transaction");
              PELOTON_ASSERT(false);
            }

            auto value = std::make_pair(record_type, len);
            all_txns.insert(std::make_pair(txn_id, value));

            break;
          }

          case LogRecordType::TRANSACTION_COMMIT:{
            all_txns[txn_id].first = record_type;
            all_txns[txn_id].second += len;

            // keeps track of the memory that
            // needs to be allocated.
            mem_size += all_txns[txn_id].second;

            commited_txns.insert(std::make_pair(txn_id, all_txns[txn_id].second));
          }

          case LogRecordType::TRANSACTION_ABORT: {
            all_txns[txn_id].first = record_type;
            all_txns[txn_id].second += len;
            break;
          }

          case LogRecordType::TUPLE_DELETE:
          case LogRecordType::TUPLE_UPDATE:
          case LogRecordType::TUPLE_INSERT: {
            all_txns[txn_id].second += len;
            break;
          }

          default: {
            LOG_ERROR("Unknown LogRecordType.");
            PL_ASSERT(false);
          }
        }

        buf_curr += len;
        buf_rem  -= (len + sizeof(len));
      } else {
        break;
      }
    }

    PELOTON_MEMCPY(buf, buf+buf_curr, buf_rem);
    PELOTON_MEMSET(buf+buf_rem, 0, buf_size - buf_rem);
    buf_curr = buf_rem;
  }

  LOG_INFO("total_length = %d", total_length);

  delete[] buf;
}



void WalRecovery::ReplayLogFile(){

  uint32_t mem_size = 0;
  ReplayMap commited_txns;
  std::map<txn_id_t, std::pair<LogRecordType, int>> all_txns;

  // Pass 1
  FirstPass(all_txns, commited_txns, mem_size);

  //char *log_buffer  = new char[mem_size];

  // Pass 2
//  SecondPass(commited_txns, log_buffer);

  for(auto it = commited_txns.begin(); it!=commited_txns.end(); it++){
    txn_id_t txn_id = it->first;
    int epoch_id = txn_id >> 32;
    int counter = txn_id & 0xFFFFFFFF;
    LOG_INFO("Replaying peloton txn : %llu, epoch %d, counter = %d", it->first, epoch_id, counter);
  }

  LOG_INFO("buf_size = %u", mem_size);



}



}
}