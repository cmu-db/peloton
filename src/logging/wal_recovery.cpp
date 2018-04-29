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

void WalRecovery::ParseLog(std::map<txn_id_t, LogRecordType> &all_txns){

  int  buf_size = 4096, buf_rem = 0, buf_curr = 0;
  char *buf = new char[buf_size];

  bool replay_complete = false;

  while(!replay_complete) {

    int len, lensize = sizeof(len), nbytes = buf_size-buf_rem;
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

    while(buf_rem >= (lensize)){
      CopySerializeInput length_decode((const void *)(buf+buf_curr), 4);
      len = length_decode.ReadInt();

      LOG_INFO("length = %d", len);

      if(buf_rem >= (len + lensize)){
        buf_curr += lensize;

        CopySerializeInput record_decode((const void *)(buf+buf_curr), len);
        LogRecordType record_type = (LogRecordType)(record_decode.ReadEnumInSingleByte());
        txn_id_t txn_id = record_decode.ReadLong();


        switch(record_type){
          case LogRecordType::TRANSACTION_BEGIN:{
            if(all_txns.find(txn_id) != all_txns.end()){
              LOG_ERROR("Duplicate transaction");
              PL_ASSERT(false);
            }
            all_txns.insert(std::make_pair(txn_id, record_type));
            break;
          }

          case LogRecordType::TRANSACTION_COMMIT:
          case LogRecordType::TRANSACTION_ABORT: {
            all_txns[txn_id] = record_type;
            break;
          }

          default: {

          }
        }

        buf_curr += len;
        buf_rem  -= (len + lensize);
      } else {
        break;
      }
    }

    PL_MEMCPY(buf, buf+buf_curr, buf_rem);
    PL_MEMSET(buf+buf_rem, 0, buf_size - buf_rem);
    buf_curr = buf_rem;
  }

  delete[] buf;
}

void WalRecovery::ReplayLogFile(){

  std::map<txn_id_t, LogRecordType> all_txns;
  ParseLog(all_txns);


  for(auto it = all_txns.begin(); it!=all_txns.end(); it++){
    if(it->second==LogRecordType::TRANSACTION_COMMIT){
      LOG_INFO("Replaying peloton txn : %" PRId64, it->first);
    }
  }



}



}
}