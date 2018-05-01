

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

void WalRecovery::ParseFromDisk(ReplayStage stage){

  int  buf_size = 4096, buf_rem = 0, buf_curr = 0,total_len=0;
  char *buf = new char[buf_size];
  bool replay_complete = false;

  fstream_.clear();
  fstream_.seekg(0, std::ios::beg);

  while(!replay_complete) {

    int len,  nbytes = buf_size-buf_rem;
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

        total_len += len;
        buf_curr += sizeof(len);

        if(stage==ReplayStage::PASS_1){
          Pass1(buf+buf_curr, len);
        } else if (stage==ReplayStage::PASS_2) {
          Pass2(buf+buf_curr, len);
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

  delete[] buf;
}

// Pass 1:
void WalRecovery::Pass1(char *buf, int len) {

  CopySerializeInput record_decode((const void *)buf, len);
  LogRecordType record_type = (LogRecordType)(record_decode.ReadEnumInSingleByte());
  txn_id_t txn_id = record_decode.ReadLong();

  switch(record_type){
    case LogRecordType::TRANSACTION_BEGIN:{
      if(all_txns_.find(txn_id) != all_txns_.end()){
        LOG_ERROR("Duplicate transaction");
        PELOTON_ASSERT(false);
      }

      auto value = std::make_pair(record_type, len);
      all_txns_.insert(std::make_pair(txn_id, value));

      break;
    }

    case LogRecordType::TRANSACTION_COMMIT:{
      // keeps track of the memory that needs to be allocated.
      log_buffer_size_ += (len+all_txns_[txn_id].second);
    }

    // intentional fall through
    case LogRecordType::TRANSACTION_ABORT:
    case LogRecordType::TUPLE_DELETE:
    case LogRecordType::TUPLE_UPDATE:
    case LogRecordType::TUPLE_INSERT: {

      if(all_txns_.find(txn_id) == all_txns_.end()){
        LOG_ERROR("Transaction not found");
        PELOTON_ASSERT(false);
      }

      all_txns_[txn_id].first = record_type;
      all_txns_[txn_id].second += len;

      break;
    }

    default: {
      LOG_ERROR("Unknown LogRecordType.");
      PELOTON_ASSERT(false);
    }
  }

}

// Pass 2:
void WalRecovery::Pass2(char *buf, int len){
  (void) buf;
  (void) len;
//
//  CopySerializeInput record_decode((const void *)buf, len);
//  LogRecordType record_type = (LogRecordType)(record_decode.ReadEnumInSingleByte());
//  txn_id_t txn_id = record_decode.ReadLong();
//
//
//  if(commited_txns_.find(txn_id)==commited_txns_.end()) {
//    return;
//  }

}

void WalRecovery::ReplayLogFile(){


  size_t curr_txn_offset = 0;

  // Pass 1
  ParseFromDisk(ReplayStage::PASS_1);

  LOG_INFO("all_txns_len = %zu", all_txns_.size());

  for(auto it = all_txns_.begin(); it!=all_txns_.end(); it++){

    if(it->second.first != LogRecordType::TRANSACTION_COMMIT)
      continue;

    auto offset_pair = std::make_pair(curr_txn_offset, curr_txn_offset);
    commited_txns_.insert(std::make_pair(it->first, offset_pair));

    curr_txn_offset += it->second.second;
  }

  for(auto it = commited_txns_.begin(); it!=commited_txns_.end(); it++){
    LOG_INFO("txn_id = %llu offset = %d", it->first, it->second.first);
  }

  // Pass 2
  log_buffer_  = new char[log_buffer_size_];
  ParseFromDisk(ReplayStage::PASS_2);


//  // DEBUG INFO
//  for(auto it = commited_txns.begin(); it!=commited_txns.end(); it++){
//    txn_id_t txn_id = it->first;
//    int epoch_id = txn_id >> 32;
//    int counter = txn_id & 0xFFFFFFFF;
//    LOG_INFO("Replaying peloton txn : %llu, epoch %d, counter = %d", it->first, epoch_id, counter);
//  }
//
//  LOG_INFO("buf_size = %zu", log_buffer_size);

}



}
}