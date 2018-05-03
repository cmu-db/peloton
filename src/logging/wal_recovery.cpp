

#include "catalog/database_catalog.h"
#include "concurrency/transaction_manager_factory.h"
#include "catalog/catalog_defaults.h"
#include "type/ephemeral_pool.h"
#include "catalog/manager.h"
#include "concurrency/epoch_manager_factory.h"
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


void WalRecovery::ParseFromDisk(ReplayStage stage){

  int  buf_size = 4096, buf_rem = 0, buf_curr = 0,total_len=0;
  char *buf = new char[buf_size];
  bool replay_complete = false;

  fstream_.clear();
  fstream_.seekg(0, std::ios::beg);

  while(!replay_complete) {

    int record_len,  nbytes = buf_size-buf_rem;
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

    while(buf_rem >= (int)sizeof(record_len)){
      CopySerializeInput length_decode((const void *)(buf+buf_curr), 4);
      record_len = length_decode.ReadInt();

      if(buf_rem >= (int)(record_len + sizeof(record_len))){

        total_len += record_len;

        int record_offset = buf_curr + sizeof(record_len);

        /***********/
        /*  PASS 1 */
        /***********/

        if(stage==ReplayStage::PASS_1){
          CopySerializeInput record_decode((const void *)(buf+record_offset), record_len);
          LogRecordType record_type = (LogRecordType)(record_decode.ReadEnumInSingleByte());
          txn_id_t txn_id = record_decode.ReadLong();

          int req_mem_size = record_len + sizeof(record_len);

          switch(record_type){
            case LogRecordType::TRANSACTION_BEGIN:{
              if(all_txns_.find(txn_id) != all_txns_.end()){
                LOG_ERROR("Duplicate transaction");
                PELOTON_ASSERT(false);
              }

              auto value = std::make_pair(record_type, req_mem_size);
              all_txns_.insert(std::make_pair(txn_id, value));

              break;
            }

            case LogRecordType::TRANSACTION_COMMIT:{
              // keeps track of the memory that needs to be allocated.
              log_buffer_size_ += (req_mem_size+all_txns_[txn_id].second);
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
              all_txns_[txn_id].second += req_mem_size;

              break;
            }

            default: {
              LOG_ERROR("Unknown LogRecordType.");
              PELOTON_ASSERT(false);
            }
          }
        }

        else if (stage==ReplayStage::PASS_2) {

          CopySerializeInput record_decode((const void *)(buf+record_offset), record_len);
          LogRecordType record_type = (LogRecordType)(record_decode.ReadEnumInSingleByte());
          txn_id_t txn_id = record_decode.ReadLong();

          (void) record_type;

          if(commited_txns_.find(txn_id)!=commited_txns_.end()) {
            int copy_len = record_len + sizeof(record_len);
            int curr_offset = commited_txns_[txn_id].second;

            LOG_INFO("txn %llu writing from %d to %d", txn_id, curr_offset, curr_offset+copy_len-1);

            PELOTON_MEMCPY(log_buffer_+curr_offset, buf+buf_curr, copy_len);
            commited_txns_[txn_id].second += copy_len;
          }
        }

        buf_curr += (record_len + sizeof(record_len));
        buf_rem  -= (record_len + sizeof(record_len));
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



bool WalRecovery::InstallCatalogTuple(LogRecordType record_type, storage::Tuple *tuple,
                               storage::DataTable *table, cid_t cur_cid,
                               ItemPointer location){

  ItemPointer *i = nullptr;

  auto& txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction(IsolationLevelType::SERIALIZABLE);

  oid_t tile_group_id = location.block;
  auto tile_group = table->GetTileGroupById(location.block);
  auto tile_group_header =
          catalog::Manager::GetInstance().GetTileGroup(tile_group_id)->GetHeader();

  if(record_type==LogRecordType::TUPLE_INSERT){

    PL_ASSERT(tile_group != nullptr);

    auto status = tile_group_header->GetEmptyTupleSlot(location.offset);

    (void) status;

    tile_group->CopyTuple(tuple, location.offset);
    bool result = table->InsertTuple(tuple, location, txn, &i);

    (void) result;

    tile_group_header->SetBeginCommitId(location.offset, cur_cid);
    tile_group_header->SetEndCommitId(location.offset, MAX_CID);
    tile_group_header->SetTransactionId(location.offset, INITIAL_TXN_ID);
    tile_group_header->SetNextItemPointer(location.offset,
                                          INVALID_ITEMPOINTER);
    tile_group_header->SetIndirection(location.offset,i);
  }

  txn_manager.CommitTransaction(txn);
  return true;
}

void WalRecovery::ReplaySingleTxn(txn_id_t txn_id){
  int start_offset = commited_txns_[txn_id].first;
  int curr_offset  = start_offset;
  int total_len = all_txns_[txn_id].second;
  std::unique_ptr<type::AbstractPool> pool(new type::EphemeralPool());


  while(total_len > 0){

    CopySerializeInput length_decode((const void *)(log_buffer_+curr_offset), sizeof(int));
    int record_len = length_decode.ReadInt();

    curr_offset += sizeof(int);

    CopySerializeInput record_decode((const void *)(log_buffer_+curr_offset), record_len);

    LogRecordType record_type = (LogRecordType)(record_decode.ReadEnumInSingleByte());
    txn_id_t txn_id = record_decode.ReadLong();
    eid_t  epoch_id = record_decode.ReadLong();
    cid_t commit_id = record_decode.ReadLong();

    (void) txn_id;

    if(max_epoch_id_==INVALID_EID)
      max_epoch_id_ = epoch_id;
    else if(epoch_id>max_epoch_id_)
      max_epoch_id_ = epoch_id;

    switch(record_type){
      case LogRecordType::TUPLE_INSERT: {
        oid_t database_id = (oid_t) record_decode.ReadLong();
        oid_t table_id = (oid_t) record_decode.ReadLong();

        oid_t tg_block = (oid_t) record_decode.ReadLong();
        oid_t tg_offset = (oid_t) record_decode.ReadLong();

        ItemPointer location(tg_block, tg_offset);
        auto table = storage::StorageManager::GetInstance()->GetTableWithOid(
                database_id, table_id);
        auto schema = table->GetSchema();
        auto tg = table->GetTileGroupById(tg_block);

        // Create the tile group if it does not exist
        if (tg.get() == nullptr) {
          table->AddTileGroupWithOidForRecovery(tg_block);
          tg = table->GetTileGroupById(tg_block);
          catalog::Manager::GetInstance().GetNextTileGroupId();
        }

        std::unique_ptr<storage::Tuple> tuple(new storage::Tuple(schema, true));
        for (oid_t oid = 0; oid < schema->GetColumns().size(); oid++) {
          type::Value val = type::Value::DeserializeFrom(
                  record_decode, schema->GetColumn(oid).GetType());
          tuple->SetValue(oid, val, pool.get());
        }


        if (database_id == CATALOG_DATABASE_OID) {  // catalog database oid

          InstallCatalogTuple(record_type, tuple.get(), table, commit_id, location);

          if(table_id==DATABASE_CATALOG_OID) {
              LOG_INFO("replaying database insert");
              auto pg_database = catalog::DatabaseCatalog::GetInstance();
              pg_database->GetNextOid();
          }
        }
        break;
      }

      case LogRecordType::TUPLE_UPDATE: {
        break;
      }

      case LogRecordType::TUPLE_DELETE: {
        break;
      }


      case LogRecordType::TRANSACTION_BEGIN: {
        // NOOP
        break;
      }

      case LogRecordType::TRANSACTION_COMMIT: {
        //(graghura): return probably?
        break;
      }

      case LogRecordType::TRANSACTION_ABORT: {
        LOG_ERROR("Should not be replaying an aborted transaction");
        PL_ASSERT(false);
      }

      default:{
        break;
      }
    }
    curr_offset += record_len;
    total_len -= record_len + sizeof(int);
  }

}

void WalRecovery::ReplayAllTxns(){

  for(auto it = commited_txns_.begin(); it!=commited_txns_.end(); it++){
    ReplaySingleTxn(it->first);
  }

  // reset epoch
  if (max_epoch_id_ != INVALID_EID) {
    auto &epoch_manager = concurrency::EpochManagerFactory::GetInstance();
    epoch_manager.Reset(max_epoch_id_+1);
  }
}


void WalRecovery::ReplayLogFile(){


  size_t curr_txn_offset = 0;

  // Pass 1
  ParseFromDisk(ReplayStage::PASS_1);


  for(auto it = all_txns_.begin(); it!=all_txns_.end(); it++){

    if(it->second.first != LogRecordType::TRANSACTION_COMMIT)
      continue;

    auto offset_pair = std::make_pair(curr_txn_offset, curr_txn_offset);
    commited_txns_.insert(std::make_pair(it->first, offset_pair));

    curr_txn_offset += it->second.second;
  }

  // Pass 2
  log_buffer_  = new char[log_buffer_size_];
  ParseFromDisk(ReplayStage::PASS_2);

  ReplayAllTxns();

}



}
}