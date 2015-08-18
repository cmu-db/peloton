/*-------------------------------------------------------------------------
 *
 * ariesfrontendlogger.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/logging/ariesfrontendlogger.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "backend/logging/loggers/ariesfrontendlogger.h"
#include "backend/logging/loggers/ariesbackendlogger.h"
#include "backend/logging/logmanager.h"

#include <sys/stat.h>
#include <sys/mman.h>

#include "backend/storage/backend_vm.h"
#include "backend/catalog/manager.h"
#include "backend/catalog/schema.h"
#include "backend/storage/database.h"
#include "backend/storage/data_table.h"
#include "backend/storage/tuple.h"

namespace peloton {
namespace logging {

AriesFrontendLogger::AriesFrontendLogger(){

  logging_type = LOGGING_TYPE_ARIES;
  // open log file and file descriptor
  logFile = fopen( filename.c_str(),"ab+");
  if(logFile == NULL){
    LOG_ERROR("LogFile is NULL");
  }
  logFileFd = fileno(logFile);
  if( logFileFd == -1){
    LOG_ERROR("LogFileFd is -1");
  }
}

AriesFrontendLogger::~AriesFrontendLogger(){

  int ret = fclose(logFile);
  if( ret != 0 ){
    LOG_ERROR("Error occured while closing LogFile");
  }
}

/**
 * @brief MainLoop
 */
//FIXME :: Performance issue remains
void AriesFrontendLogger::MainLoop(void) {

  auto& logManager = LogManager::GetInstance();

  while(logManager.GetLoggingStatus(LOGGING_TYPE_ARIES) == LOGGING_STATUS_TYPE_STANDBY ){
    sleep(1);
  }

  switch(logManager.GetLoggingStatus(LOGGING_TYPE_ARIES)){
    case LOGGING_STATUS_TYPE_ONGOING:
      Recovery();
    break;

    default:
      return;
    break;
  }

  while(logManager.GetLoggingStatus(LOGGING_TYPE_ARIES) == LOGGING_STATUS_TYPE_ONGOING){
    sleep(1);

    // Collect LogRecords from BackendLogger 
    CollectLogRecord();

    if( GetLogRecordCount() >= aries_global_queue_size ){
      Flush();
    }
  }
}

/**
 * @brief Collect the LogRecord from BackendLogger
 */
void AriesFrontendLogger::CollectLogRecord(void) {
  backend_loggers = GetBackendLoggers();

  // Look over current frontend logger's backend loggers
  for( auto backend_logger : backend_loggers){
    auto commit_offset = ((AriesBackendLogger*)backend_logger)->GetCommitOffset();

    // Skip this backend_logger, nothing to do
    if( commit_offset == 0 ) continue; 

    for(oid_t log_record_itr=0; log_record_itr<commit_offset; log_record_itr++){
      // Copy LogRecord from backend_logger to here
      aries_global_queue.push_back(backend_logger->GetLogRecord(log_record_itr));
    }
    backend_logger->Truncate(commit_offset);
  }
}

/**
 * @brief flush all record, for now it's just printing out
 */
void AriesFrontendLogger::Flush(void) {

  for( auto record : aries_global_queue ){
    fwrite( record->GetSerializedData(), 
            sizeof(char), 
            record->GetSerializedDataSize(), 
            logFile);
    //TODO :: record LSN here
  }

  //FIXME Is it right way to use fflush and fsync together? 
  int ret = fflush(logFile);
  if( ret != 0 ){
    LOG_ERROR("Error occured in fflush(%d)", ret);
  }

  ret = fsync(logFileFd);
  if( ret != 0 ){
    LOG_ERROR("Error occured in fsync(%d)", ret);
  }

  for( auto record : aries_global_queue ){
    delete record;
  }
  aries_global_queue.clear();
}

void AriesFrontendLogger::Recovery() {

  if(LogFileSize() > 0){
    bool EOF_OF_LOG_FILE = false;

    auto &txn_manager = concurrency::TransactionManager::GetInstance();
    auto recovery_txn = txn_manager.BeginTransaction();

    while(!EOF_OF_LOG_FILE){

      // Read the first sing bite so that we can distinguish log record type
      switch(GetNextLogRecordType()){

        case LOGRECORD_TYPE_TRANSACTION_BEGIN:
          AddTxnToRecoveryTable();
          break;

        case LOGRECORD_TYPE_TRANSACTION_END:
          RemoveTxnFromRecoveryTable();
          break;

        case LOGRECORD_TYPE_TRANSACTION_COMMIT:
          CommitTuplesFromRecoveryTable(recovery_txn);
          break;

        case LOGRECORD_TYPE_TRANSACTION_ABORT:
          AbortTuplesFromRecoveryTable();
          break;

        case LOGRECORD_TYPE_TUPLE_INSERT:
          InsertTuple(recovery_txn);
          break;

        case LOGRECORD_TYPE_TUPLE_DELETE:
          DeleteTuple(recovery_txn);
          break;

        case LOGRECORD_TYPE_TUPLE_UPDATE:
          UpdateTuple(recovery_txn);
          break;

        default:
          EOF_OF_LOG_FILE = true;
          break;
      }
    }

    txn_manager.CommitTransaction(recovery_txn);

    // Abort remained txn in txn_table
    AbortRemainedTxnInRecoveryTable();
  }

//After finishing recovery, set the next oid with maximum oid
//  auto &manager = catalog::Manager::GetInstance();
//  assert(max_oid);
//  manager.SetNextOid(max_oid);
}

/**
 * @brief Read single byte so that we can distinguish the log record type
 * @return LogRecordType otherwise return invalid log record type if there is no more log in the log file
 */
LogRecordType AriesFrontendLogger::GetNextLogRecordType(){
  char buffer;
  int ret = fread((void*)&buffer, 1, sizeof(char), logFile);
  if( ret <= 0 ){
    return LOGRECORD_TYPE_INVALID;
  }
  CopySerializeInput input(&buffer, sizeof(char));
  LogRecordType log_record_type = (LogRecordType)(input.ReadEnumInSingleByte());
  return log_record_type;
}

/**
 * @brief Measure the size of log file
 * @return the size if a file exists otherwise 0
 */
size_t AriesFrontendLogger::LogFileSize(){
  struct stat logStats;   
  if(stat(filename.c_str(), &logStats) == 0){
    fstat(logFileFd, &logStats);
    return logStats.st_size;
  }else{
    return 0;
  }
}

// TupleRecord consiss of two frame ( header and Body)
// Transaction Record has a single frame
size_t AriesFrontendLogger::GetNextFrameSize(){
  size_t frame_size;
  char buffer[sizeof(int32_t)];
  size_t ret = fread(buffer, 1, sizeof(buffer), logFile);
  if( ret <= 0 ){
    LOG_ERROR("Error occured in fread ");
  }

  // Read next 4 bytes as an integer
  CopySerializeInput frameCheck(buffer, sizeof(int32_t));
  frame_size = (frameCheck.ReadInt())+sizeof(int32_t);;

  /* go back 4 bytes */
  ret = fseek(logFile, -sizeof(int32_t), SEEK_CUR);
  if(ret == -1){
    LOG_ERROR("Error occured in fseek ");
  }
  return frame_size;
}

/**
 * @brief Get global_queue size
 * @return the size of global_queue
 */
size_t AriesFrontendLogger::GetLogRecordCount() const{
  return aries_global_queue.size();
}

void AriesFrontendLogger::ReadTxnRecord(TransactionRecord &txnRecord){

  auto txn_record_size = GetNextFrameSize();
  char txn_record[txn_record_size];
  size_t ret = fread(txn_record, 1, sizeof(txn_record), logFile);
  if( ret <= 0 ){
    LOG_ERROR("Error occured in fread ");
  }
  CopySerializeInput logTxnRecord(txn_record,txn_record_size);
  txnRecord.Deserialize(logTxnRecord);
}

bool AriesFrontendLogger::ReadTupleRecordHeader(TupleRecord& tupleRecord){

  auto header_size = GetNextFrameSize();

  // Read header 
  char header[header_size];
  size_t ret = fread(header, 1, sizeof(header), logFile);
  if( ret <= 0 ){
    LOG_ERROR("Error occured in fread ");
  }

  CopySerializeInput logHeader(header,header_size);
  tupleRecord.DeserializeHeader(logHeader);

  return true;
}

storage::Tuple* AriesFrontendLogger::ReadTupleRecordBody(catalog::Schema* schema){
      // Measure the body size of LogRecord
      size_t body_size = GetNextFrameSize();

      // Read Body 
      char body[body_size];
      int ret = fread(body, 1, sizeof(body), logFile);
      if( ret <= 0 ){
        LOG_ERROR("Error occured in fread ");
      }

      CopySerializeInput logBody(body, body_size);

      storage::Tuple *tuple = new storage::Tuple(schema, true);
      storage::AbstractBackend *backend = new storage::VMBackend();
      Pool *pool = new Pool(backend);
    
      tuple->DeserializeFrom(logBody, pool);
      return tuple;
}

storage::DataTable* AriesFrontendLogger::GetTable(TupleRecord tupleRecord){
  // Get db, table, schema to insert tuple
  auto &manager = catalog::Manager::GetInstance();
  storage::Database* db = manager.GetDatabaseWithOid(tupleRecord.GetDbId());
  auto table = db->GetTableWithOid(tupleRecord.GetTableId());
  return table;
}

storage::TileGroup* AriesFrontendLogger::GetTileGroup(oid_t tile_group_id){
  auto &manager = catalog::Manager::GetInstance();
  auto tile_group = manager.GetTileGroup(tile_group_id);
  return tile_group;
}

void AriesFrontendLogger::MoveTuples(concurrency::Transaction* destination,
                                     concurrency::Transaction* source){
  auto inserted_tuples = source->GetInsertedTuples();

  for (auto entry : inserted_tuples) {
    storage::TileGroup *tile_group = entry.first;
    auto tile_group_id = tile_group->GetTileGroupId();

    for (auto tuple_slot : entry.second){

      destination->RecordInsert( ItemPointer(tile_group_id, tuple_slot));
    }
  }

  auto deleted_tuples = source->GetDeletedTuples();
  for (auto entry : deleted_tuples) {
    storage::TileGroup *tile_group = entry.first;
    auto tile_group_id = tile_group->GetTileGroupId();

    for (auto tuple_slot : entry.second)
      destination->RecordDelete( ItemPointer(tile_group_id, tuple_slot));
  }
  // Clear inserted/deleted tuples from txn
  source->ResetStates();

}

void AriesFrontendLogger::AbortTuples(concurrency::Transaction* txn){

  auto inserted_tuples = txn->GetInsertedTuples();
  for (auto entry : inserted_tuples) {
    storage::TileGroup *tile_group = entry.first;

    for (auto tuple_slot : entry.second)
      tile_group->AbortInsertedTuple(tuple_slot);
  }

  // (B) rollback deletes
  auto deleted_tuples = txn->GetDeletedTuples();
  for (auto entry : txn->GetDeletedTuples()) {
    storage::TileGroup *tile_group = entry.first;

    for (auto tuple_slot : entry.second)
      tile_group->AbortDeletedTuple(tuple_slot);
  }

  // Clear inserted/deleted tuples from txn
  txn->ResetStates();
}

void AriesFrontendLogger::AbortRemainedTxnInRecoveryTable(){
  for(auto  txn : txn_table){
    auto curr_txn = txn.second;
    AbortTuples(curr_txn);
    txn_table.erase(curr_txn->GetTransactionId());
  }
}

void AriesFrontendLogger::InsertTuple(concurrency::Transaction* recovery_txn){

  TupleRecord tupleRecord(LOGRECORD_TYPE_TUPLE_INSERT);

  ReadTupleRecordHeader(tupleRecord);

  auto table = GetTable(tupleRecord);
  auto tuple = ReadTupleRecordBody(table->GetSchema());

  auto tile_group_id = tupleRecord.GetItemPointer().block;
  auto tuple_slot = tupleRecord.GetItemPointer().offset;

  auto tile_group =  GetTileGroup(tile_group_id);

  auto txn_id = tupleRecord.GetTxnId();
  LOG_INFO("1 Find txd id %d in object in table",(int)txn_id);
  auto txn = txn_table.at(txn_id);
 
  // Create new tile group if table doesn't have tile group that recored in the log
  if(tile_group == nullptr){
    //FIXME in this case, data table's # of tuples isn't updated
    table->AddTileGroupWithOid(tile_group_id);
    auto tile_group = table->GetTileGroupById(tile_group_id);
    tile_group->InsertTuple(tuple_slot/*XXX:CHECK*/, tuple);
    ItemPointer location(tile_group_id, tuple_slot);
    txn->RecordInsert(location);
  }else{
    ItemPointer location = table->InsertTuple(recovery_txn, tuple);
    if (location.block == INVALID_OID) {
      LOG_ERROR("Error !! InsertTuple in Recovery Mode");
    }
    txn->RecordInsert(location);
  }

  delete tuple;
}

void AriesFrontendLogger::DeleteTuple(concurrency::Transaction* recovery_txn){

  TupleRecord tupleRecord(LOGRECORD_TYPE_TUPLE_DELETE);

  ReadTupleRecordHeader(tupleRecord);

  auto table = GetTable(tupleRecord);

  ItemPointer delete_location = tupleRecord.GetItemPointer();

  bool status = table->DeleteTuple(recovery_txn, delete_location);

  if( status == false){
    LOG_ERROR("Error !! DeleteTuple in Recovery Mode");
  }
  auto txn_id = tupleRecord.GetTxnId();
  LOG_INFO("2 Find txd id %d in object in table",(int)txn_id);
  auto txn = txn_table.at(txn_id);
  txn->RecordDelete(delete_location);

}

void AriesFrontendLogger::UpdateTuple(concurrency::Transaction* recovery_txn){

  TupleRecord tupleRecord(LOGRECORD_TYPE_TUPLE_UPDATE);

  ReadTupleRecordHeader(tupleRecord);

  auto table = GetTable(tupleRecord);

  auto tuple = ReadTupleRecordBody(table->GetSchema());

  ItemPointer delete_location = tupleRecord.GetItemPointer();

  ItemPointer location = table->UpdateTuple(recovery_txn, tuple, delete_location);

  if (location.block == INVALID_OID) {
    LOG_ERROR("Error !! InsertTuple in Recovery Mode");
  }

  auto txn_id = tupleRecord.GetTxnId();
  LOG_INFO("3 Find txd id %d in object in table",(int)txn_id);
  auto txn = txn_table.at(txn_id);
  txn->RecordDelete(delete_location);
  txn->RecordInsert(location);
}

void AriesFrontendLogger::AddTxnToRecoveryTable(){
  // read transaction information from the log file
  TransactionRecord txnRecord(LOGRECORD_TYPE_TRANSACTION_BEGIN);
  ReadTxnRecord(txnRecord);

  auto txn_id = txnRecord.GetTxnId();

  // create the new txn object and added it into recovery txn_table
  concurrency::Transaction* txn = new concurrency::Transaction(txn_id, INVALID_CID);
  txn_table.insert(std::make_pair(txn_id, txn));
  LOG_INFO("Added txd id %d object in table",(int)txn_id);
}

void AriesFrontendLogger::RemoveTxnFromRecoveryTable(){
  // read transaction information from the log file
  TransactionRecord txnRecord(LOGRECORD_TYPE_TRANSACTION_END);
  ReadTxnRecord(txnRecord);

  auto txn_id = txnRecord.GetTxnId();

  // remove txn from recovery txn table
  txn_table.erase(txn_id);
  LOG_INFO("Erase txd id %d object in table",(int)txn_id);
}

void AriesFrontendLogger::CommitTuplesFromRecoveryTable(concurrency::Transaction* recovery_txn) {

  // read transaction information from the log file
  TransactionRecord txnRecord(LOGRECORD_TYPE_TRANSACTION_COMMIT);
  ReadTxnRecord(txnRecord);

  // get the txn
  auto txn_id = txnRecord.GetTxnId();
  auto txn = txn_table.at(txn_id);

  // Copy inserted/deleted tuples to recovery transaction
  MoveTuples(recovery_txn, txn);

  LOG_INFO("Commit txd id %d object in table",(int)txn_id);
}

void AriesFrontendLogger::AbortTuplesFromRecoveryTable(){

  // read transaction information from the log file
  TransactionRecord txnRecord(LOGRECORD_TYPE_TRANSACTION_END);
  ReadTxnRecord(txnRecord);

  auto txn_id = txnRecord.GetTxnId();

  // get the txn
  auto txn = txn_table.at(txn_id);
  //TODO :: Rename, from tuples to status ? or txn? whatever..
  AbortTuples(txn);

  LOG_INFO("Abort txd id %d object in table",(int)txn_id);
}

}  // namespace logging
}  // namespace peloton
