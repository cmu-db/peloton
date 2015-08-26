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

/**
 * @brief Open logfile and file descriptor
 */
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

/**
 * @brief close logfile
 */
AriesFrontendLogger::~AriesFrontendLogger(){

  int ret = fclose(logFile);
  if( ret != 0 ){
    LOG_ERROR("Error occured while closing LogFile");
  }
}

/**
 * @brief MainLoop
 */
void AriesFrontendLogger::MainLoop(void) {

  auto& logManager = LogManager::GetInstance();

  LOG_TRACE("Frontendlogger] Standby Mode");
  // Standby before we are ready to recovery
  while(logManager.GetLoggingStatus(LOGGING_TYPE_ARIES) == LOGGING_STATUS_TYPE_STANDBY ){
    sleep(1);
  }

  // Do recovery if we can, otherwise terminate
  switch(logManager.GetLoggingStatus(LOGGING_TYPE_ARIES)){
    case LOGGING_STATUS_TYPE_RECOVERY:{
      LOG_TRACE("Frontendlogger] Recovery Mode");
      Recovery();
      logManager.SetLoggingStatus(LOGGING_TYPE_ARIES, LOGGING_STATUS_TYPE_ONGOING);
    }
    case LOGGING_STATUS_TYPE_ONGOING:{
      LOG_TRACE("Frontendlogger] Ongoing Mode");
    }
    break;

    default:
    break;
  }

  while(logManager.GetLoggingStatus(LOGGING_TYPE_ARIES) == LOGGING_STATUS_TYPE_ONGOING){
    sleep(1);

    // Collect LogRecords from all BackendLogger 
    CollectLogRecord();
    Flush();
  }

  // flush remanent log record
  CollectLogRecord();
  Flush();

  LOG_TRACE("Frontendlogger] Sleep Mode");
  //Setting frontend logger status to sleep
  logManager.SetLoggingStatus(LOGGING_TYPE_ARIES, LOGGING_STATUS_TYPE_SLEEP);
}

/**
 * @brief Collect the LogRecord from BackendLoggers
 */
void AriesFrontendLogger::CollectLogRecord() {

  backend_loggers = GetBackendLoggers();

  {
    std::lock_guard<std::mutex> lock(backend_logger_mutex);

    // Look over hte commit mark of current frontend logger's backend loggers
    for( auto backend_logger : backend_loggers){
      auto local_queue_size = backend_logger->GetLocalQueueSize();

      // Skip current backend_logger, nothing to do
      if(local_queue_size == 0 ) continue; 

      for(oid_t log_record_itr=0; log_record_itr<local_queue_size; log_record_itr++){
        // Copy LogRecord from backend_logger to here
        aries_global_queue.push_back(backend_logger->GetLogRecord(log_record_itr));
      }
      // truncate the local queue 
      backend_logger->Truncate(local_queue_size);
    }
  }
}

/**
 * @brief flush all record to the file
 */
void AriesFrontendLogger::Flush(void) {

  for( auto record : aries_global_queue ){
    fwrite( record->GetSerializedData(), 
            sizeof(char), 
            record->GetSerializedDataSize(), 
            logFile);
  }

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

  // Commit each backend logger 
  backend_loggers = GetBackendLoggers();

  for( auto backend_logger : backend_loggers){
    if(backend_logger->IsWaitFlush()){
      backend_logger->Commit();
    }
  }
}

/**
 * @brief Recovery system based on log file
 */
void AriesFrontendLogger::Recovery() {

  if(LogFileSize() > 0){
    bool EOF_OF_LOG_FILE = false;

    auto &txn_manager = concurrency::TransactionManager::GetInstance();
    auto recovery_txn = txn_manager.BeginTransaction();

    while(!EOF_OF_LOG_FILE){

      // Read the first single bite so that we can distinguish log record type
      // otherwise, finish the recovery 
      switch(GetNextLogRecordType()){

        case LOGRECORD_TYPE_TRANSACTION_BEGIN:
          AddTxnToRecoveryTable();
          break;

        case LOGRECORD_TYPE_TRANSACTION_END:
          RemoveTxnFromRecoveryTable();
          break;

        case LOGRECORD_TYPE_TRANSACTION_COMMIT:
          MoveCommittedTuplesToRecoveryTxn(recovery_txn);
          break;

        case LOGRECORD_TYPE_TRANSACTION_ABORT:
          AbortTuplesFromRecoveryTable();
          break;

        case LOGRECORD_TYPE_ARIES_TUPLE_INSERT:
          InsertTuple(recovery_txn);
          break;

        case LOGRECORD_TYPE_ARIES_TUPLE_DELETE:
          DeleteTuple(recovery_txn);
          break;

        case LOGRECORD_TYPE_ARIES_TUPLE_UPDATE:
          UpdateTuple(recovery_txn);
          break;

        default:
          EOF_OF_LOG_FILE = true;
          break;
      }
    }

    // Abort remained txn in recovery_txn_table
    AbortTxnInRecoveryTable();

    txn_manager.CommitTransaction(recovery_txn);

    //After finishing recovery, set the next oid with maximum oid
    auto &manager = catalog::Manager::GetInstance();
    auto max_oid = manager.GetNextOid();
    manager.SetNextOid(max_oid);
  }
}

/**
 * @brief Read single byte so that we can distinguish the log record type
 * @return log record type otherwise return invalid log record type, which menas there is no more log in the log file
 */
LogRecordType AriesFrontendLogger::GetNextLogRecordType(){
  char buffer;
  if( IsFileBroken(1) ){
    return LOGRECORD_TYPE_INVALID;
  }
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
 * @return the size if the log file exists otherwise 0
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

/**
 * @brief get the next frame size
 *  TupleRecord consiss of two frame ( header and Body)
 *  Transaction Record has a single frame
 * @return the next frame size
 */
size_t AriesFrontendLogger::GetNextFrameSize(){
  size_t frame_size;
  char buffer[sizeof(int32_t)];

  if( IsFileBroken(sizeof(int32_t)) ){
    return 0;
  }

  size_t ret = fread(buffer, 1, sizeof(buffer), logFile);
  if( ret <= 0 ){
    LOG_ERROR("Error occured in fread ");
  }

  // Read next 4 bytes as an integer
  CopySerializeInput frameCheck(buffer, sizeof(int32_t));
  frame_size = (frameCheck.ReadInt())+sizeof(int32_t);;

  /* go back 4 bytes */
  int res = fseek(logFile, -sizeof(int32_t), SEEK_CUR);
  if(res == -1){
    LOG_ERROR("Error occured in fseek ");
  }

  if( IsFileBroken(frame_size) ){
    // file is broken
    return 0;
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

/**
 * @brief Read TransactionRecord
 * @param txnRecord
 */
bool AriesFrontendLogger::ReadTxnRecord(TransactionRecord &txnRecord){

  auto txn_record_size = GetNextFrameSize();

  if( txn_record_size == 0 ){
    // file is broken
    return false;
  }

  char txn_record[txn_record_size];
  size_t ret = fread(txn_record, 1, sizeof(txn_record), logFile);
  if( ret <= 0 ){
    LOG_ERROR("Error occured in fread ");
  }
  CopySerializeInput logTxnRecord(txn_record,txn_record_size);
  txnRecord.Deserialize(logTxnRecord);

  return true;
}

bool AriesFrontendLogger::IsFileBroken(size_t size_to_read){
  size_t current_position = ftell(logFile);
  if( LogFileSize() < (current_position+size_to_read)){
    fseek(logFile, 0, SEEK_END);
    return true;
  }else{
    return false;
  }
}

/**
 * @brief Read TupleRecordHeader
 * @param tupleRecord
 */
bool AriesFrontendLogger::ReadTupleRecordHeader(TupleRecord& tupleRecord){

  auto header_size = GetNextFrameSize();
  if( header_size == 0 ){
    // file is broken
    return false;
  }

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

/**
 * @brief Read TupleRecordBody
 * @param schema
 * @param pool
 * @return tuple
 */
storage::Tuple* AriesFrontendLogger::ReadTupleRecordBody(catalog::Schema* schema, 
                                                         Pool *pool){
  // Measure the body size of LogRecord
  size_t body_size = GetNextFrameSize();
  if( body_size == 0 ){
    // file is broken
    return nullptr;
  }

  // Read Body 
  char body[body_size];
  int ret = fread(body, 1, sizeof(body), logFile);
  if( ret <= 0 ){
    LOG_ERROR("Error occured in fread ");
  }

  CopySerializeInput logBody(body, body_size);

  storage::Tuple *tuple = new storage::Tuple(schema, true);

  tuple->DeserializeFrom(logBody, pool);
  return tuple;
}

/**
 * @brief Read get table based on tuple record
 * @param tuple record
 * @return data table
 */
storage::DataTable* AriesFrontendLogger::GetTable(TupleRecord tupleRecord){
  // Get db, table, schema to insert tuple
  auto &manager = catalog::Manager::GetInstance();
  storage::Database* db = manager.GetDatabaseWithOid(tupleRecord.GetDbId());
  assert(db);
  auto table = db->GetTableWithOid(tupleRecord.GetTableId());
  assert(table);
  return table;
}

/**
 * @brief Get tile group
 * @param tile group id
 * @return tile group
 */
storage::TileGroup* AriesFrontendLogger::GetTileGroup(oid_t tile_group_id){
  auto &manager = catalog::Manager::GetInstance();
  auto tile_group = manager.GetTileGroup(tile_group_id);
  return tile_group;
}

/**
 * @brief Move Tuples from source to destination
 * @param destination
 * @param source
 */
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

/**
 * @brief Abort tuples inside txn
 * @param txn
 */
void AriesFrontendLogger::AbortTuples(concurrency::Transaction* txn){

  LOG_INFO("Abort txd id %d object in table",(int)txn->GetTransactionId());

  auto inserted_tuples = txn->GetInsertedTuples();
  for (auto entry : inserted_tuples) {
    storage::TileGroup *tile_group = entry.first;

    for (auto tuple_slot : entry.second)
      tile_group->AbortInsertedTuple(tuple_slot);
  }

  auto deleted_tuples = txn->GetDeletedTuples();
  for (auto entry : txn->GetDeletedTuples()) {
    storage::TileGroup *tile_group = entry.first;

    for (auto tuple_slot : entry.second)
      tile_group->AbortDeletedTuple(tuple_slot);
  }

  // Clear inserted/deleted tuples from txn
  txn->ResetStates();
}

/**
 * @brief Abort tuples inside txn table
 */
void AriesFrontendLogger::AbortTxnInRecoveryTable(){
  for(auto  txn : recovery_txn_table){
    auto curr_txn = txn.second;
    AbortTuples(curr_txn);
    delete curr_txn;
  }
  recovery_txn_table.clear();
}

/**
 * @brief read tuple record from log file and add them tuples to recovery txn
 * @param recovery txn
 */
void AriesFrontendLogger::InsertTuple(concurrency::Transaction* recovery_txn){

  TupleRecord tupleRecord(LOGRECORD_TYPE_ARIES_TUPLE_INSERT);

  if( ReadTupleRecordHeader(tupleRecord) == false){
    // file is broken
    return;
  }

  auto table = GetTable(tupleRecord);

  storage::AbstractBackend *backend = new storage::VMBackend();
  Pool *pool = new Pool(backend);
 
  auto tuple = ReadTupleRecordBody(table->GetSchema(), pool);

  if( tuple == nullptr){
    // file is broken
    delete pool;
    delete backend;
    return;
  }

  auto tile_group_id = tupleRecord.GetInsertLocation().block;
  auto tuple_slot = tupleRecord.GetInsertLocation().offset;

  auto tile_group = GetTileGroup(tile_group_id);

  auto txn_id = tupleRecord.GetTxnId();
  auto txn = recovery_txn_table.at(txn_id);

  ItemPointer location;

  // Create new tile group if table doesn't have tile group that recored in the log
  if(tile_group == nullptr){
    table->AddTileGroupWithOid(tile_group_id);
    tile_group = table->GetTileGroupById(tile_group_id);
  }

  tile_group->InsertTuple(recovery_txn->GetTransactionId(), tuple_slot, tuple);
  location.block = tile_group_id;
  location.offset = tuple_slot;
  if (location.block == INVALID_OID) {
    recovery_txn->SetResult(Result::RESULT_FAILURE);
  }else{
    txn->RecordInsert(location);
    table->IncreaseNumberOfTuplesBy(1);
  }

  delete tuple;
  delete pool;
  delete backend;
}

/**
 * @brief read tuple record from log file and add them tuples to recovery txn
 * @param recovery txn
 */
void AriesFrontendLogger::DeleteTuple(concurrency::Transaction* recovery_txn){

  TupleRecord tupleRecord(LOGRECORD_TYPE_ARIES_TUPLE_DELETE);

  if( ReadTupleRecordHeader(tupleRecord) == false){
    // file is broken
    return;
  }

  auto table = GetTable(tupleRecord);

  ItemPointer delete_location = tupleRecord.GetDeleteLocation();

  bool status = table->DeleteTuple(recovery_txn, delete_location);
  if (status == false) {
    recovery_txn->SetResult(Result::RESULT_FAILURE);
    return;
  }

  auto txn_id = tupleRecord.GetTxnId();
  auto txn = recovery_txn_table.at(txn_id);
  txn->RecordDelete(delete_location);

}

/**
 * @brief read tuple record from log file and add them tuples to recovery txn
 * @param recovery txn
 */
void AriesFrontendLogger::UpdateTuple(concurrency::Transaction* recovery_txn){

  TupleRecord tupleRecord(LOGRECORD_TYPE_ARIES_TUPLE_UPDATE);

  if( ReadTupleRecordHeader(tupleRecord) == false){
    // file is broken
    return;
  }

  auto txn_id = tupleRecord.GetTxnId();
  auto txn = recovery_txn_table.at(txn_id);

  auto table = GetTable(tupleRecord);

  storage::AbstractBackend *backend = new storage::VMBackend();
  Pool *pool = new Pool(backend);

  auto tuple = ReadTupleRecordBody(table->GetSchema(), pool);

  if( tuple == nullptr){
    // file is broken
     delete pool;
     delete backend;
    return;
  }

  ItemPointer delete_location = tupleRecord.GetDeleteLocation();
  bool status = table->DeleteTuple(recovery_txn, delete_location);
  if (status == false) {
    recovery_txn->SetResult(Result::RESULT_FAILURE);
  }else{
    txn->RecordDelete(delete_location);

    ItemPointer location = table->UpdateTuple(recovery_txn, tuple, delete_location);
    if (location.block == INVALID_OID) {
      recovery_txn->SetResult(Result::RESULT_FAILURE);
    }else{
      txn->RecordInsert(location);
    }
  }

  delete tuple;
  delete pool;
  delete backend;
}

/**
 * @brief Add new txn to recovery table
 */
void AriesFrontendLogger::AddTxnToRecoveryTable(){
  // read transaction information from the log file
  TransactionRecord txnRecord(LOGRECORD_TYPE_TRANSACTION_BEGIN);

  if( ReadTxnRecord(txnRecord) == false ){
    // file is broken
    return;
  }

  auto txn_id = txnRecord.GetTxnId();

  // create the new txn object and added it into recovery recovery_txn_table
  concurrency::Transaction* txn = new concurrency::Transaction(txn_id, INVALID_CID);
  recovery_txn_table.insert(std::make_pair(txn_id, txn));
  LOG_TRACE("Added txd id %d object in table",(int)txn_id);
}

/**
 * @brief Remove txn from recovery table
 */
void AriesFrontendLogger::RemoveTxnFromRecoveryTable(){
  // read transaction information from the log file
  TransactionRecord txnRecord(LOGRECORD_TYPE_TRANSACTION_END);

  if( ReadTxnRecord(txnRecord) == false ){
    // file is broken
    return;
  }

  auto txn_id = txnRecord.GetTxnId();

  // remove txn from recovery txn table
  auto txn = recovery_txn_table.at(txn_id);
  recovery_txn_table.erase(txn_id);
  delete txn;
  LOG_TRACE("Erase txd id %d object in table",(int)txn_id);
}

/**
 * @brief move tuples from current txn to recovery txn so that we can commit them later
 * @param recovery txn
 */
void AriesFrontendLogger::MoveCommittedTuplesToRecoveryTxn(concurrency::Transaction* recovery_txn) {

  // read transaction information from the log file
  TransactionRecord txnRecord(LOGRECORD_TYPE_TRANSACTION_COMMIT);

  if( ReadTxnRecord(txnRecord) == false ){
    // file is broken
    return;
  }

  // get the txn
  auto txn_id = txnRecord.GetTxnId();
  auto txn = recovery_txn_table.at(txn_id);

  // Copy inserted/deleted tuples to recovery transaction
  MoveTuples(recovery_txn, txn);

  LOG_TRACE("Commit txd id %d object in table",(int)txn_id);
}

/**
 * @brief abort tuple 
 */
void AriesFrontendLogger::AbortTuplesFromRecoveryTable(){

  // read transaction information from the log file
  TransactionRecord txnRecord(LOGRECORD_TYPE_TRANSACTION_END);

  if( ReadTxnRecord(txnRecord) == false ){
    // file is broken
    return;
  }

  auto txn_id = txnRecord.GetTxnId();

  // get the txn
  auto txn = recovery_txn_table.at(txn_id);
  AbortTuples(txn);

  LOG_INFO("Abort txd id %d object in table",(int)txn_id);
}

}  // namespace logging
}  // namespace peloton
