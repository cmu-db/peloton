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

#include <sys/stat.h>
#include <sys/mman.h>

#include "backend/catalog/manager.h"
#include "backend/catalog/schema.h"
#include "backend/logging/log_manager.h"
#include "backend/logging/loggers/aries_frontend_logger.h"
#include "backend/logging/loggers/aries_backend_logger.h"
#include "backend/storage/backend_vm.h"
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
  // we open it in append + binary mode
  log_file = fopen( file_name.c_str(),"ab+");
  if(log_file == NULL) {
    LOG_ERROR("LogFile is NULL");
  }

  // also, get the descriptor
  log_file_fd = fileno(log_file);
  if( log_file_fd == -1) {
    LOG_ERROR("log_file_fd is -1");
  }

}

/**
 * @brief close logfile
 */
AriesFrontendLogger::~AriesFrontendLogger(){

  // close the log file
  int ret = fclose(log_file);
  if( ret != 0 ){
    LOG_ERROR("Error occured while closing LogFile");
  }
}

/**
 * @brief MainLoop
 */
void AriesFrontendLogger::MainLoop(void) {

  auto& log_manager = LogManager::GetInstance();

  /////////////////////////////////////////////////////////////////////
  // STANDBY MODE
  /////////////////////////////////////////////////////////////////////

  LOG_TRACE("Frontendlogger] Standby Mode");
  // Standby before we are ready to recovery
  while(log_manager.GetStatus(LOGGING_TYPE_ARIES) == LOGGING_STATUS_TYPE_STANDBY ){
    sleep(1);
  }

  // Do recovery if we can, otherwise terminate
  switch(log_manager.GetStatus(LOGGING_TYPE_ARIES)) {
    case LOGGING_STATUS_TYPE_RECOVERY: {
      LOG_TRACE("Frontendlogger] Recovery Mode");

      /////////////////////////////////////////////////////////////////////
      // RECOVERY MODE
      /////////////////////////////////////////////////////////////////////

      // First, do recovery if needed
      DoRecovery();

      // Now, enable active logging
      log_manager.SetLoggingStatus(LOGGING_TYPE_ARIES, LOGGING_STATUS_TYPE_LOGGING);

      break;
    }

    case LOGGING_STATUS_TYPE_LOGGING: {
      LOG_TRACE("Frontendlogger] Logging Mode");
      break;
    }

    default:
    break;
  }

  /////////////////////////////////////////////////////////////////////
  // LOGGING MODE
  /////////////////////////////////////////////////////////////////////


  // Periodically, wake up and do logging
  while(log_manager.GetStatus(LOGGING_TYPE_ARIES) == LOGGING_STATUS_TYPE_LOGGING){
    sleep(1);

    // Collect LogRecords from all backend loggers
    CollectLogRecord();

    // Flush the data to the file
    Flush();
  }

  /////////////////////////////////////////////////////////////////////
  // TERMINATE MODE
  /////////////////////////////////////////////////////////////////////

  // flush any remaining log records
  CollectLogRecord();
  Flush();

  /////////////////////////////////////////////////////////////////////
  // SLEEP MODE
  /////////////////////////////////////////////////////////////////////

  LOG_TRACE("Frontendlogger] Sleep Mode");

  //Setting frontend logger status to sleep
  log_manager.SetLoggingStatus(LOGGING_TYPE_ARIES,
                               LOGGING_STATUS_TYPE_SLEEP);

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
      backend_logger->TruncateLocalQueue(local_queue_size);
    }
  }
}

/**
 * @brief flush all record to the file
 */
void AriesFrontendLogger::Flush(void) {

  for( auto record : aries_global_queue ){
    fwrite( record->GetMessage(), 
            sizeof(char), 
            record->GetMessageLength(), 
            log_file);
  }

  int ret = fflush(log_file);
  if( ret != 0 ){
    LOG_ERROR("Error occured in fflush(%d)", ret);
  }

  ret = fsync(log_file_fd);
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
    if(backend_logger->IsWaitingForFlushing()){
      backend_logger->Commit();
    }
  }
}

//===--------------------------------------------------------------------===//
// Recovery
//===--------------------------------------------------------------------===//

/**
 * @brief Recovery system based on log file
 */
void AriesFrontendLogger::DoRecovery() {

  if(GetLogFileSize() > 0){
    bool EOF_OF_LOG_FILE = false;

    auto &txn_manager = concurrency::TransactionManager::GetInstance();
    auto recovery_txn = txn_manager.BeginTransaction();

    while(!EOF_OF_LOG_FILE){

      // Read the first single bite so that we can distinguish log record type
      // otherwise, finish the recovery 
      switch(GetNextLogRecordType()){

        case LOGRECORD_TYPE_TRANSACTION_BEGIN:
          AddTransactionToRecoveryTable();
          break;

        case LOGRECORD_TYPE_TRANSACTION_END:
          RemoveTransactionFromRecoveryTable();
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
    AbortTransactionInRecoveryTable();

    txn_manager.CommitTransaction(recovery_txn);

    //After finishing recovery, set the next oid with maximum oid
    auto &manager = catalog::Manager::GetInstance();
    auto max_oid = manager.GetNextOid();
    manager.SetNextOid(max_oid);
  }
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
void AriesFrontendLogger::AbortTransactionInRecoveryTable(){
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

  auto txn_id = tupleRecord.GetTransactionId();
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

  auto txn_id = tupleRecord.GetTransactionId();
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

  auto txn_id = tupleRecord.GetTransactionId();
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
void AriesFrontendLogger::AddTransactionToRecoveryTable(){
  // read transaction information from the log file
  TransactionRecord txnRecord(LOGRECORD_TYPE_TRANSACTION_BEGIN);

  if( ReadTransactionRecordHeader(txnRecord) == false ){
    // file is broken
    return;
  }

  auto txn_id = txnRecord.GetTransactionId();

  // create the new txn object and added it into recovery recovery_txn_table
  concurrency::Transaction* txn = new concurrency::Transaction(txn_id, INVALID_CID);
  recovery_txn_table.insert(std::make_pair(txn_id, txn));
  LOG_TRACE("Added txd id %d object in table",(int)txn_id);
}

/**
 * @brief Remove txn from recovery table
 */
void AriesFrontendLogger::RemoveTransactionFromRecoveryTable(){
  // read transaction information from the log file
  TransactionRecord txnRecord(LOGRECORD_TYPE_TRANSACTION_END);

  if( ReadTransactionRecordHeader(txnRecord) == false ){
    // file is broken
    return;
  }

  auto txn_id = txnRecord.GetTransactionId();

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

  if( ReadTransactionRecordHeader(txnRecord) == false ){
    // file is broken
    return;
  }

  // get the txn
  auto txn_id = txnRecord.GetTransactionId();
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

  if( ReadTransactionRecordHeader(txnRecord) == false ){
    // file is broken
    return;
  }

  auto txn_id = txnRecord.GetTransactionId();

  // get the txn
  auto txn = recovery_txn_table.at(txn_id);
  AbortTuples(txn);

  LOG_INFO("Abort txd id %d object in table",(int)txn_id);
}

//===--------------------------------------------------------------------===//
// Utility functions
//===--------------------------------------------------------------------===//

/**
 * @brief Measure the size of log file
 * @return the size if the log file exists otherwise 0
 */
size_t AriesFrontendLogger::GetLogFileSize(){
  struct stat log_stats;

  if(stat(file_name.c_str(), &log_stats) == 0){
    fstat(log_file_fd, &log_stats);
    return log_stats.st_size;
  }else{
    return 0;
  }
}

bool AriesFrontendLogger::IsFileTruncated(size_t size_to_read){
  size_t current_position = ftell(log_file);

  // Check if the actual file size is less than the expected file size
  // Current position + frame length
  if( GetLogFileSize() < (current_position+size_to_read)){
    fseek(log_file, 0, SEEK_END);
    return true;
  }else{
    return false;
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

  // Check if the frame size is broken
  if( IsFileTruncated(sizeof(int32_t)) ){
    return 0;
  }

  // Otherwise, read the frame size
  size_t ret = fread(buffer, 1, sizeof(buffer), log_file);
  if( ret <= 0 ){
    LOG_ERROR("Error occured in fread ");
  }

  // Read next 4 bytes as an integer
  CopySerializeInput frameCheck(buffer, sizeof(int32_t));
  frame_size = (frameCheck.ReadInt())+sizeof(int32_t);;

  // TODO: Do we need this check again ?
  // XXX Check if the frame is broken
  if( IsFileTruncated(frame_size) ){
    return 0;
  }

  // Move back by 4 bytes
  // So that tuple deserializer works later as expected
  int res = fseek(log_file, -sizeof(int32_t), SEEK_CUR);
  if(res == -1){
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

/**
 * @brief Read single byte so that we can distinguish the log record type
 * @return log record type otherwise return invalid log record type,
 * which menas there is no more log in the log file
 */
LogRecordType AriesFrontendLogger::GetNextLogRecordType(){
  char buffer;

  // Check if the log record type is broken
  if( IsFileTruncated(1) ){
    return LOGRECORD_TYPE_INVALID;
  }

  // Otherwise, read the log record type
  int ret = fread((void*)&buffer, 1, sizeof(char), log_file);
  if( ret <= 0 ){
    return LOGRECORD_TYPE_INVALID;
  }

  CopySerializeInput input(&buffer, sizeof(char));
  LogRecordType log_record_type = (LogRecordType)(input.ReadEnumInSingleByte());

  return log_record_type;
}

/**
 * @brief Read TransactionRecord
 * @param txnRecord
 */
bool AriesFrontendLogger::ReadTransactionRecordHeader(TransactionRecord &txn_record){

  // Check if frame is broken
  auto header_size = GetNextFrameSize();
  if( header_size == 0 ){
    return false;
  }

  // Read header
  char header[header_size];
  size_t ret = fread(header, 1, sizeof(header), log_file);
  if( ret <= 0 ){
    LOG_ERROR("Error occured in fread ");
  }

  CopySerializeInput txn_header(header, header_size);
  txn_record.Deserialize(txn_header);

  return true;
}

/**
 * @brief Read TupleRecordHeader
 * @param tupleRecord
 */
bool AriesFrontendLogger::ReadTupleRecordHeader(TupleRecord& tuple_record){

  // Check if frame is broken
  auto header_size = GetNextFrameSize();
  if( header_size == 0 ){
    return false;
  }

  // Read header
  char header[header_size];
  size_t ret = fread(header, 1, sizeof(header), log_file);
  if( ret <= 0 ){
    LOG_ERROR("Error occured in fread ");
  }

  CopySerializeInput tuple_header(header,header_size);
  tuple_record.DeserializeHeader(tuple_header);

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
  // Check if the frame is broken
  size_t body_size = GetNextFrameSize();
  if( body_size == 0 ){
    return nullptr;
  }

  // Read Body
  char body[body_size];
  int ret = fread(body, 1, sizeof(body), log_file);
  if( ret <= 0 ){
    LOG_ERROR("Error occured in fread ");
  }

  CopySerializeInput tuple_body(body, body_size);

  // We create a tuple based on the message
  storage::Tuple *tuple = new storage::Tuple(schema, true);
  tuple->DeserializeFrom(tuple_body, pool);

  return tuple;
}

/**
 * @brief Read get table based on tuple record
 * @param tuple record
 * @return data table
 */
storage::DataTable* AriesFrontendLogger::GetTable(TupleRecord tuple_record){
  // Get db, table, schema to insert tuple
  auto &manager = catalog::Manager::GetInstance();
  storage::Database* db = manager.GetDatabaseWithOid(tuple_record.GetDatabaseOid());
  assert(db);

  auto table = db->GetTableWithOid(tuple_record.GetTableId());
  assert(table);

  return table;
}

/**
 * @brief Get tile group - used to check if tile group already exists
 * @param tile group id
 * @return tile group
 */
storage::TileGroup* AriesFrontendLogger::GetTileGroup(oid_t tile_group_id){
  auto &manager = catalog::Manager::GetInstance();
  auto tile_group = manager.GetTileGroup(tile_group_id);
  return tile_group;
}

}  // namespace logging
}  // namespace peloton
