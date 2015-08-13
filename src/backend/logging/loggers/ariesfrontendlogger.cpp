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
  ret = close(logFileFd);
  if( ret == -1 ){
    LOG_ERROR("Error occured while closing LogFileFd");
  }
}

/**
 * @brief MainLoop
 */
//FIXME :: Performance issue remains
void AriesFrontendLogger::MainLoop(void) {

  //FIXME :: find better way..
  auto& logManager = LogManager::GetInstance();

  while( !logManager.IsPelotonReadyToRecovery() ){
    sleep(1);
  }

  Recovery();

  for(int i=0;;i++){
    sleep(1);

    // Collect LogRecords from BackendLogger 
    CollectLogRecord();

    // Checkpoint ?
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
    fwrite( record->GetSerializedData(), sizeof(char), record->GetSerializedDataSize(), logFile);
    //TODO :: record LSN here
  }

  int ret = fflush(logFile);
  if( ret != 0 ){
    LOG_ERROR("Error occured in fflush(%d)", ret);
  }

  ret = fsync(logFileFd);
  if( ret != 0 ){
    LOG_ERROR("Error occured in fsync(%d)", ret);
  }

  aries_global_queue.clear();
}

void AriesFrontendLogger::Recovery() {

  // TODO :: print for debugging
  storage::DataTable* table;

  if(LogFileSize() > 0){
    bool EOF_OF_LOG_FILE = false;

    auto &txn_manager = concurrency::TransactionManager::GetInstance();
    auto txn = txn_manager.BeginTransaction();

    while(!EOF_OF_LOG_FILE){

      // Read the first sing bite so that we can distinguish log record type
      switch(GetNextLogRecordType()){

        // If record is txn,
        // store it in recovery txn table
        case LOGRECORD_TYPE_TRANSACTION_BEGIN:
        break;

        case LOGRECORD_TYPE_TRANSACTION_COMMIT:
        break;

        case LOGRECORD_TYPE_TUPLE_INSERT:
          RedoInsert(txn);
          break;

        case LOGRECORD_TYPE_TUPLE_DELETE:
          RedoDelete(txn);
          break;

        case LOGRECORD_TYPE_TUPLE_UPDATE:
          RedoUpdate(txn);
          break;

        default:
          EOF_OF_LOG_FILE = true;
          break;
      }
    }

    // Read txn table and undo unfinished txn here

    txn_manager.CommitTransaction(txn);
    std::cout << *table << std::endl;
  }

  //TODO::After finishing recovery, set the next oid with maximum oid
  //void SetNextOid(oid_t next_oid) { oid = next_oid; }
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
  LOG_INFO("Frame: %zu",frame_size);

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

      //TODO :: Make this as a function
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

void AriesFrontendLogger::RedoInsert(concurrency::Transaction* txn){
  InsertTuple(txn);
}

void AriesFrontendLogger::RedoDelete(concurrency::Transaction* txn){
  DeleteTuple(txn);
}

void AriesFrontendLogger::RedoUpdate(concurrency::Transaction* txn){
  UpdateTuple(txn);
}

void AriesFrontendLogger::InsertTuple(concurrency::Transaction* txn){

  TupleRecord tupleRecord(LOGRECORD_TYPE_TUPLE_INSERT);

  ReadTupleRecordHeader(tupleRecord);

  auto table = GetTable(tupleRecord);
  auto tuple = ReadTupleRecordBody(table->GetSchema());

  auto tile_group_id = tupleRecord.GetItemPointer().block;
  auto tuple_slot = tupleRecord.GetItemPointer().offset;

  auto tile_group =  GetTileGroup(tile_group_id);

  // Create new tile group if table doesn't have tile group that recored in the log
  if(tile_group == nullptr){
    table->AddTileGroupWithOid(tile_group_id);
    auto tile_group = table->GetTileGroupById(tile_group_id);
    tile_group->InsertTuple(tuple_slot/*XXX:CHECK*/, tuple);
    ItemPointer location(tile_group_id, tuple_slot);
    txn->RecordInsert(location);
  }else{
    ItemPointer location = table->InsertTuple(txn, tuple);
    if (location.block == INVALID_OID) {
      LOG_ERROR("Error !! InsertTuple in Recovery Mode");
    }
    txn->RecordInsert(location);
  }
  delete tuple;
}

void AriesFrontendLogger::DeleteTuple(concurrency::Transaction* txn){

  TupleRecord tupleRecord(LOGRECORD_TYPE_TUPLE_DELETE);

  ReadTupleRecordHeader(tupleRecord);

  auto table = GetTable(tupleRecord);

  ItemPointer delete_location = tupleRecord.GetItemPointer();

  bool status = table->DeleteTuple(txn, delete_location);

  if( status == false){
    LOG_ERROR("Error !! DeleteTuple in Recovery Mode");
  }
  txn->RecordDelete(delete_location);
}

void AriesFrontendLogger::UpdateTuple(concurrency::Transaction* txn){

  TupleRecord tupleRecord(LOGRECORD_TYPE_TUPLE_UPDATE);

  ReadTupleRecordHeader(tupleRecord);

  auto table = GetTable(tupleRecord);

  auto tuple = ReadTupleRecordBody(table->GetSchema());

  ItemPointer delete_location = tupleRecord.GetItemPointer();

  ItemPointer location = table->UpdateTuple(txn, tuple, delete_location);

  if (location.block == INVALID_OID) {
    LOG_ERROR("Error !! InsertTuple in Recovery Mode");
  }

  txn->RecordDelete(delete_location);

  txn->RecordInsert(location);
}

}  // namespace logging
}  // namespace peloton
