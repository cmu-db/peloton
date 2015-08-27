/*-------------------------------------------------------------------------
 *
 * pelotonfrontendlogger.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/logging/pelotonfrontendlogger.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "peloton_frontend_logger.h"

#include <sys/stat.h>
#include <sys/mman.h>

#include "../log_manager.h"
#include "backend/storage/backend_vm.h"
#include "backend/catalog/manager.h"
#include "backend/catalog/schema.h"
#include "backend/storage/database.h"
#include "backend/storage/data_table.h"
#include "backend/storage/tuple.h"
#include "peloton_backend_logger.h"

namespace peloton {
namespace logging {

/**
 * @brief Open logfile and file descriptor
 */
PelotonFrontendLogger::PelotonFrontendLogger(){

  logging_type = LOGGING_TYPE_PELOTON;
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
PelotonFrontendLogger::~PelotonFrontendLogger(){

  int ret = fclose(logFile);
  if( ret != 0 ){
    LOG_ERROR("Error occured while closing LogFile");
  }
}

/**
 * @brief MainLoop
 */
void PelotonFrontendLogger::MainLoop(void) {

  auto& logManager = LogManager::GetInstance();

  LOG_TRACE("Frontendlogger] Standby Mode");
  // Standby before we are ready to recovery
  while(logManager.GetLoggingStatus(LOGGING_TYPE_PELOTON) == LOGGING_STATUS_TYPE_STANDBY ){
    sleep(1);
  }

  // Do recovery if we can, otherwise terminate
  switch(logManager.GetLoggingStatus(LOGGING_TYPE_PELOTON)){
    case LOGGING_STATUS_TYPE_RECOVERY:{
      LOG_TRACE("Frontendlogger] Recovery Mode");
      Recovery();
      logManager.SetLoggingStatus(LOGGING_TYPE_PELOTON, LOGGING_STATUS_TYPE_ONGOING);
    }
    case LOGGING_STATUS_TYPE_ONGOING:{
      LOG_TRACE("Frontendlogger] Ongoing Mode");
    }
    break;

    default:
    break;
  }

  while(logManager.GetLoggingStatus(LOGGING_TYPE_PELOTON) == LOGGING_STATUS_TYPE_ONGOING){
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
  logManager.SetLoggingStatus(LOGGING_TYPE_PELOTON, LOGGING_STATUS_TYPE_SLEEP);
}

/**
 * @brief Collect the LogRecord from BackendLoggers
 */
void PelotonFrontendLogger::CollectLogRecord() {

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
        peloton_global_queue.push_back(backend_logger->GetLogRecord(log_record_itr));
      }
      // truncate the local queue 
      backend_logger->Truncate(local_queue_size);
    }
  }
}

void PelotonFrontendLogger::CollectCommittedTuples(TupleRecord* record,
                                                  std::vector<ItemPointer> &inserted_tuples,
                                                  std::vector<ItemPointer> &deleted_tuples){

  if(record->GetType() == LOGRECORD_TYPE_PELOTON_TUPLE_INSERT){
    inserted_tuples.push_back(record->GetInsertLocation());
  }else if(record->GetType() == LOGRECORD_TYPE_PELOTON_TUPLE_DELETE){
    deleted_tuples.push_back(record->GetDeleteLocation());
  }else if(record->GetType() == LOGRECORD_TYPE_PELOTON_TUPLE_UPDATE){
    inserted_tuples.push_back(record->GetInsertLocation());
    deleted_tuples.push_back(record->GetDeleteLocation());
  }
}

/**
 * @brief flush all record to the file
 */
void PelotonFrontendLogger::Flush(void) {
 
  std::vector<ItemPointer> inserted_tuples;
  std::vector<ItemPointer> deleted_tuples;

  for( auto record : peloton_global_queue ){

    fwrite( record->GetMessage(), 
            sizeof(char), 
            record->GetMessageLength(), 
            logFile);

    CollectCommittedTuples((TupleRecord*)record, inserted_tuples, deleted_tuples );
  }

  int ret = fflush(logFile);
  if( ret != 0 ){
    LOG_ERROR("Error occured in fflush(%d)", ret);
  }

  ret = fsync(logFileFd);
  if( ret != 0 ){
    LOG_ERROR("Error occured in fsync(%d)", ret);
  }

  for( auto record : peloton_global_queue ){
    delete record;
  }
  peloton_global_queue.clear();

  // Commit each backend logger 
  backend_loggers = GetBackendLoggers();

  for( auto backend_logger : backend_loggers){
    if(backend_logger->IsWaitingForFlushing()){
      backend_logger->Commit();
    }
  }

  // set insert/delete commit based on logs
  for( auto inserted_tuple : inserted_tuples){
    SetInsertCommit(inserted_tuple, true);
  }
  for( auto deleted_tuple : deleted_tuples){
    SetDeleteCommit(deleted_tuple, true);
  }

  auto done = new TransactionRecord(LOGRECORD_TYPE_TRANSACTION_DONE, 1/*FIXME*/);

  fwrite( done->GetMessage(), 
          sizeof(char), 
          done->GetMessageLength(), 
          logFile);

  ret = fflush(logFile);
  if( ret != 0 ){
    LOG_ERROR("Error occured in fflush(%d)", ret);
  }

  ret = fsync(logFileFd);
  if( ret != 0 ){
    LOG_ERROR("Error occured in fsync(%d)", ret);
  }
  delete done;
}

/**
 * @brief Recovery system based on log file
 */
void PelotonFrontendLogger::Recovery() {

  if(LogFileSize() > 0){

    bool EOF_OF_LOG_FILE = false;

    JumpToLastUnfinishedTxn();

    while(!EOF_OF_LOG_FILE){

      // Read the first single bite so that we can distinguish log record type
      // otherwise, finish the recovery 
      switch(GetNextLogRecordType()){

        case LOGRECORD_TYPE_TRANSACTION_BEGIN:
          printf("BEGIN\n");
          SkipTxnRecord(LOGRECORD_TYPE_TRANSACTION_BEGIN);
          break;

        case LOGRECORD_TYPE_TRANSACTION_COMMIT:
          printf("COMMIT\n");
          SkipTxnRecord(LOGRECORD_TYPE_TRANSACTION_COMMIT);
          break;

        case LOGRECORD_TYPE_TRANSACTION_END:
          printf("END\n");
          SkipTxnRecord(LOGRECORD_TYPE_TRANSACTION_END);
          break;

        case LOGRECORD_TYPE_PELOTON_TUPLE_INSERT:
          printf("INSERT\n");
          InsertTuple();
          break;

        case LOGRECORD_TYPE_PELOTON_TUPLE_DELETE:
          printf("DELETE\n");
          DeleteTuple();
          break;

        case LOGRECORD_TYPE_PELOTON_TUPLE_UPDATE:
          printf("UPDATE\n");
          UpdateTuple();
          break;

        default:
          EOF_OF_LOG_FILE = true;
          break;
      }
    }
  }
}

/**
 * @brief Read single byte so that we can distinguish the log record type
 * @return log record type otherwise return invalid log record type, which menas there is no more log in the log file
 */
LogRecordType PelotonFrontendLogger::GetNextLogRecordType(){
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

void PelotonFrontendLogger::JumpToLastUnfinishedTxn(){
  char buffer;
  fseek(logFile, -1, SEEK_END);
  // Read last log record type
  int ret = fread((void*)&buffer, 1, sizeof(char), logFile);
  if( ret <= 0 ){
    LOG_ERROR("Error occured in fread(%d)", ret);
  }
  CopySerializeInput input(&buffer, sizeof(char));
  LogRecordType log_record_type = (LogRecordType)(input.ReadEnumInSingleByte());
  std::cout << "log record ::::::::: " << LogRecordTypeToString(log_record_type) << std::endl;

  if( log_record_type == LOGRECORD_TYPE_TRANSACTION_COMMIT ||
      log_record_type == LOGRECORD_TYPE_TRANSACTION_END){
    /* we need peloton recovery
     * jump to last begin txn log record
     */

    while(ftell(logFile) >= 0){
      fseek(logFile, -2, SEEK_CUR);
      if(GetNextLogRecordType() == LOGRECORD_TYPE_TRANSACTION_BEGIN){
        fseek(logFile, -1, SEEK_CUR);
        break;
      }
    }
  }else{
    /* no recovery */
    LOG_INFO("No recovery\n");
    fseek(logFile, 0, SEEK_END);
  }
}

/**
 * @brief Measure the size of log file
 * @return the size if the log file exists otherwise 0
 */
size_t PelotonFrontendLogger::LogFileSize(){
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
size_t PelotonFrontendLogger::GetNextFrameSize(){
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
 * @brief Read TransactionRecord
 * @param txnRecord
 */
bool PelotonFrontendLogger::ReadTxnRecord(TransactionRecord &txnRecord){

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

bool PelotonFrontendLogger::IsFileBroken(size_t size_to_read){
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
bool PelotonFrontendLogger::ReadTupleRecordHeader(TupleRecord& tupleRecord){

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
 * @brief read tuple record from log file and add them tuples to recovery txn
 * @param recovery txn
 */
void PelotonFrontendLogger::InsertTuple(void){

  TupleRecord tupleRecord(LOGRECORD_TYPE_PELOTON_TUPLE_INSERT);

  if( ReadTupleRecordHeader(tupleRecord) == false){
    // file is broken
    return;
  }

  SetInsertCommit(tupleRecord.GetInsertLocation(), true);

}

/**
 * @brief read tuple record from log file and add them tuples to recovery txn
 * @param recovery txn
 */
void PelotonFrontendLogger::DeleteTuple(void){

  TupleRecord tupleRecord(LOGRECORD_TYPE_PELOTON_TUPLE_DELETE);

  if( ReadTupleRecordHeader(tupleRecord) == false){
    // file is broken
    return;
  }

  SetDeleteCommit(tupleRecord.GetDeleteLocation(), true);
}

/**
 * @brief read tuple record from log file and add them tuples to recovery txn
 * @param recovery txn
 */
void PelotonFrontendLogger::UpdateTuple(void){

  TupleRecord tupleRecord(LOGRECORD_TYPE_PELOTON_TUPLE_UPDATE);

  if( ReadTupleRecordHeader(tupleRecord) == false){
    // file is broken
    return;
  }

  SetInsertCommit(tupleRecord.GetInsertLocation(), true);
  SetDeleteCommit(tupleRecord.GetDeleteLocation(), true);
}

void PelotonFrontendLogger::SkipTxnRecord(LogRecordType log_record_type){
  // read transaction information from the log file
  TransactionRecord txnRecord(log_record_type);

  if( ReadTxnRecord(txnRecord) == false ){
    // file is broken
    return;
  }
}

void PelotonFrontendLogger::SetInsertCommit(ItemPointer location, bool commit){
  //Commit Insert Mark
  auto &manager = catalog::Manager::GetInstance();
  auto tile_group = manager.GetTileGroup(location.block);
  auto tile_group_header = tile_group->GetHeader();
  tile_group_header->SetInsertCommit(location.offset, commit); 
}

void PelotonFrontendLogger::SetDeleteCommit(ItemPointer location, bool commit){
  //Commit Insert Mark
  auto &manager = catalog::Manager::GetInstance();
  auto tile_group = manager.GetTileGroup(location.block);
  auto tile_group_header = tile_group->GetHeader();
  tile_group_header->SetDeleteCommit(location.offset, commit); 
}

}  // namespace logging
}  // namespace peloton
