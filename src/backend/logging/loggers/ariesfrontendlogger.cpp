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

#include <sys/stat.h>
#include <sys/mman.h>

#include "backend/storage/backend_vm.h"

namespace peloton {
namespace logging {

AriesFrontendLogger::AriesFrontendLogger(){

  logging_type = LOGGING_TYPE_ARIES;

  // if log file already exists, do Restore  
  if(LogFileSize() > 0){
    Restore();
  }

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
    fwrite( record.GetSerializedLogRecord(),
            sizeof(char),
            record.GetSerializedLogRecordSize(),
            logFile);
  }

  // TODO :: What if db crashed during the flush ..?

  int ret = fflush(logFile);
  if( ret != 0 ){
    LOG_ERROR("Error occured in fflush ");
  }
  ret = fsync(logFileFd);
  if( ret != 0 ){
    LOG_ERROR("Error occured in fsync");
  }
  aries_global_queue.clear();
}

void AriesFrontendLogger::Restore() {

  logFile = fopen( filename.c_str(),"rb");
  if(logFile == NULL){
    LOG_ERROR("LogFile is NULL");
  }

  oid_t db_oid;
  oid_t table_oid;
  txn_id_t txn_id;
  LogRecordType log_record_type;

  char buffer[100];
  size_t n = fread(buffer, 1, sizeof(buffer), logFile);
  if( n <= 0 ){
    LOG_ERROR("!!\n");
  }

  CopySerializeInput input(buffer,100);
  log_record_type = (LogRecordType)(input.ReadEnumInSingleByte());
  db_oid = (input.ReadShort());
  table_oid = (input.ReadShort());
  txn_id = (input.ReadLong());

  std::cout << "log type  : " << LogRecordTypeToString(log_record_type) << std::endl;
  std::cout << "db oid : " << db_oid << std::endl;
  std::cout << "table oid : " << table_oid << std::endl;
  std::cout << "txn id : " << txn_id << std::endl;


  // read serialized data from file
  /*
  //open table
  storage::DataTable *table = db->GetTableWithOid(table_oid);
  table->InsertTuple(txn, &tuple)

  CopySerializeInput input(output.Data(), output.Size());
  storage::AbstractBackend *backend = new storage::VMBackend();
  Pool *pool = new Pool(backend);
  // make a tuple here with serialize data and then insert to table 
  tuple->DeserializeFrom(input, pool);
  std::cout << "input : ";
  std::cout << input.ReadInt() << std::endl;
  */


  int ret = fclose(logFile);
  if( ret != 0 ){
    LOG_ERROR("Error occured while closing LogFile");
  }
}

/**
 * @brief Get global_queue size
 * @return return the size of global_queue
 */
size_t AriesFrontendLogger::GetLogRecordCount() const{
  return aries_global_queue.size();
}

size_t AriesFrontendLogger::LogFileSize(){
  struct stat logStats;   
  if(stat(filename.c_str(), &logStats) == 0){
    logFileFd = open(filename.c_str(), O_RDONLY, mode);
    fstat(logFileFd, &logStats);
    close(logFileFd);
    return logStats.st_size;
  }else{
    return 0;
  }
}

}  // namespace logging
}  // namespace peloton
