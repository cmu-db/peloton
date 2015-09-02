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


#include <sys/stat.h>
#include <sys/mman.h>

#include "backend/catalog/manager.h"
#include "backend/catalog/schema.h"
#include "backend/storage/backend_vm.h"
#include "backend/storage/database.h"
#include "backend/storage/data_table.h"
#include "backend/storage/tuple.h"
#include "backend/logging/loggers/peloton_frontend_logger.h"
#include "backend/logging/loggers/peloton_backend_logger.h"

namespace peloton {
namespace logging {

/**
 * @brief Open logfile and file descriptor
 */
PelotonFrontendLogger::PelotonFrontendLogger(){

  logging_type = LOGGING_TYPE_PELOTON;

  // open log file and file descriptor
  log_file = fopen( file_name.c_str(),"ab+");
  if(log_file == NULL){
    LOG_ERROR("LogFile is NULL");
  }

  log_file_fd = fileno(log_file);
  if( log_file_fd == -1){
    LOG_ERROR("LogFileFd is -1");
  }
}

/**
 * @brief close logfile
 */
PelotonFrontendLogger::~PelotonFrontendLogger(){

  for(auto log_record : global_queue){
    delete log_record;
  }

  int ret = fclose(log_file);
  if( ret != 0 ){
    LOG_ERROR("Error occured while closing LogFile");
  }
}

/**
 * @brief flush all record to the file
 */
void PelotonFrontendLogger::Flush(void) {
 
  std::vector<ItemPointer> inserted_tuples;
  std::vector<ItemPointer> deleted_tuples;

  //  First, write out the log record
  for( auto record : global_queue ){
    fwrite( record->GetMessage(), 
            sizeof(char), 
            record->GetMessageLength(), 
            log_file);

    // Collect the commit information
    CollectCommittedTuples((TupleRecord*)record,
                           inserted_tuples,
                           deleted_tuples );
  }

  // Then, write out the log record
  int ret = fflush(log_file);
  if( ret != 0 ){
    LOG_ERROR("Error occured in fflush(%d)", ret);
  }

  ret = fsync(log_file_fd);
  if( ret != 0 ){
    LOG_ERROR("Error occured in fsync(%d)", ret);
  }

  for( auto record : global_queue ){
    delete record;
  }
  global_queue.clear();

  // Commit each backend logger 
  backend_loggers = GetBackendLoggers();

  for( auto backend_logger : backend_loggers){
    if(backend_logger->IsWaitingForFlushing()){
      backend_logger->Commit();
    }
  }

  // Set insert/delete COMMIT marks based on collected information
  for( auto inserted_tuple : inserted_tuples){
    SetInsertCommitMark(inserted_tuple, true);
  }
  for( auto deleted_tuple : deleted_tuples){
    SetDeleteCommitMark(deleted_tuple, true);
  }

  if( inserted_tuples.size() > 0 || deleted_tuples.size() > 0){
    auto done_mark = new TransactionRecord(LOGRECORD_TYPE_TRANSACTION_DONE, 1/*FIXME*/);
    done_mark ->Serialize();

    // Finally, write out the DONE mark
    fwrite( done_mark ->GetMessage(), sizeof(char), done_mark->GetMessageLength(), log_file);

    // And, flush it out
    ret = fflush(log_file);
    if( ret != 0 ){
      LOG_ERROR("Error occured in fflush(%d)", ret);
    }

    ret = fsync(log_file_fd);
    if( ret != 0 ){
      LOG_ERROR("Error occured in fsync(%d)", ret);
    }
    delete done_mark;
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

//===--------------------------------------------------------------------===//
// Recovery 
//===--------------------------------------------------------------------===//

/**
 * @brief Recovery system based on log file
 */
void PelotonFrontendLogger::DoRecovery() {

  if(GetLogFileSize() > 0){

    if(DoWeNeedRecovery() ){

      bool EOF_OF_LOG_FILE = false;

      //JumpToLastActiveTransaction();

      while(!EOF_OF_LOG_FILE){

        std::cout << "offset : " << ftell(log_file) << std::endl;
        // Read the first single bite so that we can distinguish log record type
        // otherwise, finish the recovery 
        switch(GetNextLogRecordType()){

          case LOGRECORD_TYPE_TRANSACTION_BEGIN:
            printf("BEGIN \n");
            SkipTransactionRecord(LOGRECORD_TYPE_TRANSACTION_BEGIN);
            break;

          case LOGRECORD_TYPE_TRANSACTION_COMMIT:
            printf("COMMIT\n");
            SkipTransactionRecord(LOGRECORD_TYPE_TRANSACTION_COMMIT);
            break;

          case LOGRECORD_TYPE_TRANSACTION_END:
            printf("END\n");
            SkipTransactionRecord(LOGRECORD_TYPE_TRANSACTION_END);
            break;

          case LOGRECORD_TYPE_TRANSACTION_DONE:
            printf("DONE\n");
            SkipTransactionRecord(LOGRECORD_TYPE_TRANSACTION_DONE);
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

  SetInsertCommitMark(tupleRecord.GetInsertLocation(), true);

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

  SetDeleteCommitMark(tupleRecord.GetDeleteLocation(), true);
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

  SetInsertCommitMark(tupleRecord.GetInsertLocation(), true);
  SetDeleteCommitMark(tupleRecord.GetDeleteLocation(), true);
}

void PelotonFrontendLogger::SkipTransactionRecord(LogRecordType log_record_type){
  // read transaction information from the log file
  TransactionRecord txnRecord(log_record_type);

  if( ReadTransactionRecordHeader(txnRecord) == false ){
    // file is broken
    return;
  }
}

void PelotonFrontendLogger::SetInsertCommitMark(ItemPointer location, bool commit){
  //Commit Insert Mark
  auto &manager = catalog::Manager::GetInstance();
  auto tile_group = manager.GetTileGroup(location.block);
  auto tile_group_header = tile_group->GetHeader();
  tile_group_header->SetInsertCommit(location.offset, commit); 
}

void PelotonFrontendLogger::SetDeleteCommitMark(ItemPointer location, bool commit){
  //Commit Insert Mark
  auto &manager = catalog::Manager::GetInstance();
  auto tile_group = manager.GetTileGroup(location.block);
  auto tile_group_header = tile_group->GetHeader();
  tile_group_header->SetDeleteCommit(location.offset, commit); 
}

//===--------------------------------------------------------------------===//
// Utility functions
//===--------------------------------------------------------------------===//

/**
 * @brief Read single byte so that we can distinguish the log record type
 * @return log record type otherwise return invalid log record type, which menas there is no more log in the log file
 */
LogRecordType PelotonFrontendLogger::GetNextLogRecordType(){
  char buffer;
  if( IsFileTruncated(1) ){
    return LOGRECORD_TYPE_INVALID;
  }
  int ret = fread((void*)&buffer, 1, sizeof(char), log_file);
  if( ret <= 0 ){
    return LOGRECORD_TYPE_INVALID;
  }
  CopySerializeInputBE input(&buffer, sizeof(char));
  LogRecordType log_record_type = (LogRecordType)(input.ReadEnumInSingleByte());
  return log_record_type;
}

bool PelotonFrontendLogger::DoWeNeedRecovery(void){
  char buffer;

  // Read last transaction record type
  fseek(log_file, -TransactionRecord::GetTransactionRecordSize(), SEEK_END);
  int ret = fread((void*)&buffer, 1, sizeof(char), log_file);
  if( ret <= 0 ){
    LOG_ERROR("Error occured in fread(%d)", ret);
  }
  CopySerializeInputBE input(&buffer, sizeof(char));
  LogRecordType log_record_type = (LogRecordType)(input.ReadEnumInSingleByte());

  std::cout << "Log Record Type : " << LogRecordTypeToString(log_record_type) << std::endl;

  // If last transaction record is COMMIT or END 
  // Do recovery
  if( log_record_type == LOGRECORD_TYPE_TRANSACTION_COMMIT ||
      log_record_type == LOGRECORD_TYPE_TRANSACTION_END){
    fseek(log_file, 0, SEEK_SET);
    return true;
  }else{
    fseek(log_file, 0, SEEK_END);
    return false;
  }
}

 

void PelotonFrontendLogger::JumpToLastActiveTransaction(){
  while(1){
    auto entire_txn_size = GetNextFrameSize();
    auto current_offset = ftell(log_file);
    if(current_offset+entire_txn_size > GetLogFileSize()){
      break;
    }
    fseek(log_file, entire_txn_size , SEEK_CUR);
  }
}

/**
 * @brief Measure the size of log file
 * @return the size if the log file exists otherwise 0
 */
size_t PelotonFrontendLogger::GetLogFileSize(){
  struct stat logStats;   
  if(stat(file_name.c_str(), &logStats) == 0){
    fstat(log_file_fd, &logStats);
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

  if( IsFileTruncated(sizeof(int32_t)) ){
    return 0;
  }

  size_t ret = fread(buffer, 1, sizeof(buffer), log_file);
  if( ret <= 0 ){
    LOG_ERROR("Error occured in fread ");
  }

  // Read next 4 bytes as an integer
  CopySerializeInputBE frameCheck(buffer, sizeof(int32_t));
  frame_size = (frameCheck.ReadInt())+sizeof(int32_t);;

  /* go back 4 bytes */
  int res = fseek(log_file, -sizeof(int32_t), SEEK_CUR);
  if(res == -1){
    LOG_ERROR("Error occured in fseek ");
  }

  if( IsFileTruncated(frame_size) ){
    // file is broken
    return 0;
  }

  return frame_size;
}

/**
 * @brief Read TransactionRecord
 * @param txnRecord
 */
bool PelotonFrontendLogger::ReadTransactionRecordHeader(TransactionRecord &txnRecord){

  auto txn_record_size = GetNextFrameSize();

  if( txn_record_size == 0 ){
    // file is broken
    return false;
  }

  char txn_record[txn_record_size];
  size_t ret = fread(txn_record, 1, sizeof(txn_record), log_file);
  if( ret <= 0 ){
    LOG_ERROR("Error occured in fread ");
  }
  CopySerializeInputBE logTxnRecord(txn_record,txn_record_size);
  txnRecord.Deserialize(logTxnRecord);

  return true;
}

bool PelotonFrontendLogger::IsFileTruncated(size_t size_to_read){
  size_t current_position = ftell(log_file);
  if( GetLogFileSize() < (current_position+size_to_read)){
    fseek(log_file, 0, SEEK_END);
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
  size_t ret = fread(header, 1, sizeof(header), log_file);
  if( ret <= 0 ){
    LOG_ERROR("Error occured in fread ");
  }

  CopySerializeInputBE logHeader(header,header_size);
  tupleRecord.DeserializeHeader(logHeader);

  return true;
}


}  // namespace logging
}  // namespace peloton
