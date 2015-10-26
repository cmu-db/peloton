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
#include "backend/logging/loggers/aries_frontend_logger.h"
#include "backend/logging/loggers/aries_backend_logger.h"
#include "backend/storage/backend_vm.h"
#include "backend/storage/database.h"
#include "backend/storage/data_table.h"
#include "backend/storage/tuple.h"
#include "backend/common/logger.h"

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

  for(auto log_record : global_queue){
    delete log_record;
  }

  // close the log file
  int ret = fclose(log_file);
  if( ret != 0 ){
    LOG_ERROR("Error occured while closing LogFile");
  }
}

/**
 * @brief flush all the log records to the file
 */
void AriesFrontendLogger::Flush(void) {

  // First, write all the record in the queue
  for( auto record : global_queue ){
    fwrite( record->GetMessage(), 
            sizeof(char), 
            record->GetMessageLength(), 
            log_file);
  }

  // Then, flush
  int ret = fflush(log_file);
  if( ret != 0 ){
    LOG_ERROR("Error occured in fflush(%d)", ret);
  }

  // Finally, sync
  ret = fsync(log_file_fd);
  if( ret != 0 ){
    LOG_ERROR("Error occured in fsync(%d)", ret);
  }

  // Clean up the frontend logger's queue
  for( auto record : global_queue ){
    delete record;
  }
  global_queue.clear();

  // Commit each backend logger
  backend_loggers = GetBackendLoggers();
  for( auto backend_logger : backend_loggers){
    backend_logger->Commit();
  }
}

//===--------------------------------------------------------------------===//
// Recovery
//===--------------------------------------------------------------------===//

/**
 * @brief Recovery system based on log file
 */
void AriesFrontendLogger::DoRecovery() {

  // Go over the log size if needed
  if(GetLogFileSize() > 0){
    bool reached_end_of_file = false;

    // Start the recovery transaction
    auto &txn_manager = concurrency::TransactionManager::GetInstance();

    // Although we call BeginTransaction here, recovery txn will not be
    // recoreded in log file since we are in recovery mode
    auto recovery_txn = txn_manager.BeginTransaction();

    // Go over each log record in the log file
    while(!reached_end_of_file){

      // Read the first byte to identify log record type
      // If that is not possible, then wrap up recovery
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
          reached_end_of_file = true;
          break;
      }
    }

    // Commit the recovery transaction
    txn_manager.CommitTransaction(recovery_txn);

    // Finally, abort ACTIVE transactions in recovery_txn_table
    AbortActiveTransactions();

    // After finishing recovery, set the next oid with maximum oid
    // observed during the recovery
    auto &manager = catalog::Manager::GetInstance();
    manager.SetNextOid(max_oid);
  }

}


/**
 * @brief Add new txn to recovery table
 */
void AriesFrontendLogger::AddTransactionToRecoveryTable(){
  // read transaction information from the log file
  TransactionRecord txn_record(LOGRECORD_TYPE_TRANSACTION_BEGIN);

  // Check for torn log write
  if( ReadTransactionRecordHeader(txn_record) == false ){
    return;
  }

  auto txn_id = txn_record.GetTransactionId();

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
  TransactionRecord txn_record(LOGRECORD_TYPE_TRANSACTION_END);

  // Check for torn log write
  if( ReadTransactionRecordHeader(txn_record) == false ){
    return;
  }

  auto txn_id = txn_record.GetTransactionId();

  // remove txn from recovery txn table
  auto txn = recovery_txn_table.at(txn_id);
  recovery_txn_table.erase(txn_id);

  // drop the txn as well
  delete txn;
  LOG_TRACE("Erase txd id %d object in table",(int)txn_id);
}

/**
 * @brief move tuples from current txn to recovery txn so that we can commit them later
 * @param recovery txn
 */
void AriesFrontendLogger::MoveCommittedTuplesToRecoveryTxn(concurrency::Transaction* recovery_txn) {

  // read transaction information from the log file
  TransactionRecord txn_record(LOGRECORD_TYPE_TRANSACTION_COMMIT);

  // Check for torn log write
  if( ReadTransactionRecordHeader(txn_record) == false ){
    return;
  }

  // Get info about the transaction from recovery table
  auto txn_id = txn_record.GetTransactionId();
  auto txn = recovery_txn_table.at(txn_id);

  // Copy inserted/deleted tuples to recovery transaction
  MoveTuples(recovery_txn, txn);

  LOG_TRACE("Commit txd id %d object in table",(int)txn_id);
}

/**
 * @brief Move Tuples from log record-based local transaction to recovery transaction
 * @param destination
 * @param source
 */
void AriesFrontendLogger::MoveTuples(concurrency::Transaction* destination,
                                     concurrency::Transaction* source){
  // This is the local transaction
  auto inserted_tuples = source->GetInsertedTuples();

  // Record the inserts in recovery txn
  for (auto entry : inserted_tuples) {
    storage::TileGroup *tile_group = entry.first;
    auto tile_group_id = tile_group->GetTileGroupId();

    for (auto tuple_slot : entry.second){
      destination->RecordInsert(ItemPointer(tile_group_id, tuple_slot));
    }
  }

  // Record the deletes in recovery txn
  auto deleted_tuples = source->GetDeletedTuples();
  for (auto entry : deleted_tuples) {
    storage::TileGroup *tile_group = entry.first;
    auto tile_group_id = tile_group->GetTileGroupId();

    for (auto tuple_slot : entry.second)
      destination->RecordDelete( ItemPointer(tile_group_id, tuple_slot));
  }

  // Clear inserted/deleted tuples from txn, just in case
  source->ResetState();

}

/**
 * @brief abort tuple
 */
void AriesFrontendLogger::AbortTuplesFromRecoveryTable(){

  // read transaction information from the log file
  TransactionRecord txn_record(LOGRECORD_TYPE_TRANSACTION_END);

  // Check for torn log write
  if( ReadTransactionRecordHeader(txn_record) == false ){
    return;
  }

  auto txn_id = txn_record.GetTransactionId();

  // Get info about the transaction from recovery table
  auto txn = recovery_txn_table.at(txn_id);

  AbortTuples(txn);

  LOG_INFO("Abort txd id %d object in table",(int)txn_id);
}

/**
 * @brief Abort tuples inside txn
 * @param txn
 */
void AriesFrontendLogger::AbortTuples(concurrency::Transaction* txn){

  LOG_INFO("Abort txd id %d object in table",(int)txn->GetTransactionId());

  // Record the aborted inserts in recovery txn
  auto inserted_tuples = txn->GetInsertedTuples();
  for (auto entry : inserted_tuples) {
    storage::TileGroup *tile_group = entry.first;

    for (auto tuple_slot : entry.second)
      tile_group->AbortInsertedTuple(tuple_slot);
  }

  // Record the aborted deletes in recovery txn
  auto deleted_tuples = txn->GetDeletedTuples();
  for (auto entry : txn->GetDeletedTuples()) {
    storage::TileGroup *tile_group = entry.first;

    for (auto tuple_slot : entry.second)
      tile_group->AbortDeletedTuple(tuple_slot, txn->GetTransactionId());
  }

  // Clear inserted/deleted tuples from txn, just in case
  txn->ResetState();
}

/**
 * @brief Abort tuples inside txn table
 */
void AriesFrontendLogger::AbortActiveTransactions(){

  // Clean up the recovery table to ignore active transactions
  for(auto  active_txn_entry : recovery_txn_table){
    // clean up the transaction
    auto active_txn = active_txn_entry.second;
    AbortTuples(active_txn);
    delete active_txn;
  }

  recovery_txn_table.clear();
}

/**
 * @brief read tuple record from log file and add them tuples to recovery txn
 * @param recovery txn
 */
void AriesFrontendLogger::InsertTuple(concurrency::Transaction* recovery_txn){

  TupleRecord tuple_record(LOGRECORD_TYPE_ARIES_TUPLE_INSERT);

  // Check for torn log write
  if( ReadTupleRecordHeader(tuple_record) == false){
    return;
  }

  auto table = GetTable(tuple_record);
  // TODO: Remove per-record backend construction
  storage::AbstractBackend *backend = new storage::VMBackend();
  VarlenPool *pool = new VarlenPool(backend);
 
  // Read off the tuple record body from the log
  auto tuple = ReadTupleRecordBody(table->GetSchema(), pool);

  // Check for torn log write
  if( tuple == nullptr) {
    delete pool;
    delete backend;
    return;
  }

  auto target_location = tuple_record.GetInsertLocation();
  auto tile_group_id = target_location.block;
  auto tuple_slot = target_location.offset;

  auto tile_group = GetTileGroup(tile_group_id);

  auto txn_id = tuple_record.GetTransactionId();
  auto txn = recovery_txn_table.at(txn_id);

  // Create new tile group if table doesn't already have that tile group
  if(tile_group == nullptr){
    table->AddTileGroupWithOid(tile_group_id);
    tile_group = table->GetTileGroupById(tile_group_id);
    if( max_oid < tile_group_id ){
      max_oid = tile_group_id;
    }
  }

  // Do the insert !
  auto inserted_tuple_slot = tile_group->InsertTuple(recovery_txn->GetTransactionId(),
                                                     tuple_slot, tuple);

  if (inserted_tuple_slot == INVALID_OID) {
    // TODO: We need to abort on failure !
    recovery_txn->SetResult(Result::RESULT_FAILURE);
  } else{
    txn->RecordInsert(target_location);
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

  TupleRecord tuple_record(LOGRECORD_TYPE_ARIES_TUPLE_DELETE);

  // Check for torn log write
  if( ReadTupleRecordHeader(tuple_record) == false){
    return;
  }

  auto table = GetTable(tuple_record);

  ItemPointer delete_location = tuple_record.GetDeleteLocation();

  // Try to delete the tuple
  bool status = table->DeleteTuple(recovery_txn, delete_location);
  if (status == false) {
    // TODO: We need to abort on failure !
    recovery_txn->SetResult(Result::RESULT_FAILURE);
    return;
  }

  auto txn_id = tuple_record.GetTransactionId();
  auto txn = recovery_txn_table.at(txn_id);
  txn->RecordDelete(delete_location);
}

/**
 * @brief read tuple record from log file and add them tuples to recovery txn
 * @param recovery txn
 */
void AriesFrontendLogger::UpdateTuple(concurrency::Transaction* recovery_txn){

  TupleRecord tuple_record(LOGRECORD_TYPE_ARIES_TUPLE_UPDATE);

  // Check for torn log write
  if( ReadTupleRecordHeader(tuple_record) == false){
    return;
  }

  auto txn_id = tuple_record.GetTransactionId();
  auto txn = recovery_txn_table.at(txn_id);

  auto table = GetTable(tuple_record);

  // TODO: Remove per-record backend construction
  storage::AbstractBackend *backend = new storage::VMBackend();
  VarlenPool *pool = new VarlenPool(backend);

  auto tuple = ReadTupleRecordBody(table->GetSchema(), pool);

  // Check for torn log write
  if( tuple == nullptr){
     delete pool;
     delete backend;
    return;
  }

  // First, redo the delete
  ItemPointer delete_location = tuple_record.GetDeleteLocation();

  bool status = table->DeleteTuple(recovery_txn, delete_location);
  if (status == false) {
    recovery_txn->SetResult(Result::RESULT_FAILURE);
  }
  else{
    txn->RecordDelete(delete_location);

    auto target_location = tuple_record.GetInsertLocation();
    auto tile_group_id = target_location.block;
    auto tuple_slot = target_location.offset;
    auto tile_group = GetTileGroup(tile_group_id);

    // Create new tile group if table doesn't already have that tile group
    if(tile_group == nullptr){
      table->AddTileGroupWithOid(tile_group_id);
      tile_group = table->GetTileGroupById(tile_group_id);
      if( max_oid < tile_group_id ){
        max_oid = tile_group_id;
      }
    }

    // Do the insert !
    auto inserted_tuple_slot = tile_group->InsertTuple(recovery_txn->GetTransactionId(),
                                                       tuple_slot, tuple);
    if (inserted_tuple_slot == INVALID_OID) {
      recovery_txn->SetResult(Result::RESULT_FAILURE);
    }
    else{
      txn->RecordInsert(target_location);
    }
  }

  delete tuple;
  delete pool;
  delete backend;
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
  CopySerializeInputBE frameCheck(buffer, sizeof(int32_t));
  frame_size = (frameCheck.ReadInt())+sizeof(int32_t);;

  // Check if the frame is broken
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
  return global_queue.size();
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

  CopySerializeInputBE input(&buffer, sizeof(char));
  LogRecordType log_record_type = (LogRecordType)(input.ReadEnumInSingleByte());

  return log_record_type;
}

/**
 * @brief Read TransactionRecord
 * @param txn_record
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

  CopySerializeInputBE txn_header(header, header_size);
  txn_record.Deserialize(txn_header);

  return true;
}

/**
 * @brief Read TupleRecordHeader
 * @param tuple_record
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

  CopySerializeInputBE tuple_header(header,header_size);
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
                                                         VarlenPool *pool){
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

  CopySerializeInputBE tuple_body(body, body_size);

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
