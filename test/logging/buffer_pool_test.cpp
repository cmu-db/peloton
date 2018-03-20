// //===----------------------------------------------------------------------===//
// //
// //                         Peloton
// //
// // buffer_pool_test.cpp
// //
// // Identification: test/logging/buffer_pool_test.cpp
// //
// // Copyright (c) 2015-16, Carnegie Mellon University Database Group
// //
// //===----------------------------------------------------------------------===//


// #include "common/harness.h"
// #include "logging/circular_buffer_pool.h"
// #include <stdlib.h>

// #include "executor/testing_executor_util.h"
// #include "logging/testing_logging_util.h"

// namespace peloton {
// namespace test {

// //===--------------------------------------------------------------------===//
// // Buffer Pool Tests
// //===--------------------------------------------------------------------===//

// class BufferPoolTests : public PelotonTest {};

// void EnqueueTest(logging::CircularBufferPool *buffer_pool, unsigned int count) {
//   for (unsigned int i = 0; i < count; i++) {
//     std::unique_ptr<logging::LogBuffer> buf(new logging::LogBuffer(nullptr));
//     buf->SetSize(i);
//     buffer_pool->Put(std::move(buf));
//   }
// }

// void DequeueTest(logging::CircularBufferPool *buffer_pool, unsigned int count) {
//   for (unsigned int i = 0; i < count; i++) {
//     auto buf = std::move(buffer_pool->Get());
//     PL_ASSERT(buf);
//     EXPECT_EQ(buf->GetSize(), i);
//   }
// }

// void BackendThread(logging::WriteAheadBackendLogger *logger,
//                    unsigned int count) {
//   for (unsigned int i = 1; i <= count; i++) {
//     logging::TransactionRecord begin_record(LOGRECORD_TYPE_TRANSACTION_COMMIT,
//                                             i);
//     logger->Log(&begin_record);
//   }
// }

// void FrontendThread(logging::WriteAheadFrontendLogger *logger,
//                     unsigned int count) {
//   srand(time(NULL));
//   while (true) {
//     for (int i = 0; i < 10; i++) {
//       logger->CollectLogRecordsFromBackendLoggers();
//     }
//     logger->FlushLogRecords();
//     auto cid = logger->GetMaxFlushedCommitId();
//     if (cid == count) {
//       break;
//     }
//     int rand_num = rand() % 5 + 1;
//     std::chrono::milliseconds sleep_time(rand_num);
//     std::this_thread::sleep_for(sleep_time);
//   }
// }

// TEST_F(BufferPoolTests, BufferPoolBasicTest) {
//   logging::CircularBufferPool buffer_pool0;
//   EnqueueTest(&buffer_pool0, 5);
//   EXPECT_EQ(buffer_pool0.GetSize(), 5);

//   DequeueTest(&buffer_pool0, 5);
//   EXPECT_EQ(buffer_pool0.GetSize(), 0);

//   EnqueueTest(&buffer_pool0, BUFFER_POOL_SIZE);
//   EXPECT_EQ(buffer_pool0.GetSize(), BUFFER_POOL_SIZE);

//   for (int i = 0; i < 10; i++) {
//     logging::CircularBufferPool buffer_pool1;
//     std::thread enqueue_thread(EnqueueTest, &buffer_pool1, BUFFER_POOL_SIZE);
//     std::thread dequeue_thread(DequeueTest, &buffer_pool1, BUFFER_POOL_SIZE);
//     enqueue_thread.join();
//     dequeue_thread.join();
//   }
// }

// TEST_F(BufferPoolTests, LogBufferBasicTest) {
//   size_t tile_group_size = TESTS_TUPLES_PER_TILEGROUP;
//   size_t table_tile_group_count = 3;

//   std::unique_ptr<storage::DataTable> recovery_table(
//       TestingExecutorUtil::CreateTable(tile_group_size));

//   // prepare tuples
//   auto mutate = true;
//   auto random = false;
//   int num_rows = tile_group_size * table_tile_group_count;
//   std::vector<std::shared_ptr<storage::Tuple>> tuples =
//       TestingLoggingUtil::BuildTuples(recovery_table.get(), num_rows, mutate,
//                                     random);
//   std::vector<logging::TupleRecord> records =
//       TestingLoggingUtil::BuildTupleRecords(tuples, tile_group_size,
//                                           table_tile_group_count);
//   logging::LogBuffer log_buffer(0);
//   size_t total_length = 0;
//   for (auto record : records) {
//     PL_ASSERT(record.GetTuple()->GetSchema());
//     CopySerializeOutput output_buffer;
//     record.Serialize(output_buffer);
//     size_t len = record.GetMessageLength();
//     total_length += len;
//     log_buffer.WriteRecord(&record);
//   }
//   EXPECT_EQ(log_buffer.GetSize(), total_length);
// }

// TEST_F(BufferPoolTests, LargeTupleRecordTest) {
//   auto testing_pool = TestingHarness::GetInstance().GetTestingPool();

//   std::vector<catalog::Column> columns;

//   catalog::Column column1(type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
//                           "A", true);
//   catalog::Column column2(type::TypeId::VARCHAR, 1024 * 1024 * 20, "B", false);

//   columns.push_back(column1);
//   columns.push_back(column2);

//   std::unique_ptr<catalog::Schema> key_schema(new catalog::Schema(columns));
//   std::unique_ptr<storage::Tuple> tuple(
//       new storage::Tuple(key_schema.get(), true));
//   tuple->SetValue(0, type::ValueFactory::GetIntegerValue(1), testing_pool);
//   tuple->SetValue(1, type::ValueFactory::GetVarcharValue(
//                          std::string(1024 * 1024 * 20, 'e').c_str()),
//                   testing_pool);

//   logging::TupleRecord record(LOGRECORD_TYPE_WAL_TUPLE_INSERT, INITIAL_TXN_ID,
//                               INVALID_OID, INVALID_ITEMPOINTER,
//                               INVALID_ITEMPOINTER, tuple.get(), DEFAULT_DB_ID);
//   record.SetTuple(tuple.get());

//   logging::LogBuffer log_buffer(0);
//   size_t total_length = 0;
//   PL_ASSERT(record.GetTuple()->GetSchema());
//   CopySerializeOutput output_buffer;
//   record.Serialize(output_buffer);
//   size_t len = record.GetMessageLength();
//   total_length += len;
//   auto success = log_buffer.WriteRecord(&record);
//   EXPECT_EQ(log_buffer.GetSize(), total_length);
//   EXPECT_TRUE(success);
// }

// TEST_F(BufferPoolTests, BufferPoolConcurrentTest) {
//   unsigned int txn_count = 9999;

//   auto &log_manager = logging::LogManager::GetInstance();
//   logging::LogManager::GetInstance().Configure(LoggingType::NVM_WAL, true);
//   log_manager.SetLoggingStatus(LoggingStatusType::LOGGING);
//   log_manager.InitFrontendLoggers();

//   logging::WriteAheadFrontendLogger *frontend_logger =
//       reinterpret_cast<logging::WriteAheadFrontendLogger *>(
//           log_manager.GetFrontendLogger(0));
//   logging::WriteAheadBackendLogger *backend_logger =
//       reinterpret_cast<logging::WriteAheadBackendLogger *>(
//           log_manager.GetBackendLogger());

//   std::thread backend_thread(BackendThread, backend_logger, txn_count);
//   std::thread frontend_thread(FrontendThread, frontend_logger, txn_count);
//   backend_thread.join();
//   frontend_thread.join();
// }

// }  // namespace test
// }  // namespace peloton
