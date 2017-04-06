// //===----------------------------------------------------------------------===//
// //
// //                         Peloton
// //
// // logger_configuration.cpp
// //
// // Identification: src/main/logger/logger_configuration.cpp
// //
// // Copyright (c) 2015-16, Carnegie Mellon University Database Group
// //
// //===----------------------------------------------------------------------===//


// #include <iomanip>
// #include <algorithm>
// #include <sys/stat.h>

// #include "common/exception.h"
// #include "common/logger.h"
// #include "storage/storage_manager.h"

// #include "benchmark/logger/logger_configuration.h"
// #include "benchmark/ycsb/ycsb_configuration.h"
// #include "benchmark/tpcc/tpcc_configuration.h"

// extern peloton::CheckpointType peloton_checkpoint_mode;

// namespace peloton {
// namespace benchmark {
// namespace logger {

// void Usage(FILE* out) {
//   fprintf(out,
//           "Command line options :  logger <options> \n"
//           "   -h --help              :  Print help message \n"
//           "   -s --asynchronous-mode :  Asynchronous mode \n"
//           "   -x --experiment-type   :  Experiment Type \n"
//           "   -f --data-file-size    :  Data file size (MB) \n"
//           "   -l --logging-type      :  Logging type \n"
//           "   -t --nvm-latency       :  NVM latency \n"
//           "   -q --pcommit-latency   :  pcommit latency \n"
//           "   -v --flush-mode        :  Flush mode \n"
//           "   -r --commit-interval   :  Group commit interval \n"
//           "   -j --log-dir           :  Log directory\n"
//           "   -y --benchmark-type    :  Benchmark type \n");
// }

// static struct option opts[] = {
//     {"asynchronous_mode", optional_argument, NULL, 's'},
//     {"experiment-type", optional_argument, NULL, 'x'},
//     {"data-file-size", optional_argument, NULL, 'f'},
//     {"logging-type", optional_argument, NULL, 'l'},
//     {"nvm-latency", optional_argument, NULL, 't'},
//     {"pcommit-latency", optional_argument, NULL, 'q'},
//     {"flush-mode", optional_argument, NULL, 'v'},
//     {"commit-interval", optional_argument, NULL, 'r'},
//     {"benchmark-type", optional_argument, NULL, 'y'},
//     {"log-dir", optional_argument, NULL, 'j'},
//     {NULL, 0, NULL, 0}};

// static void ValidateLoggingType(const configuration& state) {
//   if (state.logging_type <= LoggingType::INVALID) {
//     LOG_ERROR("Invalid logging_type :: %d", static_cast<int>(state.logging_type));
//     exit(EXIT_FAILURE);
//   }

//   LOG_INFO("Logging_type :: %s",
//            LoggingTypeToString(state.logging_type).c_str());
// }

// std::string BenchmarkTypeToString(BenchmarkType type) {
//   switch (type) {
//     case BENCHMARK_TYPE_INVALID:
//       return "INVALID";

//     case BENCHMARK_TYPE_YCSB:
//       return "YCSB";
//     case BENCHMARK_TYPE_TPCC:
//       return "TPCC";

//     default:
//       LOG_ERROR("Invalid benchmark_type :: %d", type);
//       exit(EXIT_FAILURE);
//   }
//   return "INVALID";
// }

// std::string ExperimentTypeToString(ExperimentType type) {
//   switch (type) {
//     case EXPERIMENT_TYPE_INVALID:
//       return "INVALID";

//     case EXPERIMENT_TYPE_THROUGHPUT:
//       return "THROUGHPUT";
//     case EXPERIMENT_TYPE_RECOVERY:
//       return "RECOVERY";
//     case EXPERIMENT_TYPE_STORAGE:
//       return "STORAGE";
//     case EXPERIMENT_TYPE_LATENCY:
//       return "LATENCY";

//     default:
//       LOG_ERROR("Invalid experiment_type :: %d", type);
//       exit(EXIT_FAILURE);
//   }

//   return "INVALID";
// }

// std::string AsynchronousTypeToString(AsynchronousType type) {
//   switch (type) {
//     case ASYNCHRONOUS_TYPE_INVALID:
//       return "INVALID";

//     case ASYNCHRONOUS_TYPE_SYNC:
//       return "SYNC";
//     case ASYNCHRONOUS_TYPE_ASYNC:
//       return "ASYNC";
//     case ASYNCHRONOUS_TYPE_DISABLED:
//       return "DISABLED";
//     case ASYNCHRONOUS_TYPE_NO_WRITE:
//       return "NO_WRITE";

//     default:
//       LOG_ERROR("Invalid asynchronous_mode :: %d", type);
//       exit(EXIT_FAILURE);
//   }

//   return "INVALID";
// }

// static void ValidateBenchmarkType(const configuration& state) {
//   if (state.benchmark_type <= 0 || state.benchmark_type > 3) {
//     LOG_ERROR("Invalid benchmark_type :: %d", state.benchmark_type);
//     exit(EXIT_FAILURE);
//   }

//   LOG_INFO("%s : %s", "benchmark_type",
//            BenchmarkTypeToString(state.benchmark_type).c_str());
// }

// static void ValidateDataFileSize(const configuration& state) {
//   if (state.data_file_size <= 0) {
//     LOG_ERROR("Invalid data_file_size :: %lu", state.data_file_size);
//     exit(EXIT_FAILURE);
//   }

//   LOG_INFO("data_file_size :: %lu", state.data_file_size);
// }

// static void ValidateExperimentType(const configuration& state) {
//   if (state.experiment_type < 0 || state.experiment_type > 4) {
//     LOG_ERROR("Invalid experiment_type :: %d", state.experiment_type);
//     exit(EXIT_FAILURE);
//   }

//   LOG_INFO("%s : %s", "experiment_type",
//            ExperimentTypeToString(state.experiment_type).c_str());
// }

// static void ValidateWaitTimeout(const configuration& state) {
//   if (state.wait_timeout < 0) {
//     LOG_ERROR("Invalid wait_timeout :: %d", state.wait_timeout);
//     exit(EXIT_FAILURE);
//   }

//   LOG_INFO("wait_timeout :: %d", state.wait_timeout);
// }

// static void ValidateFlushMode(const configuration& state) {
//   if (state.flush_mode <= 0 || state.flush_mode >= 3) {
//     LOG_ERROR("Invalid flush_mode :: %d", state.flush_mode);
//     exit(EXIT_FAILURE);
//   }

//   LOG_INFO("flush_mode :: %d", state.flush_mode);
// }

// static void ValidateAsynchronousMode(const configuration& state) {
//   if (state.asynchronous_mode <= ASYNCHRONOUS_TYPE_INVALID ||
//       state.asynchronous_mode > ASYNCHRONOUS_TYPE_NO_WRITE) {
//     LOG_ERROR("Invalid asynchronous_mode :: %d", state.asynchronous_mode);
//     exit(EXIT_FAILURE);
//   }

//   LOG_INFO("%s : %s", "asynchronous_mode",
//            AsynchronousTypeToString(state.asynchronous_mode).c_str());
// }

// static void ValidateNVMLatency(const configuration& state) {
//   if (state.nvm_latency < 0) {
//     LOG_ERROR("Invalid nvm_latency :: %d", state.nvm_latency);
//     exit(EXIT_FAILURE);
//   }

//   LOG_INFO("nvm_latency :: %d", state.nvm_latency);
// }

// static void ValidatePCOMMITLatency(const configuration& state) {
//   if (state.pcommit_latency < 0) {
//     LOG_ERROR("Invalid pcommit_latency :: %d", state.pcommit_latency);
//     exit(EXIT_FAILURE);
//   }

//   LOG_INFO("pcommit_latency :: %d", state.pcommit_latency);
// }

// static void ValidateLogFileDir(configuration& state) {
//   struct stat data_stat;
//   // Check the existence of the log directory
//   if (stat(state.log_file_dir.c_str(), &data_stat) != 0) {
//     LOG_ERROR("log_file_dir :: %s does not exist", state.log_file_dir.c_str());
//     exit(EXIT_FAILURE);
//   } else if (!(data_stat.st_mode & S_IFDIR)) {
//     LOG_ERROR("log_file_dir :: %s is not a directory", state.log_file_dir.c_str());
//     exit(EXIT_FAILURE);
//   }

//   LOG_INFO("log_file_dir :: %s", state.log_file_dir.c_str());
// }

// void ParseArguments(int argc, char* argv[], configuration& state) {
//   // Default Logger Values
//   state.logging_type = LoggingType::SSD_WAL;
//   state.log_file_dir = TMP_DIR;
//   state.data_file_size = 512;

//   state.experiment_type = EXPERIMENT_TYPE_THROUGHPUT;
//   state.wait_timeout = 200;
//   state.benchmark_type = BENCHMARK_TYPE_YCSB;
//   state.flush_mode = 2;
//   state.nvm_latency = 0;
//   state.pcommit_latency = 0;
//   state.asynchronous_mode = ASYNCHRONOUS_TYPE_SYNC;
//   state.checkpoint_type = CheckpointType::INVALID;

//   // YCSB Default Values
//   ycsb::state.index = IndexType::BWTREE;
//   ycsb::state.scale_factor = 1;
//   ycsb::state.duration = 10;
//   ycsb::state.profile_duration = 1;
//   ycsb::state.backend_count = 2;
//   ycsb::state.column_count = 10;
//   ycsb::state.operation_count = 10;
//   ycsb::state.update_ratio = 0.5;
//   ycsb::state.zipf_theta = 0.0;
//   ycsb::state.exp_backoff = false;
//   ycsb::state.string_mode = false;
//   ycsb::state.gc_mode = false;
//   ycsb::state.gc_backend_count = 1;

//   // TPC-C Default Values
//   tpcc::state.index = IndexType::BWTREE;
//   tpcc::state.scale_factor = 1;
//   tpcc::state.duration = 10;
//   tpcc::state.profile_duration = 1;
//   tpcc::state.backend_count = 2;
//   tpcc::state.warehouse_count = 2;
//   tpcc::state.exp_backoff = false;
//   tpcc::state.affinity = false;
//   tpcc::state.gc_mode = false;
//   tpcc::state.gc_backend_count = 1;


//   // Parse args
//   while (1) {
//     int idx = 0;
//     // logger - hs:x:f:l:t:q:v:r:y:
//     // ycsb   - hemgi:k:d:p:b:c:o:u:z:n:
//     // tpcc   - heagi:k:d:p:b:w:n:
//     int c = getopt_long(argc, argv, "hs:x:f:l:t:q:v:r:y:emgi:k:d:p:b:c:o:u:z:n:aw:j:",
//                         opts, &idx);

//     if (c == -1) break;

//     switch (c) {
//       case 's':
//         state.asynchronous_mode = (AsynchronousType)atoi(optarg);
//         break;
//       case 'x':
//         state.experiment_type = (ExperimentType)atoi(optarg);
//         break;
//       case 'f':
//         state.data_file_size = atoi(optarg);
//         break;
//       case 'j':
//         state.log_file_dir = optarg;
//         break;
//       case 'l':
//         state.logging_type = (LoggingType)atoi(optarg);
//         break;
//       case 't':
//         state.nvm_latency = atoi(optarg);
//         break;
//       case 'q':
//         state.pcommit_latency = atoi(optarg);
//         break;
//       case 'v':
//         state.flush_mode = atoi(optarg);
//         break;
//       case 'r':
//         state.wait_timeout = atoi(optarg);
//         break;
//       case 'y':
//         state.benchmark_type = (BenchmarkType)atoi(optarg);
//         break;

//       case 'i': {
//         char *index = optarg;
//         if (strcmp(index, "btree") == 0) {
//           ycsb::state.index = IndexType::BWTREE;
//           tpcc::state.index = IndexType::BWTREE;
//         } else if (strcmp(index, "bwtree") == 0) {
//           ycsb::state.index = IndexType::BWTREE;
//           tpcc::state.index = IndexType::BWTREE;
//         } else {
//           LOG_ERROR("Unknown index: %s", index);
//           exit(EXIT_FAILURE);
//         }
//         break;
//       }
//       case 'k':
//         ycsb::state.scale_factor = atoi(optarg);
//         tpcc::state.scale_factor = atof(optarg);
//         break;
//       case 'd':
//         ycsb::state.duration = atof(optarg);
//         tpcc::state.duration = atof(optarg);
//         break;
//       case 'p':
//         ycsb::state.profile_duration = atof(optarg);
//         tpcc::state.profile_duration = atof(optarg);
//         break;
//       case 'b':
//         ycsb::state.backend_count = atoi(optarg);
//         tpcc::state.backend_count = atoi(optarg);
//         break;
//       case 'c':
//         ycsb::state.column_count = atoi(optarg);
//         break;
//       case 'o':
//         ycsb::state.operation_count = atoi(optarg);
//         break;
//       case 'u':
//         ycsb::state.update_ratio = atof(optarg);
//         break;
//       case 'z':
//         ycsb::state.zipf_theta = atof(optarg);
//         break;
//       case 'e':
//         ycsb::state.exp_backoff = true;
//         tpcc::state.exp_backoff = true;
//         break;
//       case 'm':
//         ycsb::state.string_mode = true;
//         break;
//       case 'g':
//         ycsb::state.gc_mode = true;
//         tpcc::state.gc_mode = true;
//         break;
//       case 'n':
//         ycsb::state.gc_backend_count = atof(optarg);
//         tpcc::state.gc_backend_count = atof(optarg);
//         break;
//       case 'w':
//         tpcc::state.warehouse_count = atoi(optarg);
//         break;
//       case 'a':
//         tpcc::state.affinity = true;
//         break;

//       case 'h':
//         Usage(stderr);
//         ycsb::Usage(stderr);
//         tpcc::Usage(stderr);
//         exit(EXIT_FAILURE);
//         break;

//       default:
//         fprintf(stderr, "\nUnknown option: -%c-\n", c);
//         Usage(stderr);
//         ycsb::Usage(stderr);
//         tpcc::Usage(stderr);
//         exit(EXIT_FAILURE);
//         break;
//     }
//   }

//   if (state.checkpoint_type == CheckpointType::NORMAL &&
//       (state.logging_type == LoggingType::NVM_WAL ||
//        state.logging_type == LoggingType::SSD_WAL ||
//        state.logging_type == LoggingType::HDD_WAL)) {
//     peloton_checkpoint_mode = CheckpointType::NORMAL;
//   }

//   // Print Logger configuration
//   ValidateLoggingType(state);
//   ValidateExperimentType(state);
//   ValidateAsynchronousMode(state);
//   ValidateBenchmarkType(state);
//   ValidateDataFileSize(state);
//   ValidateLogFileDir(state);
//   ValidateWaitTimeout(state);
//   ValidateFlushMode(state);
//   ValidateNVMLatency(state);
//   ValidatePCOMMITLatency(state);

//   // Print YCSB configuration
//   if (state.benchmark_type == BENCHMARK_TYPE_YCSB) {
//     ycsb::ValidateIndex(ycsb::state);
//     ycsb::ValidateScaleFactor(ycsb::state);
//     ycsb::ValidateDuration(ycsb::state);
//     ycsb::ValidateProfileDuration(ycsb::state);
//     ycsb::ValidateBackendCount(ycsb::state);
//     ycsb::ValidateColumnCount(ycsb::state);
//     ycsb::ValidateOperationCount(ycsb::state);
//     ycsb::ValidateUpdateRatio(ycsb::state);
//     ycsb::ValidateZipfTheta(ycsb::state);
//     ycsb::ValidateGCBackendCount(ycsb::state);
//   }
//   // Print TPCC configuration
//   else if (state.benchmark_type == BENCHMARK_TYPE_TPCC) {
//     tpcc::ValidateIndex(tpcc::state);
//     tpcc::ValidateScaleFactor(tpcc::state);
//     tpcc::ValidateDuration(tpcc::state);
//     tpcc::ValidateProfileDuration(tpcc::state);
//     tpcc::ValidateBackendCount(tpcc::state);
//     tpcc::ValidateWarehouseCount(tpcc::state);
//     tpcc::ValidateGCBackendCount(tpcc::state);

//     // Static TPCC parameters
//     tpcc::state.item_count = 100000 * tpcc::state.scale_factor;
//     tpcc::state.districts_per_warehouse = 10;
//     tpcc::state.customers_per_district = 3000 * tpcc::state.scale_factor;
//     tpcc::state.new_orders_per_district = 900 * tpcc::state.scale_factor;
//   }
// }

// }  // namespace logger
// }  // namespace benchmark
// }  // namespace peloton
