//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// csv_scanner.h
//
// Identification: src/include/codegen/util/csv_scanner.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <vector>

#include "codegen/type/type.h"
#include "util/file.h"

namespace peloton {

namespace executor {
class ExecutorContext;
}  // namespace executor

namespace type {
class AbstractPool;
}  // namespace type

namespace codegen {
namespace util {

/**
 * This is the primary class to scan CSV files.  Callers use the constructor to
 * configure various aspects of how parsing is performed.  Callers must provide
 * a description of the rows stored in the CSV file, and a callback function
 * that is invoked once for every row in the CSV file.  The delimiter character,
 * quoting character, and escape characters can also be configured through the
 * constructor.
 *
 * This scanner class is fail-fast. If it finds an ill-formatted row, it will
 * immediately throw an error.
 *
 * TODO: implement a more generous parser that is best-effort.
 */
class CSVScanner {
 public:
  // 64K buffer size
  static constexpr uint32_t kDefaultBufferSize = (1ul << 16ul);

  // We allocate a maximum of 1GB for the line buffer
  static constexpr uint64_t kMaxAllocSize = (1ul << 30ul);

  // The signature of the callback function
  using Callback = void (*)(void *);

  /**
   * Column information
   */
  struct Column {
    // The type of data this column represents
    codegen::type::Type col_type;

    // A pointer to where the next value of this column is
    const char *ptr;

    // The number of bytes
    uint32_t len;

    // Is the next value of this column NULL
    bool is_null;
  };

  /**
   * This structure tracks various statistics while we scan the CSV
   */
  struct Stats {
    // The number of times the read-buffer was copied into the line-buffer
    uint32_t num_copies = 0;
    // The number of times we had to re-allocate the line-buffer to make room
    // for new data (i.e., to handle really long lines that don't fit into the
    // read-buffer)
    uint32_t num_reallocs = 0;
    // The number of times we had to call Read() from the file
    uint32_t num_reads = 0;
  };

  /**
   * Constructor.
   *
   * @param memory A memory pool where all allocations are sourced from
   * @param file_path The full path to the CSV file
   * @param col_types A description of the rows stored in the CSV
   * @param num_cols The number of columns to expect
   * @param func The callback function to invoke per row/line in the CSV
   * @param opaque_state An opaque state that is passed to the callback function
   * upon invocation.
   * @param delimiter The character that separates columns within a row
   * @param quote The quoting character used to quote data (i.e., strings)
   * @param escape The character that should appear before any data characters
   * that match the quote character.
   */
  CSVScanner(peloton::type::AbstractPool &memory, const std::string &file_path,
             const codegen::type::Type *col_types, uint32_t num_cols,
             Callback func, void *opaque_state, char delimiter = ',',
             char quote = '"', char escape = '"');

  /**
   * Destructor
   */
  ~CSVScanner();

  /**
   * Initialization function. This is the entry point from codegen to initialize
   * scanner instances.
   *
   * @param scanner The scanner we're initializing
   * @param memory A memory pool where all allocations are sourced from
   * @param file_path The full path to the CSV file
   * @param col_types A description of the rows stored in the CSV
   * @param num_cols The number of columns to expect
   * @param func The callback function to invoke per row/line in the CSV
   * @param opaque_state An opaque state that is passed to the callback function
   * upon invocation.
   * @param delimiter The character that separates columns within a row
   * @param quote The quoting character used to quote data (i.e., strings)
   * @param escape The character that should appear before any data characters
   * that match the quote character.
   */
  static void Init(CSVScanner &scanner,
                   executor::ExecutorContext &executor_context,
                   const char *file_path, const codegen::type::Type *col_types,
                   uint32_t num_cols, Callback func, void *opaque_state,
                   char delimiter, char quote, char escape);

  /**
   * Destruction function. This is the entry point from codegen when cleaning up
   * and reclaiming memory from scanner instances.
   *
   * @param scanner The scanner we're destroying.
   */
  static void Destroy(CSVScanner &scanner);

  /**
   * Produce all the rows stored in the configured CSV file
   */
  void Produce();

  /**
   * Return the list of columns
   *
   * @return
   */
  const Column *GetColumns() const { return cols_; }

 private:
  // Initialize the scan
  void Initialize();

  // Append bytes to the end of the line buffer
  void AppendToLineBuffer(const char *data, uint32_t len);

  // Read the next line from the CSV file
  char *NextLine();

  // Read a buffer's worth of data from the CSV file
  bool NextBuffer();

  // Produce CSV data stored in the provided line
  void ProduceCSV(char *line);

 private:
  // All memory allocations happen from this pool
  peloton::type::AbstractPool &memory_;

  // The path to the CSV file
  const std::string file_path_;

  // The CSV file handle
  peloton::util::File file_;

  // The temporary read-buffer where raw file contents are first read into
  // TODO: make these unique_ptr's with a customer deleter
  char *buffer_;
  uint32_t buffer_pos_;
  uint32_t buffer_end_;

  // A pointer to the start of a line in the CSV file
  char *line_;
  uint32_t line_len_;
  uint32_t line_maxlen_;

  // Line number
  uint32_t line_number_;

  // The column delimiter, quote, and escape characters configured for this CSV
  char delimiter_;
  char quote_;
  char escape_;

  // The callback function to call for each row of the CSV, and an opaque state
  Callback func_;
  void *opaque_state_;

  // The columns
  Column *cols_;
  uint32_t num_cols_;

  // Statistics
  Stats stats_;
};

}  // namespace util
}  // namespace codegen
}  // namespace peloton