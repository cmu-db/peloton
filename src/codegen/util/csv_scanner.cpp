//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// csv_scanner.cpp
//
// Identification: src/codegen/util/csv_scanner.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/util/csv_scanner.h"

#include <boost/filesystem.hpp>

#include "common/exception.h"
#include "executor/executor_context.h"
#include "type/abstract_pool.h"
#include "util/string_util.h"

namespace peloton {
namespace codegen {
namespace util {

CSVScanner::CSVScanner(peloton::type::AbstractPool &pool,
                       const std::string &file_path,
                       const codegen::type::Type *col_types, uint32_t num_cols,
                       CSVScanner::Callback func, void *opaque_state,
                       char delimiter, char quote, char escape)
    : memory_(pool),
      file_path_(file_path),
      file_(),
      buffer_(nullptr),
      buffer_pos_(0),
      buffer_end_(0),
      line_(nullptr),
      line_len_(0),
      line_maxlen_(0),
      line_number_(0),
      delimiter_(delimiter),
      quote_(quote),
      escape_(escape),
      func_(func),
      opaque_state_(opaque_state),
      num_cols_(num_cols) {
  // Make column array
  cols_ = static_cast<Column *>(memory_.Allocate(sizeof(Column) * num_cols_));

  // Initialize the columns
  for (uint32_t i = 0; i < num_cols_; i++) {
    cols_[i].col_type = col_types[i];
    cols_[i].ptr = nullptr;
    cols_[i].len = 0;
    cols_[i].is_null = false;
  }
}

CSVScanner::~CSVScanner() {
  if (buffer_ != nullptr) {
    memory_.Free(buffer_);
    buffer_ = nullptr;
  }

  if (line_ != nullptr) {
    memory_.Free(line_);
    line_ = nullptr;
  }

  if (cols_ != nullptr) {
    memory_.Free(cols_);
    cols_ = nullptr;
  }
}

void CSVScanner::Init(CSVScanner &scanner,
                      executor::ExecutorContext &executor_context,
                      const char *file_path,
                      const codegen::type::Type *col_types, uint32_t num_cols,
                      CSVScanner::Callback func, void *opaque_state,
                      char delimiter, char quote, char escape) {
  // Forward to constructor
  new (&scanner)
      CSVScanner(*executor_context.GetPool(), file_path, col_types, num_cols,
                 func, opaque_state, delimiter, quote, escape);
}

void CSVScanner::Destroy(CSVScanner &scanner) {
  // Forward to destructor
  scanner.~CSVScanner();
}

void CSVScanner::Produce() {
  // Initialize
  Initialize();

  // Loop lines
  while (char *line = NextLine()) {
    ProduceCSV(line);
  }
}

void CSVScanner::Initialize() {
  // Let's first perform a few validity checks
  boost::filesystem::path path(file_path_);

  if (!boost::filesystem::exists(path)) {
    throw ExecutorException(StringUtil::Format("input path '%s' does not exist",
                                               file_path_.c_str()));
  } else if (!boost::filesystem::is_regular_file(file_path_)) {
    auto msg =
        StringUtil::Format("unable to read file '%s'", file_path_.c_str());
    throw ExecutorException(msg);
  }

  // The path looks okay, let's try opening it
  file_.Open(file_path_, peloton::util::File::AccessMode::ReadOnly);

  // Allocate buffer space
  buffer_ = static_cast<char *>(memory_.Allocate(kDefaultBufferSize));

  // Fill read-buffer
  NextBuffer();

  // Allocate space for the full line, if it doesn't fit into the buffer. We
  // reserve the last byte for the null-byte terminator.
  line_ = static_cast<char *>(memory_.Allocate(kDefaultBufferSize));
  line_len_ = 0;
  line_maxlen_ = kDefaultBufferSize - 1;
}

bool CSVScanner::NextBuffer() {
  // Do read
  buffer_pos_ = 0;
  buffer_end_ = static_cast<uint32_t>(file_.Read(buffer_, kDefaultBufferSize));

  // Update stats
  stats_.num_reads++;

  return (buffer_end_ != 0);
}

void CSVScanner::AppendToLineBuffer(const char *data, uint32_t len) {
  PELOTON_ASSERT(len > 0);

  // Short-circuit if we're not appending any data
  if (len == 0) {
    return;
  }

  if (line_len_ + len > line_maxlen_) {
    // Check if we can even allocate any more bytes
    if (static_cast<uint64_t>(len) > kMaxAllocSize - line_len_) {
      const auto msg = StringUtil::Format(
          "Line %u in file '%s' exceeds maximum line length: %lu",
          line_number_ + 1, file_path_.c_str(), kMaxAllocSize);
      throw Exception(msg);
    }

    // The current line buffer isn't large enough to store the new bytes, so we
    // need to resize it. Let's find an allocation size large enough to fit the
    // new bytes.
    uint32_t new_maxlen = line_maxlen_ * 2;
    while (new_maxlen < len) {
      new_maxlen *= 2;
    }

    // Clamp
    new_maxlen = std::min(new_maxlen, static_cast<uint32_t>(kMaxAllocSize));

    auto *new_line = static_cast<char *>(memory_.Allocate(new_maxlen));

    // Copy the old data
    PELOTON_MEMCPY(new_line, line_, line_len_);

    // Free old old
    memory_.Free(line_);

    // Setup pointers and sizes
    line_ = new_line;
    line_maxlen_ = new_maxlen - 1;

    stats_.num_reallocs++;
  }

  // Copy provided data into the line buffer, ensuring null-byte termination.
  PELOTON_MEMCPY(line_ + line_len_, data, len);
  line_[line_len_ + len] = '\0';

  // Increase the length of the line
  line_len_ += len;

  // Track copy stats
  stats_.num_copies++;
}

// The objective of this function is to find a complete line in the CSV file.
// The returned value will be a valid pointer to a null-terminated string that
// is the next line in the CSV to be processed.
char *CSVScanner::NextLine() {
  line_len_ = 0;

  const char quote = quote_;
  const char escape = (quote_ == escape_ ? static_cast<char>('\0') : escape_);

  bool in_quote = false;
  bool last_was_escape = false;

  const char *buf = buffer_;
  uint32_t curr_buffer_pos = buffer_pos_;

  while (true) {
    if (curr_buffer_pos == buffer_end_) {
      // We need to read more data from the CSV file. But first, we need to copy
      // all the data in the read-buffer (i.e., [buffer_begin_, buffer_end_] to
      // the line-buffer.
      if (buffer_pos_ < curr_buffer_pos) {
        AppendToLineBuffer(buffer_ + buffer_pos_,
                           curr_buffer_pos - buffer_pos_);
        buffer_pos_ = curr_buffer_pos;
      }

      // Reset positions
      curr_buffer_pos = 0;

      // Now, read more data
      if (!NextBuffer()) {
        // We hit en EOF
        break;
      }
    }

    // Read character
    char c = buf[curr_buffer_pos++];

    if (in_quote && c == escape) {
      last_was_escape = !last_was_escape;
    }
    if (c == quote && !last_was_escape) {
      in_quote = !in_quote;
    }
    if (c != escape) {
      last_was_escape = false;
    }

    // Process the new-line character. If we a new-line and we're not currently
    // in a quoted section, we're done.
    if (c == '\n' && !in_quote) {
      break;
    }
  }

  // Flush remaining valid bytes
  if (buffer_pos_ < curr_buffer_pos) {
    AppendToLineBuffer(buffer_ + buffer_pos_, curr_buffer_pos - buffer_pos_);
    buffer_pos_ = curr_buffer_pos;
  }

  // Increment line number
  line_number_++;

  // If we didn't transfer any bytes to the line buffer, we must have reached an
  // EOF. If so, return null indicating there are no more lines.
  if (line_len_ == 0) {
    return nullptr;
  }

  // A full line has been transferred to the line buffer, but we also copied the
  // newline character. Strip it off now.
  line_len_--;
  line_[line_len_] = '\0';

  // Done
  return line_;
}

void CSVScanner::ProduceCSV(char *line) {
  const char delimiter = delimiter_;
  const char quote = quote_;
  const char escape = escape_;

  // The iterator over characters in the line
  char *iter = line;

  for (uint32_t col_idx = 0; col_idx < num_cols_; col_idx++) {
    char *col_begin = iter;
    char *col_end = nullptr;

    // We need to move col_end to the end of the column's data. Along the way,
    // we may need to shift data down due to quotes and escapes. Inspired by
    // Postgres.
    {
      char *out = col_begin;
      while (true) {
        // This first loop looks for either the delimiter character or the end
        // of the line, indicating the end of a columns data. It breaks out of
        // the loop if a quote character is found. It flows into a second loop
        // whose only purpose is to find the end of the quoted section.
        while (true) {
          char c = *iter++;

          // If we see the delimiter character, or the end of the string,
          // finish
          if (c == delimiter || c == '\0') {
            col_end = out;
            iter--;
            goto colend;
          }

          // If we see a quote character, move to the second loop to find the
          // closing quote.
          if (c == quote) {
            break;
          }

          *out++ = c;
        }

        while (true) {
          char c = *iter++;

          // If we see the end of the line *within* a quoted section, throw
          // error
          if (c == '\0') {
            throw Exception(StringUtil::Format(
                "unterminated CSV quoted field at %u", col_idx));
          }

          // If we see an escape character within a quoted section, we need to
          // check if the following character is a quote. If so, we must
          // escape it
          if (c == escape) {
            char next = *iter;
            if (next == quote || next == escape) {
              *out++ = next;
              iter++;
              continue;
            }
          }

          // If we see the closing quote, we're done.
          if (c == quote) {
            break;
          }

          *out++ = c;
        }
      }
    }

  colend:
    // If we've reached the of the line, but haven't setup all the columns, then
    // we're missing data for the remaining columns and should throw an error.
    if (*iter == '\0' && col_idx != (num_cols_ - 1)) {
      throw Exception(
          StringUtil::Format("missing data for column %u on line %u",
                             (col_idx + 2), line_number_));
    }

    // Let's setup the columns
    cols_[col_idx].ptr = col_begin;
    cols_[col_idx].len = static_cast<uint32_t>(col_end - col_begin);
    cols_[col_idx].is_null = (cols_[col_idx].len == 0);

    // Eat delimiter, moving to next column
    iter++;
  }

  // Invoke callback
  func_(opaque_state_);
}

}  // namespace util
}  // namespace codegen
}  // namespace peloton