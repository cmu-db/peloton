//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// exception.h
//
// Identification: src/include/common/exception.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cxxabi.h>
#include <errno.h>
#include <execinfo.h>
#include <signal.h>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <stdexcept>

#include "type/type.h"
#include "internal_types.h"

namespace peloton {

//===--------------------------------------------------------------------===//
// Exception Types
//===--------------------------------------------------------------------===//

enum class ExceptionType {
  INVALID = 0,            // invalid type
  OUT_OF_RANGE = 1,       // value out of range error
  CONVERSION = 2,         // conversion/casting error
  UNKNOWN_TYPE = 3,       // unknown type
  DECIMAL = 4,            // decimal related
  MISMATCH_TYPE = 5,      // type mismatch
  DIVIDE_BY_ZERO = 6,     // divide by 0
  OBJECT_SIZE = 7,        // object size exceeded
  INCOMPATIBLE_TYPE = 8,  // incompatible for operation
  SERIALIZATION = 9,      // serialization
  TRANSACTION = 10,       // transaction management
  NOT_IMPLEMENTED = 11,   // method not implemented
  EXPRESSION = 12,        // expression parsing
  CATALOG = 13,           // catalog related
  PARSER = 14,            // parser related
  PLANNER = 15,           // planner related
  SCHEDULER = 16,         // scheduler related
  EXECUTOR = 17,          // executor related
  CONSTRAINT = 18,        // constraint related
  INDEX = 19,             // index related
  STAT = 20,              // stat related
  CONNECTION = 21,        // connection related
  SYNTAX = 22,            // syntax related
  SETTINGS = 23,          // settings related
  BINDER = 24             // settings related
};

class Exception : public std::runtime_error {
 public:
  Exception(std::string message)
      : std::runtime_error(message), type(ExceptionType::INVALID) {
    exception_message_ = "Message :: " + message;
  }

  Exception(ExceptionType exception_type, std::string message)
      : std::runtime_error(message), type(exception_type) {
    exception_message_ = "Exception Type :: " +
                                    ExceptionTypeToString(exception_type) +
                                    "\nMessage :: " + message;
  }

  std::string ExceptionTypeToString(ExceptionType type) {
    switch (type) {
      case ExceptionType::INVALID:
        return "Invalid";
      case ExceptionType::OUT_OF_RANGE:
        return "Out of Range";
      case ExceptionType::CONVERSION:
        return "Conversion";
      case ExceptionType::UNKNOWN_TYPE:
        return "Unknown Type";
      case ExceptionType::DECIMAL:
        return "Decimal";
      case ExceptionType::MISMATCH_TYPE:
        return "Mismatch Type";
      case ExceptionType::DIVIDE_BY_ZERO:
        return "Divide by Zero";
      case ExceptionType::OBJECT_SIZE:
        return "Object Size";
      case ExceptionType::INCOMPATIBLE_TYPE:
        return "Incompatible type";
      case ExceptionType::SERIALIZATION:
        return "Serialization";
      case ExceptionType::TRANSACTION:
        return "TransactionContext";
      case ExceptionType::NOT_IMPLEMENTED:
        return "Not implemented";
      case ExceptionType::EXPRESSION:
        return "Expression";
      case ExceptionType::CATALOG:
        return "Catalog";
      case ExceptionType::PARSER:
        return "Parser";
      case ExceptionType::PLANNER:
        return "Planner";
      case ExceptionType::SCHEDULER:
        return "Scheduler";
      case ExceptionType::EXECUTOR:
        return "Executor";
      case ExceptionType::CONSTRAINT:
        return "Constraint";
      case ExceptionType::INDEX:
        return "Index";
      case ExceptionType::STAT:
        return "Stat";
      case ExceptionType::CONNECTION:
        return "Connection";
      case ExceptionType::SYNTAX:
        return "Syntax";
      case ExceptionType::SETTINGS:
        return "Settings";
      default:
        return "Unknown";
    }
  }

  // Based on :: http://panthema.net/2008/0901-stacktrace-demangled/
  static void PrintStackTrace(FILE *out = ::stderr,
                              unsigned int max_frames = 63) {
    ::fprintf(out, "Stack Trace:\n");

    /// storage array for stack trace address data
    void *addrlist[max_frames + 1];

    /// retrieve current stack addresses
    int addrlen = backtrace(addrlist, max_frames + 1);

    if (addrlen == 0) {
      ::fprintf(out, "  <empty, possibly corrupt>\n");
      return;
    }

    /// resolve addresses into strings containing "filename(function+address)",
    char **symbol_list = backtrace_symbols(addrlist, addrlen);

    /// allocate string which will be filled with the demangled function name
    size_t func_name_size = 1024;
    std::unique_ptr<char> func_name(new char[func_name_size]);

    /// iterate over the returned symbol lines. skip the first, it is the
    /// address of this function.
    for (int i = 1; i < addrlen; i++) {
      char *begin_name = 0, *begin_offset = 0, *end_offset = 0;

      /// find parentheses and +address offset surrounding the mangled name:
      /// ./module(function+0x15c) [0x8048a6d]
      for (char *p = symbol_list[i]; *p; ++p) {
        if (*p == '(')
          begin_name = p;
        else if (*p == '+')
          begin_offset = p;
        else if (*p == ')' && begin_offset) {
          end_offset = p;
          break;
        }
      }

      if (begin_name && begin_offset && end_offset &&
          begin_name < begin_offset) {
        *begin_name++ = '\0';
        *begin_offset++ = '\0';
        *end_offset = '\0';

        /// mangled name is now in [begin_name, begin_offset) and caller
        /// offset in [begin_offset, end_offset). now apply  __cxa_demangle():
        int status;
        char *ret = abi::__cxa_demangle(begin_name, func_name.get(),
                                        &func_name_size, &status);
        if (status == 0) {
          func_name.reset(ret);  // use possibly realloc()-ed string
          ::fprintf(out, "  %s : %s+%s\n", symbol_list[i], func_name.get(),
                    begin_offset);
        } else {
          /// demangling failed. Output function name as a C function with
          /// no arguments.
          ::fprintf(out, "  %s : %s()+%s\n", symbol_list[i], begin_name,
                    begin_offset);
        }
      } else {
        /// couldn't parse the line ? print the whole line.
        ::fprintf(out, "  %s\n", symbol_list[i]);
      }
    }
  }

  friend std::ostream& operator<<(std::ostream& os, const Exception& e);

 private:
  // type
  ExceptionType type;
  std::string exception_message_;
};

//===--------------------------------------------------------------------===//
// Exception derived classes
//===--------------------------------------------------------------------===//

class CastException : public Exception {
  CastException() = delete;

 public:
  CastException(const type::TypeId origType, const type::TypeId newType)
      : Exception(ExceptionType::CONVERSION,
                  "Type " + TypeIdToString(origType) + " can't be cast as " +
                      TypeIdToString(newType)) {}
};

class ValueOutOfRangeException : public Exception {
  ValueOutOfRangeException() = delete;

 public:
  ValueOutOfRangeException(const int64_t value, const type::TypeId origType,
                           const type::TypeId newType)
      : Exception(ExceptionType::CONVERSION,
                  "Type " + TypeIdToString(origType) + " with value " +
                      std::to_string((intmax_t)value) +
                      " can't be cast as %s because the value is out of range "
                      "for the destination type " +
                      TypeIdToString(newType)) {}

  ValueOutOfRangeException(const double value, const type::TypeId origType,
                           const type::TypeId newType)
      : Exception(ExceptionType::CONVERSION,
                  "Type " + TypeIdToString(origType) + " with value " +
                      std::to_string(value) +
                      " can't be cast as %s because the value is out of range "
                      "for the destination type " +
                      TypeIdToString(newType)) {}
  ValueOutOfRangeException(const type::TypeId varType, const size_t length)
      : Exception(ExceptionType::OUT_OF_RANGE,
                  "The value is too long to fit into type " +
                      TypeIdToString(varType) + "(" + std::to_string(length) +
                      ")"){};
};

class ConversionException : public Exception {
  ConversionException() = delete;

 public:
  ConversionException(std::string msg)
      : Exception(ExceptionType::CONVERSION, msg) {}
};

class UnknownTypeException : public Exception {
  UnknownTypeException() = delete;

 public:
  UnknownTypeException(int type, std::string msg)
      : Exception(ExceptionType::UNKNOWN_TYPE,
                  "unknown type " + std::to_string(type) + msg) {}
};

class DecimalException : public Exception {
  DecimalException() = delete;

 public:
  DecimalException(std::string msg) : Exception(ExceptionType::DECIMAL, msg) {}
};

class TypeMismatchException : public Exception {
  TypeMismatchException() = delete;

 public:
  TypeMismatchException(std::string msg, const type::TypeId type_1,
                        const type::TypeId type_2)
      : Exception(ExceptionType::MISMATCH_TYPE,
                  "Type " + TypeIdToString(type_1) + " does not match with " +
                      TypeIdToString(type_2) + msg) {}
};

class NumericValueOutOfRangeException : public Exception {
  NumericValueOutOfRangeException() = delete;

 public:
  // internal flags
  static const int TYPE_UNDERFLOW = 1;
  static const int TYPE_OVERFLOW = 2;

  NumericValueOutOfRangeException(std::string msg, int type)
      : Exception(ExceptionType::OUT_OF_RANGE,
                  msg + " " + std::to_string(type)) {}
};

class DivideByZeroException : public Exception {
  DivideByZeroException() = delete;

 public:
  DivideByZeroException(std::string msg)
      : Exception(ExceptionType::DIVIDE_BY_ZERO, msg) {}
};

class ObjectSizeException : public Exception {
  ObjectSizeException() = delete;

 public:
  ObjectSizeException(std::string msg)
      : Exception(ExceptionType::OBJECT_SIZE, msg) {}
};

class IncompatibleTypeException : public Exception {
  IncompatibleTypeException() = delete;

 public:
  IncompatibleTypeException(int type, std::string msg)
      : Exception(ExceptionType::INCOMPATIBLE_TYPE,
                  "Incompatible type " +
                      TypeIdToString(static_cast<type::TypeId>(type)) + msg) {}
};

class SerializationException : public Exception {
  SerializationException() = delete;

 public:
  SerializationException(std::string msg)
      : Exception(ExceptionType::SERIALIZATION, msg) {}
};

class TransactionException : public Exception {
  TransactionException() = delete;

 public:
  TransactionException(std::string msg)
      : Exception(ExceptionType::TRANSACTION, msg) {}
};

class NotImplementedException : public Exception {
  NotImplementedException() = delete;

 public:
  NotImplementedException(std::string msg)
      : Exception(ExceptionType::NOT_IMPLEMENTED, msg) {}
};

class ExpressionException : public Exception {
  ExpressionException() = delete;

 public:
  ExpressionException(std::string msg)
      : Exception(ExceptionType::EXPRESSION, msg) {}
};

class CatalogException : public Exception {
  CatalogException() = delete;

 public:
  CatalogException(std::string msg) : Exception(ExceptionType::CATALOG, msg) {}
};

class ParserException : public Exception {
  ParserException() = delete;

 public:
  ParserException(std::string msg) : Exception(ExceptionType::PARSER, msg) {}
};

class PlannerException : public Exception {
  PlannerException() = delete;

 public:
  PlannerException(std::string msg) : Exception(ExceptionType::PLANNER, msg) {}
};

class SchedulerException : public Exception {
  SchedulerException() = delete;

 public:
  SchedulerException(std::string msg)
      : Exception(ExceptionType::SCHEDULER, msg) {}
};

class ExecutorException : public Exception {
  ExecutorException() = delete;

 public:
  ExecutorException(std::string msg)
      : Exception(ExceptionType::EXECUTOR, msg) {}
};

class SyntaxException : public Exception {
  SyntaxException() = delete;

 public:
  SyntaxException(std::string msg) : Exception(ExceptionType::SYNTAX, msg) {}
};

class ConstraintException : public Exception {
  ConstraintException() = delete;

 public:
  ConstraintException(std::string msg)
      : Exception(ExceptionType::CONSTRAINT, msg) {}
};

class IndexException : public Exception {
  IndexException() = delete;

 public:
  IndexException(std::string msg) : Exception(ExceptionType::INDEX, msg) {}
};

class StatException : public Exception {
  StatException() = delete;

 public:
  StatException(std::string msg) : Exception(ExceptionType::STAT, msg) {}
};

class ConnectionException : public Exception {
  ConnectionException() = delete;

 public:
  ConnectionException(std::string msg)
      : Exception(ExceptionType::CONNECTION, msg) {}
};

class SettingsException : public Exception {
  SettingsException() = delete;

 public:
  SettingsException(std::string msg)
      : Exception(ExceptionType::SETTINGS, msg) {}
};

class BinderException : public Exception {
  BinderException() = delete;

 public:
  BinderException(std::string msg) : Exception(ExceptionType::BINDER, msg) {}
};

}  // namespace peloton
