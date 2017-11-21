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
#include <libunwind.h>
#include <signal.h>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <stdexcept>

#include "type/type.h"
#include "type/types.h"

namespace peloton {

//===--------------------------------------------------------------------===//
// Exception Types
//===--------------------------------------------------------------------===//

enum ExceptionType {
  EXCEPTION_TYPE_INVALID = 0,            // invalid type
  EXCEPTION_TYPE_OUT_OF_RANGE = 1,       // value out of range error
  EXCEPTION_TYPE_CONVERSION = 2,         // conversion/casting error
  EXCEPTION_TYPE_UNKNOWN_TYPE = 3,       // unknown type
  EXCEPTION_TYPE_DECIMAL = 4,            // decimal related
  EXCEPTION_TYPE_MISMATCH_TYPE = 5,      // type mismatch
  EXCEPTION_TYPE_DIVIDE_BY_ZERO = 6,     // divide by 0
  EXCEPTION_TYPE_OBJECT_SIZE = 7,        // object size exceeded
  EXCEPTION_TYPE_INCOMPATIBLE_TYPE = 8,  // incompatible for operation
  EXCEPTION_TYPE_SERIALIZATION = 9,      // serialization
  EXCEPTION_TYPE_TRANSACTION = 10,       // transaction management
  EXCEPTION_TYPE_NOT_IMPLEMENTED = 11,   // method not implemented
  EXCEPTION_TYPE_EXPRESSION = 12,        // expression parsing
  EXCEPTION_TYPE_CATALOG = 13,           // catalog related
  EXCEPTION_TYPE_PARSER = 14,            // parser related
  EXCEPTION_TYPE_PLANNER = 15,           // planner related
  EXCEPTION_TYPE_SCHEDULER = 16,         // scheduler related
  EXCEPTION_TYPE_EXECUTOR = 17,          // executor related
  EXCEPTION_TYPE_CONSTRAINT = 18,        // constraint related
  EXCEPTION_TYPE_INDEX = 19,             // index related
  EXCEPTION_TYPE_STAT = 20,              // stat related
  EXCEPTION_TYPE_CONNECTION = 21,        // connection related
  EXCEPTION_TYPE_SYNTAX = 22,            // syntax related
  EXCEPTION_TYPE_SETTINGS = 23,          // settings related
  EXCEPTION_TYPE_BINDER = 24             // settings related
};

class Exception : public std::runtime_error {
 public:
  Exception(std::string message)
      : std::runtime_error(message), type(EXCEPTION_TYPE_INVALID) {
    std::string exception_message = "Message :: " + message + "\n";
    std::cerr << exception_message;
  }

  Exception(ExceptionType exception_type, std::string message)
      : std::runtime_error(message), type(exception_type) {
    std::string exception_message =
        "\nException Type :: " + ExpectionTypeToString(exception_type) +
        "\nMessage :: " + message + "\n";
    std::cerr << exception_message;

    PrintStackTrace();
  }

  std::string ExpectionTypeToString(ExceptionType type) {
    switch (type) {
      case EXCEPTION_TYPE_INVALID:
        return "Invalid";
      case EXCEPTION_TYPE_OUT_OF_RANGE:
        return "Out of Range";
      case EXCEPTION_TYPE_CONVERSION:
        return "Conversion";
      case EXCEPTION_TYPE_UNKNOWN_TYPE:
        return "Unknown Type";
      case EXCEPTION_TYPE_DECIMAL:
        return "Decimal";
      case EXCEPTION_TYPE_MISMATCH_TYPE:
        return "Mismatch Type";
      case EXCEPTION_TYPE_DIVIDE_BY_ZERO:
        return "Divide by Zero";
      case EXCEPTION_TYPE_OBJECT_SIZE:
        return "Object Size";
      case EXCEPTION_TYPE_INCOMPATIBLE_TYPE:
        return "Incompatible type";
      case EXCEPTION_TYPE_SERIALIZATION:
        return "Serialization";
      case EXCEPTION_TYPE_TRANSACTION:
        return "Transaction";
      case EXCEPTION_TYPE_NOT_IMPLEMENTED:
        return "Not implemented";
      case EXCEPTION_TYPE_EXPRESSION:
        return "Expression";
      case EXCEPTION_TYPE_CATALOG:
        return "Catalog";
      case EXCEPTION_TYPE_PARSER:
        return "Parser";
      case EXCEPTION_TYPE_PLANNER:
        return "Planner";
      case EXCEPTION_TYPE_SCHEDULER:
        return "Scheduler";
      case EXCEPTION_TYPE_EXECUTOR:
        return "Executor";
      case EXCEPTION_TYPE_CONSTRAINT:
        return "Constraint";
      case EXCEPTION_TYPE_INDEX:
        return "Index";
      case EXCEPTION_TYPE_STAT:
        return "Stat";
      case EXCEPTION_TYPE_CONNECTION:
        return "Connection";
      case EXCEPTION_TYPE_SYNTAX:
        return "Syntax";
      case EXCEPTION_TYPE_SETTINGS:
        return "Settings";
      default:
        return "Unknown";
    }
  }

  static void PrintStackTrace(FILE* out = ::stderr) {
    unw_cursor_t cursor;
    unw_context_t context;

    unw_getcontext(&context);
    unw_init_local(&cursor, &context);

    int count = 0;
    while (unw_step(&cursor)) {
      unw_word_t ip, off;
      // get program counter register info
      unw_get_reg(&cursor, UNW_REG_IP, &ip);
      if (ip == 0) {
        break;
      }

      char sym[256] = "Unknown";
      if (unw_get_proc_name(&cursor, sym, sizeof(sym), &off) == 0) {
        char* nameptr = sym;
        int status;
        // demangle c++ function name
        char* demangled = abi::__cxa_demangle(sym, nullptr, nullptr, &status);
        if (status == 0) {
          nameptr = demangled;
        }
        ::fprintf(out, "#%-2d pc = 0x%lx %s + 0x%lx\n", ++count,
                  static_cast<uintptr_t>(ip), nameptr,
                  static_cast<uintptr_t>(off));
        std::free(demangled);
      } else {
        ::fprintf(out,
                  " -- error: unable to obtain symbol name for this frame\n");
      }
    }
  }

 private:
  // type
  ExceptionType type;
};

//===--------------------------------------------------------------------===//
// Exception derived classes
//===--------------------------------------------------------------------===//

class CastException : public Exception {
  CastException() = delete;

 public:
  CastException(const type::TypeId origType, const type::TypeId newType)
      : Exception(EXCEPTION_TYPE_CONVERSION,
                  "Type " + TypeIdToString(origType) + " can't be cast as " +
                      TypeIdToString(newType)) {}
};

class ValueOutOfRangeException : public Exception {
  ValueOutOfRangeException() = delete;

 public:
  ValueOutOfRangeException(const int64_t value, const type::TypeId origType,
                           const type::TypeId newType)
      : Exception(EXCEPTION_TYPE_CONVERSION,
                  "Type " + TypeIdToString(origType) + " with value " +
                      std::to_string((intmax_t)value) +
                      " can't be cast as %s because the value is out of range "
                      "for the destination type " +
                      TypeIdToString(newType)) {}

  ValueOutOfRangeException(const double value, const type::TypeId origType,
                           const type::TypeId newType)
      : Exception(EXCEPTION_TYPE_CONVERSION,
                  "Type " + TypeIdToString(origType) + " with value " +
                      std::to_string(value) +
                      " can't be cast as %s because the value is out of range "
                      "for the destination type " +
                      TypeIdToString(newType)) {}
  ValueOutOfRangeException(const type::TypeId varType, const size_t length)
      : Exception(EXCEPTION_TYPE_OUT_OF_RANGE,
                  "The value is too long to fit into type " +
                      TypeIdToString(varType) + "(" + std::to_string(length) +
                      ")"){};
};

class ConversionException : public Exception {
  ConversionException() = delete;

 public:
  ConversionException(std::string msg)
      : Exception(EXCEPTION_TYPE_CONVERSION, msg) {}
};

class UnknownTypeException : public Exception {
  UnknownTypeException() = delete;

 public:
  UnknownTypeException(int type, std::string msg)
      : Exception(EXCEPTION_TYPE_UNKNOWN_TYPE,
                  "unknown type " + std::to_string(type) + msg) {}
};

class DecimalException : public Exception {
  DecimalException() = delete;

 public:
  DecimalException(std::string msg) : Exception(EXCEPTION_TYPE_DECIMAL, msg) {}
};

class TypeMismatchException : public Exception {
  TypeMismatchException() = delete;

 public:
  TypeMismatchException(std::string msg, const type::TypeId type_1,
                        const type::TypeId type_2)
      : Exception(EXCEPTION_TYPE_MISMATCH_TYPE,
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
      : Exception(EXCEPTION_TYPE_OUT_OF_RANGE,
                  msg + " " + std::to_string(type)) {}
};

class DivideByZeroException : public Exception {
  DivideByZeroException() = delete;

 public:
  DivideByZeroException(std::string msg)
      : Exception(EXCEPTION_TYPE_DIVIDE_BY_ZERO, msg) {}
};

class ObjectSizeException : public Exception {
  ObjectSizeException() = delete;

 public:
  ObjectSizeException(std::string msg)
      : Exception(EXCEPTION_TYPE_OBJECT_SIZE, msg) {}
};

class IncompatibleTypeException : public Exception {
  IncompatibleTypeException() = delete;

 public:
  IncompatibleTypeException(int type, std::string msg)
      : Exception(EXCEPTION_TYPE_INCOMPATIBLE_TYPE,
                  "Incompatible type " +
                      TypeIdToString(static_cast<type::TypeId>(type)) + msg) {}
};

class SerializationException : public Exception {
  SerializationException() = delete;

 public:
  SerializationException(std::string msg)
      : Exception(EXCEPTION_TYPE_SERIALIZATION, msg) {}
};

class TransactionException : public Exception {
  TransactionException() = delete;

 public:
  TransactionException(std::string msg)
      : Exception(EXCEPTION_TYPE_TRANSACTION, msg) {}
};

class NotImplementedException : public Exception {
  NotImplementedException() = delete;

 public:
  NotImplementedException(std::string msg)
      : Exception(EXCEPTION_TYPE_NOT_IMPLEMENTED, msg) {}
};

class ExpressionException : public Exception {
  ExpressionException() = delete;

 public:
  ExpressionException(std::string msg)
      : Exception(EXCEPTION_TYPE_EXPRESSION, msg) {}
};

class CatalogException : public Exception {
  CatalogException() = delete;

 public:
  CatalogException(std::string msg) : Exception(EXCEPTION_TYPE_CATALOG, msg) {}
};

class ParserException : public Exception {
  ParserException() = delete;

 public:
  ParserException(std::string msg) : Exception(EXCEPTION_TYPE_PARSER, msg) {}
};

class PlannerException : public Exception {
  PlannerException() = delete;

 public:
  PlannerException(std::string msg) : Exception(EXCEPTION_TYPE_PLANNER, msg) {}
};

class SchedulerException : public Exception {
  SchedulerException() = delete;

 public:
  SchedulerException(std::string msg)
      : Exception(EXCEPTION_TYPE_SCHEDULER, msg) {}
};

class ExecutorException : public Exception {
  ExecutorException() = delete;

 public:
  ExecutorException(std::string msg)
      : Exception(EXCEPTION_TYPE_EXECUTOR, msg) {}
};

class SyntaxException : public Exception {
  SyntaxException() = delete;

 public:
  SyntaxException(std::string msg) : Exception(EXCEPTION_TYPE_SYNTAX, msg) {}
};

class ConstraintException : public Exception {
  ConstraintException() = delete;

 public:
  ConstraintException(std::string msg)
      : Exception(EXCEPTION_TYPE_CONSTRAINT, msg) {}
};

class IndexException : public Exception {
  IndexException() = delete;

 public:
  IndexException(std::string msg) : Exception(EXCEPTION_TYPE_INDEX, msg) {}
};

class StatException : public Exception {
  StatException() = delete;

 public:
  StatException(std::string msg) : Exception(EXCEPTION_TYPE_STAT, msg) {}
};

class ConnectionException : public Exception {
  ConnectionException() = delete;

 public:
  ConnectionException(std::string msg)
      : Exception(EXCEPTION_TYPE_CONNECTION, msg) {}
};

class SettingsException : public Exception {
  SettingsException() = delete;

 public:
  SettingsException(std::string msg)
      : Exception(EXCEPTION_TYPE_SETTINGS, msg) {}
};

class BinderException : public Exception {
  BinderException() = delete;

 public:
  BinderException(std::string msg) : Exception(EXCEPTION_TYPE_BINDER, msg) {}
};

}  // namespace peloton
