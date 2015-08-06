//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// logger.h
//
// Identification: src/backend/common/logger.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <ctime>
#include <sys/time.h>
#include <stdio.h>

namespace peloton {

//===--------------------------------------------------------------------===//
// Simple Logger
//===--------------------------------------------------------------------===//

/**
 * Debug logging functions.
 * Turned on/off by LOG_LEVEL compile option.
 */

// Log levels

#define LOG_LEVEL_OFF 1000
#define LOG_LEVEL_ERROR 500
#define LOG_LEVEL_WARN 400
#define LOG_LEVEL_INFO 300
#define LOG_LEVEL_DEBUG 200
#define LOG_LEVEL_TRACE 100
#define LOG_LEVEL_ALL 0

#define LOG_TIME_FORMAT "%02d:%02d:%02d,%03d"
#define LOG_OUTPUT_STREAM stdout
#define LOG_TIME_MILLISECONDS 1

// Compile time Option

#ifndef LOG_LEVEL
// Enable debugging mode if needed
#ifdef DEBUG
#define LOG_LEVEL LOG_LEVEL_DEBUG
#else
// Defaults to LOG_LEVEL_INFO
#define LOG_LEVEL LOG_LEVEL_INFO
#endif
#endif

// Disable logging if requested
#ifdef NDEBUG
#undef LOG_LEVEL
#define LOG_LEVEL LOG_LEVEL_OFF
#endif

// For compilers which do not support __FUNCTION__
#if !defined(__FUNCTION__) && !defined(__GNUC__)
#define __FUNCTION__ ""
#endif

void OutputLogHeader(const char *file, int line, const char *func, int level);

// Two convenient macros for debugging
// 1. Logging macros.
// 2. LOG_XXX_ENABLED macros. Use these to "eliminate" all the debug blocks from
// release binary.
#ifdef LOG_ERROR_ENABLED
#undef LOG_ERROR_ENABLED
#endif

#if LOG_LEVEL <= LOG_LEVEL_ERROR
#define LOG_ERROR_ENABLED
#define LOG_ERROR(...)                                                \
  OutputLogHeader(__FILE__, __LINE__, __FUNCTION__, LOG_LEVEL_ERROR); \
  ::fprintf(LOG_OUTPUT_STREAM, __VA_ARGS__);                          \
  fprintf(LOG_OUTPUT_STREAM, "\n");                                   \
  ::fflush(LOG_OUTPUT_STREAM)
#else
#define LOG_ERROR(...) ((void)0)
#endif

#ifdef LOG_WARN_ENABLED
#undef LOG_WARN_ENABLED
#endif
#if LOG_LEVEL <= LOG_LEVEL_WARN
#define LOG_WARN_ENABLED
#define LOG_WARN(...)                                                \
  OutputLogHeader(__FILE__, __LINE__, __FUNCTION__, LOG_LEVEL_WARN); \
  ::fprintf(LOG_OUTPUT_STREAM, __VA_ARGS__);                         \
  fprintf(LOG_OUTPUT_STREAM, "\n");                                  \
  ::fflush(LOG_OUTPUT_STREAM)
#else
#define LOG_WARN(...) ((void)0)
#endif

#ifdef LOG_INFO_ENABLED
#undef LOG_INFO_ENABLED
#endif
#if LOG_LEVEL <= LOG_LEVEL_INFO
#define LOG_INFO_ENABLED
#define LOG_INFO(...)                                                \
  OutputLogHeader(__FILE__, __LINE__, __FUNCTION__, LOG_LEVEL_INFO); \
  ::fprintf(LOG_OUTPUT_STREAM, __VA_ARGS__);                         \
  fprintf(LOG_OUTPUT_STREAM, "\n");                                  \
  ::fflush(LOG_OUTPUT_STREAM)
#else
#define LOG_INFO(...) ((void)0)
#endif

#ifdef LOG_DEBUG_ENABLED
#undef LOG_DEBUG_ENABLED
#endif
#if LOG_LEVEL <= LOG_LEVEL_DEBUG
#define LOG_DEBUG_ENABLED
#define LOG_DEBUG(...)                                                \
  OutputLogHeader(__FILE__, __LINE__, __FUNCTION__, LOG_LEVEL_DEBUG); \
  ::fprintf(LOG_OUTPUT_STREAM, __VA_ARGS__);                          \
  fprintf(LOG_OUTPUT_STREAM, "\n");                                   \
  ::fflush(LOG_OUTPUT_STREAM)
#else
#define LOG_DEBUG(...) ((void)0)
#endif

#ifdef LOG_TRACE_ENABLED
#undef LOG_TRACE_ENABLED
#endif
#if LOG_LEVEL <= LOG_LEVEL_TRACE
#define LOG_TRACE_ENABLED
#define LOG_TRACE(...)                                                \
  OutputLogHeader(__FILE__, __LINE__, __FUNCTION__, LOG_LEVEL_TRACE); \
  ::fprintf(LOG_OUTPUT_STREAM, __VA_ARGS__);                          \
  fprintf(LOG_OUTPUT_STREAM, "\n");                                   \
  ::fflush(LOG_OUTPUT_STREAM)
#else
#define LOG_TRACE(...) ((void)0)
#endif

// Output log message header in this format: [type] [file:line:function] time -
// ex: [TRACE] [logger.cpp:23:log()] 2015/04/20 10:00:00 -
inline void OutputLogHeader(const char *file, int line, const char *func,
                            int level) {
  char time_str[64];

  if (LOG_TIME_MILLISECONDS) {
    struct timeval tv;
    struct timezone tz;
    struct tm *tm;
    gettimeofday(&tv, &tz);
    tm = localtime(&tv.tv_sec);
    ::snprintf(time_str, 32, LOG_TIME_FORMAT, tm->tm_hour, tm->tm_min,
               tm->tm_sec, (int)(tv.tv_usec / 1000));
  } else {
    time_t t = ::time(NULL);
    tm *curTime = localtime(&t);
    ::strftime(time_str, 32, LOG_TIME_FORMAT, curTime);
  }

  const char *type;
  switch (level) {
    case LOG_LEVEL_ERROR:
      type = "ERROR";
      break;
    case LOG_LEVEL_WARN:
      type = "WARN ";
      break;
    case LOG_LEVEL_INFO:
      type = "INFO ";
      break;
    case LOG_LEVEL_DEBUG:
      type = "DEBUG";
      break;
    case LOG_LEVEL_TRACE:
      type = "TRACE";
      break;
    default:
      type = "UNKWN";
  }

  fprintf(LOG_OUTPUT_STREAM, "%s [%s:%d:%s] %s - ", time_str, file, line, func,
          type);
}

}  // End peloton namespace
