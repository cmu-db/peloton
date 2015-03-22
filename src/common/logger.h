/*-------------------------------------------------------------------------
 *
 * logger.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/common/logger.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include <log4cxx/logger.h>
#include <log4cxx/helpers/pool.h>
#include <log4cxx/basicconfigurator.h>
#include <log4cxx/fileappender.h>
#include <log4cxx/simplelayout.h>

namespace nstore {

// Define static logger variable
static log4cxx::LoggerPtr logger(log4cxx::Logger::getRootLogger());

// Disable logging in production
#ifdef NDEBUG
#define LOG4CXX_TRACE(logger, expression)
#define LOG4CXX_DEBUG(logger, expression)
#define LOG4CXX_INFO(logger, expression)
#define LOG4CXX_WARN(logger, expression)
#define LOG4CXX_ERROR(logger, expression)
#define LOG4CXX_FATAL(logger, expression)
#endif

class Logger {
 public:

  Logger() {
    log4cxx::FileAppender* file_appender = new
        log4cxx::FileAppender(log4cxx::LayoutPtr(new log4cxx::SimpleLayout()), "nstore.log", false);

    log4cxx::helpers::Pool p;
    file_appender->activateOptions(p);

    log4cxx::BasicConfigurator::configure(log4cxx::AppenderPtr(file_appender));

    // Set to DEBUG level
    log4cxx::Logger::getRootLogger()->setLevel(log4cxx::Level::getDebug());

    nstore::logger = log4cxx::Logger::getLogger("logger");
  }


};


} // End nstore namespace
