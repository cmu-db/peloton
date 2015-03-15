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

#define BOOST_LOG_DYN_LINK 1 // necessary when linking the boost_log library dynamically

#include <boost/log/core.hpp>
#include <boost/log/trivial.hpp>
#include <boost/log/expressions.hpp>
#include <boost/log/sinks/text_file_backend.hpp>
#include <boost/log/utility/setup/file.hpp>
#include <boost/log/utility/setup/common_attributes.hpp>
#include <boost/log/sources/severity_logger.hpp>
#include <boost/log/sources/record_ostream.hpp>
#include <boost/log/support/date_time.hpp>

#include <thread>

namespace nstore {

// Disable logging in production

#ifdef NDEBUG
#define NLOG(level, message)
#else
#define NLOG(level, message) BOOST_LOG_SEV(boost::log::trivial::logger::get(), boost::log::trivial::level) << message
#endif

class Logger {
	Logger(Logger const&) = delete;
	Logger& operator=(Logger const&) = delete;

public:

	Logger(boost::log::trivial::severity_level level = boost::log::trivial::warning,
	       std::string log_file_name = "nstore.log") :
	         log_file_name(log_file_name),
	         level(level) {

	  boost::log::add_common_attributes();

		boost::log::core::get()->set_filter(boost::log::trivial::severity >= level);
	}

	// set severity level
	static void SetLevel(boost::log::trivial::severity_level level) {
		boost::log::core::get()->set_filter(boost::log::trivial::severity >= level);
	}

	// getters

	std::string GetLogFileName() const {
		return log_file_name;
	}

	boost::log::trivial::severity_level GetLevel() const {
		return level;
	}

private:
	std::string log_file_name;

	boost::log::trivial::severity_level level;
};


} // End nstore namespace
