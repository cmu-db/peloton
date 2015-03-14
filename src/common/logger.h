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
			std::string log_file_name = "nstore.log",
			size_t rotation_size = 10 * 1024 * 1024) :
				log_file_name(log_file_name),
				rotation_size(rotation_size),
				level(level) {

		boost::log::add_common_attributes();

		// set up log attributes
		boost::log::add_file_log(
				boost::log::keywords::file_name = log_file_name,
				boost::log::keywords::rotation_size = rotation_size,
				boost::log::keywords::format = (
						boost::log::expressions::stream
						<< std::setw(7) << std::setfill('0')
						<< boost::log::expressions::attr< unsigned int >("LineID")
						<< std::setfill(' ') << " | "
						<< boost::log::expressions::format_date_time< boost::posix_time::ptime >("TimeStamp", "%Y-%m-%d %H:%M:%S") << " "
						<< boost::log::expressions::attr<boost::log::attributes::current_thread_id::value_type>("ThreadID") << " "
						<< "[" << std::setw(8) << boost::log::trivial::severity << "]"
						<< " - " << boost::log::expressions::smessage
				)
		);

		// set severity level
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

	size_t GetRotationSize() const {
		return rotation_size;
	}

	boost::log::trivial::severity_level GetLevel() const {
		return level;
	}

private:
	std::string log_file_name;

	size_t rotation_size;

	boost::log::trivial::severity_level level;
};


} // End nstore namespace
