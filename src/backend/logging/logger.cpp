#include "backend/logging/logger.h"

namespace peloton {
namespace logging {

/**
 * @brief Return the logging type
 */
LoggingType Logger::GetLoggingType() const {
  return logging_type;
}

/**
 * @brief Return the logger type
 */
LoggerType Logger::GetLoggerType() const {
  return logger_type;
}

}  // namespace logging
}  // namespace peloton
