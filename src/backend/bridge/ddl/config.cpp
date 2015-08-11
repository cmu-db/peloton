/*
 * config.cpp
 *
 *  Created on: Aug 11, 2015
 *      Author: vivek
 */

#include "backend/bridge/ddl/config.h"

#include "postgres.h"
#include "utils/guc_tables.h"

extern struct config_bool *ConfigureNamesBool;

namespace peloton {
namespace bridge {

std::map<std::string, std::string> ConstructConfigMap() {

	std::map<std::string, std::string> config_map;

	char sizeof_config_bool[10];
	sprintf(sizeof_config_bool, "%lu", sizeof(config_bool));
	puts("size of config_bool");
	puts(sizeof_config_bool);

	char sizeof_ConfigureNamesBool[10];
	sprintf(sizeof_ConfigureNamesBool, "%lu", sizeof(ConfigureNamesBool));
	puts("size of ConfigureNamesBool");
	puts(sizeof_ConfigureNamesBool);

	char *config_option_name;
	strcpy(config_option_name,ConfigureNamesBool[0].gen.name);
	puts("Testing reading from ConfigureNamesBool array ...");
	puts(GetConfigOption(config_option_name, false, false));

	return config_map;
}

} // namespace peloton
} // namespace bridge

