//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// configuration.cpp
//
// Identification: src/backend/bridge/ddl/configuration.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/bridge/ddl/configuration.h"

#include <cassert>
#include "postgres.h"
#include "utils/guc_tables.h"

// Defined in guc.cpp

extern struct config_bool ConfigureNamesBool[];
extern struct config_int ConfigureNamesInt[];
extern struct config_real ConfigureNamesReal[];
extern struct config_string ConfigureNamesString[];
extern struct config_enum ConfigureNamesEnum[];

namespace peloton {
namespace bridge {

std::map<std::string, ConfigManager::config_details>
ConfigManager::BuildConfigMap() {

	std::map<std::string, config_details> config_map;

	int count;
	config_details temp;

	/* For bool options */
	count = 0;
	while(ConfigureNamesBool[count].gen.name != NULL) {

		temp.type = BOOLEAN_TYPE;
		auto val = ::GetConfigOption(ConfigureNamesBool[count].gen.name, false, false);
		temp.value = std::string(val);

		config_map.insert(std::map<std::string, config_details>::value_type(
									ConfigureNamesBool[count].gen.name,	temp));

		count++;
	}

	/* For int options */
	count = 0;
	while(ConfigureNamesInt[count].gen.name != NULL) {

		temp.type = INTEGER_TYPE;
		auto val = ::GetConfigOption(ConfigureNamesInt[count].gen.name, false, false);
		temp.value = std::string(val);

		config_map.insert(std::map<std::string, config_details>::value_type(
									ConfigureNamesInt[count].gen.name,	temp));

		count++;
	}

	/* For real options */
	count = 0;
	while(ConfigureNamesReal[count].gen.name != NULL) {

		temp.type = REAL_TYPE;
		auto val = ::GetConfigOption(ConfigureNamesReal[count].gen.name, false, false);
		temp.value = std::string(val);

		config_map.insert(std::map<std::string, config_details>::value_type(
									ConfigureNamesReal[count].gen.name,	temp));

		count++;
	}

	/* For string options */
	count = 0;
	while(ConfigureNamesString[count].gen.name != NULL) {

		temp.type = STRING_TYPE;
		auto val = ::GetConfigOption(ConfigureNamesString[count].gen.name, false, false);
		if(val == NULL)
			temp.value = "NULL";
		else
			temp.value = std::string(val);

		config_map.insert(std::map<std::string, config_details>::value_type(
								ConfigureNamesString[count].gen.name,	temp));

		count++;
	}

	/* For enum options */
	count = 0;
	while(ConfigureNamesEnum[count].gen.name != NULL) {

		temp.type = ENUM_TYPE;
		auto val = ::GetConfigOption(ConfigureNamesEnum[count].gen.name, false, false);
		temp.value = std::string(val);

		config_map.insert(std::map<std::string, config_details>::value_type(
									ConfigureNamesEnum[count].gen.name,	temp));

		count++;
	}

	return config_map;
}


std::string ConfigManager::GetConfigOption(std::string option_name) {
	return ::GetConfigOption(option_name.c_str(), false, false);
}


void ConfigManager::SetConfigOption(std::string option_name, std::string option_value) {
  ::SetConfigOption(option_name.c_str(), option_value.c_str(), PGC_USERSET, PGC_S_USER);
}


} // namespace peloton
} // namespace bridge

