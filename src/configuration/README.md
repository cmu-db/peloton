# Configurations
There are two major parts in this PR:
- SettingsCatalog
- ConfigurationManager
## SettingsCatalog
I create a new catalog table 'pg_settings' to store configuration information which contains several columns:
- name (varchar) : the name of a parameter
- value (varchar) : the current value of a parameter
- value_type (integer) : the type (type::TypeId) of the value
- description (varchar) : a short description of usage
- min_value (varchar) : the minimum legal value
- max_value (varchar) : the maximum legal value
- default_value (varchar) : default value
- is_mutable (boolean) : be true if the parameter is allowed to modify during the system running
- is_persistent (boolean) : be true if the value should be restored from logs/checkpoints when the system restarts
## ConfigurationManager
This is a wrapper of SettingsCatalog. It provides several APIs that DefineConfig, GetValue and SetValue. It also keeps a map from name to value which is used during bootstrap before catalog being initialized.
## Other Things
- ConfigurationId : An enum class contains members whose names are the same as the names of parameters.
- ConfigurationUtil : A wrapper for GetValue and SetValue. Support three types temporarily (SET_INT/GET_INT, SET_BOOL/GET_BOOL, SET_STRING/GET_STRING).
## How To Use It
When you want to add a new parameter, define it with macros CONFIG_(int, bool, string) in [configuration.h](https://github.com/lixupeng/peloton/blob/pg_config/src/include/configuration/configuration.h)
For example:
`//CONFIG_int(name, description, default_value, is_mutable, is_persistent)`
`CONFIG_int(port, "Peloton port (default: 15721)", 15721, false, false)`
The macro defined in [configuration_macro.h](https://github.com/lixupeng/peloton/blob/pg_config/src/include/configuration/configuration_macro.h) will be replaced by four types of content on demand:
- a gflags definition
- a gflags declaration
- a member in ConfigurationId
- a statement of ConfigurationManager::DefineConfig

To get or set values, use ConfigurationUtil. For example:
`ConfigurationUtil::GET_INT(ConfigurationId::port)`
`ConfigurationUtil::SET_BOOL(ConfigurationId::index_tuner, true)`

### TODO
- Support more types, not only INTEGER, BOOLEAN, STRING
- Support validation check, I leave blank in min_value and max_value temporarily.
