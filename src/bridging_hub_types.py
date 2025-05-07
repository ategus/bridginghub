ConfigBaseType = dict[str, str]
ConfigSubType = ConfigBaseType
ConfigFilterType = dict[str, str | ConfigSubType | list[str]]
ConfigDataType = dict[str, str | dict[str, ConfigSubType]]

# Define a type hint for the config
ConfigType = (
    ConfigBaseType
    | dict[str, str | ConfigSubType]
    | dict[str, str | ConfigFilterType]
    | dict[str, str | ConfigDataType]
)
