from abc import ABC, abstractmethod
from datetime import datetime, timezone
from importlib import import_module
from typing import Type

from bridging_hub_types import (
    ConfigBaseType,
    ConfigDataType,
)


class DuplicatedModuleException(Exception):
    """
    Exception to be thrown on duplicated entries in the
    BridgingHubModuleRegistry.
    """

    pass


class NoSuchModuleException(Exception):
    """
    Exception to be thrown on missing entries in the
    BridgingHubModuleRegistry.
    """

    pass


class BrokenConfigException(Exception):
    """
    Exception to be thrown on invalid or missing content
    in the config file(s).
    """

    pass


class InputModuleException(Exception):
    """
    Exception to be thrown on errors while reading from input.
    """

    pass


class OutputModuleException(Exception):
    """
    Exception to be thrown on errors while writing to output.
    """

    pass


class StorageModuleException(Exception):
    """
    Exception to be thrown on errors while writing to disk.
    """

    pass


class BridgingHubBaseModule(ABC):
    """
    Abstract base class for all action modules.
    """

    # display generic keywords and prevent typos etc.
    KEY_BRIDGE: str = "bridge"
    KEY_CLEANUP: str = "cleanup"
    KEY_DATA: str = "data"
    KEY_INPUT: str = "input"
    KEY_OUTPUT: str = "output"
    KEY_FILTER: str = "filter"
    KEY_STORAGE: str = "storage"

    # these are actions a user can choose from at runtime from the cli
    KEY_ACTION_TYPES: list[str] = [
        KEY_BRIDGE,
        KEY_CLEANUP,
        KEY_INPUT,
        KEY_OUTPUT,
    ]

    # keys refering to module loading
    KEY_ACTION_MODULE_NAME: str = "module_class_name"
    KEY_ACTION_MODULE_PATH: str = "module_path"

    # these are keys used in the _custom_name map and are ment
    # to guarantee the accessibilty between different modulse
    KEY_BH_STATUS_NAME: str = "bHstatus_name"
    KEY_DATA_VALUE_MAP: str = "value_register_map"
    KEY_GEOHASH_NAME: str = "geohash_name"
    KEY_ID_NAME: str = "id_name"
    KEY_LOCATION_NAME: str = "location_name"
    KEY_TIMESTAMP_NAME: str = "timestamp_name"
    KEY_TYPE_NAME: str = "type_name"
    KEY_VALUE_NAME: str = "value_name"
    KEY_UNIT_NAME: str = "unit_name"

    # the default relative path for modules"
    DEFAULT_ACTION_MODULE_PATH: str = "module"

    # The action type this modules belongs to.
    # These are the (possible) candidates:
    # * collect (standard - synchronous input action)
    # * consume (pending - asynchronous input action)
    # * send (standard = synchronous output action)
    # * produce (pending - asynchronous output action)
    # * bridge (standard - connecting in-/output)
    # * filter (pending - as bridge but edit content)
    action_type: str = ""

    def __init__(self):
        # Store the config details related to the action type.
        self._action_detail: dict[str, str] = {}
        # This will be filled with the KEY_DATA_VALUE_MAP on run init
        # We need to keep this (and the next) dict separate for the
        # modules even if the content MUST be compatible inside of
        # one run, and mostly actually is identical...
        self._data: ConfigDataType = {}
        # For a minimal compatibility between the modules, the basic data
        # structure is defined and filled with defaults. This will be
        # overwritten by the config parameters. Keeping a map of customizable
        # names allows for a translation beween different modules if
        # necessary.
        self._custom_name: ConfigBaseType = {
            BridgingHubBaseModule.KEY_BH_STATUS_NAME: "bHstatus",
            BridgingHubBaseModule.KEY_GEOHASH_NAME: "geohash",
            BridgingHubBaseModule.KEY_ID_NAME: "id",
            BridgingHubBaseModule.KEY_LOCATION_NAME: "location",
            BridgingHubBaseModule.KEY_TIMESTAMP_NAME: "timestamp",
            BridgingHubBaseModule.KEY_TYPE_NAME: "type",
            BridgingHubBaseModule.KEY_VALUE_NAME: "value",
            BridgingHubBaseModule.KEY_UNIT_NAME: "unit",
        }

    @abstractmethod
    def input(self) -> dict[str, dict[str, str]]:
        pass

    @abstractmethod
    def output(
        self, message: dict[str, dict[str, str]]
    ) -> dict[str, dict[str, str]]:
        pass

    def configure(self, config: dict) -> None:
        """Configure the module with the specific config. This is intended
        to happen after Object creation.
        :param dict config:
        :raise BrokenConfigException:"""
        # init the _data
        if (
            BridgingHubBaseModule.KEY_DATA in config
            and config[BridgingHubBaseModule.KEY_DATA]
        ):
            self._data = config[BridgingHubBaseModule.KEY_DATA]
            # set the custom names from config
            for k in self._custom_name.keys():
                if k in self._data and self._data[k]:
                    if not isinstance(self._data[k], str):
                        raise BrokenConfigException(
                            f"The 'data:{k}' config parameter MUST be a 'str'"
                        )
                    self._custom_name[k] = str(self._data[k])
        else:
            raise BrokenConfigException(
                "No 'data' section found in the config."
            )
        if self.action_type in config and config[self.action_type]:
            self._action_detail = config[self.action_type]
        else:
            raise BrokenConfigException(
                f"No '{self.action_type}' section found in the config."
            )

    def current_timestamp(self) -> int:
        """Generate a timestamp in nano seconds since Unix Epoch.
        :rtype: int
        :return: nano second timestamp a.U."""
        epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        return int((now - epoch).total_seconds() * 1000000000)


class BridgingHubModuleRegistry:
    """
    Registry class for dynamic module access.
    """

    _registry: dict[str, str] = {}

    @classmethod
    def register_module(cls, module_class_name: str, module_path: str):
        """Register a method in the table for later.

        :param str module_class_name: The (unique) name of the class.
        :param str module_path: The path of the python module.
        :raise DuplicatedModuleException:"""
        if module_class_name not in cls._registry:
            cls._registry[module_class_name] = module_path
        elif cls._registry[module_class_name] != module_path:
            raise DuplicatedModuleException(
                f"""This module ({module_class_name}) was already \
registered for '{cls._registry[module_class_name]}'"""
            )

    @classmethod
    def load_module(cls, module_class_name: str) -> BridgingHubBaseModule:
        """Look up and return a new instance of a registered module.

        :param str module_class_name: The name of the module as key.
        :rtype: a subclass of BridgingHubBaseModule
        :return: The class of the module.
        :raise NoSuchModuleException:"""
        if module_class_name not in cls._registry:
            raise NoSuchModuleException(
                f"There is no module registered by the name of \
'{module_class_name}'."
            )
        module_path = cls._registry[module_class_name]
        module = import_module(module_path)
        module_class: Type = getattr(module, module_class_name)
        if not issubclass(module_class, BridgingHubBaseModule):
            raise NoSuchModuleException(
                f"An incompatible module was registered for '{module_path}.'"
            )
        return module_class()


class CollectorBaseModule(BridgingHubBaseModule):
    """
    Abstract base collector module used by input modules.
    """

    action_type: str = BridgingHubBaseModule.KEY_INPUT

    def input(self) -> dict[str, dict[str, str]]:
        return self.collect()

    def output(
        self, message: dict[str, dict[str, str]]
    ) -> dict[str, dict[str, str]]:
        raise BrokenConfigException(
            "This input module was configured in output."
        )
        return {}

    @abstractmethod
    def collect(self) -> dict[str, dict[str, str]]:
        pass


class SenderBaseModule(BridgingHubBaseModule):
    """
    Abstract base sender module used by output modules.
    """

    action_type: str = BridgingHubBaseModule.KEY_OUTPUT

    def input(self) -> dict[str, dict[str, str]]:
        raise BrokenConfigException(
            "This output module was configured in input."
        )
        return {}

    def output(
        self, message: dict[str, dict[str, str]]
    ) -> dict[str, dict[str, str]]:
        return self.send(message)

    @abstractmethod
    def send(
        self, message: dict[str, dict[str, str]]
    ) -> dict[str, dict[str, str]]:
        pass


class StorageBaseModule(BridgingHubBaseModule):
    """
    Abstract base storage module. There is only one type of storage modules.
    """

    action_type: str = BridgingHubBaseModule.KEY_STORAGE

    def input(self) -> dict[str, dict[str, str]]:
        raise BrokenConfigException(
            "This storage module was configured in input."
        )
        return {}

    def output(
        self, message: dict[str, dict[str, str]]
    ) -> dict[str, dict[str, str]]:
        raise BrokenConfigException(
            "This storage module was configured in output."
        )
        return {}

    @abstractmethod
    def write_cache(
        self, message: dict[str, dict[str, str]]
    ) -> dict[str, dict[str, str]]:
        """Remember message content between in- and output."""
        pass

    @abstractmethod
    def read_cache(self) -> dict[str, dict[str, str]]:
        """Look up message content between in- and output."""
        pass

    @abstractmethod
    def clean_cache(
        self, message: dict[str, dict[str, str]]
    ) -> dict[str, dict[str, str]]:
        """Clean up the files remembered between in- and output."""
        pass

    @abstractmethod
    def store(
        self, message: dict[str, dict[str, str]]
    ) -> dict[str, dict[str, str]]:
        """Remember message content after output."""
        pass
