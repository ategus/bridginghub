from abc import ABC, abstractmethod
from datetime import datetime, timezone
from importlib import import_module
from typing import Type


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
    Exception to be thrown on errors while reading input.
    """

    pass


class BridgingHubBaseModule(ABC):
    """
    Abstract base class for all action modules.
    """

    KEY_BRIDGE: str = "bridge"
    KEY_CLEANUP: str = "cleanup"
    KEY_DATA: str = "data"
    KEY_INPUT: str = "input"
    KEY_OUTPUT: str = "output"
    KEY_STORAGE: str = "storage"

    KEY_ACTION_TYPES = [
        KEY_BRIDGE,
        KEY_CLEANUP,
        KEY_INPUT,
        KEY_OUTPUT,
    ]

    KEY_ACTION_MODULE_NAME = "module_class_name"
    KEY_ACTION_MODULE_PATH = "module_path"

    KEY_DATA_VALUE_MAP = "value_register_map"
    KEY_GEOHASH_NAME = "geohash_name"
    KEY_ID_NAME = "id_name"
    KEY_LOCATION_NAME = "location_name"
    KEY_STATUS_NAME = "status_name"
    KEY_TIMESTAMP_NAME = "timestamp_name"
    KEY_TYPE_NAME = "type_name"
    KEY_VALUE_NAME = "value_name"
    KEY_UNIT_NAME = "unit_name"

    # the default relative path for modules"
    DEFAULT_ACTION_MODULE_PATH = "module"

    # The action type this modules belongs to.
    # These are the (possible) candidates:
    # * collect (standard - synchronous input action)
    # * consume (pending - asynchronous input action)
    # * send (standard = synchronous output action)
    # * produce (pending - asynchronous output action)
    # * bridge (standard - connecting in-/output)
    # * filter (pending - as bridge but edit content)
    _action_type: str = ""

    # For a minimal compatibility between the modules, the basic data
    # structure is defined and filled with defaults. This will be
    # overwritten by the config parameters. Keeping a map of customizable
    # names allows for a translation beween different modules if necessary.
    _custom_name: dict[str, str] = {
        KEY_DATA_VALUE_MAP: "value_register_map",
        KEY_GEOHASH_NAME: "geohash",
        KEY_ID_NAME: "id",
        KEY_LOCATION_NAME: "location",
        KEY_STATUS_NAME: "status",
        KEY_TIMESTAMP_NAME: "timestamp",
        KEY_TYPE_NAME: "type",
        KEY_VALUE_NAME: "value",
        KEY_UNIT_NAME: "unit",
    }

    # This will be filled with the KEY_DATA_VALUE_MAP on run init
    _data: dict[str, dict] = {}

    # Store the config details related to the action type.
    _action_detail: dict[str, str] = {}

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
        # set the custom names from config
        # TODO
        for k in self._custom_name.keys():
            if k in config and config[k]:
                self._custom_name = config[k]
        # init the _data
        if (
            BridgingHubBaseModule.KEY_DATA in config
            and config[BridgingHubBaseModule.KEY_DATA]
        ):
            self._data = config[BridgingHubBaseModule.KEY_DATA]
        else:
            raise BrokenConfigException(
                "No 'data' section found in the config."
            )
        if self._action_type in config and config[self._action_type]:
            self._action_detail = config[self._action_type]
        else:
            raise BrokenConfigException(
                f"No '{self._action_type}' section found in the config."
            )

    def current_timestamp(self) -> int:
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
                f"""This module ({module_class_name}) was already
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
                f"There is no module by name '{module_class_name}' registered."
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

    _action_type: str = BridgingHubBaseModule.KEY_INPUT

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

    _action_type: str = BridgingHubBaseModule.KEY_OUTPUT

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

    _action_type: str = BridgingHubBaseModule.KEY_STORAGE

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
