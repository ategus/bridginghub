import logging
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from importlib import import_module
from typing import Callable, Type, cast

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


class FilterModuleException(Exception):
    """
    Exception to be thrown on errors while filtering messages.
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
    KEY_INPUT: str = "input"
    KEY_OUTPUT: str = "output"
    KEY_FILTER: str = "filter"
    KEY_STORAGE: str = "storage"

    KEY_DATA: str = "_data"

    # these are actions a user can choose from at runtime from the cli
    KEY_ACTION_TYPES: list[str] = [
        KEY_BRIDGE,  # default, subscribe to modules - fall back to linear
        # TODO        KEY_CLEANUP,
        KEY_INPUT,  # stop after input/buffer, subscribe or linear
        KEY_OUTPUT,  # start at buffer, subscribe or linear
    ]

    # keys refering to module loading
    KEY_MODULE_NAME: str = "module_class_name"
    KEY_MODULE_PATH: str = "module_path"
    KEY_MODULE_TYPE: str = "module_type"
    KEY_TYPE_SPLIT: str = ":"
    KEY_ACTION_SUBSCRIBE: str = "module_subscription"

    # these are keys used in the _custom_name map and are ment
    # to guarantee the accessibilty between different modulse
    KEY_BH_STATUS_NAME: str = "bHstatus_name"
    KEY_DATA_VALUE_MAP: str = "value_register_map"  # excluded in __init__
    KEY_GEOHASH_NAME: str = "geohash_name"
    KEY_ID_NAME: str = "id_name"
    KEY_LOCATION_NAME: str = "location_name"
    KEY_TIMESTAMP_NAME: str = "timestamp_name"
    KEY_DATETIME_NAME: str = "datetime_name"
    KEY_DATETIME_FORMAT_NAME: str = "datetime_format_name"
    KEY_TYPE_NAME: str = "type_name"
    KEY_VALUE_NAME: str = "value_name"
    KEY_UNIT_NAME: str = "unit_name"

    # the default relative path for modules"
    DEFAULT_ACTION_MODULE_PATH: str = "module"

    # The action type this modules belongs to.
    # These are the (possible) candidates:
    # * input
    #   (* collect (standard - synchronous input action))
    #   (* consume (optional - a/synchronous input action))
    # * output
    #   (* send (standard = synchronous output action))
    #   (* produce (pending - a/synchronous output action))
    # * filter (edit content)
    # * buffer (temporary storage in case output fails)
    # * archive (long term storage for control and backup)
    action_type: str = ""
    # The name of this module as in the queue (just for fun/debugging)
    the_name: str = ""

    def __init__(self, name: str = "") -> None:
        """Instanciate the object.

        :param name: obtional name"""
        logging.debug(f"Instanciate *{name}<{self.__class__.__name__}>*")
        self.the_name = name
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
            BridgingHubBaseModule.KEY_DATETIME_NAME: "datetime",
            BridgingHubBaseModule.KEY_DATETIME_FORMAT_NAME: "datetimeformat",
            BridgingHubBaseModule.KEY_TYPE_NAME: "type",
            BridgingHubBaseModule.KEY_VALUE_NAME: "value",
            BridgingHubBaseModule.KEY_UNIT_NAME: "unit",
        }
        # Let others subscribe to the data that get's processed
        self._subscription: list[Callable] = []

    @abstractmethod
    def dispatch(self, action_name: str) -> Callable | None:
        """Abstract generic method for all runs, i.e. the implementing
        module can deside, based on the config and action_name, whether
        to subscribe with the main loop or with another module segment.

        :param action_name: context to consider
        :raise BrokenConfigException:"""
        pass

    def listen(self, observer: Callable) -> Callable | None:
        """Try to subscribe with subjects defined in the config or
        return the observer method to be added to the main loop.

        :param Callable observer: the method subscribing with
        :rtype: Callable
        :return: None if subscriptions added or the observer
        :raise: BrokenConfigException"""
        if (
            BridgingHubBaseModule.KEY_ACTION_SUBSCRIBE in self._action_detail
        ) and (
            self._action_detail[BridgingHubBaseModule.KEY_ACTION_SUBSCRIBE]
        ):
            s = self._action_detail[BridgingHubBaseModule.KEY_ACTION_SUBSCRIBE]
            if not isinstance(s, list):
                raise BrokenConfigException(
                    f"'{BridgingHubBaseModule.KEY_ACTION_SUBSCRIBE}' \
MUST be a list, if defined."
                )
            else:
                for subscription in s:
                    mod = BridgingHubModuleRegistry.load_module(subscription)
                    mod.subscribe(observer)
                return None
        return observer

    def subscribe(self, call: Callable) -> None:
        """
        Any 'observer' interested can subscribe here in order to receive
        the data gathered in this input module.
        """
        self._subscription.append(call)

    def on_data(self, data: dict[str, dict[str, str]]) -> None:
        """
        On data input, call this method to 'notify' the 'observers'
        and pass the data to all parties interested.

        :param data: single data element as dict
        :rtype: dict
        """
        for c in self._subscription:
            c(data)

    @abstractmethod
    def input(
        self, message: dict[str, dict[str, str]] = {}
    ) -> dict[str, dict[str, str]]:
        """Abstract input method.

        :param message: optional data set as dict
        :rtype: dict
        :return: data set
        :raise BrokenConfigException:"""
        pass

    @abstractmethod
    def output(
        self, message: dict[str, dict[str, str]]
    ) -> dict[str, dict[str, str]]:
        """Abstract output method.

        :param message: data set as dict
        :rtype: dict
        :return: data (sub-)set processed
        :raise BrokenConfigException:"""
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
        logging.debug("  * base configuration successful.")

    def current_timestamp(self) -> int:
        """Generate a timestamp in nano seconds since Unix Epoch.
        :rtype: int
        :return: nano second timestamp a.U."""
        epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        return int((now - epoch).total_seconds() * 1000000000)

    def current_datetimestr(self, form: str = "") -> str:
        """Generate a formated date-time string representation.
        :rtype: str
        :return: date-time string"""
        logging.debug(f"bhm.current_datetimestr({form})")
        if form:
            return datetime.now().strftime(form)
        else:
            return datetime.now().isoformat()


class InputBaseModule(BridgingHubBaseModule):
    """
    Abstract base module for all input modules.
    """

    action_type: str = BridgingHubBaseModule.KEY_INPUT

    def dispatch(self, action_name: str) -> Callable | None:
        """All input modules can be called from the main loop,
        (eventually even blocking type) or by some other
        module...

        :param action_name: context to consider
        :raise BrokenConfigException:"""
        # Input modules:
        # in 'bridge' or 'input' mode subscribe to defined modules
        # or dispatch directly to main loop
        if (
            action_name == BridgingHubBaseModule.KEY_BRIDGE
            or action_name == BridgingHubBaseModule.KEY_INPUT
        ):
            return self.listen(self.input)
        else:  # OUTPUT or CLEANUP
            return None

    def output(
        self, message: dict[str, dict[str, str]]
    ) -> dict[str, dict[str, str]]:
        """
        Fake output method in input module.

        :param message: data set as dict
        :rtype: dict
        :return: data (sub-)set processed
        :raise BrokenConfigException:
        """
        raise BrokenConfigException(
            "This output module was configured in input."
        )
        return {}


class OutputBaseModule(BridgingHubBaseModule):
    """
    Abstract base module for all input modules.
    """

    action_type: str = BridgingHubBaseModule.KEY_OUTPUT

    def dispatch(self, action_name: str) -> Callable | None:
        """Output modules are to be called from the main loop,
        (eventually even blocking type).

        :param action_name: context to consider
        :raise BrokenConfigException:"""
        # Output modules:
        # In 'bridge' or 'output' mode, subscribe to defined modules
        # or dispatch directly to main loop
        if (
            action_name == BridgingHubBaseModule.KEY_BRIDGE
            or action_name == BridgingHubBaseModule.KEY_OUTPUT
        ):
            return self.listen(self.output)
        else:  # INPUT or CLEANUP
            return None

    def input(
        self, message: dict[str, dict[str, str]] = {}
    ) -> dict[str, dict[str, str]]:
        """
        Fake input method in output module.

        :rtype: dict
        :return: data set
        :raise BrokenConfigException:
        """
        raise BrokenConfigException(
            "This input module was configured in output."
        )
        return {}


class CollectorBaseModule(InputBaseModule):
    """
    Abstract base module used by modules collecting data.
    """

    def input(
        self, message: dict[str, dict[str, str]] = {}
    ) -> dict[str, dict[str, str]]:
        """
        Default input method for collector modules.

        :rtype: dict
        :return: data set
        :raise BrokenConfigException:
        """
        return self.collect()

    @abstractmethod
    def collect(self) -> dict[str, dict[str, str]]:
        """
        Abstract methods to be implemented to gather data info
        following the config data section and return a dict.

        :rtype: dict
        :return: data set
        :raise BrokenConfigException:
        """
        pass


class ConsumerBaseModule(InputModuleException):
    """
    Abstract base module used by modules optionally connected to
    data streams.
    """

    @abstractmethod
    def consume(self) -> None:
        """
        Abstract method used to connect to an external service.
        """
        pass


class SenderBaseModule(OutputBaseModule):
    """
    Abstract base sender module used by output modules.
    """

    def output(
        self, message: dict[str, dict[str, str]]
    ) -> dict[str, dict[str, str]]:
        """
        Default output method in sender module.

        :param message: data set as dict
        :rtype: dict
        :return: data (sub-)set processed
        :raise BrokenConfigException:
        """
        return self.send(message)

    @abstractmethod
    def send(
        self, message: dict[str, dict[str, str]]
    ) -> dict[str, dict[str, str]]:
        """
        Abstract method to be implemented to send data info
        following the config data section and return the
        dict of the data really sent away.

        :param message: data set as dict
        :rtype: dict
        :return: data (sub-)set processed
        :raise BrokenConfigException:
        """
        pass


class FilterBaseModule(BridgingHubBaseModule):
    """
    Abstract base filter module.
    """

    action_type: str = BridgingHubBaseModule.KEY_FILTER

    def dispatch(self, action_name: str) -> Callable | None:
        """All output modules can be called from the main loop,
        but usually follow some other module...

        :param action_name: context to consider
        :raise BrokenConfigException:"""
        # Filter modules:
        # In 'input' or 'bridge' mode subscribe to defined modules
        # or to main loop with write_buffer.
        # In 'output' mode, subscribe to modules or main loop,
        # except as first one, i.e. an empty list, it would not
        # change anything, as the messages will be empty..
        return self.listen(self.filter)

    def input(
        self, message: dict[str, dict[str, str]] = {}
    ) -> dict[str, dict[str, str]]:
        raise BrokenConfigException(
            "This filter module was configured in input."
        )
        return {}

    def output(
        self, message: dict[str, dict[str, str]]
    ) -> dict[str, dict[str, str]]:
        raise BrokenConfigException(
            "This filter module was configured in output."
        )
        return {}

    @abstractmethod
    def filter(
        self, message: dict[str, dict[str, str]]
    ) -> dict[str, dict[str, str]]:
        """Filter content between storage, in-, and output."""
        pass


class StorageBaseModule(BridgingHubBaseModule):
    """
    Abstract base storage module. There is only one type of storage
    module usable at a time.
    """

    KEY_SUBTYPE_BUFFER: str = "buffer"
    action_type: str = BridgingHubBaseModule.KEY_STORAGE

    def dispatch(self, action_name: str) -> Callable | None:
        """All storage modules can be called from the main loop,
        or follow some other module...

        :param action_name: context to consider
        :raise BrokenConfigException:"""
        # Storage modules:
        # a) acting as 'buffer'
        # In 'bridge' or 'input' mode, subscribe to defined modules or
        # main loop with self.write_buffer
        # In 'output' mode dispatch to main loop with self.read_buffer
        # b) acting as 'archive'
        # In 'input' mode, do nothing
        # In 'bridge' or 'output' mode though, dispatch to modules or
        # main with self.store
        mt: list = self._action_detail[
            BridgingHubBaseModule.KEY_MODULE_TYPE
        ].split(BridgingHubBaseModule.KEY_TYPE_SPLIT)
        # extract and see if we are in 'buffer' mode
        buffer = len(mt) >= 1 and mt[1] == StorageBaseModule.KEY_SUBTYPE_BUFFER
        if action_name == BridgingHubBaseModule.KEY_BRIDGE:
            if buffer:
                return self.listen(self.write_buffer)
            else:
                return self.listen(self.store)
        elif action_name == BridgingHubBaseModule.KEY_INPUT:
            if buffer:
                return self.listen(self.write_buffer)
        elif action_name == BridgingHubBaseModule.KEY_OUTPUT:
            if buffer:
                return self.listen(self.read_buffer)
            else:
                return self.listen(self.store)
        # INPUT:archive; TODO: CLEANUP not handled yet..
        return None

    def input(
        self, message: dict[str, dict[str, str]] = {}
    ) -> dict[str, dict[str, str]]:
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
    def write_buffer(
        self, message: dict[str, dict[str, str]]
    ) -> dict[str, dict[str, str]]:
        """Remember message content between in- and output."""
        pass

    @abstractmethod
    def read_buffer(self) -> dict[str, dict[str, str]]:
        """Look up message content between in- and output."""
        pass

    @abstractmethod
    def clean_buffer(
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


RegistryType = dict[str, dict[str, str | BridgingHubBaseModule]]


class BridgingHubModuleRegistry:
    """
    Registry class for dynamic module access.
    """

    KEY_MOD_PATH = "module_path"
    KEY_MOD_NAME = "module_name"
    KEY_MOD_OBJ = "module_object"

    _registry: RegistryType = {}

    @classmethod
    def register_module(
        cls, segment_name: str, module_class_name: str, module_path: str
    ) -> BridgingHubBaseModule:
        """Register a method in the table for later.

        :param str segment_name: The name of the segment in the chain.
        :param str module_class_name: The name of the class.
        :param str module_path: The path of the python module.
        :rtype: BridgingHubBaseModule
        :return: The registered BridgingHubBaseModule
        :raise NoSuchModuleException: (pass on)"""
        n: str = segment_name
        if n not in cls._registry:
            cls._registry[n] = {
                BridgingHubModuleRegistry.KEY_MOD_PATH: module_path
            }
            cls._registry[n][
                BridgingHubModuleRegistry.KEY_MOD_NAME
            ] = module_class_name
        return cls.load_module(segment_name, module_class_name)

    @classmethod
    def load_module(
        cls, segment_name: str, module_class_name: str
    ) -> BridgingHubBaseModule:
        """Look up and return a new instance of a registered module.

        :param str segment_name: The name of the segment in the chain.
        :param str module_class_name: The name of the module as key.
        :rtype: a subclass of BridgingHubBaseModule
        :return: The class of the module.
        :raise NoSuchModuleException:"""
        n: str = segment_name
        # Test if the module was indeed registered
        if n not in cls._registry or not cls._registry[n]:
            raise NoSuchModuleException(
                f"There is no module registered by the name of \
'{module_class_name}'."
            )
        # Test if the module was loaded before..
        if not (
            BridgingHubModuleRegistry.KEY_MOD_OBJ in cls._registry[n]
            and cls._registry[n][BridgingHubModuleRegistry.KEY_MOD_OBJ]
        ):
            module_path: str = str(
                cls._registry[n][BridgingHubModuleRegistry.KEY_MOD_PATH]
            )
            try:
                pymod = import_module(module_path)
            except ModuleNotFoundError as e:
                raise NoSuchModuleException(
                    f"Module '{module_class_name}' not found. {e}"
                )
            except ImportError as e:
                raise NoSuchModuleException(f"Import error: {e}")
            except SyntaxError as e:
                raise NoSuchModuleException(
                    f"Syntax error in module '{module_class_name}': {e}"
                )
            except Exception as e:
                raise NoSuchModuleException(
                    f"""An unexpected error occurred in '{module_class_name}'\
: {e}"""
                )
            module_class: Type = getattr(pymod, module_class_name)
            if not issubclass(module_class, BridgingHubBaseModule):
                raise NoSuchModuleException(
                    f"An incompatible module ('{module_class}') was \
registered for '{module_path}.'"
                )
            else:
                cls._registry[n][BridgingHubModuleRegistry.KEY_MOD_OBJ] = cast(
                    BridgingHubBaseModule, module_class(segment_name)
                )
        if isinstance(
            cls._registry[n][BridgingHubModuleRegistry.KEY_MOD_OBJ],
            BridgingHubBaseModule,
        ):
            return cast(
                BridgingHubBaseModule,
                cls._registry[n][BridgingHubModuleRegistry.KEY_MOD_OBJ],
            )
        else:
            t = type(cls._registry[n][BridgingHubModuleRegistry.KEY_MOD_OBJ])
            raise NoSuchModuleException(
                f"Unexpected module type found for {n}: {t}."
            )
