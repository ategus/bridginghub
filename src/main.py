#!/usr/bin/env python3
# using argparse as we want to keep dependencies minimal
import argparse
import logging
import os

# import shutil
import sys
from os.path import join
from typing import Callable, cast

from bridging_hub_module import (  # InputModuleException,; StorageBaseModule,
    BridgingHubBaseModule,
    BridgingHubModuleRegistry,
    BrokenConfigException,
    DuplicatedModuleException,
    NoSuchModuleException,
)
from bridging_hub_types import (
    ConfigDataType,
    ConfigSubType,
    ConfigType,
)

KEY_BH_CONFIG = "_bH"
KEY_BH_VERSION_COMPAT = "compat"
KEY_BH_LOGFILE = "log_file"
KEY_BH_LOGENCODING = "log_encoding"
KEY_BH_LOGLEVEL = "log_level"
KEY_BH_VERBOSE = "verbose"

verbose = False


class IllegalFileOperation(Exception):
    """Custom Error to be raised on illegal file operations in order
    to treat them uniformly."""

    pass


class ModuleLoaderException(Exception):
    """Marks a problem in connection with configuration and loading of
    modules."""

    pass


class ModuleFlowException(Exception):
    """Failed to run the module pipe as requested."""

    pass


def load_config(
    filename, workdir=""
) -> ConfigType | ConfigSubType | ConfigDataType:
    """Load the configuration for actions and data from YAML/JSON file(s).

    :param str filename: The name of the file (mandatory)
    :rtype: dict
    :return: The config from YAML/JSON
    :raise IllegalFileOperation:"""
    j: dict = {}
    if not filename.startswith("/"):
        filename = join(workdir, filename)
    if filename[-5:] == ".json":
        import json

        try:
            with open(filename) as f:
                j = json.load(f)
        except Exception as e:
            raise IllegalFileOperation(
                f"""Could not read the config file {filename} - the \
problem was: {e}"""
            )
    if filename[-5:] == ".yaml" or filename[-4:] == ".yml":
        try:
            import ruamel.yaml as yaml  # type: ignore[import-untyped]
        except ImportError:
            try:
                import yaml  # type: ignore[import-untyped, no-redef]
            except ImportError:
                raise IllegalFileOperation(
                    """Install at least either 'ruamel.yaml' or 'yaml' \
(PyYAML) or use JSON configs."""
                )
        try:
            with open(filename) as f:
                j = yaml.safe_load(f)
        except Exception as e:
            raise IllegalFileOperation(
                f"""Could not read the config file {filename} - \
the problem was: {e}"""
            )
    return j


def load_module(
    config: ConfigType, module_type: str, segment_name: str = "default"
) -> BridgingHubBaseModule:
    """Wrapper to load all modulues uniformely (and no-brain-ly).

    :param config config: The parameter map that was read from file
    :param str module_type: The control type of the module
    :param str segment: The name of the segment to register module with
    :rtype: BridgingHubBaseModule
    :return: Return the requested module
    :raise: ModuleLoaderException"""
    try:
        ac = config[module_type]
        assert isinstance(
            ac, dict
        ), f"Expected action_config to be a dictionary, but got {type(ac)}"

        try:
            mn = ac[BridgingHubBaseModule.KEY_MODULE_NAME]
            mp = ac[BridgingHubBaseModule.KEY_MODULE_PATH]
        except KeyError as e:
            raise BrokenConfigException(
                f"Relevant config: {module_type} {ac}: KeyError {e}"
            )
        assert isinstance(
            mn, str
        ), f"""Expected module_name to be a string, \
but got {type(mn)}"""
        assert isinstance(
            mp, str
        ), f"""Expected module_path to be a string, \
but got {type(mp)}"""

        m = BridgingHubModuleRegistry.register_module(segment_name, mn, mp)
        if m.action_type != module_type:
            raise BrokenConfigException(
                f"""The config for '{module_type}' holds a module of type \
'{m.action_type}'."""
            )
        m.configure(config)
        return m
    except (
        AssertionError,
        DuplicatedModuleException,
        NoSuchModuleException,
        BrokenConfigException,
    ) as e:
        raise ModuleLoaderException(f"Unable to load module: {e}")


def run_data_flow(action_name, config) -> bool:
    """Execute the modules as defined by the config file.

    :param str action_name: The name of the action type from the registry
    :param dict config: The parameter map that was read from file
    :rtype: bool
    :return: Report success/failure and leave the rest to the caller
    :raise: ModuleFlowException"""
    # extract the data once
    data: ConfigDataType = config.pop(BridgingHubBaseModule.KEY_DATA)
    # this stores the processing order
    flow: list[Callable] = []
    # filter by action_name..
    if verbose:
        print(f"Loading all relevant modules for action '{action_name}'...")
    for segment_name, segment_config in config.items():
        module_type: str = segment_config[
            BridgingHubBaseModule.KEY_MODULE_TYPE
        ].split(BridgingHubBaseModule.KEY_TYPE_SPLIT)[0]
        # register the module, and the processing order
        if verbose:
            print("  - Processing:", action_name, segment_name)
        m = load_module(
            {
                module_type: segment_config,
                BridgingHubBaseModule.KEY_DATA: data,
            },
            module_type,
            segment_name,
        )
        # let the module subscribe with others and tell it
        # about the action context the user requested
        c = m.dispatch(action_name)
        if c:
            flow.append(c)
        print(len(flow))

    msg: dict = {}
    for c in flow:
        print("Module:", dir(c))
        msg = c(msg)

    return True


if __name__ == "__main__":
    """Main function called if the command line was the entry point."""
    parser = argparse.ArgumentParser(
        description="""Modular approach to collect, handle and distribute
            measurement data."""
    )
    parser.add_argument(
        "action",
        help="""Action type: `collect` and `send` are in-/output only actions,
            while `bridge` binds an in- to an output channel (optionally
            storing and filtering the content on the go).
            """,
        choices=BridgingHubBaseModule.KEY_ACTION_TYPES,
    )
    parser.add_argument(
        "-c",
        "--config",
        help="""Central config file (mandatory).""",
        required=True,
    )
    parser.add_argument("-l", "--logfile", help="""Name of the logfile."""),
    parser.add_argument(
        "-L",
        "--loglevel",
        help="""Log level: INFO, WARNING, ERROR, CRITICAL. (see --config.)""",
    ),
    cwd = os.getcwd()
    parser.add_argument(
        "-w",
        "--workdir",
        help="""Top-level directory of the project for relative paths
            (default: `cwd`).""",
        default=cwd,
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="""Be more explicit on what happens.""",
    )
    args = parser.parse_args()
    # the action defines the mode to run in
    action_name = args.action
    # args.workdir defaults to `cwd` if not set
    cfg_dir = args.workdir
    if not os.path.isdir(cfg_dir):
        print(f"Please provide a valid WORKDIR: {cfg_dir}")
        sys.exit(1)
    # initialize the config dictionary
    cfg: dict = {}
    # try to load the config context first
    try:
        if args.config:
            # get the config from the location provided by the user
            cfg = cast(ConfigType, load_config(args.config, cfg_dir))

        # define meta config values and defaults
        cfg_version: float = 0
        logfile: str = ""
        logenc: str = "utf-8"
        loglevel: str = "ERROR"
        # overwrite them with actual config settings
        if KEY_BH_CONFIG in cfg and cfg[KEY_BH_CONFIG]:
            bh = cfg.pop(KEY_BH_CONFIG)
            if not isinstance(bh, dict):
                raise BrokenConfigException(
                    f"""'{KEY_BH_CONFIG}' must be a map in the config."""
                )
            if KEY_BH_VERSION_COMPAT in bh and bh[KEY_BH_VERSION_COMPAT]:
                cfg_version = float(bh[KEY_BH_VERSION_COMPAT])
            if KEY_BH_VERBOSE in bh and bh[KEY_BH_VERBOSE]:
                verbose = bh[KEY_BH_VERBOSE]
            if KEY_BH_LOGFILE in bh and bh[KEY_BH_LOGFILE]:
                logfile = bh[KEY_BH_LOGFILE]
            if KEY_BH_LOGENCODING in bh and bh[KEY_BH_LOGENCODING]:
                logenc = bh[KEY_BH_LOGENCODING]
            if KEY_BH_LOGLEVEL in bh and bh[KEY_BH_LOGLEVEL]:
                loglevel = bh[KEY_BH_LOGLEVEL]
        # again overwrite config params with CLI (it always wins)
        if args.verbose:
            verbose = True
        if args.logfile:
            logfile = args.logfile
        if args.loglevel:
            loglevel = args.loglevel

        # add logging
        logging.basicConfig(filename=logfile, encoding=logenc, level=loglevel)

        # the rest of the config may either also come from cli, or the
        # single or split config file(s)

        for k, v in cfg.items():
            if isinstance(v, str):
                cfg[k] = cast(ConfigSubType, load_config(v, cfg_dir))

        if BridgingHubBaseModule.KEY_DATA not in cfg:
            raise ModuleFlowException(
                f"""Please configure '\
{BridgingHubBaseModule.KEY_DATA}' in the config file."""
            )

        if verbose:
            print(f"Import config:\n{cfg}")

        if cfg_version >= 1:
            run_data_flow(action_name, cfg)
        else:
            raise BrokenConfigException(
                "Config version not supported. See `_bH: version`"
            )
        sys.exit(0)  # all done here..
    except BrokenConfigException as e:
        print(f"Can not go on without config: {e}")
        sys.exit(1)
    except IllegalFileOperation as e:
        print(f"Could not load the config: {e}")
        sys.exit(2)
    except ModuleFlowException as e:
        print(f"The following error occurred: {e}")
        sys.exit(98)
