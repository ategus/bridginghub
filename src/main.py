#!/usr/bin/env python3
# using argparse as we want to keep dependencies minimal
import argparse
import json

# import logging
# TODO use logfile on demand..
import os

# import shutil
import sys
from os.path import join

from bridging_hub_module import (
    BridgingHubModuleRegistry,
    DuplicatedModuleException,
    NoSuchModuleException,
)

# define config parameter keys as 'constants' in order to get errors on typos
KEY_CONFIG_COLLECTOR = "config_collector"
KEY_CONFIG_DATASET = "config_dataset"
KEY_CONFIG_SENDER = "config_sender"
KEY_DATA_CONFIG_FILE = "data_config_file"
KEY_DATA_CONFIG = "data"
KEY_COLLECT_ACTION = "collect"
KEY_SEND_ACTION = "send"
KEY_ACTION_TYPES = {
    KEY_COLLECT_ACTION: KEY_CONFIG_COLLECTOR,
    KEY_SEND_ACTION: KEY_CONFIG_SENDER,
}

KEY_ACTION_MODULE_NAME = "module_class_name"
KEY_ACTION_MODULE_PATH = "module_path"

# the default relative path for modules
DEFAULT_ACTION_MODULE_PATH = "module"


class IllegalFileOperation(Exception):
    """Custom Error to be raised on illegal file operations in order
    to treat them uniformly."""

    pass


def load_config(filename, workdir="") -> dict:
    """Load the configuration for actions and data from YAML/JSON file(s).

    :param str filename: The name of the file (mandatory)
    :rtype: dict
    :return: The config from YAML/JSON
    :raise IllegalFileOperation:"""
    j = {}
    if not filename.startswith("/"):
        filename = workdir + "/" + filename
    if filename[-5:] == ".json":
        try:
            with open(filename) as f:
                j = json.load(f)
        except Exception as e:
            raise IllegalFileOperation(
                f"""Could not read the config file {filename} - the
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
                    """Install at least either 'ruamel.yaml' or 'yaml'
                    (PyYAML) or use JSON configs."""
                )
        try:
            with open(filename) as f:
                j = yaml.safe_load(f)
        except Exception as e:
            raise IllegalFileOperation(
                f"""Could not read the config file {filename} -
                the problem was: {e}"""
            )
    return j


def run_module(action_name, config) -> bool:
    """Execute a module using the parameters found in the config.

    :param str action_name: The name of the action type from the registry
    :param dict config: The parameter map that was read from file
    :rtype: bool
    :return: Report success/failure and leave the rest to the caller"""
    try:
        # TODO testing here..
        BridgingHubModuleRegistry.register_module(
            config[action_name][KEY_ACTION_MODULE_NAME],
            config[action_name][KEY_ACTION_MODULE_PATH],
        )
        f = BridgingHubModuleRegistry.load_module(
            config[action_name][KEY_ACTION_MODULE_NAME]
        )
        f.run()
    except (NoSuchModuleException, DuplicatedModuleException) as e:
        print(e)
        return False
    return True
    # TODO how shall we exit eventually? Clean up should be done in the
    # submodule..?!


if __name__ == "__main__":
    """Main function called if the command line was the entry point."""
    parser = argparse.ArgumentParser(
        description="""Modular approach to collect, handle and distribute
            measurement data."""
    )
    parser.add_argument(
        "action",
        help="Action type",
        choices=KEY_ACTION_TYPES.keys(),
    )
    parser.add_argument(
        "-c", "--config", help="One file to rule them all.. (action and data)"
    )
    parser.add_argument(
        "-a",
        "--actiondir",
        help="Directory containing action parameters (alternative: --config).",
    )
    parser.add_argument(
        "-d",
        "--datadir",
        help="""Directory containing parameters describing the data
            (alternative: --config).""",
    )
    parser.add_argument(
        "-m",
        "--moduledir",
        help="""Directory containing compatible action modules
            (alternative: --config).""",
    )
    cwd = os.getcwd()
    parser.add_argument(
        "-w",
        "--workdir",
        help="""Top-level directory of the project for relative paths
            (default: `cwd`).""",
        default=cwd,
    )
    args = parser.parse_args()
    # TODO       filter_name = ""
    # the action defines the mode to run in
    action_name = args.action

    # prepare the data and action dirs
    data_config_dir = ""
    action_config_dir = ""
    action_module_dir = ""
    if args.datadir:
        data_config_dir = args.datadir
    if args.actiondir:
        action_config_dir = args.actiondir
    if args.moduledir:
        action_module_dir = args.moduledir

    # try to load the config context first
    cfg_dir = args.workdir
    if not os.path.isdir(cfg_dir):
        print(f"Please provide a valid WORKDIR: {cfg_dir}")
        sys.exit(1)
    # initialize the config dictionary
    cfg = {}
    try:
        if args.config:
            # get the config from the location provided by the user
            cfg = load_config(args.config, cfg_dir)
        # the rest of the config may either also come from cli, or the
        # single or split config file(s)
        ac = KEY_ACTION_TYPES[action_name]
        if ac in cfg and cfg[ac]:
            if action_config_dir:
                print(
                    """The config is inconsistent, please use either the cli
                        option or the config for the action dir."""
                )
                sys.exit(2)
            action_config_dir = cfg[ac]

        if action_name in cfg and cfg[action_name]:
            if action_config_dir:
                print(
                    f"""The config is inconsistent, please use either cli,
                        split or single file configs, i.e. either '{ac}'
                    OR '{action_name}' in '{cfg_dir}'"""
                )
                sys.exit(4)
            # if there is only one action to be processed ready available,
            # let's do it
            run_module(action_name, cfg)
            sys.exit(0)  # all done here..

        # if still here, try to find the path to the action configs
        if not action_config_dir.startswith("/"):
            action_config_dir = cfg_dir + "/" + action_config_dir
        if not os.path.isdir(action_config_dir):
            print(
                f"""The specified config file dir '{action_config_dir}'
                is not a directory.."""
            )
            sys.exit(4)

        for f in os.listdir(action_config_dir):
            # every sub-config file will trigger a separate action
            if f.startswith("."):
                # ignore hidden files
                continue
            # load the contents from file and override the action into config
            cfg[action_name] = load_config(action_config_dir + "/" + f)
            # search for the relevant data config
            data_file_name = cfg[action_name][KEY_DATA_CONFIG_FILE]
            if data_file_name:
                if KEY_DATA_CONFIG in cfg and cfg[KEY_DATA_CONFIG]:
                    print(
                        f"""There already was something in the
                        '{KEY_DATA_CONFIG}' section, but also
                        '{KEY_CONFIG_DATASET}' was specified."""
                    )
                    sys.exit(6)
                if not data_file_name.startswith("/"):
                    data_file_name = join(
                        cfg[KEY_CONFIG_DATASET],
                        data_file_name,
                    )
                if not cfg[KEY_CONFIG_DATASET].startswith("/"):
                    data_file_name = cfg_dir + "/" + data_file_name
                # if the data had a config file defined, get it's content
                cfg[KEY_DATA_CONFIG] = load_config(data_file_name)
            else:
                print(
                    f"""Please either specify '{KEY_CONFIG_DATASET}'
                    or '{KEY_DATA_CONFIG_FILE}'"""
                )
                sys.exit(6)
            if (
                KEY_ACTION_MODULE_NAME not in cfg
                or not cfg[KEY_ACTION_MODULE_NAME]
            ):
                action_module_dir = (
                    DEFAULT_ACTION_MODULE_PATH + "/" + action_name
                )
                if not action_module_dir.startswith("/"):
                    action_module_dir = args.workdir + "/" + action_module_dir
            if KEY_ACTION_MODULE_PATH in cfg and cfg[KEY_ACTION_MODULE_PATH]:
                action_module_dir = cfg[KEY_ACTION_MODULE_PATH]
            # let's do the work
            run_module(action_name, cfg)
    except IllegalFileOperation as e:
        print(e)
        sys.exit(2)
    except SystemExit as e:
        # TODO: after refactoring, raise and catch and exit from here now
        print(f"TODO... {e}")
        sys.exit(98)
    except Exception as e:
        print(f"Last resort error... {e}")
        sys.exit(99)
