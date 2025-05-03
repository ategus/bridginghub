#!/usr/bin/env python3
# using argparse as we want to keep dependencies minimal
import argparse

# import logging
# TODO use logfile on demand..
import os

# import shutil
import sys
from os.path import join

from bridging_hub_module import (  # InputModuleException,
    BridgingHubBaseModule,
    BridgingHubModuleRegistry,
    BrokenConfigException,
    DuplicatedModuleException,
    InputModuleException,
    NoSuchModuleException,
    StorageBaseModule,
)


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
        filename = join(workdir, filename)
    if filename[-5:] == ".json":
        import json

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


def load_module(action_name, config) -> BridgingHubBaseModule:
    """Wrapper to load all modulues uniformely (and no-brain-ly).

    :param str action_name: The name/type to register the module with
    :param config config: The parameter map that was read from file
    :rtype: BridgingHubBaseModule
    :return: Return the requested module"""
    BridgingHubModuleRegistry.register_module(
        config[action_name][BridgingHubBaseModule.KEY_ACTION_MODULE_NAME],
        config[action_name][BridgingHubBaseModule.KEY_ACTION_MODULE_PATH],
    )
    m = BridgingHubModuleRegistry.load_module(
        config[action_name][BridgingHubBaseModule.KEY_ACTION_MODULE_NAME]
    )
    m.configure(config)
    return m


def run_module(action_name, config) -> bool:
    """Execute a module using the parameters found in the config.

    :param str action_name: The name of the action type from the registry
    :param dict config: The parameter map that was read from file
    :rtype: bool
    :return: Report success/failure and leave the rest to the caller"""
    try:
        # TODO testing here..
        # Load the storage module on request by config
        m: BridgingHubBaseModule | None = None
        s: BridgingHubBaseModule | None = None
        ri: dict[str, dict[str, str]] = {}
        rc: dict[str, dict[str, str]] = {}
        ro: dict[str, dict[str, str]] = {}
        re: dict[str, dict[str, str]] = {}
        if (
            BridgingHubBaseModule.KEY_STORAGE in config
            and config[BridgingHubBaseModule.KEY_STORAGE]
            and BridgingHubBaseModule.KEY_ACTION_MODULE_NAME
            in config[BridgingHubBaseModule.KEY_STORAGE]
            and config[BridgingHubBaseModule.KEY_STORAGE][
                BridgingHubBaseModule.KEY_ACTION_MODULE_NAME
            ]
        ):
            s = load_module(BridgingHubBaseModule.KEY_STORAGE, config)

        # Load input module on request by action or default to data
        if (
            action_name == BridgingHubBaseModule.KEY_BRIDGE
            or action_name == BridgingHubBaseModule.KEY_INPUT
        ):
            m = load_module(BridgingHubBaseModule.KEY_INPUT, config)
            # get the infos from input
            ri = m.input()
            if isinstance(s, StorageBaseModule):
                # write input to cache and return cached items
                rc = s.write_cache(ri)
        elif action_name == BridgingHubBaseModule.KEY_OUTPUT:
            if isinstance(s, StorageBaseModule):
                # w/o input, just read from cache
                rc = s.read_cache()
            else:
                # w/o input and cache, just load the config
                ri = config[BridgingHubBaseModule.KEY_DATA]
        if (
            action_name == BridgingHubBaseModule.KEY_BRIDGE
            or action_name == BridgingHubBaseModule.KEY_OUTPUT
        ):
            m = load_module(BridgingHubBaseModule.KEY_OUTPUT, config)
            if ri:
                ro = m.output(ri)
            elif rc:
                ro = m.output(rc)
            print("rc:", rc)
            if isinstance(s, StorageBaseModule):
                # process current data mv timestamp_datakey.json from new
                # to junk or archive
                re = s.store(ro)
                print(re)
                pass
            if "verbose" in config and config["verbose"] == "True":
                if ri:
                    print("Info from input: ", ri)
                if rc:
                    print("Info from cache: ", rc)
                if ro:
                    print("Info from output:", ro)
        elif action_name == BridgingHubBaseModule.KEY_CLEANUP:
            if isinstance(s, StorageBaseModule):
                pass  # cleanup
            else:
                raise BrokenConfigException(
                    "Action 'cleanup' only makes sense if "
                    + "'storage' is configured."
                )
        else:  # action_name == BridgingHubBaseModule.KEY_INPUT:
            if ri:
                print("Info from input: ", ri)
            if rc:
                print("Info from cache: ", rc)

    # TODO:
    # input -> prefilter -> cache -> filter -> output -> postfilter -> store
    except (NoSuchModuleException, DuplicatedModuleException) as e:
        print(e)
        return False
    except AssertionError as e:
        print("Wrong type of module loaded", e)
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
    cfg = {}
    # try to load the config context first
    try:
        if args.config:
            # get the config from the location provided by the user
            cfg = load_config(args.config, cfg_dir)
        if "verbose" in cfg and cfg["verbose"]:
            if cfg["verbose"] != "True" or cfg["verbose"] != "False":
                raise BrokenConfigException(
                    "The 'verbose' parameter is reserved, see flag '--verbose'"
                )
        if args.verbose:
            cfg["verbose"] = "True"
        # the rest of the config may either also come from cli, or the
        # single or split config file(s)
        if isinstance(cfg[BridgingHubBaseModule.KEY_INPUT], str):
            cfg[BridgingHubBaseModule.KEY_INPUT] = load_config(
                cfg[BridgingHubBaseModule.KEY_INPUT], cfg_dir
            )
        if isinstance(cfg[BridgingHubBaseModule.KEY_OUTPUT], str):
            cfg[BridgingHubBaseModule.KEY_OUTPUT] = load_config(
                cfg[BridgingHubBaseModule.KEY_OUTPUT], cfg_dir
            )
        if isinstance(cfg[BridgingHubBaseModule.KEY_DATA], str):
            cfg[BridgingHubBaseModule.KEY_DATA] = load_config(
                cfg[BridgingHubBaseModule.KEY_DATA], cfg_dir
            )
        # TODO duplicate for storage
        # TODO duplicate for filter

        run_module(action_name, cfg)
        sys.exit(0)  # all done here..
    except IllegalFileOperation as e:
        print(e)
        sys.exit(2)
    except (
        DuplicatedModuleException,
        NoSuchModuleException,
        BrokenConfigException,
        InputModuleException,
    ) as e:
        print("The following error occurred:", e)
        sys.exit(98)
    except Exception as e:
        print(f"Last resort error... {e}")
        sys.exit(99)
