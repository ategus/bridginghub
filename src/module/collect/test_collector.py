# import logging

import os

from bridging_hub_module import CollectorBaseModule


class TestCollector(CollectorBaseModule):
    """
    Just for a test, collect some info about the system
    the software is running on.
    """

    def __init__(self):
        self._element = None

    def collect(self) -> dict[str, dict[str, str]]:
        r: dict[str, dict[str, str]] = {}
        d = list(self._data["value_register_map"])
        # only use the first data point
        k = d.pop()
        r[k] = {}
        n = self.current_timestamp()
        r[k][self._custom_name["timestamp_name"]] = n
        u = os.uname()
        s = f"""The current system is: {u.sysname} {u.nodename} {u.release}",
        {u.version} {u.machine}"""
        r[k][self._custom_name["value_name"]] = s
        return r
