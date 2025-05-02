# import logging

import sys

from bridging_hub_module import CollectorBaseModule


class StdinCollector(CollectorBaseModule):
    """
    Read from stdin and return the result.
    """

    def collect(self) -> dict[str, dict[str, str]]:
        r: dict[str, dict[str, str]] = {}
        d = list(self._data["value_register_map"])
        try:
            # use only as many input lines as there are
            # data points defined in the config..
            for line in sys.stdin:
                k = d.pop()
                r[k] = {}
                n = self.current_timestamp()
                r[k][self._custom_name["timestamp_name"]] = n
                r[k][self._custom_name["value_name"]] = line
        except IndexError:
            # latest configured element reached
            pass
        return r
