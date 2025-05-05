# import logging

import sys

from bridging_hub_module import CollectorBaseModule, InputModuleException


class StdinCollector(CollectorBaseModule):
    """
    Input module that combines info from Stdin with data configuration.
    """

    def collect(self) -> dict[str, dict[str, str]]:
        """Read from stdin and return the result.

        :rtype: dict
        :return: mapping of the input info
        :raise InputModuleError:"""
        r: dict[str, dict[str, str]] = {}
        d = list(self._data[StdinCollector.KEY_DATA_VALUE_MAP])
        try:
            # use only as many input lines as there are
            # data points defined in the config..
            for line in sys.stdin:
                k = d.pop()
                r[k] = {}
                n = str(self.current_timestamp())
                r[k][
                    self._custom_name[StdinCollector.KEY_BH_STATUS_NAME]
                ] = "in"
                r[k][self._custom_name[StdinCollector.KEY_TIMESTAMP_NAME]] = n
                r[k][self._custom_name[StdinCollector.KEY_VALUE_NAME]] = line
                if len(d) == 0:
                    break
        except Exception as e:
            raise InputModuleException(
                f"An error occurred during reading input: {e}"
            )
        return r
