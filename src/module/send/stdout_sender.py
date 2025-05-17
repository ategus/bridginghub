# import logging

from bridging_hub_module import SenderBaseModule


class StdoutSender(SenderBaseModule):
    """
    Write to stdout and return the result.
    """

    def send(
        self, message: dict[str, dict[str, str]]
    ) -> dict[str, dict[str, str]]:
        m: dict[str, dict[str, str]] = {}
        for k, v in message.items():
            s = ""
            if v[self._custom_name[StdoutSender.KEY_BH_STATUS_NAME]] == "in":
                s = "INPUT"
            else:
                s = "BUFFER"
            # NOTE: If you write other output modules, you might want to
            # catch certain Exceptions here and try to go on with the next
            # datapoint marking it 'failed' instead of 'out'... (see also
            # 'module/storage/default_storage.py') - files not in 'm' will
            # be skipped in 'store(m)'.
            # TODO: Enrich with data conf params set in _custom_name...
            print(f"Data '{k}' received from '{s}': {v}")
            m[k] = v
            m[k][self._custom_name[StdoutSender.KEY_BH_STATUS_NAME]] = "out"
        return m
