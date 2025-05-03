import json
import os
from pathlib import Path

from bridging_hub_module import BrokenConfigException, StorageBaseModule


class DefaultStorageModule(StorageBaseModule):
    """
    Message content can be cached and archived or broken messages identified.
    """

    KEY_CACHE: str = "cache"
    KEY_JUNK: str = "junk"
    KEY_ARCHIVE: str = "archive"

    _cachedir: str = ""

    def test_dir(self, dir_type: str) -> str:
        """Check for and prepare storage directory.
        :param dir_type: the kind of directory in question
        :rtype: bool
        :return: whether directory was configured
        :raise: BrokenConfigException"""
        if dir_type in self._action_detail and self._action_detail[dir_type]:
            if os.path.isabs(self._action_detail[dir_type]):
                try:
                    os.makedirs(
                        self._action_detail[dir_type],
                        exist_ok=True,
                    )
                    return self._action_detail[dir_type]
                except Exception as e:
                    raise BrokenConfigException(
                        f"""Directory {dir_type} was requested but failed:""",
                        e,
                    )
            else:
                raise BrokenConfigException(
                    f"""Please use an absolute path for {dir_type} storage."""
                )
        else:
            return ""

    def write_cache(
        self, message: dict[str, dict[str, str]]
    ) -> dict[str, dict[str, str]]:
        """Remember message content between in- and output."""
        d = self.test_dir(DefaultStorageModule.KEY_CACHE)
        m: dict[str, dict[str, str]] = {}
        if d:
            for k, v in message.items():
                n = os.path.join(
                    d,
                    str(
                        v[
                            self._custom_name[
                                StorageBaseModule.KEY_TIMESTAMP_NAME
                            ]
                        ]
                    )
                    + "_"
                    + str(k)
                    + ".json",
                )
                with open(n, "w") as f:
                    json.dump(v, f)
                m[k] = v
        return m

    def read_cache(self) -> dict[str, dict[str, str]]:
        """Look up message content between in- and output."""
        d = self.test_dir(DefaultStorageModule.KEY_CACHE)
        m: dict[str, dict[str, str]] = {}
        if d:
            for k in self._data:
                for p in Path(d).glob("*" + "_" + k + ".json"):
                    with open(p, "r") as f:
                        m[k] = json.load(f)
        return m

    def clean_cache(
        self, message: dict[str, dict[str, str]]
    ) -> dict[str, dict[str, str]]:
        """Clean up the files remembered between in- and output."""
        return message

    def store(
        self, message: dict[str, dict[str, str]]
    ) -> dict[str, dict[str, str]]:
        """Remember message content after output."""
        return message
