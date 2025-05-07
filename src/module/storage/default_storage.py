import json
from datetime import datetime, timezone
from pathlib import Path

from bridging_hub_module import (
    StorageBaseModule,
    StorageModuleException,
)


class DirectoryAccessException(Exception):
    """
    Exception to indicate problems with the storage location.
    """

    pass


class FileWriteException(Exception):
    """
    Exception to indicate problems while writing a file.
    """

    pass


class FileReadException(Exception):
    """
    Exception to indicate problems while reading a file.
    """

    pass


class DefaultStorageModule(StorageBaseModule):
    """
    Message content can be cached and archived or broken messages identified.
    """

    KEY_CACHE: str = "cache"
    KEY_JUNK: str = "junk"
    KEY_ARCHIVE: str = "archive"

    _cachedir: str = ""

    def _test_dir(self, dir_type: str, sub_dir: str = "") -> Path | None:
        """Check for and prepare storage directory.
        :param dir_type: the kind of directory in question
        :param sub_dir: optional sub directories
        :rtype: bool
        :return: whether directory was configured and ready
        :raise: DirectoryAccessException"""
        if dir_type in self._action_detail and self._action_detail[dir_type]:
            p = Path(self._action_detail[dir_type]) / sub_dir
            if p.is_absolute():
                try:
                    p.mkdir(exist_ok=True, parents=True)
                except Exception as e:
                    raise DirectoryAccessException(
                        f"""Request for directory '{dir_type}'/{sub_dir} \
failed: {e}"""
                    )
            else:
                raise DirectoryAccessException(
                    f"""Please configure an absolute path for {dir_type} \
storage."""
                )
            return p
        else:
            return None

    def _name_file(
        self,
        dir: Path | str,
        message_id: str,
        message_content: dict[str, str],
    ) -> Path:
        """Create a file name and return its path.
        :param dir: the files directory context
        :param message_id: the message key name
        :param message_content: the content of the message
        :rtype: Path
        :return: the path of the file"""
        t = message_content[
            self._custom_name[DefaultStorageModule.KEY_TIMESTAMP_NAME]
        ]
        return Path(dir) / f"{t}_{message_id}.json"

    def _write_files(
        self,
        message: dict[str, dict[str, str]],
        directory: Path | str,
        status: str,
    ) -> dict[str, dict[str, str]]:
        """Write file to disk.
        :param message: the data set to store
        :param directory: the folder to write to
        :rtype: dict[str, dict[str, str]]
        :return: the data written to disk
        :raise: FileWriteException"""
        m: dict[str, dict[str, str]] = {}
        if directory:
            for k, v in message.items():
                try:
                    v[
                        self._custom_name[
                            DefaultStorageModule.KEY_BH_STATUS_NAME
                        ]
                    ] = status
                    p = self._name_file(directory, k, v)
                    with open(p, "w") as f:
                        json.dump(v, f)
                    m[k] = v
                except Exception as e:
                    raise FileWriteException(
                        f"""Unable to write file for '{k}' to '{directory}', \
due to: {e}"""
                    )
        return m

    def write_cache(
        self, message: dict[str, dict[str, str]]
    ) -> dict[str, dict[str, str]]:
        """Remember message content between in- and output.
        :param message: the data set to store
        :rtype: dict[str, dict[str, str]]
        :return: the data written to disk
        :raise: StorageModuleException"""
        try:
            d = self._test_dir(DefaultStorageModule.KEY_CACHE)
            if d:
                return self._write_files(message, d, "cached")
            else:
                return {}
        except (
            DirectoryAccessException,
            FileWriteException,
        ) as e:
            raise StorageModuleException(
                f"Failed to write to cache, due to: {e}"
            )

    def _find_cached(self) -> dict[str, list[Path]]:
        """Find messages hanging between in- and output.
        :rtype: dict[str, list[Path]]
        :return: A list of file paths per data entry
        :raise: DirectoryAccessException"""
        l: dict[str, list[Path]] = {}
        try:
            d = self._test_dir(DefaultStorageModule.KEY_CACHE)
            if d:
                for k in self._data[DefaultStorageModule.KEY_DATA_VALUE_MAP]:
                    l[k] = []
                for k in self._data[DefaultStorageModule.KEY_DATA_VALUE_MAP]:
                    l[k] += Path(d).glob(f"*_{k}.json")
            return l
        except Exception as e:
            raise DirectoryAccessException(
                f"Blocked from searching cache: {e}"
            )

    def read_cache(self) -> dict[str, dict[str, str]]:
        """Look up first message content hanging between in- and output.
        :rtype: dict[str, dict[str, str]]
        :return: the oldest message from cache
        :raise: StorageModuleException"""
        m: dict[str, dict[str, str]] = {}
        try:
            c = self._find_cached()
            for k in c:
                with open(c[k][0], "r") as f:
                    m[k] = json.load(f)
            return m
        except Exception as e:
            raise StorageModuleException(f"Could not read cache: {e}")

    def clean_cache(
        self, message: dict[str, dict[str, str]]
    ) -> dict[str, dict[str, str]]:
        """Clean up the files remembered between in- and output.
        :param message: the data items to go through
        :rtype: dict[str, dict[str, str]]
        :return: the messages removed from cache
        :raise: StorageModuleException"""
        m: dict[str, dict[str, str]] = {}
        try:
            d = self._test_dir(DefaultStorageModule.KEY_CACHE)
            if d:
                for k, v in message.items():
                    f = self._name_file(d, k, v)
                    f.unlink()
                    m[k] = v
            return m
        except Exception as e:
            raise StorageModuleException(f"Unable to clean cache: {e}")

    def store(
        self, message: dict[str, dict[str, str]]
    ) -> dict[str, dict[str, str]]:
        """Remember message content after output.
        :param message: the sent data to be compared to the cache
        :rtype: dict[str, dict[str, str]]
        :return: the messages processed
        :raise: StorageModuleException"""
        m: dict[str, dict[str, str]] = {}
        d = datetime.now(timezone.utc).date().strftime("%Y/%m/%d")

        try:
            cache_path = self._test_dir(DefaultStorageModule.KEY_CACHE)
            archive_path = self._test_dir(DefaultStorageModule.KEY_ARCHIVE, d)
            junk_path = self._test_dir(DefaultStorageModule.KEY_JUNK)
        except DirectoryAccessException as e:
            raise StorageModuleException(f"A storage path issue occured: {e}")

        # if only one of archive or junk path is set, use that for both
        # as we have the status marked in the data anyway..
        if archive_path:
            if not junk_path:
                junk_path = archive_path
        else:
            if junk_path:
                archive_path = junk_path
            else:
                # as both are empty, just return
                return m

        # define a separate map for failed messages to handle later
        mfailed: dict[str, dict[str, str]] = {}
        if cache_path:
            for k, v in message.items():
                if (
                    v[
                        self._custom_name[
                            DefaultStorageModule.KEY_BH_STATUS_NAME
                        ]
                    ]
                    == "failed"
                ):
                    mfailed[k] = v
                else:
                    # only remember the good ones..
                    m[k] = v
            try:
                _ = self._write_files(mfailed, junk_path, "broken")
                _ = self.clean_cache(mfailed)
                m = self._write_files(m, archive_path, "done")
                _ = self.clean_cache(m)
            except (FileWriteException, StorageModuleException) as e:
                raise StorageModuleException(
                    f'Died while "moving" cached files to archive: {e}'
                )
        else:
            # as cache path is not set, write output

            for k, v in message.items():
                if (
                    v[
                        self._custom_name[
                            DefaultStorageModule.KEY_BH_STATUS_NAME
                        ]
                    ]
                    == "failed"
                ):
                    mfailed[k] = v
                else:
                    # only remember the good ones..
                    m[k] = v

            try:
                _ = self._write_files(mfailed, junk_path, "broken")
                m = self._write_files(m, archive_path, "done")
            except FileWriteException as e:
                raise StorageModuleException(
                    f"Died while writing uncached files to archive: {e}"
                )
        return m
