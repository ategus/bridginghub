from bridging_hub_module import StorageBaseModule


class DefaultStorageModule(StorageBaseModule):
    """
    Message content can be cached and archived or broken messages identified.
    """

    def cache(
        self, message: dict[str, dict[str, str]]
    ) -> dict[str, dict[str, str]]:
        """Remember message content between in- and output."""
        return message

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
