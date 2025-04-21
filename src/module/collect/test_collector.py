# import logging

from bridging_hub_module import CollectorBaseModule


class TestCollector(CollectorBaseModule):

    def __init__(self):
        self._element = None

    def configure(self, json):
        pass

    def collect(self):
        print("I am collecting something, finally")
