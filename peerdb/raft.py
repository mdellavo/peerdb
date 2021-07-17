import collections
from enum import IntEnum

class NodeType:
    FOLLOWER = 0
    CANDIDATE = 1
    LEADER = 2


class LogEntry:
    pass


class LogWatcher:
    def on_log_appended(self, name, sql, params):
        pass


class LogManager:
    def __init__(self):
        self.logs = collections.defaultdict(list)
        self.watchers = []

    def append_log(self, name, sql, params):
        self.logs[name].append((sql, params))
