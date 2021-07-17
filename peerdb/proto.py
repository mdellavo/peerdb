from enum import IntEnum, auto

class PeerMessageTypes(IntEnum):
    HELLO = auto()
    HEARTBEAT = auto()
    APPEND_LOG = auto()


class ClientMessageTypes(IntEnum):
    HELLO = auto()
    GOODBYE = auto()
    PING = auto()
    CURSOR = auto()
    CLOSE = auto()
    EXECUTE = auto()
    EXECUTEMANY = auto()
    FETCHMANY = auto()


class ResponseTypes(IntEnum):
    OK = 1
    ERR = -1
