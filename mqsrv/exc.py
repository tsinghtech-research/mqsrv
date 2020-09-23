import sys
import tblib
import jsonpickle

def pack_exc(error):
    et, ev, tb = error
    assert isinstance(ev, BaseException)

    tb = tblib.Traceback(tb).to_dict()
    out = ev.asdict()
    out['raw'] = jsonpickle.encode([et, ev, tb])
    return out

def unpack_exc(error):
    et, ev, tb = jsonpickle.decode(error['raw'])
    tb = tblib.Traceback.from_dict(tb).as_traceback()
    return et, ev, tb

class BaseException(Exception):
    def __init__(self, code=-1, msg='', data={}):
        self.code = code
        self.msg = msg
        self.data = data

    def asdict(self):
        return {
            'code': self.code,
            'message': self.msg,
            'data': self.data,
        }

class JsonRpcError(BaseException):
    pass

class MethodNotFound(JsonRpcError):
    def __init__(self, msg='', **kws):
        super().__init__(-32601, msg, **kws)
