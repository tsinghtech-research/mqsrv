from mqsrv.exc import BaseException

class AdamError(BaseException):
    def __init__(code, msg='', details='', data={}):
        if details:
            data = {**data, 'details': details}
        super().__init__(code, msg, data)

ERR_CAMERA_NOT_FOUND = -10000
class CameraNotFound(AdamError):
    def __init__(details='', data={}):
        super().__init__(ERR_CAMERA_NOT_FOUND, 'camera not found', details, data)

ERR_CAMERA_OPEN = -10001
class CameraOpenError(AdamError):
    def __init__(details='', data={}):
        super().__init__(ERR_CAMERA_OPEN, 'camera open error', detauls, data)

ERR_CAMERA_ATTR = -10002
class CameraAttrError(AdamError):
    def __init__(details='', data={}):
        super().__init__(ERR_CAMERA_ATTR, 'camera attribute error', detauls, data)

ERR_CAMERA_START = -10003
class CameraStartError(AdamError):
    def __init__(details='', data={}):
        super().__init__(ERR_CAMERA_START, 'camera start error', detauls, data)

ERR_CAMERA_READ = -10004
class CameraReadError(AdamError):
    def __init__(details='', data={}):
        super().__init__(ERR_CAMERA_READ, 'camera read error', detauls, data)
