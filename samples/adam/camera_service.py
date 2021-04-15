import os.path as osp
import eventlet
eventlet.monkey_patch()

from threading import Lock
from mqsrv.logger import get_logger, set_logger_level
from pycamera_hikmvs.base import enumerate_cameras_hikmvs
from .exc import *

DFLT_CAM_ATTRS = {
    'pixel_format': 'RGB8_Packed',
    'exposure_auto': 0,
    'exposure_time': 20000,
    'gain': 17,
}

DFLT_PLC_PARAMS = {
    'rack': 0,
    'slot': 1,
    'port': 102,
}

class CameraService:
    def __init__(self, name='camera_service', cam_attrs={}, plc_params={},
                 save_pattern='/home/vision/data/adam/workspace/images/{mode}/{time}_{cam}.jpg',
                 log_level='INFO',
                 with_log=True,
                 stop_timeout=1,
                 interval=0.1,
                 max_read_tries=10):

        save_dir = osp.dirname(save_pattern)
        osp.makedirs(save_dir.format(mode='top'), exist_ok=True)
        osp.makedirs(save_dir.format(mode='bot'), exist_ok=True)

        self.with_log = with_log
        self.cam_attrs = {**DFLT_CAM_ATTRS, **cam_attrs}
        self.cams = {}
        self.camsys = None
        self.logger = get_logger(name)
        set_logger_level(self, log_level)
        self.cur_frame = {}
        self.is_running = False
        self.stop_timeout = stop_timeout
        self.interval = interval
        self.cur_read_tries = 0
        self.max_read_tries = max_read_tries

        self.plc_params = {**DFLT_PLC_PARAMS, **plc_params}
        self.plc_conn = None
        self.plc = None

        self.command = False

    def camera_init(self):
        self.dev_infos = enumerate_cameras_hikmvs()
        if len(self.dev_infos) < 2:
            raise CameraNotFound(f'dev_nr({len(dev_infos)} < 2')

        self.cams = {k: HikCameraGreen(v, with_log=self.with_log)
                     for k, v in self.dev_infos.items()}
        self.camsys = HikCameraSystemGreen(self.cams)

        for cam in self.cams.values():
            for key, val in self.cam_attrs.items():
                if key == 'pixel_format':
                    val = getattr(pb, f'PixelType_Gvsp_{val}')
                cam.set_attr(key, val)

        return True

    def camera_open(self):
        if not self.camsys:
            details = 'not initialized'
            self.logger.error(details)
            raise CameraNotFound(details)

        if self.camsys.is_opened:
            self.logger.info("already opened")
            return True

        for k, cam in self.cams.items():
            try:
                cam.open()
            except:
                details = "open error"
                self.logger.error(details)
                raise CameraOpenError(details, data={'which': k})

            try:
                cam.set_trigger_mode(mode='SOFTWARE')
            except Exception as e:
                details = "set_trigger_mode error"
                self.logger.error(details)
                raise CameraAttrError(details, data={'which': k})

        self.camsys.is_opened = True
        return True

    def camera_close(self):
        if not self.camsys:
            self.logger.error("not initialized")
            return True

        if not self.camsys.is_opened:
            self.logger.info("already closed")
            return True

        try:
            self.camsys.close()
        except Exception as e:
            self.logger.error("close failed!")
            return True

        return True

    def camera_start(self):
        assert self.camsys and self.camsys.is_opened
        if self.camsys.is_started:
            self.logger.info("already started")
            return True

        for k, cam in self.cams.items():
            try:
                cam.start()
            except:
                details = "start error"
                self.logger.error(details)
                raise CameraStartError(details, data={'which': k})

        self.camsys.is_started = True
        return True

    def camera_stop(self):
        assert self.camsys and self.camsys.is_opened
        if not self.camsys.is_started:
            self.logger.info("already stopped")
            return True

        try:
            self.camsys.stop()
        except Exception:
            self.logger.errror("stop error")
            return True

        return True

    def _camera_read(self):
        assert self.is_running
        self.camsys.trigger()
        plc_data = self._get_plc_data()
        if not plc_data:
            return

        img_dic = self.camsys.read()
        if not img_dic or len(img_dic) < 2:
            self.cur_read_tries += 1
            if self.cur_read_tries > self.max_read_tries:
                details = f'read exceed max_read_tries(self.max_read_tries)'
                self.logger.error(details)
                raise CameraReadError(details)
            return

        self.cur_read_tries = 0

        # publish event

        # save
        # out = {'plc': plc_data, 'image': {'top_img_path': , 'bot_img_path': , 'timestamp': , }}
        return out

    def get_current_frame(self):
        self.is_running = True
        return self.cur_frame

    def open(self):
        self.camera_init()
        self.camera_open()

    def close():
        self.camera_close()

    def start():
        self.camera_start()
        self.cur_frame = {}
        self.stop_evt = event.Event()
        self.cmd_evt = event.Event()
        self.has_send_stop_evt = False
        self.cur_read_tries = 0
        self.is_running = True

        self.command = ''

    def stop():
        self.is_running = False
        self.stop_evt.wait(self.stop_timeout)
        self.camera_stop()

    def command(self, cmd):
        if cmd == 'capture':
            self.command = cmd
            return True

        self.cmd_evt = event.Event()
        self.command = cmd
        data = self.cmd_evt.wait()
        self.cmd_evt = None
        return data

    def loop_camera(self):
        while True:
            if not self.is_running and not self.has_send_stop_evt:
                self.stop_evt.send()
                self.has_send_stop_evt = True

            if not self.is_running:
                sleep(self.interval)
                self.prev_is_running = self.is_running
                continue

            if self.command == 'capture':
                ret = self._camera_read()
                if ret:
                    self.cur_frame = ret

            elif self.command = 'check_too_light':
                # too light function
                # ret = check_too_light()
                self.cmd_evt.send(ret)

            elif self.command == 'check_too_dark':
                pass

    def _get_plc_data(self):
        p = self.plc_params
        lock = Lock()
        out = {}
        try:
            plc_conn = Snap7PLCConnector(
                p['ip'], p['rack'], p['slot'], p['port'], lock)
            plc = AdamPLC(self.plc_conn)

            with AdamPLCSem(plc):
                out.update({
                    'rotunda_angle': adam_plc.get_rotunda_angle(),
                    'elongation': adam_plc.get_elongation(),
                    'cabin_angle': adam_plc.get_cabin_angle(),
                    'lifting_height': adam_plc.get_lifting_height(),
                    'dx_left': adam_plc.get_dx_left(),
                    'dx_right': adam_plc.get_dx_right(),
                })
        except:
            self.logger.error("PLC data retrieve error")
            return {}

        return out

def main():
    evt_q = 'adam_event_queue'
    obj = CameraService(evt_q)

    eventlet.spawn_n(obj.loop_camera)

    server = make_server(
        rpc_routing_key='adam_rpc_queue',
        # event_routing_keys=[evt_q],
    )

    server.register_rpc(obj.start, "open")
    server.register_rpc(obj.stop, "close")
    server.register_rpc(obj.start, "start")
    server.register_rpc(obj.stop, "stop")
    server.register_rpc(obj.stop, "get_current_frame")

    set_logger_level(server, 'debug')
    print ("controller started")
    run_server(server)

if __name__ == '__main__':
    main()
