import os
import os.path as osp
from easydict import EasyDict as edict
import yaml
from jinja2 import Environment, BaseLoader, FileSystemLoader
import toml

def load_config(cfg_f):
    if cfg_f.endswith('.yaml'):
        with open(cfg_f) as fle:
            config = yaml.unsafe_load(fle)

    elif cfg_f.endswith('.toml'):
        cfg_d = osp.dirname(osp.abspath(cfg_f))
        loader = FileSystemLoader(cfg_d)
        with open(cfg_f) as fle:
            s = Environment(loader=loader).from_string(fle.read()).render(env=os.environ)
        config = toml.loads(s)

    else:
        raise

    return edict(config)
