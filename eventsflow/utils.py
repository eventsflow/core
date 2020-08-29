
import os
import json
import yaml
import jinja2
import logging

try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader


logger = logging.getLogger(__name__)


def load_extra_vars(extra_vars):
    ''' reused from https://github.com/ansible/ansible/blob/devel/lib/ansible/utils/vars.py
        and modified according to eventsflow requirements 
    ''' 
    extra_vars_result = {}

    if not extra_vars:
        return extra_vars_result

    for _vars in extra_vars:
        data = None
        if _vars.startswith(u"@"):
            # Argument is a YAML file (JSON is a subset of YAML)
            try:
                with open(_vars[1:], 'r', encoding='utf8') as source:
                    try:
                        data = yaml.load(source, Loader=Loader)
                    except yaml.YAMLError as err:
                        logger.error('{}, {}'.format(err, _vars))
            except FileNotFoundError as err:
                logger.error(err)
        else:
            try:
                data = json.loads(_vars)
            except json.JSONDecodeError as err:
                logger.error('{}, {}'.format(err, _vars))
        
        if data and isinstance(data, dict):
            extra_vars_result.update(data)

    return extra_vars_result


def parse_with_extra_vars(flow_path, extra_vars):
    ''' parse flow with extra vars
    '''
    path = os.path.dirname(flow_path)
    filename = os.path.basename(flow_path)

    template = jinja2.Environment(
                    loader=jinja2.FileSystemLoader(path),
                    undefined=jinja2.StrictUndefined
                ).get_template(filename)
        
    output = None
    try:
        output = template.render(**extra_vars) 
    except jinja2.exceptions.UndefinedError as err:
        logger.error('Topology templating, {}'.format(err))
    # except jinja2.exceptions.TemplateNotFound as err:
    #     logger.error('Topology templating, {}'.format(err))

    return output


def split_worker_uri(uri):
    ''' split worker uri to worker module and class
    '''
    _module, _class = None, None
    if uri:
        _module, _class = uri.rsplit('.', 1)
    return _module, _class

