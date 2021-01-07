''' Eventsflow CLI Module
'''
import os
import sys
import logging
import argparse

from eventsflow.flow.process import Flow

from eventsflow.utils import load_extra_vars
from eventsflow.version import version

logger = logging.getLogger(__name__)


class CLI:
    ''' Eventsflow CLI
    '''
    def __init__(self):

        parser = argparse.ArgumentParser()

        parser.add_argument('-v', '--version',
                            action='version',
                            version='eventsflow-v%s' % version)
        parser.add_argument('-l', '--log-level',
                            default='INFO',
                            help='Log level: DEBUG, INFO, WARNING, ERROR, CRITICAL')
        parser.add_argument('--libs',
                            action='append',
                            help='The path to the directory with modules in zip format')
        parser.add_argument('--lib',
                            action='append',
                            help='The path to the module in zip format')
        parser.add_argument('--flow',
                            dest='flow_config_path',
                            required=True,
                            help='The path to flow configuration')
        parser.add_argument('--vars', dest="extra_vars",
                            action="append",
                            help="set additional variables as JSON, " +
                            "if filename prepend with @. Support YAML/JSON file",
                            default=[])

        args = parser.parse_args()

        logging.basicConfig(level=args.log_level,
                            format="%(asctime)s.%(msecs)03d (%(name)s) [%(levelname)s] %(message)s",
                            datefmt='%Y-%m-%dT%H:%M:%S')

        if not args.flow_config_path:
            logger.error('The path to flow configuration shall be specified')
            sys.exit(1)

        # The path to flow configuration
        self.flow_config_path = args.flow_config_path

        # add zip modules in directory(-ies)
        if args.libs:
            for libs_path in args.libs:
                for path in [p for p in os.listdir(libs_path) if p.endswith('.zip')]:
                    sys.path.append(os.path.join(libs_path, path))

        # add zip modules from specific location
        if args.lib:
            for lib_path in args.lib:
                sys.path.append(lib_path)

        # extra variables, passed via `--vars` cli argument
        self.extra_vars = load_extra_vars(args.extra_vars)

    def run(self):
        ''' run topology processing
        '''
        try:
            flow = Flow(self.flow_config_path, self.extra_vars)
        except IOError as err:
            logger.info(err)
            sys.exit(1)

        if not flow.queues or not flow.workers:
            logger.error('Incorrect flow format: no queue or worker definition(-s)')
            sys.exit(1)

        try:
            flow.run()
        except KeyboardInterrupt:
            logger.info('The processing was interrupted by user')

    @staticmethod
    def launch_new_instance():
        ''' launch new flow instance
        '''
        CLI().run()


if __name__ == '__main__':

    CLI.launch_new_instance()
