import argparse
import os
import yaml

from hugin.flowcell_monitor import FlowcellMonitor

CONFIG = {}
DEFAULT_CONFIG = os.path.join(os.environ['HOME'], '.hugin/config.yaml')

def monitor_flowcells():
    parser = argparse.ArgumentParser(description="A script that will monitor specified run folders and update a Trello board as the status of runs change")
    parser.add_argument('--config', default=DEFAULT_CONFIG, action='store', help="Config file with e.g. Trello credentials and options")
    args = parser.parse_args()

    assert os.path.exists(args.config), "Could not locate config file {}".format(args.config)

    with open(args.config) as config:
        CONFIG.update(yaml.load(config) or {})


    flowcell_monitor = FlowcellMonitor(CONFIG)
    flowcell_monitor.update_trello_board()


if __name__ == "__main__":
    monitor_flowcells()


