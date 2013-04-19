
import argparse
import yaml
import os

from hugin.run_monitor import RunMonitor

def monitor(config):
    rm = RunMonitor(config)
    rm.update_trello_board()

def main():
    parser = argparse.ArgumentParser(description="A script that will monitor specified run folders and update a Trello board as the status of runs change")
    parser.add_argument('config', action='store', help="Config file with e.g. Trello credentials and options")
    args = parser.parse_args()

    assert os.path.exists(args.config), "Could not locate config file {}".format(args.config)
    with open(args.config) as fh:
        config = yaml.load(fh)
    
    monitor(config)
    
if __name__ == "__main__":
    main()
    