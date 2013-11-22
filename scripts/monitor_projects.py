
import argparse
import yaml
import os

from hugin.project_monitor import ProjectMonitor

def monitor(config):
    pm = ProjectMonitor(config)
    pm.update_run_status()
    pm.update_trello_board()
    pm.archive_cards()
    
def main():
    parser = argparse.ArgumentParser(description="A script that will monitor specified project folders and update a Trello board as the status of projects change")
    parser.add_argument('config', action='store', help="Config file with e.g. Trello credentials and options")
    args = parser.parse_args()

    assert os.path.exists(args.config), "Could not locate config file {}".format(args.config)
    with open(args.config) as fh:
        config = yaml.load(fh)
    
    monitor(config)
    
if __name__ == "__main__":
    main()
 