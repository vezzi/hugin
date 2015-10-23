import argparse
import os

import yaml
from old_hugin.gdocs_updater import GDocsUpdater


def updater(config):
    gdu = GDocsUpdater(config)
    #pprint.pprint(gdu.reshape_run_info(gdu.coming_runs(),gdu.get_skiplist()))
    #pprint.pprint(gdu.gdocs_finished_runs())
    gdu.update_gdocs()

def main():
    parser = argparse.ArgumentParser(description="A script that will fetch the runs in progress and if needed update the google docs checklist")
    parser.add_argument('config', action='store', help="Config file with e.g. Trello and GDocs credentials and options")
    args = parser.parse_args()

    assert os.path.exists(args.config), "Could not locate config file {}".format(args.config)
    with open(args.config) as fh:
        config = yaml.load(fh)
    
    updater(config)
    
if __name__ == "__main__":
    main()
    