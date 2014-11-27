
import argparse
import yaml
import os
from hugin.run_monitor import RunMonitor

def monitor(config,check_descrip,check_finish):
	rm = RunMonitor(config)
	if check_descrip:
		rm.check_missing_description()
    elif check_finish:
        rm.check_finish_status()
	else:
		rm.update_trello_board()
		rm.update_trello_project_board()
		rm.archive_cards()

def main():
	parser = argparse.ArgumentParser(description="A script that will monitor specified run folders and update a Trello board as the status of runs change")
	parser.add_argument('config', action='store', help="Config file with e.g. Trello credentials and options")
	parser.add_argument('--check_descrip', action='store_true', default=False, help="ONLY UPDATES, it checks for missing description for \
                                                                                     all cards and tries to update it.")
	parser.add_argument('--check_finish', action='store_true', default=False, help="Only to check if the processed run are transferred to UPPMAX")
    args = parser.parse_args()

	assert os.path.exists(args.config), "Could not locate config file {}".format(args.config)
	with open(args.config) as fh:
		config = yaml.load(fh)
	monitor(config,args.check_descrip,args.check_finish)

if __name__ == "__main__":
	main()

