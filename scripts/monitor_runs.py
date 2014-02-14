
import argparse
import yaml
import os
from hugin.run_monitor import RunMonitor

def monitor(config,check):
	rm = RunMonitor(config)
	if check:
		rm.check_missing_description()
	else:
		rm.update_trello_board()
		rm.update_trello_project_board()
		rm.archive_cards()

def main():
	parser = argparse.ArgumentParser(description="A script that will monitor specified run folders and update a Trello board as the status of runs change")
	parser.add_argument('config', action='store', help="Config file with e.g. Trello credentials and options")
	parser.add_argument('--check_descrip', action='store_true', help="ONLY UPDATES, it checks for missing description for all cards and tries to update it.")
	args = parser.parse_args()

	assert os.path.exists(args.config), "Could not locate config file {}".format(args.config)
	with open(args.config) as fh:
		config = yaml.load(fh)
	monitor(config,args.check_descrip)
    
if __name__ == "__main__":
	main()

