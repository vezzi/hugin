
import os
import re
from hugin.trello_utils import TrelloUtils

class RunMonitor(object):
    
    def __init__(self, config):
        self.trello = TrelloUtils(config)
        self.trello_board = self.trello.get_board(config.get("trello",{}).get("run_tracking_board"))
        assert self.trello_board is not None, "Could not locate run tracking board in Trello"
        self.dump_folders = [d.strip() for d in config.get("run_folders","").split(",")]
        
    def list_runs(self):
        """Get a list of folders matching the run folder pattern"""
        pattern = r'(\d{6})_([SNM]+\d+)_\d+_([AB])([A-Z0-9\-]+)'
        runs = []
        for dump_folder in self.dump_folders:
            for fname in os.listdir(dump_folder):
                m = re.match(pattern, fname)
                if m is not None:
                    run = {'name': fname,
                           'path': os.path.join(dump_folder,fname),
                           'date': m.group(1),
                           'instrument': m.group(2),
                           'position': m.group(3),
                           'flowcell_id': m.group(4)}
                    runs.append(run)
        return runs

    def update_trello_board(self):
        """Update the Trello board based on the contents of the run folder
        """
        runs = self.list_runs()
        read1_list_name = "First read"
        read1_list = self.trello.add_list(self.trello_board,read1_list_name)
        for run in runs:
            self.trello.add_card(read1_list, run['name'])
