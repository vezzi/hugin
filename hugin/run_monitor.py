
import os
import re
import csv
import glob
import scilifelab.illumina as illumina
from hugin.trello_utils import TrelloUtils

FIRSTREAD = "First read"
INDEXREAD = "Index read"
SECONDREAD = "Second read"
PROCESSING = "Processing"
FINISHED = "Finished"
    
class RunMonitor(object):
    
    def __init__(self, config):
        self.trello = TrelloUtils(config)
        self.trello_board = self.trello.get_board(config.get("trello",{}).get("run_tracking_board"))
        assert self.trello_board is not None, "Could not locate run tracking board in Trello"
        self.dump_folders = [d.strip() for d in config.get("run_folders","").split(",")]
        self.samplesheet_folders = [d.strip() for d in config.get("samplesheet_folders","").split(",")]
        
    def list_runs(self):
        """Get a list of folders matching the run folder pattern"""
        pattern = r'(\d{6})_([SNM]+\d+)_\d+_([AB])([A-Z0-9\-]+)'
        runs = []
        for dump_folder in self.dump_folders:
            for fname in os.listdir(dump_folder):
                if not os.path.isdir(os.path.join(dump_folder,fname)):
                    continue
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

    def get_run_projects(self, run):
        """Locate and parse the samplesheet to extract projects in the run"""
        fname = "{}.csv".format(run.get("flowcell_id","SampleSheet"))
        ssheet = None
        for folder in self.samplesheet_folders + [run.get("path","")]:
            f = os.path.join(folder,fname)
            if os.path.exists(f):
                ssheet = f
                break
        if ssheet is None:
            return []
        
        ss = illumina.hiseq.HiSeqSampleSheet(ssheet)
        projects = list(set([s['SampleProject'] for s in ss]))
        return projects
    
    def _get_run_info_reads(self, run):
        """Silly method to be able to use mock for tests"""
        runobj = illumina.IlluminaRun(run['path'])
        return runobj.run_info.get('Reads',[])
    
    def get_status_list(self, run):
        """Determine the status list where the run belongs"""
        
        # Get the highest file flag
        pattern = os.path.join(run['path'],'Basecalling_Netcopy_complete_Read*.txt')
        rpat = r'Basecalling_Netcopy_complete_Read(\d).txt'
        last = 0
        for flag in glob.glob(pattern):
            m = re.match(rpat,os.path.basename(flag))
            read = int(m.group(1))
            if read > last:
                last = read
        
        # Get the base mask to compare with
        reads = []
        for read in self._get_run_info_reads(run):
            if read.get('IsIndexedRead','N') == 'Y':
                reads.append('I')
            else:
                reads.append('N')
                
        if last == len(reads):
            return PROCESSING
        if reads[last] == 'I':
            return INDEXREAD
        n=0
        for i in range(last+1):
            if reads[i] == 'N':
                n += 1
        
        if n == 1:
            return FIRSTREAD
        return SECONDREAD
        
    def update_trello_board(self):
        """Update the Trello board based on the contents of the run folder
        """
        runs = self.list_runs()
        for run in runs:
            lst = self.get_status_list(run)
            lst = self.trello.add_list(self.trello_board,lst)
            card = self.trello.get_card_on_board(self.trello_board,run['name'])
            if card is not None:
                card.change_list(lst.id)
            else:
                card = self.trello.add_card(lst, run['name'])
                projects = self.get_run_projects(run)
                card.set_description("- {}".format("\n- ".join(projects)))
            
