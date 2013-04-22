
import os
import re
import csv
import glob
import datetime
import scilifelab.illumina as illumina
from scilifelab.illumina.hiseq import HiSeqSampleSheet
from scilifelab.bcbio.qc import RunInfoParser
from hugin.trello_utils import TrelloUtils

FIRSTREAD = "First read"
INDEXREAD = "Index read"
SECONDREAD = "Second read"
PROCESSING = "Processing"
UPPMAX = "Uppmax"
STALLED = "Stalled - check status"
    
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
        
        ss = HiSeqSampleSheet(ssheet)
        projects = list(set([s['SampleProject'] for s in ss]))
        return projects
    
    def get_run_info(self, run):
        """Parse the RunInfo.xml file into a dict"""
        with open(os.path.join(run['path'],'RunInfo.xml')) as fh:
            rip = RunInfoParser()
            runinfo = rip.parse(fh)
        return runinfo
    
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
        
        # Check for stalled flowcells
        started_pattern = "*_processing_started.txt"
        completed_pattern = "*_processing_completed.txt"
        started_flags = glob.glob(os.path.join(run['path'],started_pattern))
        completed_flags = glob.glob(os.path.join(run['path'],completed_pattern))
        for flag in started_flags:
            if flag.replace("_started.txt","_completed.txt") in completed_flags:
                continue
            started = self.get_timestamp(flag)
            duration = datetime.datetime.utcnow() - started
            # If the processing step has been ongoing for more than 8 hours, put it in the STALLED list
            if duration.total_seconds() > 8*60*60: 
                return STALLED
            
        # Get the base mask to compare with
        reads = []
        for read in self.get_run_info(run).get('Reads',[]):
            if read.get('IsIndexedRead','N') == 'Y':
                reads.append('I')
            else:
                reads.append('N')
               
        n = len([r for r in reads if r == 'N']) 
        if last == len(reads):
            if (n == 1 and os.path.exists(os.path.join(run['path'],'first_read_processing_completed.txt'))) or \
                (n == 2 and os.path.exists(os.path.join(run['path'],'second_read_processing_completed.txt'))):
                return UPPMAX
            return PROCESSING
        if reads[last] == 'I':
            return INDEXREAD
        if len([reads[i] for i in range(last) if reads[i] == 'N']) == 0:
            return FIRSTREAD
        return SECONDREAD
        
    def update_trello_board(self):
        """Update the Trello board based on the contents of the run folder
        """
        runs = self.list_runs()
        for run in runs:
            print("Adding run {}".format(run['name']))
            lst = self.get_status_list(run)
            lst = self.trello.add_list(self.trello_board,lst)
            card = self.trello.get_card_on_board(self.trello_board,run['name'])
            metadata = self.get_run_metadata(run)
            if card is not None:
                card.set_closed(False)
                card.change_list(lst.id)
                card.fetch()
                current = self.parse_description(card.description)
                current.update(metadata)
                card.set_description(self.create_description(current))
            else:
                card = self.trello.add_card(lst, run['name'])
                projects = self.get_run_projects(run)
                card.set_description(self.create_description(metadata))
    
    def parse_description(self, description):
        metadata = {}
        rows = [r.strip() for r in description.split("-")]
        for row in rows:
            s = row.split(":")
            if len(s) > 1:
                metadata[s[0]] = s[1].split(",")
            elif len(s) > 0 and len(s[0]) > 0:
                metadata[s[0]] = ""
        return metadata
    
    def create_description(self, metadata):
        rows = []
        for key in sorted(metadata.keys()):
            value = metadata[key]
            if type(value) is list:
                value = ",".join(value)
            if len(value) > 0:
                rows.append("{}:{}".format(key,value))
            else:
                rows.append(key)
        return "- {}".format("\n- ".join(rows))
            
    def get_run_metadata(self, run):
        metadata = {}
        metadata['projects'] = self.get_run_projects(run)         
        return metadata
    
    def get_timestamp(self, logfile):
        
        TIMEFORMAT = "%Y-%m-%d %H:%M:%S.%fZ"
        timestamp = ""
        if not os.path.exists(logfile):
            return timestamp
        
        with open(logfile) as fh:
            for line in fh:
                try:
                    timestamp = datetime.datetime.strptime(line.strip(), TIMEFORMAT)
                except ValueError:
                    pass 
        
        return timestamp
