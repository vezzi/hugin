
import os
import re
import csv
import glob
import datetime
import scilifelab.illumina as illumina
from scilifelab.illumina.hiseq import HiSeqSampleSheet
from scilifelab.bcbio.qc import RunInfoParser, RunParametersParser
from hugin.trello_utils import TrelloUtils
import smtplib
from email.mime.text import MIMEText
import socket

FIRSTREAD = "First read"
INDEXREAD = "Index read"
SECONDREAD = "Second read"
PROCESSING = "Processing"
UPPMAX = "Sent to Uppmax"
COMPLETED = "Handed over"
STALLED = "Check status"
ABORTED = "Aborted"

SENDER = "hugin@{}".format(socket.gethostname())

PER_CYCLE_MINUTES = {'RapidRun': 10,
                     'HighOutput': 90}
    
class RunMonitor(object):
    
    def __init__(self, config):
        self.trello = TrelloUtils(config)
        self.trello_board = self.trello.get_board(config.get("trello",{}).get("run_tracking_board",None),True)
        assert self.trello_board is not None, "Could not locate run tracking board in Trello"
        self.dump_folders = [d.strip() for d in config.get("run_folders","").split(",")]
        self.samplesheet_folders = [d.strip() for d in config.get("samplesheet_folders","").split(",")]
        self.config = config
        
    def list_runs(self):
        """Get a list of folders matching the run folder pattern"""
        pattern = r'(\d{6})_([SNMD]+\d+)_\d+_([AB])([A-Z0-9\-]+)'
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
                    run['run_info'] = self.get_run_info(run)
                    run['run_parameters'] = self.get_run_parameters(run)
                    runs.append(run)
        return runs

    def set_run_completed(self, run):
        """Set the status of the run to completed"""
        card = self.trello.get_card_on_board(self.trello_board,run['name'])
        
        # Skip if the card is not on the board or if it has been closed
        if card is None:
            return
        card.fetch()
        if card.closed:
            return
        
        lst = self.trello.add_list(self.trello_board,COMPLETED)
        if card.list_id != lst.id:
            card.change_list(lst.id)

    def get_run_samplesheet(self, run):
        """Locate and parse the samplesheet for a run"""
        
        fname = "{}.csv".format(run.get("flowcell_id","SampleSheet"))
        ssheet = None
        for folder in self.samplesheet_folders + [run.get("path","")]:
            f = os.path.join(folder,fname)
            if os.path.exists(f):
                ssheet = f
                break
        if ssheet is None:
            return None
        
        return HiSeqSampleSheet(ssheet)
        
    def get_run_projects(self, run):
        """Locate and parse the samplesheet to extract projects in the run"""
        
        ss = self.get_run_samplesheet(run)
        projects = list(set([s['SampleProject'].replace("__",".") for s in ss]))
        return projects
    
    def get_run_info(self, run):
        """Parse the RunInfo.xml file into a dict"""
        f = os.path.join(run['path'],'RunInfo.xml')
        if not os.path.exists(f):
            return {}
        with open(f) as fh:
            rip = RunInfoParser()
            runinfo = rip.parse(fh)
        return runinfo
    
    def get_run_parameters(self, run):
        """Parse the runParameters.xml file into a dict"""
        f = os.path.join(run['path'],'runParameters.xml')
        if not os.path.exists(f):
            return {}
        with open(f) as fh:
            rpp = RunParametersParser()
            runparameters = rpp.parse(fh)
        return runparameters
    
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
        for read in run['run_info'].get('Reads',[]):
            if read.get('IsIndexedRead','N') == 'Y':
                reads.append(['I', int(read.get('NumCycles','0')), int(read.get('Number','0'))])
            else:
                reads.append(['N', int(read.get('NumCycles','0')), int(read.get('Number','0'))])
        n = len([r for r in reads if r[0] == 'N'])
        
        # Check for stalled flowcells
        
        ## Stalled in processing
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
            
        ## Stalled in sequencing
        last_event_flag = os.path.join(run['path'],"Basecalling_Netcopy_complete_Read{}.txt".format(str(last)))
        if last == 0:
            last_event_flag = os.path.join(run['path'],"First_Base_Report.htm")
        
        # If the flag does not exist, use the directory itself
        if not os.path.exists(last_event_flag):
            last_event_flag = run['path']
            # Set last to -1 to indicate that the first read has not yet begun
            last = -1
            
        # Get creation time of the event flag
        event_time = datetime.datetime.fromtimestamp(os.path.getmtime(last_event_flag))
        duration = datetime.datetime.utcnow() - event_time
        # Calculate the expected duration for the step
        max_duration = 0
        if last < len(reads):
            try:
                [cycles] = [r[1] for r in reads if r[2] == last+1]
            except ValueError:
                cycles = 2
                last = 0
            max_duration = cycles * 60 * PER_CYCLE_MINUTES[run['run_parameters'].get('RunMode','HighOutput')]
        else:
            # Check if processing has been completed
            if (n == 1 and os.path.exists(os.path.join(run['path'],'first_read_processing_completed.txt'))) or \
                (n == 2 and os.path.exists(os.path.join(run['path'],'second_read_processing_completed.txt'))):
                return UPPMAX
            if (n == 1 and os.path.exists(os.path.join(run['path'],'first_read_processing_started.txt'))) or \
                (n == 2 and os.path.exists(os.path.join(run['path'],'second_read_processing_started.txt'))):
                return PROCESSING
            # Processing should start once every hour but allow a couple of hours in case many FCs finish at the same time
            # 6 hours should be plenty
            max_duration = 6*60*60
            # Do last-1 to indicate that we are still on the last read since we haven't started processing
            last = last - 1
        
        # This is taking too much time, indicate that we have stalled 
        if duration.total_seconds() > max_duration:
            return STALLED
        
        # Otherwise, indicate the read we are currently sequencing
        if reads[last][0] == 'I':
            return INDEXREAD
        if len([reads[i] for i in range(last) if reads[i][0] == 'N']) == 0:
            return FIRSTREAD
        return SECONDREAD
    
    def send_notification(self, run, status):
        """Send an email notification that a run has been moved to a list
        """
        recipients = self.config.get("email",{}).get("recipients","").split(",")
        if len(recipients) == 0:
            return
        
        msg = MIMEText("The run {} has been moved to the '{}' list on the Trello board "\
                       "'{}' and may need your attention on {}".format(run['name'],
                                                                 status,
                                                                 self.trello_board.name,
                                                                 socket.gethostname()))
        msg['Subject'] = "[hugin]: The run {} needs attention".format(run['name'])
        msg['From'] = SENDER
        msg['To'] = ",".join(recipients)
        s = smtplib.SMTP(self.config.get("email",{}).get("smtp_host","localhost"))
        s.sendmail(SENDER, recipients, msg.as_string())
        s.quit()   
    
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
            old_list_id = ""
            if card is not None:
                card.fetch()
                
                # Skip if the card is in the completed or aborted list
                old_list_id = card.list_id
                if old_list_id == self.trello.get_list_id(self.trello_board,COMPLETED) or \
                    old_list_id == self.trello.get_list_id(self.trello_board,ABORTED):
                    continue
                    
                card.set_closed(False)
                card.change_list(lst.id)
                current = self.parse_description(card.description)
                metadata.update(current)
                if cmp(metadata,current) != 0:
                    card.set_description(self.create_description(metadata))
            else:
                card = self.trello.add_card(lst, run['name'])
                card.set_description(self.create_description(metadata))
                
            # If the card was moved to the STALLED list, send a notification                
            if lst.name == STALLED and old_list_id != lst.id:
                self.send_notification(run,lst.name)
    
    def update_trello_project_board(self):
        """Update the project cards for projects in ongoing runs
        """
        from hugin.project_monitor import ProjectMonitor
        pm = ProjectMonitor(self.config)
        runs = self.list_runs()
        for run in runs:
            projects = self.get_run_projects(run)
            for project in projects:
                print("Adding run {} to project {}".format(run['name'],project))
                pm.add_run_to_project(project,run)
            
    def parse_description(self, description):
        metadata = {}
        rows = [r.strip() for r in description.split("-")]
        for row in rows:
            s = [s.strip() for s in row.split(":")]
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
                rows.append("{}: {}".format(key,value))
            else:
                rows.append(key)
        return "- {}".format("\n- ".join(rows))
            
    def get_run_metadata(self, run):
        metadata = {}
        metadata['Projects'] = self.get_run_projects(run)
        metadata['Setup'] = self.get_run_setup(run) 
        metadata['Flowcell'] = run['run_info'].get('Flowcell','NA')
        metadata['Instrument'] = run['run_info'].get('Instrument','NA')
        metadata['Date'] = run['run_info'].get('Date','NA')      
        metadata['Run mode'] = run['run_parameters'].get('RunMode','HighOutput')  
        return metadata
    
    def get_run_setup(self, run):
        reads = run['run_info'].get('Reads',[])
        read_cycles = [r.get('NumCycles','?') for r in reads if r.get('IsIndexedRead','N') == 'N']
        index_cycles = [r.get('NumCycles','?') for r in reads if r.get('IsIndexedRead','N') == 'Y']
        nreads = len(read_cycles)
        nindex = len(index_cycles)
        
        c = list(set(read_cycles))
        if len(c) == 1:
            setup = "{}x{}".format(str(nreads),c[0])
        else:
            setup = ",".join(c)
        
        return setup

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
