import os
import re
import csv
import glob
import datetime
from scilifelab.illumina.hiseq import HiSeqSampleSheet
from scilifelab.bcbio.qc import RunInfoParser, RunParametersParser
from hugin.trello_utils import TrelloUtils
import smtplib
from email.mime.text import MIMEText
import socket

SENDER = "hugin@{}".format(socket.gethostname())
  
class Monitor(object):
    
    def __init__(self, config):
        self.trello = TrelloUtils(config)
        self.config = config

    def list_runs(self):
        """Get a list of folders matching the run folder pattern"""
        pattern = r'(\d{6})_([SNMD]+\d+)_\d+_([AB]?)([A-Z0-9\-]+)'
        runs = []
        for dump_folder in self.run_folders:
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
                           'flowcell_id': m.group(4),
                           'short_name': "{}_{}{}".format(m.group(1),m.group(3),m.group(4))}
                    run['technology'] = 'MiSeq' if self.is_miseq_run(run) else 'HiSeq'
                    run['run_info'] = self.get_run_info(run)
                    run['run_parameters'] = self.get_run_parameters(run)
                    run['samplesheet'] = self.get_run_samplesheet(run)
                    run['projects'] = self.get_run_projects(run)
                    # Don't track MiSeq runs that are not production, qc or applications
                    if self.is_miseq_run(run) and len([d for d in self.get_samplesheet_descriptions(run) if d.lowercase() in ["qc","production","applications"]]) == 0:
                        continue
                    runs.append(run)
        return runs

    def is_miseq_run(self, run):
        """Determine whether this is a MiSeq run, from the flowcell-id format
        """
        return not run['flowcell_id'].endswith("XX")

    def get_samplesheet_descriptions(self, run):
        """Return the set of descriptions in the samplesheet"""
        return list(set([s['Description'] for s in ss])) if ss else []

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
        
        ss = run['samplesheet']
        projects = list(set([s['SampleProject'].replace("__",".") for s in ss])) if ss else []
        return projects
           
    def get_run_project_samples(self, run, project):
        """Locate and parse the samplesheet to extract samples for a project in the run"""
        
        ss = run['samplesheet']
        samples = list(set([s['SampleID'].replace("__",".") for s in ss if s['SampleProject'].replace("__",".") == project])) if ss else []
        return samples
           
    def send_notification(self, subject, msg, users=[]):
        """Send an email notification that a run has been moved to a list
        """
        addresses = self.config.get("email",{})
        recipients = []
        for user in users:
            try:
                recipients.extend(addresses[user].split(","))
            except:
                pass
        if len(recipients) == 0:
            recipients = self.config.get("email",{}).get("default","").split(",")
        if len(recipients) == 0:
            return
        
        msg = MIMEText("{}\nSent from {}".format(msg,
                                                 socket.gethostname()))
        msg['Subject'] = subject
        msg['From'] = SENDER
        msg['To'] = ",".join(recipients)
        s = smtplib.SMTP(self.config.get("email",{}).get("smtp_host","localhost"))
        s.sendmail(SENDER, recipients, msg.as_string())
        s.quit()   
  
    def set_description(self, card, description={}, merge=False):
        """Update the description on the card if necessary. Assumes description is supplied
        as a dict with key: value pairs that will be displayed as a list on the description.
        If merge=True, the current description will be merged with the supplied description
        before updating. Merging is done so that the description of the card overwrites the
        supplied description
        Returns True if the description was updated, False otherwise
        """
        if card is None:
            return False
        
        # Get the current description and check if it is different from the new
        current = self.description_to_dict(card.description)
        # If merging, update the description with the current
        description.update(current)
        if cmp(current, description) == 0:
            return False
        
        # Convert the description dict to a string that will be formatted as a list
        description = self.dict_to_description(description)
        card.set_description(description)
        return True
    
    def set_due(self, card, due):
        """Set or update the due date on the card if necessary"""
        
        try:
            old_due = datetime.datetime.strptime(card.due,"%Y-%m-%dT%X.%fZ")
            # Skip updating due time if the time difference is less than one minute
            if abs((due - old_due).total_seconds()) < 60:
                return
        except:
            pass 
            
        card.set_due(due) 
    
    def description_to_dict(self, description):
        metadata = {}
        rows = [r.strip() for r in description.split("-")]
        for row in rows:
            s = [s.strip() for s in row.split(":")]
            if len(s) > 1:
                metadata[s[0]] = s[1].split(",")
            elif len(s) > 0 and len(s[0]) > 0:
                metadata[s[0]] = ""
        return metadata
    
    def dict_to_description(self, metadata):
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
