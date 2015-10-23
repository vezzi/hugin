import os
import datetime
import smtplib
from email.mime.text import MIMEText
import socket

from old_hugin.hugin.trello_utils import TrelloUtils

SENDER = "hugin@{}".format(socket.gethostname())
  
class Monitor(object):

    def __init__(self, config):
        self.trello = TrelloUtils(config)
        self.config = config

    def list_trello_cards(self, lists):
        # Loop over the lists and fetch the cards, returning a dictionary keyed with the card title
        cards = {}
        for tlist in lists:
            list_obj = self.trello.get_list(self.trello_board,tlist,True)
            if not list_obj:
                continue
            
            # Loop over the cards in the list
            for card in list_obj.list_cards():
                # Get the description and convert it to a dictionary
                cards[card.name] = card
                
        return cards
    

    #this needs to disappear
    def is_miseq_run(self, run):
        """Determine whether this is a MiSeq run, from the flowcell-id format
            """
        return not run['flowcell_id'].endswith("XX")


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
        if len(recipients) == 0 or all([len(r) == 0 for r in recipients]):
            return
        
        msg = MIMEText("{}\nSent from {}".format(msg,
                                                 socket.gethostname()))
        msg['Subject'] = subject
        msg['From'] = SENDER
        msg['To'] = ",".join(recipients)
        try:
            s = smtplib.SMTP(self.config.get("email",{}).get("smtp_host","localhost"))
            s.sendmail(SENDER, recipients, msg.as_string())
            s.quit()
        except Exception, e:
            print("WARNING: Sending email to {} failed: {}".format(",".join(recipients),str(e)))   
  
    def set_description(self, card, description={}, merge=False):
        """Update the description on the card if necessary. Assumes description is supplied
        as a dict with key: value pairs that will be displayed as a list on the description.
        If merge=True, the current description will be merged with the supplied description
        before updating. Merging is done so that the current(existing) description of the 
        card overwrites the supplied description
        Returns True if the description was updated, False otherwise
        """
        if card is None:
            return False
        
        if merge:
            current = self.description_to_dict(card.description)
            if cmp(current, description) == 0:
                return False
            current.update(description)
            description = current
        
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

    def _days_to_seconds(self, days=0):
        return days*self._hours_to_seconds(24)

    def _hours_to_seconds(self, hours=0):
        return hours*self._minutes_to_seconds(60)

    def _minutes_to_seconds(self, minutes=0):
        return minutes*60
    
    @staticmethod
    def _chronologically(obj):
        try:
            return str(int(datetime.datetime.strptime(obj.name,"%b %Y").strftime("%m")))
        except:
            return obj.name
    @staticmethod
    def _by_last_name(card):
            pcs = card.name.split(".")
            if len(pcs) > 1:
                return "".join(pcs[1:] + [pcs[0]])
            return pcs[0]
        
