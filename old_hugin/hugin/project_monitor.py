import os
import glob
import datetime
import re

from old_hugin.hugin import Monitor

RUN_PROCESS_STEPS = ["bcbb analysis started",
                     "bcbb analysis completed"
                     ]

BEST_PRACTICE_ANALYSIS_STEPS = ["data from sample runs merged",
                                 "best practice analysis started",
                                 "best practice analysis completed",
                                 "best practice analysis results delivered to customer inbox",
                                 "best practice analysis report generated",
                                 "best practice analysis report copied to customer inbox",
                                 "best practice analysis delivery marked in Genomics Project List"
                                 ]

PROJECT_COMPLETION_STEPS = ["all raw data delivered",
                            "best practice analysis delivered",
                            "project finished"]

SEQUENCING_IN_PROGRESS = "Sequencing"
BCBB_ANALYSIS_IN_PROGRESS = "bcbb analysis"
BP_AND_DELIVERY_IN_PROGRESS = "Best practice and delivery"
PROJECT_DELIVERED = "Finished and delivered"
PROJECT_REMOVED = "Finished and removed"
STALLED = "Check status"

DAYS_TO_KEEP_CARD = 14
DAYS_TO_KEEP_DATA = 14
DAYS_FOR_DELIVERY = 7
DAYS_FOR_ANALYSIS = 2
HOURS_BCBB_INACTIVE = 3

class ProjectMonitor(Monitor):
    
    def __init__(self, config):
        super(ProjectMonitor, self).__init__(config)
        self.trello_board = self.trello.get_board(config.get("trello",{}).get("project_tracking_board",None),True)
        self.trello_board_archive = self.trello.get_board(config.get("trello",{}).get("project_tracking_board_archive",None),True)
        assert self.trello_board is not None, "Could not locate project tracking board in Trello"
        self.archive_folders = [d.strip() for d in config.get("archive_folders","").split(",")]
        self.analysis_folders = [d.strip() for d in config.get("analysis_folders","").split(",")]
        self.samplesheet_folders = [d.strip() for d in config.get("samplesheet_folders","").split(",")]
    
    def get_status_due(self, project):
        
        prjtree = self.get_project_analysis_tree(project)
        if len(prjtree.get('samples',[])) > 0:
            
            # If all samples have been removed, the card should be on the PROJECT_REMOVED list
            if all([s.get('removed') is not None for s in prjtree['samples']]):
                status = PROJECT_REMOVED
                due = max([s['removed'] for s in prjtree['samples']]) + datetime.timedelta(days=DAYS_TO_KEEP_CARD)
                return status, due
            
            # If all samples have been delivered, the card should be on the PROJECT_DELIVERED list
            if all([s.get('delivered') is not None for s in prjtree['samples']]):
                status = PROJECT_DELIVERED
                due = max([s['delivered'] for s in prjtree['samples']]) + datetime.timedelta(days=DAYS_TO_KEEP_DATA)
                return status, due
            
            # If all samples have been analyzed, the card should be on the BP_AND_DELIVERY_IN_PROGRESS list
            if all([fc.get('project_summary') is not None for s in prjtree['samples'] for fc in s['flowcells']]):
                status = BP_AND_DELIVERY_IN_PROGRESS
                due = max([datetime.datetime.fromtimestamp(os.path.getmtime(fc['project_summary'])) for s in prjtree['samples'] for fc in s['flowcells']]) + datetime.timedelta(days=DAYS_FOR_DELIVERY) 
                return status, due
            
            # If all samples have started analyzed, the card should be in the BCBB_ANALYSIS_IN_PROGRESS list
            if all([fc.get('logfile') is not None and datetime.datetime.fromtimestamp(os.path.getmtime(fc['logfile'])) + datetime.timedelta(hours=HOURS_BCBB_INACTIVE) > datetime.datetime.utcnow() for s in prjtree['samples'] for fc in s['flowcells'] if not fc.get('project_summary')]):
                status = BCBB_ANALYSIS_IN_PROGRESS
                due = max([datetime.datetime.fromtimestamp(os.path.getctime(fc['logfile'])) for s in prjtree['samples'] for fc in s['flowcells']]) + datetime.timedelta(days=DAYS_FOR_ANALYSIS) 
                return status, due
            
        status = STALLED
        due = datetime.datetime.utcnow()
        return status, due

    def update_trello_board(self):
        cards = self.list_trello_cards([BCBB_ANALYSIS_IN_PROGRESS,BP_AND_DELIVERY_IN_PROGRESS,PROJECT_DELIVERED,STALLED])
        updated = False
        for project, card in cards.items():
            status, due = self.get_status_due(project)
            
            # If the assigned due time has passed, the card will be put on the stalled list
            if due < datetime.datetime.utcnow() and status not in [PROJECT_DELIVERED, PROJECT_REMOVED]:
                status = STALLED
            
            was_moved = self.trello.change_list(card, status)
            self.set_due(card,due)
            
            # If the card was moved to the STALLED list, send a notification                
            if status == STALLED and was_moved:
                users = [self.trello.client.get_member(mid) for mid in card.member_ids]
                self.send_status_notification(project,status,users)
            
            updated = updated or was_moved
        
        # Lastly, sort the cards in the lists
        if updated:
            for lst in self.trello_board.all_lists():
                self.trello.sort_cards_on_list(lst,key=self._by_last_name)
            
    def archive_cards(self):
        """Archive cards that are finished and older than the limit for keeping them on the board
        """
        if not self.trello_board_archive:
            print("No archive board specified in config")
            return
        
        cards = self.list_trello_cards([PROJECT_REMOVED])
        archived = False
        for card in cards.values():
            # If we are not ready to archive, continue to the next project
            card.fetch()
            due = datetime.datetime.strptime(card.due[0:10],'%Y-%m-%d')
            if due < datetime.datetime.utcnow():
                archive_list_name = due.strftime("%b %Y")
                print("Archiving card {} to list {}, project removed on {}".format(card.name,archive_list_name,str(due)))
                self.trello.change_list(card,archive_list_name,board_id=self.trello_board_archive.id)
                archived = True
        
        if archived:
            self.trello.sort_lists_on_board(self.trello_board_archive, key=self._chronologically)
            for lst in self.trello_board_archive.all_lists():
                self.trello.sort_cards_on_list(lst,key=self._by_last_name)
                    
    def add_project_card(self, project, status=SEQUENCING_IN_PROGRESS):
        """Add a project card"""
        
        card = self.trello.get_card_on_board(self.trello_board, project)
        lst = self.trello.add_list(self.trello_board, status)
        if card is None:
            desc = self.get_project_metadata(project)
            card = self.trello.add_card(lst,project,desc)
            card.add_checklist("Project", PROJECT_COMPLETION_STEPS)
            card.add_checklist("Best practice analysis", BEST_PRACTICE_ANALYSIS_STEPS)
            self.trello.sort_cards_on_list(lst,key=self._by_last_name)
        
        card.set_closed(False)
        card.change_list(lst.id)
        return card
    
    def add_run_to_project(self, project, run):
        """Add a run to a project card
        """
        
        card = self.add_project_card(project)
        # Fetch the checklists on this card
        card.fetch() 
        if run['short_name'] not in [chklst.name for chklst in card.checklists]:
            card.add_checklist(run['short_name'], RUN_PROCESS_STEPS)
        
        # Make sure to uncheck any incompatible completed events
        for chklst in card.checklists:
            if chklst.name in ["Project","Best practice analysis"]:
                for item in chklst.items:
                    chklst.set_checklist_item(item.get('name',''),False)

    def get_run_status(self, run):
        """Check if all projects and samples in a run has been transferred to the analysis folder
        """
        ssheet = self.get_run_samplesheet(run)
        if ssheet is None:
            print("Could not locate samplesheet for run {}".format(run['name']))
            return False
        
        for sample_data in ssheet:
            if not self.get_sample_analysis_folder(sample_data['SampleProject'].replace("__","."),
                                                   sample_data['SampleID'],
                                                   "_".join([run['date'],"{}{}".format(run['position'],run['flowcell_id'])])):
                return False
        
        return True

    def update_run_status(self):
        """Update the status of runs on the run tracking board"""
        
        # Create a RunMonitor object to update the run tracking board
        from old_hugin.hugin import RunMonitor
        rm = RunMonitor(self.config)
        
        rm.run_folders = self.archive_folders
        rm.samplesheet_folders = []
        
        # Loop over the runs and check whether all samples and projects have been transferred to the 
        # analysis folder
        for run in rm.list_runs():
            print("Checking run {}".format(run['name']))
            if self.get_run_status(run):
                rm.set_run_completed(run)
                # Loop over the projects in the run and move them to the bcbb list
                # Note that this can cause the card to be moved back to sequencing by the run_monitor in case
                # the project is sequenced on multiple flowcells. Has no good solution at this moment.
                for project in run.get('projects',[]):
                     self.add_project_card(project,BCBB_ANALYSIS_IN_PROGRESS)
        
    def set_card_checklist_item(self, card, chklist_name, item_name, state):
        """Mark the bcbb analysis as started for a project and run"""
        try:
            [chklist] = [c for c in card.checklists if c.name == chklist_name]
            
        except ValueError:
            return None
        
    def get_project_analysis_tree(self, project):
        """Get a data structure representing the folder with diagnostic files
        """
        path = self.get_sample_analysis_folder(project=project,sample="",run_id="")
        tree = {"name": project,
                "path": path,
                "samples": []}
        for spath in self._list_folders(r'.*',path):
            sample = {'name': os.path.basename(spath),
                      'path': spath,
                      'flowcells': []}
            
            for key, fname in [('delivered','FINISHED_AND_DELIVERED'), 
                               ('removed','FINISHED_AND_REMOVED')]:
                fpath = os.path.join(spath,fname)
                if os.path.exists(fpath):
                    sample[key] = self.get_timestamp(fpath)
                
            for fpath in self._list_folders(r'\d{6}_\S+',spath):
                fc = {'name': os.path.basename(fpath),
                      'path': fpath}
                fc['fastq'] = glob.glob(os.path.join(fpath,"*.fastq*"))
                summary = os.path.join(fpath,'project-summary.csv')
                if os.path.exists(summary):
                    fc['project_summary'] = summary
                for lname in ["{}-drmaa.err".format(sample['name']),
                              "{}-bcbb.log".format(sample['name'])]:
                    lfile = os.path.join(fpath,lname)
                    if os.path.exists(lfile):
                        fc['logfile'] = lfile
                sample["flowcells"].append(fc)
                
            tree["samples"].append(sample)
            
        return tree
        
    def get_sample_analysis_folder(self, project, sample, run_id):
        sample_dir = os.path.join(project,sample,run_id)
        for analysis_folder in self.analysis_folders:
            path = os.path.join(analysis_folder,sample_dir)
            if os.path.exists(path):
                return path
        return None
          
    def get_project_metadata(self, project):
        return ""

    def _list_folders(self, pattern, path):
        folders = []
        if not path or not os.path.exists(path):
            return folders
        for fname in os.listdir(path):
            m = re.match(pattern,fname)
            fpath = os.path.join(path,fname)
            if not (m and os.path.exists(fpath) and os.path.isdir(fpath)):
                continue
            folders.append(fpath)
        return folders
        
    def list_flowcells(self, path):
        """Get a list of the flowcells in a sample folder"""
        pattern = r'(\d{6})_([AB]?)([A-Z0-9\-]+)'
        fcs = []
        for path in self._list_folders(pattern,path):
            m = re.match(pattern,os.path.basename(path))
            if not m or len(m.groups()) != 3:
                continue
            try:
                datetime.datetime.strptime(m.group(1),"%y%m%d")
            except ValueError:
                continue
            
            fc = {'path': path,
                  'name': os.path.basename(path),
                  'date': m.group(1),
                  'position': m.group(2),
                  'flowcell_id': m.group(3)}
            fcs.append(fc)
        return fcs

    def list_samples(self, path):
        """Get a list of the samples in a project folder"""
        pattern = r'.*'
        samples = []
        for path in self._list_folders(pattern,path):
            sample = {'path': path,
                      'name': os.path.basename(path),
                      'flowcells': self.list_flowcells(path)}
            if len(sample['flowcells']) == 0:
                continue
            samples.append(sample)
        return samples
            
    def list_projects(self):
        """Get a list of the projects in the analysis folder"""
        
        pattern = r'[A-Za-z\._]+\d{2}_\d{2,}'
        projects = []
        for analysis_folder in self.analysis_folders:
            for path in self._list_folders(pattern,analysis_folder):
                project = {'path': path,
                           'name': os.path.basename(path),
                           'samples': self.list_samples(path)
                           }
                projects.append(project)
        return projects
    
    def send_status_notification(self, project, status, users=[]):
        """Send an email notification that a project has been moved to a list
        """
        subject = "[hugin]: The project {} needs attention".format(project)
        msg = "The project {} has been moved to the '{}' list on the Trello board "\
              "'{}' and may need your attention.".format(project,
                                                         status,
                                                         self.trello_board.name)
        self.send_notification(subject,msg,users)
    