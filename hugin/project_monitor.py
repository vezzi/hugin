
import os
import glob
import datetime
from hugin.monitor import Monitor
    
RUN_PROCESS_STEPS = ["bcbb analysis started",
                     "bcbb analysis completed",
                     "sample qualities evaluated",
                     "sample species confirmed by blast",
                     "customer uppnex id verified",
                     "raw data delivered to customer inbox",
                     "sample status note generated",
                     "project status note generated",
                     "delivery notes copied to customer inbox",
                     "delivery email sent to customer and application specialists",
                     "delivery marked in Genomics Project List"
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
                            "all raw data delivered marked in Genomics Project List",
                            "best practice analysis delivered",
                            "project finished"]

SEQUENCING_IN_PROGRESS = "Sequencing"
BCBB_ANALYSIS_IN_PROGRESS = "bcbb analysis"
BP_AND_DELIVERY_IN_PROGRESS = "Best practice and delivery"
PROJECT_FINISHED = "Finished"
STALLED = "Check status"

# The number of seconds we allow the bcbb logfile to be inactive before we flag the project as stalled
BCBB_LOGFILE_INACTIVE = 60*60*3

class ProjectMonitor(Monitor):
    
    def __init__(self, config):
        super(ProjectMonitor, self).__init__(config)
        self.trello_board = self.trello.get_board(config.get("trello",{}).get("project_tracking_board",None),True)
        assert self.trello_board is not None, "Could not locate project tracking board in Trello"
        self.archive_folders = [d.strip() for d in config.get("archive_folders","").split(",")]
        self.analysis_folders = [d.strip() for d in config.get("analysis_folders","").split(",")]
        
    def add_project_card(self, project, status=SEQUENCING_IN_PROGRESS):
        """Add a project card"""
        
        card = self.trello.get_card_on_board(self.trello_board, project)
        lst = self.trello.add_list(self.trello_board, status)
        if card is None:
            desc = self.get_project_metadata(project)
            card = self.trello.add_card(lst,project,desc)
            card.add_checklist("Project", PROJECT_COMPLETION_STEPS)
            card.add_checklist("Best practice analysis", BEST_PRACTICE_ANALYSIS_STEPS)
        
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
        from hugin.run_monitor import RunMonitor
        rm = RunMonitor(self.config)
        
        self.run_folders = self.archive_folders
        self.samplesheet_folders = []
        runs = self.list_runs()
        
        # Loop over the runs and check whether all samples and projects have been transferred to the 
        # analysis folder
        for run in runs:
            print("Checking run {}".format(run['name']))
            run_complete = True
            for project in self.get_run_projects(run):
                project_complete = True
                for sample in self.get_run_project_samples(run,project):
                    if get_sample_analysis_folder(project, sample, run['short_name']) is None:
                        project_complete = False
                if project_complete:
                    self.set_run_project_started(run, project)
                else:
                    run_complete = False
                    
            if run_complete:
                rm.set_run_completed(run)
        
    def set_run_project_started(self, run, project):
        pass
      
    def get_sample_analysis_folder(self, project, sample, run_id):
        sample_dir = os.path.join(project,sample,run_id)
        for analysis_folder in self.analysis_folders:
            path = os.path.join(analysis_folder,sample_dir)
            if os.path.exists(path):
                return path
        return None
          
    def get_project_metadata(self, project):
        return ""

    def list_samples(self, path):
        """Get a list of the samples in a project folder"""
        pattern = r'.*'

    def list_projects(self):
        """Get a list of the projects in the analysis folder"""
        
        pattern = r'[A-Za-z\._]+\d{2}_\d{2,}'
        projects = []
        for analysis_folder in self.analysis_folders:
            for fname in os.listdir(analysis_folder):
                m = re.match(pattern, fname)
                path = os.path.join(analysis_folder,fname)
                if not (m and os.path.isdir(path)):
                    continue
                project = {'path': path,
                           'name': fname,
                           'samples': self.list_samples(path)
                           }
                project['flowcells'] = list(set([self.list_flowcells(sample) for sample in project['samples']]))

