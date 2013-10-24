
import os
import hugin.run_monitor as rm
from scilifelab.db.statusdb import ProjectSummaryConnection
from scilifelab.db import ConnectionError
from scilifelab.google.google_docs import SpreadSheet
from scilifelab.google import get_credentials 

# The row where the actual user-entered data starts in the worksheets
ONGOING_HEADER_OFFSET = 5
COMING_HEADER_OFFSET = 2
FINISHED_HEADER_OFFSET = 3

# The GDOcsUpdater class checks the Trello board and fills in runs in "Coming" and "Ongoing"
# in the checklist as necessary 
class GDocsUpdater(rm.RunMonitor):
    
    def __init__(self, config):
        super(GDocsUpdater, self).__init__(config)
            
        # Connect to the Google Docs api
        gdconf = self.config.get("gdocs",{})
        creds = os.path.expanduser(gdconf.get("credentials_file",""))
        assert os.path.exists(creds), "Supplied GDocs credentials file does not exist"
        self.gdcon = SpreadSheet(get_credentials(creds))
        assert self.gdcon, "Could not get a SpreadSheet object, please verify gdocs credentials"
        doc = gdconf.get("qc_checklist",None)
        assert doc, "No QC checklist specified in configuration, please specify"
        ssheet = self.gdcon.get_spreadsheet(doc)
        assert ssheet, "Could not locate QC checklist '{}' on Google Docs. Please make sure it exists".format(doc)
        self.gdcon.ssheet = ssheet
        
        # Get the Ongoing, Finished and Coming worksheets
        self.ongoing = self.gdcon.get_worksheet("Ongoing")
        self.coming = self.gdcon.get_worksheet("Coming")
        self.finished = self.gdcon.get_worksheet("Finished")
        assert self.ongoing and self.coming and self.finished, "Could not get 'Ongoing', 'Finished' and 'Coming' worksheets from '{}'. Please make sure that they exist".format(doc)
        
        # Get a connection to the StatusDB project database
        dbconf = self.config.get("statusdb",{})
        try:
            self.pcon = ProjectSummaryConnection(url=dbconf.get("url","localhost"), 
                                                 username=dbconf.get("user","user"), 
                                                 password=dbconf.get("password","pass"))
        except ConnectionError:
            self.pcon = None
        
        
        
    def _list_runs(self, lists):
        # Loop over the lists and fetch the cards
        runs = {}
        for tlist in lists:
            list_obj = self.trello.get_list(self.trello_board,tlist,True)
            if not list_obj:
                continue
            
            # Loop over the cards in the list
            for card in list_obj.list_cards():
                # Get the description and convert it to a dictionary
                runs[card.name] = self.description_to_dict(card.description)
                
        return runs
    
    def coming_runs(self):
        """Return a dictionary with runs that are currently in process, i.e. not handed over to 
        the processing pipeline on Uppmax. The key in the dictionary is the run id and the values
        is a metadata dictionary
        """
        
        # Runs in these lists are to be considered "coming"
        lists = [rm.FIRSTREAD,
                 rm.INDEXREAD,
                 rm.SECONDREAD,
                 rm.PROCESSING,
                 rm.UPPMAX,
                 rm.STALLED]
        return self._list_runs(lists)
        
        
    def ongoing_runs(self):
        """Return a dictionary with runs that have finished and have been handed over to 
        the processing pipeline on Uppmax. The key in the dictionary is the run id and the values
        is a metadata dictionary
        """
        
        # Runs in these lists are to be considered "coming"
        lists = [rm.COMPLETED]
        return self._list_runs(lists)
        
    def reshape_run_info(self, runs, skiplist=[]):
        """Take the dictionary of runs and convert to a sorted list of lists with elements 
        corresponding to the columns in the checklist"""
        
        run_projects = []
        for id,data in runs.items():
            for project in data.get('Projects',[]):
                if "{}_{}".format(id,project) not in skiplist:
                    application, type = '',''#self.lookup_project(project)
                    run_projects.append([id,project,application,type,'',data.get('Run mode',[''])[0]])

        return run_projects
        
    def lookup_project(self, project):
        """Lookup project application and type in StatusDB"""
        
        application = ""
        type = ""
        if self.pcon:
            pdoc = self.pcon.get_entry(project)
            if pdoc:
                application = str(pdoc.get("application",""))
                type = str(pdoc.get("type",pdoc.get("details",{}).get("type","")))
                
        return application, type
    
    def get_skiplist(self):
        """Get the runs and projects already listed in the GDocs spreadsheet
        """
        
        skiplist = []
        # Get the contents from the finished worksheet
        for run_project in self.gdocs_finished_runs():
            skiplist.append("{}_{}".format(run_project[0],run_project[1]))
    
        return skiplist
    
    def gdocs_coming_runs(self):
        return self._get_gdocs_run_projects(self.coming,COMING_HEADER_OFFSET)
    def gdocs_ongoing_runs(self):
        return self._get_gdocs_run_projects(self.ongoing,ONGOING_HEADER_OFFSET)
    def gdocs_finished_runs(self):
        return self._get_gdocs_run_projects(self.finished,FINISHED_HEADER_OFFSET)
    
    def _get_gdocs_run_projects(self, wsheet, header_offset):
        
        # Get the cell data
        run_projects = {}
        rows = self.gdcon.get_cell_content(wsheet,header_offset,1,0,6)
        for row in rows:
            if len(str(row[0])) == 0:
                continue
            data = [str(r) for r in row]
            key = "{}{}".format(data[0],data[1])
            if key in run_projects:
                continue
            run_projects[key] = data
        
        # Only return unique rows
        return run_projects.values()
        
    def update_gdocs(self):
        
        # Get the coming runs from Trello but Exclude runs that are already in gdocs
        gdocs_finished = self.gdocs_finished_runs()
        gdocs_ongoing = self.gdocs_ongoing_runs()
        gdocs_coming = self.gdocs_coming_runs()
        trello_coming = self.reshape_run_info(self.coming_runs(), ["{}_{}".format(r[0],r[1]) for r in gdocs_finished + gdocs_ongoing + gdocs_coming])
        # Get the ongoing runs from Trello but exclude runs that are already in the finished or ongoing tab
        trello_ongoing = self.reshape_run_info(self.ongoing_runs(), ["{}_{}".format(r[0],r[1]) for r in gdocs_finished + gdocs_ongoing])
        
        # Add each coming run to the next empty row
        for run in trello_coming:
            self.update_empty_row(self.coming,run,COMING_HEADER_OFFSET)
        
        # Move each run from coming if it exists there to the ongoing tab or just add it
        for run in trello_ongoing:
            status = self.run_project_match(run,gdocs_coming)
            if status == 0:
                self.update_empty_row(self.ongoing,run,ONGOING_HEADER_OFFSET)
                continue
            # Find the row index of the run in the coming tab
            row_index = self.gdcon.get_row_index(self.coming,run[0:2],COMING_HEADER_OFFSET)
            # Get the data from the coming tab, add it to an empty row in the ongoing tab and replace it with empty values
            row_data = self.gdcon.get_cell_content(self.coming,row_index,0,row_index,0)
            self.update_empty_row(self.ongoing,row_data[0],ONGOING_HEADER_OFFSET)
            self.gdcon.update_row(self.coming,row_index,["" for i in xrange(len(row_data[0]))])
    
        def last_name(data):
            pcs = data[1].split('.')
            if len(pcs) == 1:
                return pcs[0]
            return "".join(pcs[1:])
    
        # Lastly, update the application and type fields in gdocs if they are empty
        for wsheet, offset in [(self.coming, COMING_HEADER_OFFSET), (self.ongoing, ONGOING_HEADER_OFFSET)]:
            # Print a reader-friendly text to stdout
            print("{}\n{}\n".format(wsheet.title.text,"".join(['-' for i in xrange(len(wsheet.title.text))])))
            for run in sorted(self._get_gdocs_run_projects(wsheet,offset), key=last_name):
                if len(run) < 4:
                    continue
                if run[2] == "" or run[3] == "":
                    app, tp = self.lookup_project(run[1])
                    if run[2] == "":
                        run[2] = app
                    if run[3] == "":
                        run[3] = tp
                row_index = self.gdcon.get_row_index(wsheet,run[0:2],offset)
                self.gdcon.update_row(wsheet,row_index,run[0:4])
                
                print("{} - {}{}".format(run[1],"{} - ".format(run[3]) if len(run[3]) > 0 else "",run[4]))
                print("{}{}\n".format("{}\n".format(run[2]) if len(run[2]) > 0 else "",run[0]))
            
    def update_empty_row(self, wsheet, data, offset, merged=False):
        """Update the next empty row after the specified offset with the supplied data
        """
        updated = False
        # Require two empty rows in succession
        row_index = offset
        r2 = row_index
        while r2-row_index != 1:
            row_index = self.gdcon.get_row_index(wsheet,["" for i in xrange(len(data))],r2)
            # If we're writing a merged row, we need two consecutive empty rows
            if merged:
                r2 = self.gdcon.get_row_index(wsheet,["" for i in xrange(len(data))],row_index+1)
            else:
                r2 = row_index+1
            
        assert row_index > 0, "***ERROR*** No more rows left in spreadsheet"
        updated = self.gdcon.update_row(wsheet,row_index,data)
        # FIXME: do this better.. if the row is merged, write the same data to the second "hidden" row
        if merged:
            self.gdcon.update_row(wsheet,row_index+1,data)
            
        return updated
        
    def run_project_match(self, needle, haystack):
        """Checks if a run and project exist in a list of lists. Determines identity by the two first 
        columns in each list, the third and fourth are checked to determine if they need updating.
        Return 0 for no match, 1 for match that needs updating and 2 for a match that does not need updating
        """
        if len(needle) < 4:
            return 0
        
        for straw in haystack:
            if len(straw) < 4:
                continue
            if needle[0] != straw[0] or needle[1] != straw[1]:
                continue
            if needle[2] != straw[2] or needle[3] != straw[3]:
                return 1
            return 2
        
        return 0
    
          
        
                 
