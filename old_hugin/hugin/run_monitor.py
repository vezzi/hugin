import os
import re
import datetime

from old_hugin.hugin import Monitor
from old_hugin.hugin import HiSeq_Run
from old_hugin.hugin import HiSeqX_Run

ABORTED        = "Aborted"         # something went wrong in the FC
CHECKSTATUS    = "Check status"    # demultiplex failure
SEQUENCING     = "Sequencing"      # under sequencing
DEMULTIPLEXING = "Demultiplexing"  # under demultiplexing
TRANFERRING    = "Transferring"    # tranferring to HPC resource
NOSYNC         = "Nosync"          # moved to no sync folder
ARCHIVED       = "Archived"        # archived to long term storage


class RunMonitor(Monitor):

    def __init__(self, config):
        super(RunMonitor, self).__init__(config)
        self.trello_board = self.trello.get_board(config.get("trello",{}).get("run_tracking_board",None),True)
        assert self.trello_board is not None, "Could not locate run tracking board in Trello"
        self.run_folders = [d.strip() for d in config.get("run_folders","").split(",")]
        self.samplesheet_folders = [d.strip() for d in config.get("samplesheet_folders","").split(",")]
        self.instruments = config.get("instruments", {})


    def get_due_datetime(self, run, step, started=datetime.datetime.utcnow()):
        """Get the expected due date for a particular sequencing/processing step"""

        # Return None if we are not familiar with the step
        if step not in ["Pre-seq",FIRSTREAD,INDEXREAD,SECONDREAD,PROCESSING,UPPMAX]:
            return None

        # processing step
        if step == PROCESSING:
            max_duration = 8*60*60
        # uploaded to uppmax but not handed over
        elif step == UPPMAX:
            max_duration = 3*60*60
        # sequencing steps
        else:
            # Get the expected length of a cycle in seconds
            cycle_duration = 60 * PER_CYCLE_MINUTES[run['run_parameters'].get('RunMode','HighOutput' if not self.is_miseq_run(run) else 'MiSeq')]
            reads = run['run_info'].get('Reads',[])
            index_cycles = [int(r.get('NumCycles','0')) for r in reads if r.get('IsIndexedRead','N') == 'Y']
            read_cycles = [int(r.get('NumCycles','0')) for r in reads if r.get('IsIndexedRead','N') == 'N']
            if step == "Pre-seq":
                max_duration = 2 * cycle_duration
            elif step == INDEXREAD:
                max_duration = max(index_cycles) * cycle_duration
            elif step == FIRSTREAD:
                max_duration = read_cycles[0] * cycle_duration
            else:
                max_duration = read_cycles[1] * cycle_duration

        return started + datetime.timedelta(seconds=max_duration)


# get the expected due date
#due = self.get_due_datetime(run, status, started=datetime.datetime.fromtimestamp(os.path.getmtime(last_event_flag)))

        return status, due


    def _sequencer_type(self, run_id):
        """Returns the sequencer type based on the run folder name
        contains ST-       --> is a Xten
        ends with XX       --> is a HiSeq
        contians 000000000 --> is a MiSeq
        """
        pattern = r'(\d{6})_([ST-]*\w+\d+)_\d+_([AB]?)([A-Z0-9\-]+)'
        m = re.match(pattern, run_id)
        if m is None:
            return "invalid_folder"
        instrument  = m.group(2)
        flowcell_id = m.group(4)
        if instrument.startswith("ST-"):
            return "HiSeqX"
        if flowcell_id.endswith("XX"):
            return "HiSeq"
        if flowcell_id.startwith("000000000"):
            return "MiSeq"

    def update_trello_board(self):
        """
            Update the Trello board based on the contents of the run folder
            
        """
        updated = False
        
        #Step 1: colllect all runs in run folder
        runs = []
        for dump_folder in self.run_folders:
            for fname in os.listdir(dump_folder):
                if not os.path.isdir(os.path.join(dump_folder,fname)):
                    continue
                #I have a run folder, I need to get the type and create it
                run_type = self._sequencer_type(fname)
                if run_type is "HiSeqX":
                    runs.append(HiSeqX_Run(os.path.join(dump_folder,fname), self.samplesheet_folders ))
                elif run_type is "HiSeq" or run_type is "MiSeq":
                    runs.append(HiSeq_Run(os.path.join(dump_folder,fname), self.samplesheet_folders ))
    
        #Step 2: upadte runs found in run folder accordingly to their status
        run_names = []
        for run in runs:
            print("Processing run {}".format(run.name))
            run_names.append(run.name)
            #check if this is a new run or if it is already in the trallo board
            run_status = run.get_run_status()
            #either this is a new run and I need to insert it in the trello board or I need to update it
            card = self.trello.get_card_on_board(self.trello_board,run.name)
            if card is None:
                #new run seen, need to be created
                card = self.trello.add_card(self.trello.add_list(self.trello_board,run_status), run.name)
                metadata = run.get_run_metadata(self.instruments)
                self.set_description(card,metadata,True)
            else:
                self.trello.change_list(card, run_status)
                card.set_closed(False) # no idea what this does
            #I do not get this, I must set a due data otherwise I cannot fetch the card back!!!!
            #need to implement a proper due data counting
            self.set_due(card, datetime.datetime.now())
            
        
        #step 3: parse all runs in Trello and update their status --> only runs in transerring and nosyn
        import pdb
        pdb.set_trace()
        
        for card in self.trello.get_list(self.trello_board, "Transferring").list_cards():
            print card.name
            if card.name not in run_names:
            #another software or operator has removed this card
                print card.name
                
                
        for cards in self.trello.get_list(self.trello_board, "Nosync").list_cards():
            print card.name
        
        # Lastly, sort the cards in the lists
        #if updated:
        #    for lst in self.trello_board.all_lists():
        #        self.trello.sort_cards_on_list(lst)


