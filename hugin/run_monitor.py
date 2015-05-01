
import os
import re
import csv
import glob
import datetime
import platform

from hugin.monitor import Monitor
from hugin.Runs.HiSeq_Runs import HiSeq_Run
from hugin.Runs.HiSeqX_Runs import HiSeqX_Run


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

    def archive_cards(self):
        """Archive cards that are finished or aborted and older than the limit for keeping them on the board
        """
        if not self.trello_board_archive:
            print("No archive board specified in config")
            return

        td = datetime.timedelta(seconds=DAYS_TO_KEEP*24*60*60)
        completed_cards = self.list_trello_cards([ABORTED,ARCHIVED])
        archived = False
        for card in completed_cards.values():
            date = self.description_to_dict(card.description).get("Date")
            if not date:
                continue
            try:
                started = datetime.datetime.strptime(date[0],"%y%m%d")
            except ValueError:
                continue
            if datetime.datetime.utcnow() - started >  td:
                archive_list_name = started.strftime("%b %Y")
                print("Archiving card {} to list {}, run started on {}".format(card.name,archive_list_name,date[0]))
                card.fetch()
                self.trello.change_list(card,archive_list_name,board_id=self.trello_board_archive.id)
                archived = True
        # Sort the lists on the board and then the cards in the list
        if archived:
            self.trello.sort_lists_on_board(self.trello_board_archive, key=self._chronologically)
            for lst in self.trello_board_archive.all_lists():
                self.trello.sort_cards_on_list(lst)
    
    
    
    #needs some changes
    def set_run_completed(self, run):
        """Set the status of the run to completed"""
        card = self.trello.get_card_on_board(self.trello_board,run['name'])

        # Skip if the card is not on the board or if it has been closed
        if card is None or card.closed:
            return

        lst = self.trello.add_list(self.trello_board,COMPLETED)
        if card.list_id != lst.id:
            card.change_list(lst.id)
            self.trello.sort_cards_on_list(lst)

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

    def get_status_due(self, run):
        """Get the current status and the maximum expected due time for a run"""

        reads = run['run_info'].get('Reads',[])
        status_flags = ["First_Base_Report.htm"] + ["Basecalling_Netcopy_complete_Read{}.txt".format(str(i+1)) for i in range(len(reads))]
        started_processing_flags = ["initial_processing_started.txt", "first_read_processing_started.txt", "second_read_processing_started.txt"]
        completed_processing_flags = [f.replace("started","completed") for f in started_processing_flags]

        # Get number of non-index reads
        treads = len([r for r in reads if r.get("IsIndexedRead","N") == "N"])

        # Get the highest file flag
        last_event_flag = run['path']
        for f in status_flags:
            f = os.path.join(run['path'],f)
            if os.path.exists(f):
                last_event_flag = f

        try:
            flag_index = status_flags.index(os.path.basename(last_event_flag))
        except ValueError:
            flag_index = -1

        # flag_index will be equal to the read number that has been finished
        # thus we are currently sequencing read flag_index+1

        # first base report has not yet appeared
        if flag_index < 0:
            status = FIRSTREAD
            due = self.get_due_datetime(run, "Pre-seq" if not self.is_miseq_run(run) else status, started=datetime.datetime.fromtimestamp(os.path.getmtime(last_event_flag)))
        else:
            # sequencing has finished
            if flag_index == len(reads):
                complete_flag = os.path.join(run['path'],completed_processing_flags[treads])
                start_flag = os.path.join(run['path'],started_processing_flags[treads])
                if os.path.exists(complete_flag):
                    status = UPPMAX
                    last_event_flag = complete_flag
                elif os.path.exists(start_flag):
                    status = PROCESSING
                    last_event_flag = start_flag
                else:
                    status = PROCESSING
            # in the middle of sequencing
            else:
                # sequencing an index read
                if [r for r in reads if int(r["Number"]) == (flag_index + 1)][0].get("IsIndexedRead","N") == "Y":
                    status = INDEXREAD
                # sequencing a non-index read
                else:
                    # Count the number of non-index reads that have already been sequenced
                    if len([r for r in reads if int(r["Number"]) <= flag_index and r.get("IsIndexedRead","N") == "N"]) == 0:
                        status = FIRSTREAD
                    else:
                        status = SECONDREAD

            # get the expected due date
            due = self.get_due_datetime(run, status, started=datetime.datetime.fromtimestamp(os.path.getmtime(last_event_flag)))

        return status, due


    def _sequencer_type(self, run_id):
        """Returns the sequencer type based on the run folder name
        contains ST-       --> is a Xten
        ends with XX       --> is a HiSeq
        contians 000000000 --> is a MiSeq
        """
        pattern = r'(\d{6})_([ST-]*\w+\d+)_\d+_([AB]?)([A-Z0-9\-]+)'
        m = re.match(pattern, run_id)
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
                if run_type is "HiSeq" or run_type is "MiSeq":
                    runs.append(HiSeq_Run(os.path.join(dump_folder,fname), self.samplesheet_folders ))
    
        #Step 2: upadte runs found in run folder accordingly to their status
        for run in runs:
            print("Processing run {}".format(run.name))
            #check if this is a new run or if it is already in the trallo board
            run.get_run_status()
        
        
        #step 3: parse all runs in Trello and update their status (with the expcetion of...)
        
        for run in runs:
            print("Processing run {}".format(run['name']))

            card = self.trello.get_card_on_board(self.trello_board,run['name'])
            if card is not None and card.list_id in skip_list_ids:
                print("run {} is in {}, skipping".format(run['name'],card.list_id))
                continue

            status, due = self.get_status_due(run)

            # If due time has passed and didn't find all status files, set status to stalled.
            # if status == UPPMAX means that the processing has finished, and should have more
            # priority than due date
            if due < datetime.datetime.utcnow() and status != UPPMAX:
                status = STALLED

            if card is None:
                card = self.trello.add_card(self.trello.add_list(self.trello_board,status), run['name'])
                was_moved= True
            else:
                was_moved = self.trello.change_list(card, status, skip_list_ids)
                card.set_closed(False)

            # Gather the information on the run and update the description on the card as necessary
            metadata = self.get_run_metadata(run)
            self.set_description(card,metadata,True)
            self.set_due(card,due)

            # If the card was moved to the STALLED list, send a notification
            if status == STALLED and was_moved:
                users = [self.trello.client.get_member(mid) for mid in card.member_ids]
                self.send_status_notification(run,status,users)

            updated = updated or was_moved

        # Lastly, sort the cards in the lists
        if updated:
            for lst in self.trello_board.all_lists():
                self.trello.sort_cards_on_list(lst)


    def send_status_notification(self, run, status, users=[]):
        """Send an email notification that a run has been moved to a list
        """
        subject = "[hugin]: The run {} needs attention".format(run['name'])
        msg = "The run {} has been moved to the '{}' list on the Trello board "\
              "'{}' and may need your attention.".format(run['name'],
                                                         status,
                                                         self.trello_board.name)
        self.send_notification(subject,msg,users)

    def get_run_metadata(self, run):
        metadata = {}
        metadata['Projects'] = run['projects']
        metadata['Setup'] = self.get_run_setup(run)
        metadata['Flowcell'] = run['run_info'].get('Flowcell','NA')
        instrument = run['run_info'].get('Instrument','NA')
        if self.instruments:
            instrument_data = self.instruments.get(instrument, '')
            if instrument_data:
                #instrument has a comma separated value: name,source_nas
                name, nas = instrument_data.split(',')
                instrument += " ({name}), comming from {nas}".format(name=name, nas=nas)
        metadata['Instrument'] = instrument
        metadata['Date'] = run['run_info'].get('Date','NA')
        metadata['Run mode'] = run['run_parameters'].get('RunMode','HighOutput' if not self.is_miseq_run(run) else 'MiSeq')
        metadata['Processed in'] = platform.node()
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

    def check_missing_description(self):
        run_dict = {}
        runs = self.list_runs()
        for r in runs:
            run_dict[r['name']] = r
        all_cards_in_board =  self.trello_board.all_cards()
        for card in all_cards_in_board:
            card.fetch()
            if card.name in run_dict:
                run = run_dict[card.name]
            else:
                continue
            descrip = self.description_to_dict(card.description)
            empty = [k for k in descrip.keys() if re.search(r'^$|^NA$',''.join(descrip[k]))]
            if len(empty) > 0 or not card.description:
                metadata = self.get_run_metadata(run)
                self.set_description(card,metadata,False)

    def check_finish_status(self):
        """Get the runs in given list and check if the transfer is dont to UPPMAX"""
        from hugin.project_monitor import ProjectMonitor
        pm = ProjectMonitor(self.config)
        pm.samplesheet_folders = []
        uppmax_list = self.trello.get_list(self.trello_board,UPPMAX)
        runs = []
        # Gathering required keys for the purpose of this method
        for card in uppmax_list.list_cards():
            run_name = card.name
            miseq = True if re.search(r'-',run_name.split('_')[-1]) else False
            runs.append({
                'name' : run_name,
                'path' : os.path.join(self.config.get('archive_folders'),run_name),
                'date' : run_name.split("_")[0],
                'position' : run_name.split("_")[-1][0] if not miseq else '',
                'flowcell_id' : run_name.split("_")[-1][1:] if not miseq else run_name.split("_")[-1]})
        for run in runs:
            if pm.get_run_status(run):
                self.set_run_completed(run)
