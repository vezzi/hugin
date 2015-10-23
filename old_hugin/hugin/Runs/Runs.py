import os
import re
import datetime
import platform

from old_hugin.hugin.parser import CycleTimes

ABORTED        = "Aborted"         # something went wrong in the FC
CHECKSTATUS    = "Check status"    # demultiplex failure
SEQUENCING     = "Sequencing"      # under sequencing
DEMULTIPLEXING = "Demultiplexing"  # under demultiplexing
TRANFERRING    = "Transferring"    # tranferring to HPC resource



PER_CYCLE_MINUTES = {'RapidRun': 12,
                     'HighOutput': 100,
                     'RapidHighOutput': 43,
                     'MiSeq': 6,
                     'HiSeqX': 10}

class Run(object):

    def __init__(self, path_to_run, samplesheet_folders):
        pattern = r'(\d{6})_([ST-]*\w+\d+)_\d+_([AB]?)([A-Z0-9\-]+)'
        (dump_folder, fname) = os.path.split(path_to_run)
    
        m = re.match(pattern, fname)
        self.name        = fname
        self.path        = path_to_run
        self.date        = m.group(1)
        self.instrument  = m.group(2)
        self.position    = m.group(3)
        self.flowcell_id = m.group(4)
        self.short_name  = "{}_{}{}".format(m.group(1),m.group(3),m.group(4))
        self.technology  = self._sequencer_type()
        #get Run Info
        self.run_info    = self.get_run_info()
        #runMode from the runParametes or form the machine type
        self.run_mode    = self.get_run_mode()
        #get the samplesheet
        self.samplesheet = self._get_run_samplesheet(samplesheet_folders)
        #write these wo parts for Xten FCs
        self.projects    = self.get_projects()
        #self.samples     = self.get_samples() #this is needed in the future in case I want to track projects
        self.cycles   = self._get_log_info()
    


    def _get_log_info(self):
    
        """
            Locate and parse the CycleTimes.txt file to get current cycle and expcted time
        """
        CycleTimesFile = os.path.join(self.path, "Logs", "CycleTimes.txt")
        if not os.path.exists(CycleTimesFile):
            return None
        CycleTimes_Obj = CycleTimes(CycleTimesFile)
        return CycleTimes_Obj.cycles


    

    def _get_run_samplesheet(self, samplesheet_folders):
        """
            Locate and parse the samplesheet for a run
        """
        ssheet = None
        for folder in samplesheet_folders:
            f = os.path.join(folder,"{}.csv".format(self.flowcell_id))
            if os.path.exists(f):
                ssheet = f
                break
        if ssheet is None:
            ssheet = os.path.join(self.path, "SampleSheet.csv")
            if not os.path.exists(ssheet):
                return None
        return ssheet


    def _sequencer_type(self):
        raise NotImplementedError("Please Implement this method")

    def get_run_info(self):
        raise NotImplementedError("Please Implement this method")
    
    def get_run_mode(self):
        raise NotImplementedError("Please Implement this method")

    def get_projects(self):
        raise NotImplementedError("Please Implement this method")

    def get_samples(self):
        raise NotImplementedError("Please Implement this method")

    def get_run_status(self):
        """
            return the status of the run, that is the trello card where it needs to be placed
        """
        demux_dir       = self._get_demux_dir()             # if this is not None status is Demultiplexing or Tranferring or CheckStatus
        demux_started   = self._is_demultiplexing_started() # if true status is Demultiplexing or Tranferring or CheckStatus
        demux_done      = self._is_demultiplexing_done()    # if true status needs to be put to Tranferring
        sequencing_done = self._is_sequencing_done()        # if true status is not Sequencing
        ##TRANFERRING --> demultiplexing is done
        if sequencing_done and demux_done:
            return TRANFERRING # run is done, tranfer is ongoing. Onother software/operator is responisble to move the run to nosync
        elif sequencing_done and demux_started and not demux_done:
            #Demultiplexing is ongoing byt need to check if everything is ok looking at the time from sequencing end to now
            sequencing_done_time         = self.get_sequencing_done_time()
            time_from_sequencing         = datetime.datetime.now() - sequencing_done_time
            time_from_sequencing_minutes = divmod(time_from_sequencing.days * 86400 + time_from_sequencing.seconds, 60)[0]
            if time_from_sequencing_minutes > 480:
                return CHECKSTATUS #demultiplexing is taking more than 8 hours, likely it failed
            else:
                return DEMULTIPLEXING
        elif sequencing_done and not demux_started:
            #special case, sequencing is done but the demux is not yet started, card will be updated at next iteration
            return SEQUENCING
        elif not sequencing_done:
            #Sequencing is ongoing but I need to check that everything is ok looking from last cycle to now
            last_cycle_time         = self.get_sequencing_done_time()
            time_from_last_cycle    = datetime.datetime.now() - last_cycle_time
            time_from_last_cycle__minutes = divmod(time_from_last_cycle.days * 86400 + time_from_last_cycle.seconds, 60)[0]
            if time_from_last_cycle__minutes > 60:
                return ABORTED #sequencign stalled
            else:
                return SEQUENCING
        
        


    def _is_sequencing_done(self):
        if os.path.exists(os.path.join(self.path, 'RTAComplete.txt')):
            return True
        else:
            return False

    def _get_demux_dir(self):
        raise NotImplementedError("Please Implement this method")

    def get_run_current_cycle(self):
        if self.cycles is not None:
            return self.cycles[-1][0]

    def get_sequencing_done_time(self):
        if self.cycles is not None:
            return self.cycles[-1][2]



    def get_run_metadata(self, instruments):
        metadata             = {}
        metadata['Projects'] = self.projects
        metadata['Setup']    = self.get_run_setup()
        metadata['Flowcell'] = self.run_info.get('Flowcell','NA')
        instrument           = self.instrument
        instrument_data      = instruments.get(instrument, '')
        if instrument_data:
            #instrument has a comma separated value: name,source_nas
            name, nas = instrument_data.split(',')
            instrument += " ({name}), coming from {nas}".format(name=name, nas=nas)
        metadata['Instrument'] = instrument
        metadata['Date']     =  self.run_info.get('Date','NA')
        metadata['Run mode'] =  self.run_mode
        metadata['Processed in'] = platform.node()
        return metadata

    def get_run_setup(self):
        reads = self.run_info.get('Reads',[])
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



