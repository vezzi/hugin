import os
import socket
import datetime

from flowcell_parser.classes import RunParametersParser, RunInfoParser, CycleTimesParser

from hugin.flowcell_status import FC_STATUSES

CYCLE_DURATION = {
    'RapidRun'          : datetime.timedelta(minutes=12),
    'HighOutput'        : datetime.timedelta(minutes=100),
    'RapidHighOutput'   : datetime.timedelta(minutes=43),
    'MiSeq'             : datetime.timedelta(minutes=6),
    'HiSeqX'            : datetime.timedelta(minutes=10),
}

DURATIONS = {
    'DEMULTIPLEXING'    : datetime.timedelta(hours=4),
    'TRANSFERING'       : datetime.timedelta(hours=12)
}

class Flowcell(object):
    def __init__(self, status):
        self._status = status

        self._id = None
        self._run_parameters = None
        self._run_info = None
        self._cycle_times = None

    @property
    def status(self):
        return self._status

    @property
    def list(self):
        return self.status.status


    @property
    def path(self):
        return self._status.path

    @property
    def id(self):
        if self._id is None:
            self._id =  os.path.basename(self.path)
        return self._id

    @status.setter
    def status(self, value):
        if value in FC_STATUSES.items():
            self.status.status = value

    @property
    def run_info(self):
        if self._run_info is None:
            run_info_path = os.path.join(self.path, 'RunInfo.xml')
            if not os.path.exists(run_info_path):
                raise RuntimeError('RunInfo.xml cannot be found in {}'.format(self.path))

            self._run_info = RunInfoParser(run_info_path).data
        return self._run_info

    @property
    def run_parameters(self):
        if self._run_parameters is None:
            run_parameters_path = os.path.join(self.path, 'runParameters.xml')
            if not os.path.exists(run_parameters_path):
                raise RuntimeError('runParameters.xml cannot be found in {}'.format(self.path))
            self._run_parameters = RunParametersParser(run_parameters_path).data['RunParameters']
        return  self._run_parameters

    @property
    def cycle_time(self):
        raise NotImplementedError('Flowcell.cycle_times must be implemented in {}'.format(self.__class__.__name__))

    @property
    def name(self):
        raise NotImplementedError("@property 'name' must be implemented in subclass {}".format(self.__class__.__name__))

    @classmethod
    def init_flowcell(cls, status):
        flowcell_dir = status.path
        try:
            parser = RunParametersParser(os.path.join(flowcell_dir, 'runParameters.xml'))
            # print rp.data

        except OSError:
            raise RuntimeError("Cannot find the runParameters.xml file at {}. This is quite unexpected.".format(flowcell_dir))
        else:
            try:
                runtype = parser.data['RunParameters']["Setup"]["Flowcell"]
            except KeyError:
                # logger.warn("Parsing runParameters to fecth instrument type, not found Flowcell information in it. Using ApplicaiotnName")
                runtype = parser.data['RunParameters']['Setup'].get("ApplicationName", '')

            # depending on the type of flowcell, return instance of related class
            if "HiSeq X" in runtype:
                return HiseqXFlowcell(status)
            elif "MiSeq" in runtype:
                return MiseqRun(status)
            elif "HiSeq" in runtype or "TruSeq" in runtype:
                return HiseqRun(status)
            else:
                raise RuntimeError("Unrecognized runtype {} of run {}. Someone as likely bought a new sequencer without telling it to the bioinfo team".format(runtype, flowcell_dir))

    def get_formatted_description(self):
        raise NotImplementedError('get_formatted_description() must be implemented in {}'.format(self.__class__.__name__))


class HiseqXFlowcell(Flowcell):
    def __init__(self, status):
        super(HiseqXFlowcell, self).__init__(status)


    @property
    def full_name(self):
        return os.path.basename(self.status.path)

    @property
    def formatted_reads(self):
        # get number of cycles for all reads if read is NOT index
        reads = [read['NumCycles'] for read in self.run_info['Reads'] if read['IsIndexedRead'] != 'Y']

        # if only one read
        if len(reads) == 1:
            return reads[0]
        # if all values are the same
        elif len(set(reads)) == 1:
            return "{}x{}".format(len(reads), reads[0])
        # if there are different values
        else:
            # '/' will separate the values
            return "/".join(reads)

    @property
    def formatted_index(self):
        # get number of cycles for all reads if read IS index
        indices = [read['NumCycles'] for read in self.run_info['Reads'] if read['IsIndexedRead'] == 'Y']

        # if only one index
        if len(indices) == 1:
            return indices[0]
        # if more than one index and all values are the same
        elif len(set(indices)) == 1:
            return "{}x{}".format(len(indices), indices[0])
        # if there are different values
        else:
            return "/".join(read for read in indices)

    @property
    def chemistry(self):
        return self.run_parameters['ChemistryVersion']

    @property
    def run_parameters(self):
        # dangerous
        # call run_parameters from the base class
        return Flowcell.run_parameters.fget(self)['Setup']

    def get_formatted_description(self):
        print self.name
        description = """
    Date: {date}
    Flowcell: {flowcell}
    Instrument: {instrument}
    Preprocessing server: {localhost}
    Lanes: {lanes}
    Tiles: {tiles}
    Reads: {reads}
    Index: {index}
    Chemistry: {chemistry}
        """.format(
                date=self.run_info['Date'],
                flowcell=self.run_info['Flowcell'],
                instrument=self.run_info['Instrument'],
                localhost=socket.gethostname(),
                lanes=self.run_info['FlowcellLayout']['LaneCount'],
                tiles=self.run_info['FlowcellLayout']['TileCount'],
                reads=self.formatted_reads,
                index=self.formatted_index,
                chemistry=self.chemistry,
        )
        return description

    @property
    def name(self):
        # todo: returns the wrong name
        return self.run_info['Flowcell']


    def check_status(self):
        if self.status.status == FC_STATUSES['SEQUENCING']:
            return self._check_sequencing()
        elif self.status.status == FC_STATUSES['DEMULTIPLEXING']:
            return self._check_demultiplexing()
        elif self.status.status == FC_STATUSES['TRANFERRING']:
            return self._check_transferring()

    # todo: this is how it should be implemented
    def _check_sequencing_properly(self):
        if self.cycle_times and len(self.cycle_times) > 5:
            average_duration = self.cycle_times.average_duration
            last_cycle = self.cycle_times.last
            last_change = last_cycle.end or last_cycle.start # if it's not over yet
            current_time = datetime.datetime.now()
            last_cycle_duration = current_time - last_change
            if last_cycle_duration > average_duration + datetime.timedelta(hours=1):
                self._warning = "Cycle {} lasts too long. Flowcell status: {}".format(last_cycle['cycle_number'], self.status.status)
                self.status = FC_STATUSES['CHECKSTATUS']
        else:
            # todo: last_change?
            pass
        return self.status.status



    def _check_sequencing(self):
        # todo: return warning message, but not True/False
        if self.status.status != FC_STATUSES['SEQUENCING']:
            return self.status.status

        if self.cycle_times:
            sum_duration = datetime.timedelta(0)
            for cycle in self.cycle_times:
                duration = cycle['end'] - cycle['start']
                sum_duration += duration

            if len(self.cycle_times) < 10:
                # todo: depending on RunMode
                average_duration = CYCLE_DURATION['HiSeqX']
            else:
                average_duration = sum_duration / len(self.cycle_times)

            number_of_cycles = 0
            for read in self.run_info['Reads']:
                number_of_cycles += int(read['NumCycles'])

            # sum_duration = average_duration * number_of_cycles

            # todo: the difference between the last record from cycle_times and datetime.now compare with average duration
            current_time = datetime.datetime.now()

            last_cycle = self.cycle_times[-1]
            # if cycle has not finished yet, take start time
            last_change = last_cycle['end'] or last_cycle['start']
            current_duration = current_time - last_change
            if current_duration > average_duration + datetime.timedelta(hours=1):
                self._warning = "Cycle {} lasts too long. Flowcell status: {}".format(last_cycle['cycle_number'], 'Sequencing')
                return self._warning
            else:
                return None
        else:
            current_time = datetime.datetime.now()
            # todo: compare with CYCLE_DURATION
            # todo: how to define the last change?
            raise NotImplementedError('CycleTimes.txt is not present. Extend {}.check_status()'.format(self.__class__.__name__))


    def _check_demultiplexing(self):
        if self.status.status == FC_STATUSES['DEMULTIPLEXING']:
            current_time = datetime.datetime.now()
            duration = current_time - self.status.demultiplexing_started
            if duration > DURATIONS['DEMULTIPLEXING']:
                self._warning = "Demultiplexing takes too long. Started: {}".format(self.status.demultiplexing_started)
                self.status = FC_STATUSES['CHECKSTATUS']
        return self.status.status

    def _check_transferring(self):
        if self.status.status == FC_STATUSES['TRANFERRING']:
            # todo: check when transfering has started
            duration = DURATIONS['TRANSFERING']
            return False

    @property
    def cycle_times(self):
        if self._cycle_times is None:
            cycle_times_path = os.path.join(self.path, 'Logs/CycleTimes.txt')
            if os.path.exists(cycle_times_path):
                # todo: CycleTimesParser fails when no file found
                self._cycle_times = CycleTimesParser(cycle_times_path).cycles
        return self._cycle_times

# class HiseqRun(Run):
#     def __init__(self, flowcell_dir):
#         super(HiseqRun, self).__init__(flowcell_dir)
#
#     @property
#     def name(self):
#         # todo: if 'Flowcell' not in RunInfo?
#         return self.run_info.get('Flowcell')
#
#
#
#
# class MiseqRun(Run):
#     def __init__(self, flowcell_dir):
#         super(MiseqRun, self).__init__(flowcell_dir)
#
#     @property
#     def name(self):
#         # todo: if 'Flowcell' not in RunInfo?
#         return self.run_info['Flowcell']
#
#     @property
#     def chemistry(self):
#         return self.run_parameters['Chemistry']
#
#     @property
#     def formatted_reads(self):
#         # get number of cycles for all reads if read is NOT index
#         reads = [read['NumCycles'] for read in self.run_info['Reads'] if read['IsIndexedRead'] != 'Y']
#
#         # if only one read
#         if len(reads) == 1:
#             return reads[0]
#         # if all values are the same
#         elif len(set(reads)) == 1:
#             return "{}x{}".format(len(reads), reads[0])
#         # if there are different values
#         else:
#             # '/' will separate the values
#             return "/".join(reads)
#
#     @property
#     def formatted_index(self):
#         # get number of cycles for all reads if read IS index
#         indices = [read['NumCycles'] for read in self.run_info['Reads'] if read['IsIndexedRead'] == 'Y']
#
#         # if only one index
#         if len(indices) == 1:
#             return indices[0]
#         # if more than one index and all values are the same
#         elif len(set(indices)) == 1:
#             return "{}x{}".format(len(indices), indices[0])
#         # if there are different values
#         else:
#             return "/".join(read for read in indices)
#
#
#     def get_formatted_description(self):
#         description = """
#     Date: {date}
#     Flowcell: {flowcell}
#     Instrument: {instrument}
#     Preprocessing server: {localhost}
#     Lanes: {lanes}
#     Tiles: {tiles}
#     Reads: {reads}
#     Indices: {index}
#     Chemistry: {chemistry}
#         """.format(
#                     date=self.run_info['Date'],
#                     flowcell=self.run_info['Flowcell'],
#                     instrument=self.run_info['Instrument'],
#                     localhost=socket.gethostname(),
#                     lanes=self.run_info['FlowcellLayout']['LaneCount'],
#                     tiles=self.run_info['FlowcellLayout']['TileCount'],
#                     reads=self.formatted_reads,
#                     index=self.formatted_reads,
#                     chemistry=self.chemistry,
#         )
#         return description