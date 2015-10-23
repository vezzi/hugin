import os
import datetime
import subprocess

from flowcell_parser.classes import CycleTimesParser

# flowcell statuses
FC_STATUSES =  {
	'ABORTED'       : "Aborted",         # something went wrong in the FC
	'CHECKSTATUS'   : "Check status",    # demultiplex failure
	'SEQUENCING'    : "Sequencing",      # under sequencing
	'DEMULTIPLEXING': "Demultiplexing",  # under demultiplexing
	'TRANFERRING'   : "Transferring",    # tranferring to HPC resource
	'NOSYNC'        : "Nosync",          # in nosync folder
	'ARCHIVED'      : 'Archived',        # removed from nosync folder
}


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

class FlowcellStatus(object):
	def __init__(self, flowcell_path):
		self._path = flowcell_path

		# a timestamp when the status has changed
		self._sequencing_started    = None
		self._sequencing_done       = None
		self._demultiplexing_done   = None
		self._transfering_done      = None
		self._demultiplexing_started = None
		self._transfering_started   = None
		self._nosync = None

		# static values
		self.demux_file = "./Demultiplexing/Stats/ConversionStats.xml"
		self.demux_dir = "./Demultiplexing"
		self.transfering_file = "~.logs/transfer.tsv"
		self.cycle_times_file = "Logs/CycleTimes.txt"

		self._status = None
		# message if status is 'CHECKSTATUS'
		self._warning = None

		# flag if the flowcell has the same status too long
		self._check_status = None

	@property
	def status(self):
		if self._status is None:
			if os.path.basename(os.path.dirname(self.path)) == 'nosync':
				self._nosync = True
				self._status = FC_STATUSES['NOSYNC']
			elif self.transfering_started and not self.transfering_done:
				self._status = FC_STATUSES['TRANFERRING']
			elif self.demultiplexing_started and not self.demultiplexing_done:
				self._status = FC_STATUSES['DEMULTIPLEXING']
			else:
				self._status = FC_STATUSES['SEQUENCING']
		return self._status

	@status.setter
	def status(self, value):
		self._status = value

	@property
	def check_status(self):
		return self._check_status

	@check_status.setter
	def check_status(self, value):
		self._check_status = value

	@property
	def nosync(self):
		if self._nosync is None:
			self._nosync = os.path.basename(os.path.dirname(self.path)).lower() == 'nosync'
		return self._nosync

	@property
	def warning(self):
		return self._warning

	@warning.setter
	def warning(self, value):
		self._warning = value

	@property
	def path(self):
		return self._path

	@property
	def sequencing_started(self):
		if self._sequencing_started is None:
			self._sequencing_started = datetime.datetime.fromtimestamp(os.path.getctime(self.path))
		return self._sequencing_started

	@property
	def sequencing_done(self):
		if self._sequencing_done is None:
			# if RTAComplete.txt is present, sequencing is done
			rta_file = os.path.join(self.path, 'RTAComplete.txt')
			if os.path.exists(rta_file):
				self._sequencing_done = datetime.datetime.fromtimestamp(os.path.getmtime(rta_file))
		return self._sequencing_done

	@property
	def demultiplexing_started(self):
		if self._demultiplexing_started is None:
			demux_dir = os.path.join(self.path, self.demux_dir)
			if os.path.exists(demux_dir):
				self._demultiplexing_started = datetime.datetime.fromtimestamp(os.path.getctime(demux_dir))
		return self._demultiplexing_started

	@property
	def demultiplexing_done(self):
		if self._demultiplexing_done is None:
			if self.demultiplexing_started:
				demux_file = os.path.join(self.path, self.demux_file)
				if os.path.exists(demux_file):
					self._demultiplexing_done = datetime.datetime.fromtimestamp(os.path.getmtime(demux_file))
		return self._demultiplexing_done

	@property
	def transfering_started(self):
		if self._transfering_started is None:
			command = "{ls} {path}".format(path=config['transfering']['path'])
			# proc = subprocess.Popen(['ssh', '-t', '{}@{}' %(config['user'], server_url), command],

			# transfering_file = os.path.join(self.path, self.transfering_file)
			# if os.path.exists(transfering_file):
			#     self._transfering_started = datetime.datetime.fromtimestamp(os.path.getctime(transfering_file))

		return self._transfering_started

	@property
	def transfering_done(self):
		if self._transfering_done is None:
			if self.transfering_started:
				# todo: moved to nosync
				pass
		return self._transfering_done

	@property
	def demultiplexing_end_time(self):
		if self.status == FC_STATUSES['DEMULTIPLEXING']:
			return self.demultiplexing_started + DURATIONS['DEMULTIPLEXING']
		else: return None

	@property
	def transferring_end_time(self):
		if self.status == FC_STATUSES['TRANFERRING']:
			return self.transfering_started + DURATIONS['TRANSFERING']
		else: return None

	# def _sequencing_end_time(self):
	#     if self.cycle_times is None:
	#         start_time = self.sequencing_started
	#         # todo duration depending on the run mode!
	#         duration = CYCLE_DURATION['HiSeqX'] * self.number_of_cycles
	#         end_time = start_time + duration
	#     else:
	#         duration = self.average_cycle_time * self.number_of_cycles
	#         start_time = self.cycle_times[0]['start']
	#         end_time = start_time + duration
	#     return end_time
	#
	# @property
	# def sequencing_end_time(self):
	#     if self.cycle_times and len(self.cycle_times) > 5:
	#
	#         if self.cycle_times and len(self.cycle_times) > 5:
	#             average_duration = self.average_cycle_time
	#             last_cycle = self.cycle_times[-1]
	#             last_change = last_cycle['end'] or last_cycle['start']  # if cycle has not finished yet, take start time
	#             current_time = datetime.datetime.now()
	#
	#             current_duration = current_time - last_change
	#             end_time = self.sequencing_started + average_duration *
	#
	#         else:
	#             current_time = datetime.datetime.now()
	#             # todo: run_mode
	#             run_mode = 'HiSeqX'
	#             if current_time > DURATIONS[run_mode]:
	#                 self.status.warning = 'Sequencing takes too long!'
	#                 self.status = FC_STATUSES['CHECKSTATUS']
	#
	#
	#
	#
	# @property
	# def cycle_times(self):
	#     if self._cycle_times is None:
	#         cycle_times_path = os.path.join(self.path, self.cycle_times_file)
	#         if os.path.exists(cycle_times_path):
	#             self._cycle_times = CycleTimesParser(cycle_times_path).cycles
	#     return self._cycle_times

