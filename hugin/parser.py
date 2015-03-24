import os
import re
import csv
import glob
import datetime



class HiSeqSampleSheet(list):
    def __init__(self, samplesheet, lane=None, sample_project=None, index=None):
        self.header = ["FCID",
                       "Lane",
                       "SampleID",
                       "SampleRef",
                       "Index",
                       "Description",
                       "Control",
                       "Recipe",
                       "Operator",
                       "SampleProject"]
            
        if isinstance(samplesheet, list):
            self.extend(samplesheet)
        else:
            self.samplesheet = samplesheet
            self._parse_sample_sheet(lane=None, sample_project=None, index=None)


    def _parse_sample_sheet(self, lane=None, sample_project=None, index=None):
        """Parse a .csv samplesheet and return a list of dictionaries with
        elements corresponding to rows of the samplesheet and keys
        corresponding to the columns in the header. Optionally filter by lane
        and/or sample_project and/or index.
        """
        with open(self.samplesheet,"rU") as fh:
            csvr = csv.DictReader(fh, dialect='excel')
            for row in csvr:
                if (lane is None or row["Lane"] == lane) \
                and (sample_project is None or row["SampleProject"] == sample_project) \
                and (index is None or row["Index"] == index):
                    self.append(row)

    def write(self, samplesheet):
        """Write samplesheet to .csv file
                                """
        with open(samplesheet, "w") as outh:
            csvw = csv.writer(outh)
            if len(self) > 0:
                csvw.writerow(self[0].keys())
            else:
                csvw.writerow(self.header)
        csvw.writerows([row.values() for row in self])



class RunInfoParser():
    """RunInfo parser"""
    def __init__(self):
        self._data = {}
        self._element = None
    
    def parse(self, fp):
        self._parse_RunInfo(fp)
        return self._data
    
    def _start_element(self, name, attrs):
        self._element=name
        if name == "Run":
            self._data["Id"] = attrs["Id"]
            self._data["Number"] = attrs["Number"]
        elif name == "FlowcellLayout":
            self._data["FlowcellLayout"] = attrs
        elif name == "Read":
            self._data["Reads"].append(attrs)

    def _end_element(self, name):
        self._element=None
    
    def _char_data(self, data):
        want_elements = ["Flowcell", "Instrument", "Date"]
        if self._element in want_elements:
            self._data[self._element] = data
        if self._element == "Reads":
            self._data["Reads"] = []

    def _parse_RunInfo(self, fp):
        p = xml.parsers.expat.ParserCreate()
        p.StartElementHandler = self._start_element
        p.EndElementHandler = self._end_element
        p.CharacterDataHandler = self._char_data
        p.ParseFile(fp)



class RunParametersParser():
    """runParameters.xml parser"""
    def __init__(self):
        self.data = {}
    
    def parse(self, fh):
        tree = ET.parse(fh)
        root = tree.getroot()
        self.data = XmlToDict(root)
        # If not a MiSeq run, return the contents of the Setup tag
        if 'MCSVersion' not in self.data:
            self.data = self.data['Setup']
        return self.data














