import os
import re
import csv
import glob
import datetime
import xml.parsers.expat
import xml.etree.ElementTree as ET
from datetime import datetime


class SampleSheet(list):
    def __init__(self, samplesheet):
        self.projects = []
        if isinstance(samplesheet, list):
            self.extend(samplesheet)
        else:
            self.samplesheet = samplesheet
            self._parse_sample_sheet()

    def _parse_sample_sheet(self):
        raise NotImplementedError("Please Implement this method")

    def return_projects(self):
        raise NotImplementedError("Please Implent this method")


class HiSeqSampleSheet(SampleSheet):

    def _parse_sample_sheet(self):
        """Parse a .csv samplesheet and return a list of dictionaries with
        elements corresponding to rows of the samplesheet and keys
        corresponding to the columns in the header.
        """
        with open(self.samplesheet,"rU") as fh:
            csvr = csv.DictReader(fh, dialect='excel')
            for row in csvr:
                self.append(row)

    def return_projects(self):
        projects = set()
        for row in self:
            projects.add(row['SampleProject'].replace("__","."))
        return list(projects)

    def get_samplesheet_descriptions(self):
        """Return the set of descriptions in the samplesheet"""
        descriptions = set()
        for row in self:
            descriptions.add(row['Description'])
        return list(descriptions)




class HiSeqXSampleSheet(SampleSheet):

    def _parse_sample_sheet(self, lane=None, sample_project=None, index=None):
        """Parse a .csv samplesheet and return a list of dictionaries with
        elements corresponding to rows of the samplesheet and keys
        corresponding to the columns in the header. Optionally filter by lane
        and/or sample_project and/or index.
        """
        self.samplesheet_dict = self._samplesheet_to_dict()
    
    
    def return_projects(self):
        projects = set()
        for row in self.samplesheet_dict["Data"]:
            if row[0] == 'Lane':
                continue
            else:
                projects.add(row[7].replace("_", "."))
        return list(projects)
                

    def _samplesheet_to_dict(self):
        """ takes as input a samplesheet (Xten compatible) and stores all field in an hash table.
            Samplesheet should look something like:
            [Section1]
                section1,row,1
                section1,row,2
            [Section2]
                section2,row,1
                section2,row,2
            ...
            the hash structure will look like
            "Section1" --> [["section1", "row", "1"],
                            ["section1", "row" , "2"]
                            ]
            "Section2" --> [["section2", "row", "1"],
                            ["section2", "row" , "2"]
                            ]
            :param str samplesheet: the sample sheet to be stored in the hash table
        """
        samplesheet_dict = {}
        section = ""
        header = re.compile("^\[(.*)\]")
        with open(self.samplesheet, "r") as csvfile:
            reader = csv.reader(csvfile, delimiter=',')
            for line in reader:
                if len(line)>0 and header.match(line[0]): # new header (or first) section and empy line
                    section = header.match(line[0]).group(1) # in this way I get the section
                    samplesheet_dict[section] = [] #initialise
                else:
                    if section == "":
                        samplesheet_dict = {}
                        return samplesheet_dict
                    samplesheet_dict[section].append(line)
            return samplesheet_dict





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


# Generic XML to dict parsing
# See http://code.activestate.com/recipes/410469-xml-as-dictionary/
class XmlToList(list):
    def __init__(self, aList):
        for element in aList:
            if len(element):
                # treat like dict
                if len(element) == 1 or element[0].tag != element[1].tag:
                    self.append(XmlToDict(element))
                # treat like list
                elif element[0].tag == element[1].tag:
                    self.append(XmlToList(element))
            elif element.text:
                text = element.text.strip()
                if text:
                    self.append(text)
            else:
                # Set dict for attributes
                self.append({k:v for k,v in element.items()})



class XmlToDict(dict):
    '''
        Example usage:
        >>> tree = ET.parse('your_file.xml')
        >>> root = tree.getroot()
        >>> xmldict = XmlToDict(root)
        Or, if you want to use an XML string:
        >>> root = ET.XML(xml_string)
        >>> xmldict = XmlToDict(root)
        And then use xmldict for what it is... a dict.
        '''

    def __init__(self, parent_element):
        if parent_element.items():
            self.update(dict(parent_element.items()))
        for element in parent_element:
            if len(element):
                # treat like dict - we assume that if the first two tags
                # in a series are different, then they are all different.
                if len(element) == 1 or element[0].tag != element[1].tag:
                    aDict = XmlToDict(element)
                # treat like list - we assume that if the first two tags
                # in a series are the same, then the rest are the same.
                else:
                    # here, we put the list in dictionary; the key is the
                    # tag name the list elements all share in common, and
                    # the value is the list itself
                    aDict = {element[0].tag: XmlToList(element)}
                # if the tag has attributes, add those to the dict
                if element.items():
                    aDict.update(dict(element.items()))
                self.update({element.tag: aDict})
            # this assumes that if you've got an attribute in a tag,
            # you won't be having any text. This may or may not be a
            # good idea -- time will tell. It works for the way we are
            # currently doing XML configuration files...
            elif element.items():
                self.update({element.tag: dict(element.items())})
                # add the following line
                self[element.tag].update({"__Content__":element.text})
            
            # finally, if there are no child tags and no attributes, extract
            # the text
            else:
                self.update({element.tag: element.text})


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


class CycleTimes():
    def __init__(self, CycleTimesFile):
        self.cycles = []
        self._parse(CycleTimesFile)

    def _parse(self, CycleTimesFile):
        """
            parse CycleTimes.txt file and returns an ordered list of cycle
            cycles:
                [[cycle_num, start, end, time] ..]
                
        """
        DATE='%m/%d/%Y-%H:%M:%S.%f'
        with open(CycleTimesFile,"rU") as fh:
            csvr = csv.reader(fh, delimiter='\t')
            header = csvr.next()
            currentRow   = csvr.next()
            cycleNum     = currentRow[3]
            cycleStart   = datetime.strptime(currentRow[0]+"-"+currentRow[1], DATE)
            cycleEnd     = datetime.strptime(currentRow[0]+"-"+currentRow[1], DATE)
            for currentRow in csvr:
                if currentRow[3] == cycleNum:
                #I am still parsing the same cycle
                    cycleEnd = datetime.strptime(currentRow[0]+"-"+currentRow[1], DATE)
                else:
                    self.cycles.append([cycleNum,cycleStart,cycleEnd, cycleEnd - cycleStart])
                    cycleNum = currentRow[3]
                    cycleStart   = datetime.strptime(currentRow[0]+"-"+currentRow[1], DATE)
                    cycleEnd     = datetime.strptime(currentRow[0]+"-"+currentRow[1], DATE)
            #I am done, I only need to save the last cycle
            self.cycles.append([cycleNum,cycleStart,cycleEnd, cycleEnd - cycleStart])













