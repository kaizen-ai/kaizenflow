#!/usr/bin/python3

# Importing necessary libraries and modules
import sys
import os
import csv
from itertools import groupby
from operator import itemgetter

# Separator for data fields
SEPARATOR = "\t"

class Streaming(object):

    @staticmethod
    # Replacing dots in name for environmental variables
    def get_job_conf(name):
        name = name.replace(".", "_")
        return os.environ.get(name)

    def __init__(self, infile=sys.stdin, separator=SEPARATOR):
        # Initializing input file and separator
        self.infile = infile
        self.sep = separator

    def emit(self, key, value):
        # Writing key-value pairs to stdout
        sys.stdout.write("{}{}{}\n".format(key, self.sep, value))

    def read(self):
        # Reading lines from input file
        for line in self.infile:
            yield line.rstrip()

    def __iter__(self):
        # Iterating through lines of input file
        for line in self.read():
            yield line


class Mapper(Streaming):

    def map(self):
        # Abstract method for mapping, must be implemented by subclasses
        raise NotImplementedError("Mappers must implement a map method")

    def __iter__(self):
        # Iterating through lines of input file and splitting by comma
        for line in self.read():
            yield line.split(",")


class Reducer(Streaming):

    def reduce(self):
        # Abstract method for reducing, must be implemented by subclasses
        raise NotImplementedError("Reducers must implement a reduce method")

    def __iter__(self):
        # Grouping key-value pairs by key
        generator = (line.split(self.sep, 1) for line in self.read())
        for key, val in groupby(generator, itemgetter(0)):
            yield key, val


class AggMapper(Mapper):

    def map(self):
        # Mapping function for aggregation
        for row in self:
            try:
                # Emitting key-value pairs based on specific columns
                self.emit(row[4], float(row[11]))
            except Exception as e:
                pass


if __name__ == '__main__':
    # Creating instance of AggMapper and calling map function
    mapper = AggMapper(sys.stdin)
    mapper.map()
