#!/usr/bin/python3

# Importing necessary libraries and modules
import os
import sys
import csv
from itertools import groupby
from operator import itemgetter

# Separator for data fields
SEPERATOR = "\t"

class Streaming(object):

    @staticmethod
    def get_job_conf(name):
        # Replace dots in name with underscores for environmental variables
        name = name.replace(".", "_")
        return os.environ.get(name)

    def __init__(self, infile=sys.stdin, separator=SEPERATOR):
        # Initializing input file and separator
        self.infile = infile
        self.sep = separator

    def emit(self, key, value):
        # Writing key-value pairs to stdout with separator
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

class AggReducer(Reducer):

    def reduce(self):
        # Reducing function for aggregation
        for key, val in self:
            total = 0
            count = 0

            # Calculating total and count for values of each key
            for item in val:
                total += float(item[1])
                count += 1

            # Emitting key-value pair with average value
            self.emit(key, float(total) / float(count))

if __name__ == '__main__':
    # Creating instance of AggReducer and calling reduce function
    reducer = AggReducer(sys.stdin)
    reducer.reduce()
