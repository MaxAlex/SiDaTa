import six
import csv
known_delimiters = [',', '\t']


class csv_reader:
    """
    Simply reads a CSV as a list of lists; simple wrapper
    over base CSV module.
    """
    def __init__(self, target):
        if six.PY2:
            self.filept = open(target, 'r')
        elif six.PY3:
            self.filept = open(target, newline = '')
        self.source = csv.reader(self.filept)

    def __iter__(self):
        # Will handle type conversion!
        for row in self.source:
            yield row