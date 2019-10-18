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
        self.header = next(self.source)

    def __iter__(self):
        # Will handle type conversion!
        for row in self.source:
            yield row

    def close(self):
        self.filept.close()



class csv_writer:
    """
    Simply writes a CSV given a rows from a Writer object.  Simple wrapper
    over base CSV module.
    """

    def __init__(self, filename, columns):
        self.columns = columns

        if six.PY2:
            self.filept = open(filename, 'w')
        elif six.PY3:
            self.filept = open(filename, mode = 'w', newline = '')
        self.source = csv.writer(self.filept)
        self.source.writerow(columns)

    def write(self, row):
        if isinstance(row, dict):
            self.source.writerow([row[x] for x in self.columns])
        elif isinstance(row, list):
            self.source.writerow(row)
        else:
            raise ValueError

    def close(self):
        self.filept.close()

