import warnings
import six


class Reader:
    """
    Basic interface for reading tables in CSV, XLSX, etc form.  Yields
    a sequence of dictionaries corresponding to rows in the table.
    """
    def __init__(self, filename, table = None, open_as_type = None):
        if open_as_type is not None:
            file_ext = open_as_type
        else:
            file_ext = filename.split('.')[-1]


        if file_ext in {'csv', 'tsv'}:
            from sidata.csv_r import csv_reader
            self.source = csv_reader(filename)
        elif file_ext in {'xlsx'}:
            from sidata.excel_r import excel_reader
            self.source = excel_reader(filename, table)
        else:
            from sidata.sqlite_r import sqilte_reader
            self.source = sqlite_reader(filename, table)

        self.columns = self.source.header

    def __iter__(self):
        for values in self.source:
            row = dict(zip(self.columns, values))
            yield row

        self.source.close()

    def close(self):
        self.source.close()



class Writer:
    """
    Basic interface for writing tables in CSV, XLSX, etc form.  Takes
    a sequence of dictionaries which are written as rows into the
    table.  Dictionary keys must match specified column names.
    """

    def __init__(self, filename, columns, table = None, open_as_type = None):
        if open_as_type is not None:
            file_ext = open_as_type
        else:
            file_ext = filename.split('.')[-1]

        if file_ext in {'csv', 'tsv'}:
            from sidata.csv_r import csv_writer
            self.destination = csv_writer(filename, columns)
        elif file_ext in {'xlsx'}:
            from sidata.excel_r import excel_writer
            self.destination = excel_writer(filename, columns, table)
        elif file_ext in {'sql', 'sqlite'}:
            from sidata.sqlite_r import sqlite_writer
            self.destination = sqlite_writer(filename, columns, table)

        self.columns = columns

    def __call__(self, row):
        self.destination.write(row)

    def close(self):
        self.destination.close()


class Modifier:
    """
    Utility function to open a Reader on the specified file, as well
    as a Writer to a new file with a modified file name and potentially
    additional columns; useful for scripts that annotate a pre-existing
    table.
    """

    def __init__(self, filename,
                 outputfile = None, tag = None, ext = None,
                 columns = [], add_cols = []):

        if not (outputfile or tag or ext):
            raise IOError("Cannot modify file in place; specify "
                          "outputfile, tag, or ext to generate new "
                          "file")
        if columns and add_cols:
            raise ValueError("Specify one and only one of columns "
                             "or add_cols.")

        if not outputfile:
            outputfile = filename
        if tag:
            comps = outputfile.split('.')
            comps.insert(-1, tag)
            outputfile = '.'.join(comps)
        if ext:
            comps = outputfile.split('.')
            comps[-1] = ext
            outputfile = '.'.join(comps)

        self.rdr = Reader(filename)
        self.read_columns = self.rdr.columns

        if not columns:
            columns = self.rdr.columns + add_cols
        self.wtr = Writer(outputfile, columns)
        self.write_columns = columns

    def __iter__(self):
        for row in self.rdr:
            yield row

    def __call__(self, row):
        self.wtr(row)

    def close(self):
        self.rdr.close()
        self.wtr.close()
