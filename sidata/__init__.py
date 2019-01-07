import warnings



class Entry(dict):
    def __init__(self, parent, index, is_row):
        self.parent = parent
        self.index = index
        self.is_row = is_row

    def __getitem__(self, key):
        if self.is_row:
            self.parent.get_cell(row_index = self.index, column_name = key)
        else:
            self.parent.get_cell(col_index = self.index, row_name = key)

    def __setitem__(self, key, value):
        if self.is_row:
            self.parent.set_cell(row_index = self.index, column_name = key,
                                value = value)
        else:
            self.parent.set_cell(col_index = self.index, row_name = key,
                                value = value)


class Table(object):
    def __init__(self, target, column_headers = None, row_headers = None):
        if not isinstance(target, basestring):
            raise AssertionError("Matrix and Pandas conversion, but not yet!")

        if target.lower().endswith('.csv'):
            from csv_r import csv_reader
            source = csv_reader(target)
        elif (target.lower().endswith('.xlsx')
              or target.lower().endswith('.xls')):
            from excel_r import excel_reader
            source = excel_reader(target)
        else:
            raise IOError("Perhaps sqlite, but not yet!")

        self.table = []
        
        if column_headers is None:
            column_headers = source.next()
            if not (len(self.column_headers) == len(set(self.column_headers))):
                raise IOError("No valid table header, and column_header not specified.")

        self.column_headers = {ch: i for i, ch in enumerate(column_headers)}

        for i, row in enumerate(source):
            if len(row) != len(column_headers):
                raise IOError("Inconsistent number of columns.  (Row %d)" % i)
            self.table.append(row)

        if row_headers is not None:
            if len(row_headers) != len(self.table):
                raise IOError("row_headers does not match length of table."
                              "(%d vs %d)" % (len(row_headers), len(self.table)))
            self.row_headers = {rh: i for i, rh in enumerate(row_headers)}
        else:
            # Would be better to use an identity-lookup-object, but first draft!
            self.row_headers = {i: i for i in range(len(self.table))}

        if self.table[0] == column_headers:
            warnings.warn("First row is identical to column headers.")
            #self.table = self.table[1:] ## Warn or fix it?

    def __getitem__(self, row):
        return self.get_row(row)

    def get_row(self, row):
        return Entry(self, row, True)

    def get_col(self, col):
        return Entry(self, col, False)

    def get_cell(self,
                 row_index = None, col_index = None,
                 row_name = None, col_name = None):
        if row_index is None and row_name is None:
            raise IndexError("Row not specified.")
        if col_index is None and col_name is None:
            raise IndexError("Column not specified.")

        if row_name and not row_index:
            row_index = self.row_headers[row_name]
        if col_name and not col_index:
            col_index = self.column_headers[col_name]

        return self.table[col_index][row_index]

    def set_cell(self,
                 row_index = None, col_index = None,
                 row_name = None, col_name = None,
                 value = None):
        if row_index is None and row_name is None:
            raise IndexError("Row not specified.")
        if col_index is None and col_name is None:
            raise IndexError("Column not specified.")

        if row_name and not row_index:
            row_index = self.row_headers[row_name]
        if col_name and not col_index:
            col_index = self.column_headers[col_name]

        self.table[col_index][row_index] = value

        return value




        
