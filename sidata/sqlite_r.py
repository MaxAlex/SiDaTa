import sqlite3

DEFAULT_COMMIT_FREQUENCY = 1000


class sqlite_reader:
    """
    Reads a sqlite file (.sqlite or etc) as a list of dictionaries; a wrapper
    over the built-in Python sqlite3 package.

    This format requires a table parameter, which specifies the name of the
    table to be written to within the sqlite database.

    It is also allowed to specify a set of columns, in which case only these
    columns will be read from the database.
    """

    def __init__(self, target, table, columns=None, select=None):
        self.con = sqlite3.connect(target)


        if columns:
            col_selector = ', '.join(['[%s]' % x for x in columns])
            # self.cur = self.con.execute("SELECT %s FROM %s" % (col_string, table))
        else:
            col_selector = "*"
            # self.cur = self.con.execute("SELECT * FROM %s" % table)

        if select:
            row_items = ["%s = '%s'" % (k, v) for k, v in select.items()]
            row_selector = " WHERE %s" % ','.join(row_items)
        else:
            row_selector = ''

        self.cur = self.con.execute("SELECT %s FROM %s%s" % (col_selector, table, row_selector))

        self.header = (columns if columns else
                        [x[0].strip('[]') for x in self.cur.description])

    def __iter__(self):
        while True:
            rows = self.cur.fetchmany()
            if not rows:
                break
            for row in rows:
                yield row

    def close(self):
        self.con.close()


class sqlite_writer:
    """
    Writes to a sqlite database (.sqlite or etc.)  Creates a new database
    file if one does not already exist.

    Requires the name of the table to be written.

    Attempts to infer the data types of each column by the values of the
    first row; this is unreliable, so it is also possible to specify the
    types directly by giving a list of (column_name, column_type) tuples
    to the "columns" parameter (column_type being the name of the type
    in sqlite.)
    """

    def __init__(self, filename, columns, table, commit_interval=None):
        self.con = sqlite3.connect(filename)
        self.table = table
        self.commit_waiting = 0
        self.commit_interval = commit_interval if commit_interval is not None else DEFAULT_COMMIT_FREQUENCY

        table_manifest = self.con.execute("SELECT * FROM sqlite_master WHERE type='table';").fetchall()
        if self.table in [x[1] for x in table_manifest]:
            manifest = [x for x in table_manifest if self.table == x[1]][0]
            table_desc = manifest[-1]
            table_columns = {}
            for col_type in table_desc.strip(')').split(' (')[1].split(', '):
                col, typ = col_type.rsplit(' ', 1)
                table_columns[col.strip('[]')] = typ
            if set(table_columns) != set(columns):
                raise RuntimeError("Column mismatch with existing database: %s" %
                                   (set(table_columns) ^ set(columns)))
            self.columns = columns
            self.col_types = [table_columns[c] for c in columns]
            self.type_inf = False
        elif isinstance(columns[0], tuple):
            assert set(map(len, columns)) == {2}
            self.columns, self.col_types = list(zip(*columns))
            self.type_inf = False
        else:
            self.columns = columns
            self.col_types = None
            self.type_inf = True

    def typify_columns(self, values):
        self.col_types = []
        for value in values:
            if isinstance(value, float):
                t = 'real'
            elif isinstance(value, int):
                t = 'int'
            else:
                t = 'text'
            self.col_types.append(t)

        columns = ', '.join(["[%s] %s" % (c, t) for c, t in zip(self.columns, self.col_types)])

        cmd = "CREATE TABLE %s (%s)" % (self.table, columns)
        try:
            self.con.execute(cmd)
        except sqlite3.OperationalError as err:
            print(cmd)
            raise err

    def write(self, row):
        if isinstance(row, dict):
            row = [row[x] for x in self.columns]

        if not self.col_types:
            self.typify_columns(row)
            
        if self.type_inf:
            # Check to make sure inferred types are still valid, just
            # because the sqlite errors aren't always helpful.
            for t, val in zip(self.col_types, row):
                if (t == 'real' or t == 'int') and val is not None:
                    assert(isinstance(val, int) or
                           isinstance(val, float)), "Inferred numeric type but got value %s" % val
                    
                if t == 'int' and val is not None:
                    assert(val % 1 == 0), "Inferred integer type but got value %s" % val


        self.con.execute("INSERT INTO %s VALUES (%s)" %
                         (self.table, ', '.join('?' * len(row))),
                         list(map(str, row)))

        self.commit_waiting += 1
        if self.commit_waiting > self.commit_interval:
            self.con.commit()
            self.commit_waiting = 0

    def close(self):
        self.con.commit()
        self.con.close()
