import sqlite3

ROWS_PER_COMMIT = 100


class sqlite_reader:
    """
    Reads a sqlite file (.sqlite or etc) as a list of dictionaries; a wrapper
    over the built-in Python sqlite3 package.

    This format requires a table parameter, which specifies the name of the
    table to be written to within the sqlite database.

    It is also allowed to specify a set of columns, in which case only these
    columns will be read from the database.
    """

    def __init__(self, target, table, columns = None):
        self.con = sqlite3.connect(target)

        if columns:
            col_string = ', '.join(columns)
            self.cur = self.con.execute("SELECT %s FROM %s" % (col_string, table))
        else:
            self.cur = self.con.execute("SELECT * FROM %s" % table)

        self.header = (columns if columns else
                        [x[0] for x in self.cur.description])

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

    def __init__(self, filename, columns, table):
        self.con = sqlite3.connect(filename)
        self.table = table
        self.commit_waiting = 0

        if isinstance(columns[0], tuple):
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

        columns = ', '.join([c + ' ' + t for c, t in
                              zip(self.columns, self.col_types)])

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
        if self.commit_waiting > ROWS_PER_COMMIT:
            self.con.commit()

    def close(self):
        self.con.commit()
        self.con.close()
