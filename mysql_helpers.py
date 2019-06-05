"""
This class provides functionality for managing a generig sqlite or mysql
database:

* reading specific fields (with the possibility to filter by field values)
* storing calculated values in the dataset

Created on May 11 2018

@author: Jerónimo Arenas García

"""

from __future__ import print_function  # For python 2 copmatibility
import os
import pandas as pd
import pymysql
import sqlite3
import numpy as np
import copy


class BaseDMsql(object):
    """
    Data manager base class.
    """

    def __init__(self, db_name, db_connector, db_server=None, db_user=None, db_password=None,
                 db_port=None):
        """
        Initializes a DataManager object

        Args:
            db_name      :Name of the DB
            db_connector :Connector. Available options are mysql or sqlite
            db_server    :Server (mysql only)
            db_user      :User (mysql only)
            db_password  :Password (mysql only)
            db_port      :port(mysql only) Necessary if not 3306
        """

        # Store paths to the main project folders and files
        self.dbname = db_name
        self.connector = db_connector
        self.server = db_server
        self.user = db_user
        self.password = db_password
        self.port = db_port

        # Other class variables
        self.dbON = False  # Will switch to True when the db is connected.
        # Connector to database
        self._conn = None
        # Cursor of the database
        self._c = None

        # Try connection
        try:
            if self.connector == 'mysql':
                if self.port:
                    self._conn = pymysql.connect(self.server, self.user, self.password, self.dbname, port=self.port,
                                                 charset='utf8mb4', autocommit=True)
                else:
                    self._conn = pymysql.connect(self.server, self.user, self.password, self.dbname,
                                                 charset='utf8mb4', autocommit=True)
                self._c = self._conn.cursor()
                print("MySQL database connection successful")
                self.dbON = True
            elif self.connector == 'sqlite3':
                # sqlite3
                # sqlite file will be in the root of the project, we read the
                # name from the config file and establish the connection
                db_fname = os.path.join(self._path2db, self.dbname + '.db')
                print("---- Connecting to {}".format(db_fname))
                self._conn = sqlite3.connect(db_fname)
                self._c = self._conn.cursor()
                self.dbON = True
            else:
                print("---- Unknown DB connector {}".format(self.connector))
        except:
            print("---- Error connecting to the database")

    def __del__(self):
        """
        When destroying the object, it is necessary to commit changes
        in the database and close the connection
        """

        try:
            self._conn.commit()
            self._conn.close()
        except:
            print("---- Error closing database")

    def deleteDBtables(self, tables=None):
        """
        Delete existing database, and regenerate empty tables

        Args:
            tables: If string, name of the table to reset.
                    If list, list of tables to reset
                    If None (default), all tables are deleted, and all tables
                    (inlcuding those that might not exist previously)
        """

        # If tables is None, all tables are deleted an re-generated
        if tables is None:
            # Delete all existing tables
            table_names = self.getTableNames()
            for table in table_names:
                self._c.execute("DROP TABLE " + table)

        else:

            # It tables is not a list, make the appropriate list
            if type(tables) is str:
                tables = [tables]

            # Remove all selected tables (if exist in the database).
            for table in set(tables) & set(self.getTableNames()):
                self._c.execute("DROP TABLE " + table)

        self._conn.commit()
        return

    def deleteRowTables(self, table, cond):
        """
        Delete all rows in a table satisfying a certain condition

        Args:
            tables: If string, name of the table to reset.
                    If list, list of tables to reset
                    If None (default), all tables are deleted, and all tables
                    (inlcuding those that might not exist previously)
        """

        # If tables is None, all tables are deleted an re-generated
        try:
            self._c.execute("DELETE FROM %s WHERE %s" % (table, cond))
        except BaseException as e:
            print(str(e))
        self._conn.commit()
        return


    def createDBtable(self, table, field_dict, primary_key=None, foreign_keys=dict()):
        """
        Create empty tables with mandatory fields

        Args:
            tables: Name of table to create
            columns: Mandatory fields to be included in the table
            primary_key: Primary key of created table. None by default
            foreign_keys: List of foreign keys to include in table
        """
        mysql_cmd = 'CREATE TABLE '+ table +' ('
        for type in field_dict:
            for field in field_dict[type]:
                if primary_key == field or (foreign_keys is not None and foreign_keys.get(field, None) is not None):
                    mysql_cmd += field + ' VARCHAR(250) NOT NULL,'
                else:
                    mysql_cmd += f'{field} {type},'
        mysql_cmd = mysql_cmd[0:-1]
        if primary_key is not None:
            mysql_cmd += ',PRIMARY KEY (' + primary_key + ')'
        for fk in foreign_keys:
            mysql_cmd += ',FOREIGN KEY ('+ foreign_keys[fk].split(" ")[1] +') REFERENCES '+foreign_keys[fk].split(" ")[0]+'('+fk+')'
        mysql_cmd += ') DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;'
        self._c.execute(mysql_cmd)
        self._conn.commit()

        return

    def clearDBtables(self, tables=None):
        """
        Delete existing database, and regenerate empty tables

        Args:
            tables: If string, name of the table to reset.
                    If list, list of tables to reset
                    If None (default), all tables are deleted, and all tables
                    (inlcuding those that might not exist previously)
        """

        # If tables is None, all tables are cleared
        if tables is None:
            # Delete all existing tables
            for table in self.getTableNames():
                self._c.execute("TRUNCATE TABLE " + table)

        else:

            # It tables is not a list, make the appropriate list
            if type(tables) is str:
                tables = [tables]

            # Remove all selected tables (if exist in the database).
            for table in set(tables) & set(self.getTableNames()):
                self._c.execute("TRUNCATE TABLE " + table)

        self._conn.commit()

        return

    def addTableColumn(self, tablename, columnname, columntype):
        """
        Add a new column to the specified table.

        Args:
            tablename  :Table to which the column will be added
            columnname :Name of new column
            columntype :Type of new column.

        Note that, for mysql, if type is TXT or VARCHAR, the character set if
        forzed to be utf8.
        """

        # Check if the table exists
        if tablename in self.getTableNames():

            # Check that the column does not already exist
            if columnname not in self.getColumnNames(tablename):

                # Allow columnames with spaces
                columnname = columnname

                # Fit characters to the allowed format if necessary
                fmt = ''
                if (self.connector == 'mysql' and ('TEXT' in columntype or 'VARCHAR' in columntype) and not (
                        'CHARACTER SET' in columntype or 'utf8' in columntype)):
                    # We need to enforze utf8 for mysql
                    fmt = ' CHARACTER SET utf8'

                sqlcmd = (f"ALTER TABLE {tablename} ADD COLUMN {columnname} {columntype} {fmt}")
                self._c.execute(sqlcmd)

                # Commit changes
                self._conn.commit()

            else:
                print(("WARNING: Column {0} already exists in table {1}.").format(columnname, tablename))

        else:
            print('Error adding column to table. Please, select a valid ' + 'table name from the list')
            print(self.getTableNames())

    def dropTableColumn(self, tablename, columnname):
        """
        Remove column from the specified table

        Args:
            tablename    :Table to which the column will be added
            columnname   :Name of column to be removed

        """

        # Check if the table exists
        if tablename in self.getTableNames():

            # Check that the column exists
            if columnname in self.getColumnNames(tablename):

                # Allow columnames with spaces
                columname = '`' + columnname + '`'

                # ALTER TABLE DROP COLUMN IS ONLY SUPPORTED IN MYSQL
                if self.connector == 'mysql':

                    sqlcmd = ('ALTER TABLE ' + tablename + ' DROP COLUMN ' + columnname)
                    self._c.execute(sqlcmd)

                    # Commit changes
                    self._conn.commit()

                else:
                    print('Error deleting column. Column drop not yet supported for SQLITE')

            else:
                print('Error deleting column. The column does not exist')
                print(tablename, columnname)

        else:
            print('Error deleting column. Please, select a valid table name' + ' from the list')
            print(self.getTableNames())

        return

    def readDBtable(self, tablename, limit=None, selectOptions=None, filterOptions=None, orderOptions=None):
        """
        Read data from a table in the database can choose to read only some
        specific fields

        Args:
            tablename    :  Table to read from
            selectOptions:  string with fields that will be retrieved
                            (e.g. 'REFERENCIA, Resumen')
            filterOptions:  string with filtering options for the SQL query
                            (e.g., 'WHERE UNESCO_cd=23')
            orderOptions:   string with field that will be used for sorting the
                            results of the query
                            (e.g, 'Cconv')
            limit:          The maximum number of records to retrieve

        """

        try:

            # Check that table name is valid

            if tablename in self.getTableNames():

                sqlQuery = 'SELECT '
                if selectOptions:
                    sqlQuery = sqlQuery + selectOptions
                else:
                    sqlQuery = sqlQuery + '*'

                sqlQuery = sqlQuery + ' FROM ' + tablename + ' '

                if filterOptions:
                    sqlQuery = sqlQuery + ' WHERE ' + filterOptions

                if orderOptions:
                    sqlQuery = sqlQuery + ' ORDER BY ' + orderOptions

                if limit:
                    sqlQuery = sqlQuery + ' LIMIT ' + str(limit)

                # This is to update the connection to changes by other
                # processes.
                self._conn.commit()

                # Return the pandas dataframe. Note that numbers in text format
                # are not converted to
                return pd.read_sql(sqlQuery, con=self._conn, coerce_float=False)

            else:
                print('Error in query. Please, select a valid table name ' + 'from the list')
                print(self.getTableNames())

        except Exception as E:
            print(str(E))

    def getTableNames(self):
        """
        Returns a list with the names of all tables in the database
        """

        # The specific command depends on whether we are using mysql or sqlite
        if self.connector == 'mysql':
            sqlcmd = ("SELECT table_name FROM INFORMATION_SCHEMA.TABLES " + "WHERE table_schema='" + self.dbname + "'")
        else:
            sqlcmd = "SELECT name FROM sqlite_master WHERE type='table'"

        self._c.execute(sqlcmd)
        self._conn.commit()
        table_tuple = self._c.fetchall()
        tbnames = list()
        for el in table_tuple:
            tbnames.append(el[0])

        return tbnames

    def getColumnNames(self, tablename):
        """
        Returns a list with the names of all columns in the indicated table

        Args:
            tablename: the name of the table to retrieve column names
        """

        # Check if tablename exists in database
        if tablename in self.getTableNames():
            # The specific command depends on whether we are using mysql or
            #  sqlite
            if self.connector == 'mysql':
                sqlcmd = "SHOW COLUMNS FROM " + tablename
                self._c.execute(sqlcmd)
                self._conn.commit()
                columnnames = [el[0] for el in self._c.fetchall()]
            else:
                sqlcmd = "PRAGMA table_info(" + tablename + ")"
                self._c.execute(sqlcmd)
                self._conn.commit()
                columnnames = [el[1] for el in self._c.fetchall()]

            return columnnames

        else:
            print('Error retrieving column names: Table does not exist on ' + 'database')
            return []

    def getTableInfo(self, tablename):

        # Get columns
        cols = self.getColumnNames(tablename)

        # Get number of rows
        sqlcmd = "SELECT COUNT(*) FROM " + tablename
        self._c.execute(sqlcmd)
        n_rows = self._c.fetchall()[0][0]

        return cols, n_rows

    def insertInTable(self, tablename, columns, arguments):
        """
        Insert new records into table

        Args:
            tablename:  Name of table in which the data will be inserted
            columns:    Name of columns for which data are provided
            arguments:  A list of lists or tuples, each element associated
                        to one new entry for the table
        """

        # Make sure columns is a list, and not a single string
        if not isinstance(columns, (list,)):
            columns = [columns]

        # To allow for column names that have spaces
        columns = list(map(lambda x: '`' + x + '`', columns))

        ncol = len(columns)

        if len(arguments[0]) == ncol:
            # Make sure the tablename is valid
            if tablename in self.getTableNames():
                # Make sure we have a list of tuples; necessary for mysql
                arguments = list(map(tuple, arguments))

                # # Update DB entries one by one.
                # for arg in arguments:
                #     # sd
                #     sqlcmd = ('INSERT INTO ' + tablename + '(' +
                #               ','.join(columns) + ') VALUES(' +
                #               ','.join('{}'.format(a) for a in arg) + ')'
                #               )

                #     try:
                #         self._c.execute(sqlcmd)
                #     except:
                #         import ipdb
                #         ipdb.set_trace()

                sqlcmd = ('INSERT INTO ' + tablename + '(' + ','.join(columns) + ') VALUES (')
                if self.connector == 'mysql':
                    sqlcmd += '%s' + (ncol - 1) * ',%s' + ')'
                else:
                    sqlcmd += '?' + (ncol - 1) * ',?' + ')'

                self._c.executemany(sqlcmd, arguments)

                # Commit changes
                self._conn.commit()
        else:
            print('Error inserting data in table: number of columns mismatch')

        return

    def setField(self, tablename, keyfld, valueflds, values):
        """
        Update records of a DB table

        Args:
            tablename:  Table that will be modified
            keyfld:     string with the column name that will be used as key
                        (e.g. 'REFERENCIA')
            valueflds:  list with the names of the columns that will be updated
                        (e.g., 'Lemas')
            values:     A list of tuples in the format
                            (keyfldvalue, valuefldvalue)
                        (e.g., [('Ref1', 'gen celula'),
                                ('Ref2', 'big_data, algorithm')])

        """

        # Auxiliary function to circularly shift a tuple one position to the
        # left
        def circ_left_shift(tup):
            ls = list(tup[1:]) + [tup[0]]
            return tuple(ls)

        # Make sure valueflds is a list, and not a single string
        if not isinstance(valueflds, (list,)):
            valueflds = [valueflds]

        # To allow for column names that have spaces
        valueflds = list(map(lambda x: '`' + x + '`', valueflds))

        ncol = len(valueflds)

        if len(values[0]) == (ncol + 1):
            # Make sure the tablename is valid
            if tablename in self.getTableNames():

                # # Update DB entries one by one.
                # # WARNING: THIS VERSION MAY NOT WORK PROPERLY IF v
                # #          HAS A STRING CONTAINING "".
                # for v in values:
                #     sqlcmd = ('UPDATE ' + tablename + ' SET ' +
                #               ', '.join(['{0} ="{1}"'.format(f, v[i + 1])
                #                          for i, f in enumerate(valueflds)]) +
                #               ' WHERE {0}="{1}"'.format(keyfld, v[0]))
                #     self._c.execute(sqlcmd)

                # This is the old version: it might not have the problem of
                # the above version, but did not work properly with sqlite.
                # Make sure we have a list of tuples; necessary for mysql
                # Put key value last in the tuples
                values = list(map(circ_left_shift, values))

                sqlcmd = 'UPDATE ' + tablename + ' SET '
                if self.connector == 'mysql':
                    sqlcmd += ', '.join([el + '=%s' for el in valueflds])
                    sqlcmd += ' WHERE ' + keyfld + '=%s'
                else:
                    sqlcmd += ', '.join([el + '=?' for el in valueflds])
                    sqlcmd += ' WHERE ' + keyfld + '=?'

                self._c.executemany(sqlcmd, values)

                # Commit changes
                self._conn.commit()
        else:
            print('Error updating table values: number of columns mismatch')

        return

    def upsert(self, tablename, keyfld, df, robust=True):

        """
        Update records of a DB table with the values in the df
        This function implements the following additional functionality:
        * If there are columns in df that are not in the SQL table,
          columns will be added
        * New records will be created in the table if there are rows
          in the dataframe without an entry already in the table. For this,
          keyfld indicates which is the column that will be used as an
          index

        Args:
            tablename:  Table that will be modified
            keyfld:     string with the column name that will be used as key
                        (e.g. 'REFERENCIA')
            df:         Dataframe that we wish to save in table tablename
            robust:     If False, verifications are skipped
                        (for a faster execution)

        """

        # Check that table exists and keyfld exists both in the Table and the
        # Dataframe
        if robust:
            if tablename in self.getTableNames():
                if not ((keyfld in df.columns) and (keyfld in self.getColumnNames(tablename))):
                    print("Upsert function failed: Key field does not exist", "in the selected table and/or dataframe")
                    return
            else:
                print('Upsert function failed: Table does not exist')
                return

        # Reorder dataframe to make sure that the key field goes first
        flds = [keyfld] + [x for x in df.columns if x != keyfld]
        df = df[flds]

        if robust:
            # Create new columns if necessary
            for clname in df.columns:
                if clname not in self.getColumnNames(tablename):
                    if df[clname].dtypes == np.float64:
                        self.addTableColumn(tablename, clname, 'DOUBLE')
                    else:
                        if df[clname].dtypes == np.int64:
                            self.addTableColumn(tablename, clname, 'INTEGER')
                        else:
                            if 'date' in clname:
                                self.addTableColumn(tablename, clname, 'DATETIME')
                            else:
                                self.addTableColumn(tablename, clname, 'TEXT')

        # Check which values are already in the table, and split
        # the dataframe into records that need to be updated, and
        # records that need to be inserted
        keyintable = self.readDBtable(tablename, limit=None, selectOptions=keyfld)
        keyintable = keyintable[keyfld].tolist()
        values = [tuple(x) for x in df.values]
        values_insert = list(filter(lambda x: x[0] not in keyintable, values))
        values_update = list(filter(lambda x: x[0] in keyintable, values))

        if len(values_update):
            self.setField(tablename, keyfld, df.columns[1:].tolist(), values_update)
        if len(values_insert):
            self.insertInTable(tablename, df.columns.tolist(), values_insert)

        return

    def exportTable(self, tablename, fileformat, path, filename, cols=None):
        """
        Export columns from a table to a file.

        Args:
            :tablename:  Name of the table
            :fileformat: Type of output file. Available options are
                            - 'xlsx'
                            - 'pkl'
            :filepath:   Route to the output folder
            :filename:   Name of the output file
            :columnames: Columns to save. It can be a list or a string
                         of comma-separated columns.
                         If None, all columns saved.
        """

        # Path to the output file
        fpath = os.path.join(path, filename)

        # Read data:
        if cols is list:
            options = ','.join(cols)
        else:
            options = cols

        df = self.readDBtable(tablename, selectOptions=options)

        # ######################
        # Export results to file
        if fileformat == 'pkl':
            df.to_pickle(fpath)

        else:
            df.to_excel(fpath, engine='xlsxwriter')

        return
