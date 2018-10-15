'''
Created on 21 feb. 2018

@author: thomasgumbricht
'''

import psycopg2
from base64 import b64decode

class PGsession:
    '''
    Connects the postgres database.
    '''

    def __init__(self, query, name='unknown'):
        '''
        Constructor that opens the session, expects a dictionary with keys for 'db', 'user' and 'pswd', and a
        name [optional]
        '''
        query['pswd'] = b64decode(query['pswd']).decode('ascii')
        conn_string = "host='localhost' dbname='%(db)s' user='%(user)s' password='%(pswd)s'" %query
        self.conn = psycopg2.connect(conn_string)
        self.cursor = self.conn.cursor() 
        self.name = name
        
    def _Close(self):
        self.cursor.close()
        self.conn.close()
        
        
    def _CheckInsertSingleRecord(self,queryD,schema,table, tabkeys = [[]]):
        self._GetTableKeys(schema, table)
        selectQuery = {}
        if len(self.tabkeys) == 0:
            self.tabkeys = tabkeys
        #print ('self.tabkeys XXX',self.tabkeys)
        for item in self.tabkeys:
            '''
            val = '\'%(v)s\'' %{'v':queryD[item[0]]}
            selectQuery[item[0]] = {'op':'=', 'val':val}
            '''
            #print ('item',item)
            selectQuery[item[0]] = {'op':'=', 'val':queryD[item[0]]}
        wherestatement = self._DictToSelect(selectQuery)
        selectQuery = {'schema':schema, 'table':table, 'select': wherestatement}
        query = "SELECT * FROM %(schema)s.%(table)s %(select)s" %selectQuery
        #print ('query in _CheckInsertSingleRecord', query)
        #print (query)
        self.cursor.execute(query)
        records = self.cursor.fetchone()
        if records == None:
            self._InsertRecord(queryD, schema, table)
        return records
            
    def _SingleSearch(self,queryD,schema,table):
        #self._GetTableKeys(schema, table)
        selectQuery = {}
        for item in queryD:
            '''
            val = '\'%(v)s\'' %{'v':queryD[item]}
            val = val.replace("''","'")
            selectQuery[item] = {'op':'=', 'val':val}
            '''
            selectQuery[item] = {'op':'=', 'val':queryD[item]}
        wherestatement = self._DictToSelect(selectQuery)  
        selectQuery = {'schema':schema, 'table':table, 'select': wherestatement}
        query = "SELECT * FROM %(schema)s.%(table)s %(select)s" %selectQuery
        #print (query)
        self.cursor.execute(query)
        self.records = self.cursor.fetchone()
        return self.records
        #if records == None:
        #    self._InsertRecord(queryD, schema, table)
            
    def _InsertRecord(self, queryD, schema, table):
        self._DictToColumnsValues(queryD, schema, table)
        query = "INSERT INTO %(schema)s.%(table)s (%(columns)s) VALUES (%(values)s);" %self.query
        self.cursor.execute(query)
        self.conn.commit()
        
    def _UpdateRecord(self, queryD, schema, table, searchD):
        selectQuery = {}
        for item in searchD:
            selectQuery[item] = {'op':'=', 'val':searchD[item]}
        wherestatement = self._DictToSelect(selectQuery)
        self._DictToColumnsValues(queryD, schema, table)
        self.query['where'] = wherestatement
        query = "UPDATE %(schema)s.%(table)s SET (%(columns)s) = (%(values)s) %(where)s;" %self.query

        self.cursor.execute(query)
        self.conn.commit()
    
    def _DictToColumnsValues(self,queryD,schema,table):   
        '''
        Converts a dictionary, a schema and a table to a query
        ''' 
        cols = queryD.keys()
        vals = queryD.values()
        columns =  ','.join(cols)
        values =  ','.join(map(lambda x: "'" + str(x) +"'", vals))
        self.query ={'schema':schema,'table':table,'columns':columns,'values':values}  
        
  
    def _DictToSelect(self, queryD):
        '''
        Converts a dictionary to Select statement 
        '''
        selectL = []
        for key in queryD:
            #statement = key operator value
            statement = ' %(col)s %(op)s \'%(val)s\'' %{'col':key.replace('#',''), 'op':queryD[key]['op'], 'val':queryD[key]['val']}
            selectL.append(statement)
        self.select_query = "WHERE %(where)s" %{'where':' AND '.join(selectL)}  
        return self.select_query
        
    def _SeleatAllSchema(self):
        self.cursor.execute("SELECT schema_name FROM information_schema.schemata;")
        #records = self.cursor.fetchone()
        records = self.cursor.fetchall()
        schemaL = []
        for rec in records:

            if rec[0][0:2] == 'pg' or rec[0] == 'information_schema':
                continue
            schemaL.append(rec[0])
        return schemaL
        
    def _SelectAllSchemaTables(self,schema):
        query = {'schema':schema}
        self.cursor.execute("""SELECT table_name FROM information_schema.tables WHERE table_schema = '%(schema)s'""" %query)
        return [row[0] for row in self.cursor]
    
    def _GetTableKeysOLD(self,schema,table):
        #TGTODO duplicate in specimen
        query = "SELECT \
                c.column_name, c.data_type \
                FROM \
                information_schema.table_constraints tc \
                JOIN information_schema.constraint_column_usage AS ccu USING (constraint_schema, constraint_name) \
                JOIN information_schema.columns AS c ON c.table_schema = tc.constraint_schema AND tc.table_name = c.table_name AND ccu.column_name = c.column_name \
                where constraint_type = 'PRIMARY KEY' AND  c.table_schema ='%s' AND tc.table_name = '%s';" %(schema,table)
        
        self.cursor.execute(query)

        self.tabkeys = self.cursor.fetchall()
        return self.tabkeys
    
    def _GetTableKeys(self,schema,table):
        #TGTODO duplicate in specimen
        query = "SELECT column_name FROM information_schema.table_constraints \
                JOIN information_schema.key_column_usage USING (constraint_catalog, constraint_schema, constraint_name, table_catalog, table_schema, table_name) \
                WHERE constraint_type = 'PRIMARY KEY' \
                AND (table_schema, table_name) = ('%s', '%s')" %(schema,table)
        self.cursor.execute(query)
        self.tabkeys = self.cursor.fetchall()
        return self.tabkeys

    def _GetTableColumns(self,schema,table):
        query = {'table':table,'schema':schema}
        self.cursor.execute("SELECT column_name, data_type, character_maximum_length, numeric_precision, column_default FROM information_schema.columns WHERE table_schema = '%(schema)s' and table_name='%(table)s';" %query)
        return self.cursor.fetchall()
        #column_names = [row[0] for row in self.cursor]
        #return column_names
        
    def _GetTableColumnsComplete(self,schema,table):
        query = {'table':table,'schema':schema}
        self.cursor.execute("SELECT * FROM information_schema.columns where table_schema = '%(schema)s' and table_name='%(table)s';" %query)
        return self.cursor.fetchall()