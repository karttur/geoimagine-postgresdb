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
                
    def _CheckDeleteSingleRecord(self, queryD, schema, table, tabkeys = [[]]):
        self._GetTableKeys(schema, table)
        selectQuery = {}
        if len(self.tabkeys) == 0:
            self.tabkeys = tabkeys

        for item in self.tabkeys:
            '''
            val = '\'%(v)s\'' %{'v':queryD[item[0]]}
            selectQuery[item[0]] = {'op':'=', 'val':val}
            '''
            selectQuery[item[0]] = {'op':'=', 'val':queryD[item[0]]}
        wherestatement = self._DictToSelect(selectQuery)
        selectQuery = {'schema':schema, 'table':table, 'select': wherestatement}
        query = "SELECT * FROM %(schema)s.%(table)s %(select)s" %selectQuery
        #print (query)

        self.cursor.execute(query)
        records = self.cursor.fetchall()
        if len(records) == 1:
            #print (query)
            query = "DELETE FROM %(schema)s.%(table)s %(select)s" %selectQuery
            #print (query)
            #BALLE
            self.cursor.execute(query)
            self.conn.commit()

    def _CheckInsertSingleRecord(self, queryD, schema, table, tabkeys = [[]], overwrite=False, delete=False):
        self._GetTableKeys(schema, table)

        selectQuery = {}

        if len(self.tabkeys) == 0:
            self.tabkeys = tabkeys

        for item in self.tabkeys:
            '''
            val = '\'%(v)s\'' %{'v':queryD[item[0]]}
            selectQuery[item[0]] = {'op':'=', 'val':val}
            '''

            selectQuery[item[0]] = {'op':'=', 'val':queryD[item[0]]}
        wherestatement = self._DictToSelect(selectQuery)
        selectQuery = {'schema':schema, 'table':table, 'select': wherestatement}
        query = "SELECT * FROM %(schema)s.%(table)s %(select)s" %selectQuery

        self.cursor.execute(query)
        rec = self.cursor.fetchone()

        if rec != None and delete:
            query = "DELETE FROM %(schema)s.%(table)s %(select)s" %selectQuery
            
            self.cursor.execute(query)
            self.conn.commit()
            return rec
        elif rec != None and overwrite:
            query = "DELETE FROM %(schema)s.%(table)s %(select)s" %selectQuery

            self.cursor.execute(query)
            
            self.conn.commit()
            self._InsertRecord(queryD, schema, table)
            return rec
        elif rec == None and not delete:
            self._InsertRecord(queryD, schema, table)

        return rec
            
    def _SingleSearch(self,queryD, paramL,schema,table,pq = False):
        #self._GetTableKeys(schema, table)
        selectQuery = {}
        for item in queryD:
            '''
            val = '\'%(v)s\'' %{'v':queryD[item]}
            val = val.replace("''","'")
            selectQuery[item] = {'op':'=', 'val':val}
            '''
            if isinstance(queryD[item],dict):
                #preset operator and value
                selectQuery[item] = queryD[item]
            else:
                selectQuery[item] = {'op':'=', 'val':queryD[item]}
        wherestatement = self._DictToSelect(selectQuery)  
        cols =  ','.join(paramL)
        selectQuery = {'schema':schema, 'table':table, 'select': wherestatement, 'cols':cols}
        
        query = "SELECT %(cols)s FROM %(schema)s.%(table)s %(select)s" %selectQuery
        if pq:
            print ('SingleSearch query',query)
        self.cursor.execute(query)
        self.records = self.cursor.fetchone()
        return self.records

    def _MultiSearch(self,queryD,paramL,schema,table, qp = False):
        #self._GetTableKeys(schema, table)
        '''
        queryD = {}
            queryD['compid'] = {'val':campid, 'op':'=' }
            queryD['regionid'] = {'val':regionid, 'op':'=' }
        '''
        selectQuery = {}
        for item in queryD:
            '''
            val = '\'%(v)s\'' %{'v':queryD[item]}
            val = val.replace("''","'")
            selectQuery[item] = {'op':'=', 'val':val}
            '''
            if isinstance(queryD[item],dict):
                #preset operator and value
                selectQuery[item] = queryD[item]
            else:
                selectQuery[item] = {'op':'=', 'val':queryD[item]}
        wherestatement = self._DictToSelect(selectQuery) 

        if len(paramL) == 1:
            cols = paramL[0]
        else:
            cols =  ','.join(paramL)
        selectQuery = {'schema':schema, 'table':table, 'select': wherestatement, 'cols':cols}      
        query = "SELECT %(cols)s FROM %(schema)s.%(table)s %(select)s" %selectQuery
        if qp:
            print (query)

        self.cursor.execute(query)
        self.records = self.cursor.fetchall()
        return self.records
    
    def _InsertRecord(self, queryD, schema, table):
        self._DictToColumnsValues(queryD, schema, table)
        query = "INSERT INTO %(schema)s.%(table)s (%(columns)s) VALUES (%(values)s);" %self.query
        print (query)
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
        
    def _GetTableKeys(self,schema,table):
        #TGTODO duplicate in specimen
        query = "SELECT column_name FROM information_schema.table_constraints \
                JOIN information_schema.key_column_usage USING (constraint_catalog, constraint_schema, constraint_name, table_catalog, table_schema, table_name) \
                WHERE constraint_type = 'PRIMARY KEY' \
                AND (table_schema, table_name) = ('%s', '%s')" %(schema,table)
        print (query)
        self.cursor.execute(query)
        self.tabkeys = self.cursor.fetchall()
        print ('self.tabkeys',self.tabkeys)
        return self.tabkeys

    def _GetTableColumns(self,schema,table):
        query = {'table':table,'schema':schema}
        self.cursor.execute("SELECT column_name, data_type, character_maximum_length, numeric_precision, column_default FROM information_schema.columns WHERE table_schema = '%(schema)s' and table_name='%(table)s';" %query)
        return self.cursor.fetchall()
    
    def _InsertQuery(self,query):
        self.cursor.execute ("INSERT INTO %(schematab)s (%(cols)s) VALUES (%(values)s);" %query)
        self.conn.commit()
        
    def _GetTableColumnsComplete(self,schema,table):
        query = {'table':table,'schema':schema}
        self.cursor.execute("SELECT * FROM information_schema.columns where table_schema = '%(schema)s' and table_name='%(table)s';" %query)
        return self.cursor.fetchall()
    
    def _SelectLayoutItem(self,query,paramL,table):
        '''
        '''
        query['table'] = table 
        query['cols'] = ",".join(paramL )
        self.cursor.execute("SELECT  %(cols)s FROM layout.%(table)s \
        WHERE compid = '%(compid)s';" %query)

        recs = self.cursor.fetchall()
        if len(recs) == 1:
            return recs[0]

        self.cursor.execute("SELECT  %(cols)s FROM layout.%(table)s \
        WHERE compid = '%(compid)s' AND suffix = '%(suffix)s';" %query)
        rec = self.cursor.fetchone()
        if rec == None:
            print ("SELECT  %(cols)s FROM layout.%(table)s\
                WHERE compid = '%(compid)s' AND suffix = '%(suffix)s';" %query)        
        return rec
        
    def IniSelectScaling(self, compD):
        compid = '%(f)s_%(b)s' %{'f':compD['folder'].lower(),'b':compD['band'].lower()}
        query = {'compid':compid,'source':compD['source'], 
                 'product':compD['product'], 
                 'suffix':compD['suffix']}
        paramL = ['power', 'powerna', 'mirror0', 'scalefac' ,'offsetadd', 'srcmin','srcmax','dstmin','dstmax']
        rec = self._SelectLayoutItem(query,paramL,'scaling')
        if rec == None:
            
            exitstr = 'No scaling for compid %s' %(compid)
            print (exitstr)
            ADDSCALING
            exit(exitstr)
        scalingD = dict(zip(paramL,rec))
        for item in scalingD:
            if scalingD[item] == 'N':
                scalingD[item] = False
            elif scalingD[item] == 'Y':
                scalingD[item] = True
        return scalingD
         
    def IniSelectLegendOld(self,compD):
        compid = '%(f)s_%(b)s' %{'f':compD['folder'].lower(),'b':compD['band'].lower()}
        query = {'compid':compid,'source':compD['source'], 
                 'product':compD['product'], 
                 'suffix':compD['suffix']}

        paramL = ['palmin','palmax','two51','two52','two53','two54','two55','height','width',
                  'measure','buffer','separatebuffer','frame','framecolor','label','font',
                  'fontcolor','fontsize','sticklen','compresslabels','columns','matrix',
                  'columntext','rowtext','columnhead','rowhead', 'precision']
        rec = self._SelectLayoutItem(query,paramL,'legend')
        if rec == None:
            exitstr = 'No legend for compid %s' %(self.compid)
            print (exitstr)
            ADDLEGEND
            exit(exitstr)
        legendD = dict(zip(paramL,rec))
        for item in legendD:
            if legendD[item] == 'N':
                legendD[item] = False
            elif legendD[item] == 'Y':
                legendD[item] = True 
        return legendD
    
    def IniSelectLegend(self,compD):
        compid = '%(f)s_%(b)s' %{'f':compD['folder'].lower(),'b':compD['band'].lower()}
        query = {'compid':compid,'source':compD['source'], 
                 'product':compD['product'], 
                 'suffix':compD['suffix']}

        paramL = ['palmin','palmax','two51','two52','two53','two54','two55','height','width',
                  'soloheight','pngwidth','pngheight',
                  'measure','buffer','margin','textpadding','separatebuffer','framestrokewidth','framecolor',
                  'framefill','label','font','fontcolor','fontsize','fonteffect',
                  'titlefont','titlefontcolor','titlefontsize','titlefonteffect',
                  'sticklen','compresslabels','precision','columns','matrix',
                  'columntext','rowtext','columnhead','rowhead']
        rec = self._SelectLayoutItem(query,paramL,'legend')
        if rec == None:
            exitstr = 'No legend for compid %s' %(compid)
            print (exitstr)
            ADDLEGEND
            exit(exitstr)
        legendD = dict(zip(paramL,rec))
        for item in legendD:
            if legendD[item] == 'N':
                legendD[item] = False
            elif legendD[item] == 'Y':
                legendD[item] = True 
        return legendD
    
    def _SelectCount(self,schema,table):
        schematab = '%s.%s' %(schema,table)
        query = {'schematab':schematab}
        self.cursor.execute("SELECT COUNT(*) FROM %(schematab)s;" %query)
        return self.cursor.fetchone()

        