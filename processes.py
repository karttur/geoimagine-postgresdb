'''
Created on 21 feb. 2018

@author: thomasgumbricht
'''

from geoimagine.postgresdb import PGsession
from base64 import b64encode
import netrc

class SelectProcess(PGsession):
    '''
    DB support for setting up processes
    '''

    def __init__(self):
        """The constructor connects to the database"""
        HOST = 'localhost99'
        secrets = netrc.netrc()
        username, account, password = secrets.authenticators( HOST )
        pswd = b64encode(password.encode())
        #create a query dictionary for connecting to the Postgres server
        query = {'db':'postgres','user':username,'pswd':pswd}
        #Connect to the Postgres Server
        self.session = PGsession.__init__(self,query,'SelectProcess')
        
    def _SelectStratum(self,query):
        self.cursor.execute("SELECT minuserstratum FROM process.subprocesses WHERE subprocid = '%(subprocid)s';" %query)
        record = self.cursor.fetchone()
        return record
        
    def _SelectRootProcess(self,query):  
        self.cursor.execute("SELECT rootprocid FROM process.subprocesses WHERE subprocid = '%(subprocid)s';" %query)
        record = self.cursor.fetchone()
        return record
    
    def _SelectProcessSystems(self,query):
        self.cursor.execute("SELECT system, srcsystem, dstsystem, srcdivision, dstdivision FROM process.procsys WHERE subprocid = '%(subprocid)s';" %query)
        records = self.cursor.fetchall()
        if len(records) == 0:
            print ("SELECT system, srcsystem, dstsystem, srcdivision, dstdivision FROM process.procsys WHERE subprocid = '%(subprocid)s';" %query)
        print ("SELECT system, srcsystem, dstsystem, srcdivision, dstdivision FROM process.procsys WHERE subprocid = '%(subprocid)s';" %query)

        return records

    def _SelectProcessTagAttr(self,subprocess,parent,tag):
        query = {'sub':subprocess, 'par':parent, 'tag':tag}
        self.cursor.execute("SELECT tagorattr, paramid, paramtyp, required, defaultvalue, parent, element, bandid FROM process.processparams WHERE subprocid = '%(sub)s' AND parent = '%(par)s' AND element = '%(tag)s';" %query)
        #print ("SELECT tagorattr, paramid, paramtyp, required, defaultvalue, parent, element, bandid FROM process.processparams WHERE subprocid = '%(sub)s' AND parent = '%(par)s' AND element = '%(tag)s';" %query)
        records = self.cursor.fetchall()
        if len(records) == 0:
            print ("SELECT tagorattr, paramid, paramtyp, required, defaultvalue, parent, element, bandid FROM process.processparams WHERE subprocid = '%(sub)s' AND parent = '%(par)s' AND element = '%(tag)s';" %query)
            self.cursor.execute("SELECT tagorattr, paramid, paramtyp, required, defaultvalue, parent, element, bandid FROM process.processparams;")
            #print ("SELECT tagorattr, paramid, paramtyp, required, defaultvalue, parent, element, bandid FROM process.processparams WHERE subprocid = '%(sub)s' AND parent = '%(par)s' AND element = '%(tag)s';" %query)
            records = self.cursor.fetchall()
            #print ('records',records)

        return records
    
    def _SelectCompBands(self,subprocess,parent,tag):
            query = {'sub':subprocess, 'par':parent, 'tag':tag}
            self.cursor.execute("SELECT bandid FROM process.processparams WHERE subprocid = '%(sub)s' AND parent = '%(par)s' AND element = '%(tag)s' AND paramid = 'band';" %query)
            records = self.cursor.fetchall()
            if len(records) == 0:
                print ("SELECT bandid FROM process.processparams WHERE subprocid = '%(sub)s' AND parent = '%(par)s' AND element = '%(tag)s' AND paramid = 'band';" %query)
                
            return records
        
    def _SelectProcessCompTagAttr(self, subprocess, parent, tag, bandid):
        query = {'sub':subprocess, 'par':parent, 'tag':tag,'bandid':bandid}
        "SELECT tagorattr, paramid, paramtyp, required, defaultvalue FROM process.processparams WHERE subprocid = '%(sub)s' AND parent = '%(par)s' AND element = '%(tag)s' AND bandid = '%(bandid)s';" %query
        self.cursor.execute("SELECT tagorattr, paramid, paramtyp, required, defaultvalue FROM process.processparams WHERE subprocid = '%(sub)s' AND parent = '%(par)s' AND element = '%(tag)s' AND bandid = '%(bandid)s';" %query)
        records = self.cursor.fetchall()
        return records
        
    def _SelectProcessAdditionalTagAttrOld(self,subprocess,parent):
        query = {'sub':subprocess, 'par':parent}
        self.cursor.execute("SELECT DISTINCT ON (element) element FROM process.processparams WHERE subprocid = '%(sub)s' AND parent = '%(par)s';" %query)
        records = self.cursor.fetchall()
        return records
    
    
    def _SelectTractDefRegion(self,tract):
        query = {'tract': tract}
        self.cursor.execute("SELECT parentid FROM regions.tracts WHERE tractid = '%(tract)s';" %query)
        return self.cursor.fetchone()
    
    def _SelectUserTract(self,userid,tract):
        query = {'userid':userid, 'tract':tract}
        self.cursor.execute("SELECT tractid FROM regions.tracts WHERE tractid = '%(tract)s' AND owner = '%(userid)s';" %query)
        return self.cursor.fetchone()
    
    def _SelectTractDefregid(self, userid, tract):
        rec = self._SelectUserTract(userid, tract)
        if rec == None:
            return False
        query = {'tract':tract}
        self.cursor.execute("SELECT D.regionid, D.regioncat FROM regions.tracts AS T LEFT JOIN regions.defregions AS D ON (T.parentid = D.regionid) WHERE T.tractid = '%(tract)s';" %query)
        return self.cursor.fetchone()

class ManageProcess(PGsession):
    '''
    DB support for setting up processes
    '''

    def __init__(self):
        """The constructor connects to the database"""
        HOST = 'localhost98'
        secrets = netrc.netrc()
        username, account, password = secrets.authenticators( HOST )
        pswd = b64encode(password.encode())
        #create a query dictionary for connecting to the Postgres server
        query = {'db':'postgres','user':username,'pswd':pswd}
        #Connect to the Postgres Server
        self.session = PGsession.__init__(self,query,'ManageProcess')  

        
    def _ManageRootProcess(self,process):
        self.cursor.execute("SELECT * FROM process.rootprocesses WHERE rootprocid = '%(rootprocid)s';" %process.paramsD)
        records = self.cursor.fetchone()
        if records == None and not process.delete:
            self.cursor.execute("INSERT INTO process.rootprocesses (rootprocid, title, label, creator) VALUES \
                    ('%(rootprocid)s', '%(title)s', '%(label)s', '%(creator)s');" %process.paramsD)
         
            self.conn.commit()
        elif process.overwrite:
            self.cursor.execute("UPDATE process.rootprocesses SET (title,label) = ('%(title)s', '%(label)s') WHERE rootprocid = '%(rootprocid)s';" %process.paramsD)         
            self.conn.commit()

        elif process.delete:
            self.cursor.execute("SELECT * FROM process.subprocesses WHERE rootprocid = '%(root)s';" %query)
            records = self.cursor.fetchone()
            if records == None:
                self.cursor.execute("DELETE FROM process.rootprocesses WHERE rootprocid = '%(root)s';" %query)
                self.conn.commit()
            else:
                exitstr = 'A Root process can only be deleted if there are no subprocesses assigned to it'
                exit(exitstr)
                
    def _ManageSubProcess(self,process):

        #start by checking the compdef
        if process.processid not in ['organizelandsat','explodelandsatscene','organizeancillary']:
            if hasattr(process, 'dstcomp'):
                for comp in process.dstcomp:

                    self.ManageCompDefs(process, process.parameters.version, process.proj.system, paramCDL)
        #Check that the rootprocess is OK

        self.cursor.execute("SELECT * FROM process.rootprocesses WHERE rootprocid = '%(rootprocid)s';" %process.paramsD)
        records = self.cursor.fetchone()
        if records == None:
            print (process.paramsD)
            paramsD = {'rootprocid':'ERRORCHECK'}
            exitstr = 'The root process %(rootprocid)s is not defined, can not add sub process %(subprocid)s' %process.paramsD
            exit(exitstr)
            
        self.cursor.execute("SELECT * FROM process.subprocesses WHERE rootprocid = '%(rootprocid)s' AND subprocid = '%(subprocid)s' AND version = '%(version)s';" %process.paramsD)
        records = self.cursor.fetchone()

        if records == None and not process.delete:
            self.cursor.execute("SELECT * FROM process.subprocesses WHERE rootprocid = '%(rootprocid)s' AND subprocid = '%(subprocid)s' AND version = '%(version)s';" %process.paramsD)
            records = self.cursor.fetchone()
            if records == None and not process.delete:
                self.cursor.execute("SELECT rootprocid FROM process.subprocesses WHERE subprocid = '%(subprocid)s' AND version = '%(version)s';" %process.paramsD)
                record = self.cursor.fetchone()
                if record:
                    exitstr = 'The subprocess %s is already defined, but under root %s (%s)' %(process.parameters.subprocid,record[0],process.parameters.rootprocid)
                    exit(exitstr)
                self.cursor.execute("INSERT INTO process.subprocesses (rootprocid, subprocid, version, minuserstratum, title, label, creator, createdate) VALUES \
                    ('%(rootprocid)s', '%(subprocid)s', '%(version)s', '%(minuserstratum)s', '%(title)s', '%(label)s', '%(creator)s', '%(today)s')" %process.paramsD)
                for s in process.system.paramD:
                    process.system.paramD[s]['subprocid'] = process.paramsD['subprocid']
 
                    self.cursor.execute("INSERT INTO process.procsys (subprocid, system, srcsystem, dstsystem, srcdivision, dstdivision) VALUES \
                            ('%(subprocid)s', '%(system)s', '%(srcsystem)s', '%(dstsystem)s', '%(srcdivision)s', '%(dstdivision)s')" %process.system.paramD[s])

            
            self.conn.commit()  
            
        elif process.overwrite:
            self.cursor.execute("UPDATE process.subprocesses SET (minuserstratum,title,label) = (%(minuserstratum)s, '%(title)s', '%(label)s') WHERE \
                rootprocid = '%(rootprocid)s' AND subprocid = '%(subprocid)s' AND version = '%(version)s';"  %process.paramsD)
            self.conn.commit() 
            self.cursor.execute("DELETE FROM process.procsys WHERE subprocid = '%(subprocid)s';" %process.paramsD)
            self.conn.commit()
            for s in process.system.paramD:
                process.system.paramD[s]['subprocid'] = process.paramsD['subprocid']

                self.cursor.execute("INSERT INTO process.procsys (subprocid, system, srcsystem, dstsystem, srcdivision, dstdivision) VALUES \
                        ('%(subprocid)s', '%(system)s', '%(srcsystem)s', '%(dstsystem)s', '%(srcdivision)s', '%(dstdivision)s')" %process.system.paramD[s])
  
            
        elif process.delete:
            self.cursor.execute("DELETE FROM process.subprocesses WHERE rootprocid = '%(rootprocid)s' AND subprocid = '%(subprocid)s' AND version = '%(version)s'" %process.paramsD)

            self.conn.commit()
            
        if process.delete or process.overwrite:
            #Delete all the processparams
            self.cursor.execute("DELETE FROM process.processparams WHERE rootprocid = '%(rootprocid)s' AND subprocid = '%(subprocid)s' AND version = '%(version)s';" %process.paramsD)
            self.conn.commit()
            #Delete all the processparamSetValues
            self.cursor.execute("DELETE FROM process.processparamSetValues WHERE  rootprocid = '%(rootprocid)s' AND subprocid = '%(subprocid)s' AND version = '%(version)s';" %process.paramsD)
            self.conn.commit()
            #Delete all the processparamSetMinMax
            self.cursor.execute("DELETE FROM process.processparamSetMinMax WHERE  rootprocid = '%(rootprocid)s' AND subprocid = '%(subprocid)s' AND version = '%(version)s';" %process.paramsD)
            self.conn.commit()
            if process.delete:
                #return - all deleted nothing to add
                return

        for element in process.node.paramsD:

            for parent in process.node.paramsD[element]:

                bandid = False
                #Find out if this is a band
                for p in process.node.paramsD[element][parent]:
                    if p['paramid'] == 'band' and p['defaultvalue'] != '':
                        bandid =  p['defaultvalue'] 

                if parent in ['dstcomp','srccomp']:

                    if not bandid:
                        exitstr = 'All compositions must have a bandid (defaultvalue) %s %s' %(rootprocid, subprocid)
                        print (process.node.params[element])
                        print (exitstr)
                        ERRORCHECK
                        exit(exitstr)
                        
                for param in process.node.paramsD[element][parent]:

                    #paramid = param['paramid']
                    if 'setvalue' in param:
                        setValueL = param['setvalue']
                    else:
                        setValueL = []
                    if 'minmax' in param: 
                        minMaxD = param['minmax']
                    else:
                        minMaxD = {}
                    query =  {**param, **process.paramsD}
                    query['parent'] = parent
                    query['element'] = element
                    query['bandid'] = bandid
                    query['tagorattr'] = query['tagorattr'].upper()[0]
                    if parent in ['srccomp','dstcomp'] and bandid:
                        
                        self.cursor.execute("SELECT * FROM process.processparams WHERE rootprocid = '%(rootprocid)s' AND subprocid = '%(subprocid)s' AND paramid = '%(paramid)s' AND version = '%(version)s' AND parent = '%(parent)s' AND element = '%(element)s' AND defaultvalue = '%(defaultvalue)s' AND bandid = '%(bandid)s';" %query)
                    else:
                        
                        self.cursor.execute("SELECT * FROM process.processparams WHERE rootprocid = '%(rootprocid)s' AND subprocid = '%(subprocid)s' AND paramid = '%(paramid)s' AND version = '%(version)s' AND parent = '%(parent)s' AND element = '%(element)s';" %query)
                    records = self.cursor.fetchone()            
                    
                    if records == None: 
                        self.cursor.execute("INSERT INTO process.processparams (rootprocid, subprocid, version, parent, element, paramid, paramtyp, tagorattr, required, defaultvalue,bandid) VALUES \
                                ('%(rootprocid)s','%(subprocid)s','%(version)s','%(parent)s','%(element)s','%(paramid)s','%(paramtyp)s','%(tagorattr)s','%(required)s','%(defaultvalue)s','%(bandid)s');" %query)
                        self.conn.commit()
                    
                    
                    for setValue in setValueL: 
                        query['setvalue'] = setValue['value']
                        self.cursor.execute("SELECT * FROM process.processparamSetValues WHERE \
                            rootprocid = '%(rootprocid)s' AND subprocid = '%(subprocid)s' AND paramid = '%(paramid)s' AND \
                            parent = '%(parent)s'  AND element = '%(element)s' AND setvalue = '%(setvalue)s' AND version = '%(version)s';" %query)
                        records = self.cursor.fetchone()
                        if records == None: 
                            query['valuelabel'] = setValue['label']
                            self.cursor.execute("INSERT INTO process.processparamSetValues (rootprocid, subprocid, version, paramid, parent, element, setvalue, label) VALUES \
                            ('%(rootprocid)s','%(subprocid)s','%(version)s','%(paramid)s','%(parent)s','%(element)s', '%(setvalue)s', '%(valuelabel)s');" %query)
                            self.conn.commit()
                    if minMaxD: 
                        self.cursor.execute("SELECT * FROM process.processparamSetMinMax WHERE \
                                rootprocid = '%(rootprocid)s' AND subprocid = '%(subprocid)s' AND paramid = '%(paramid)s' AND \
                                parent = '%(parent)s'  AND element = '%(element)s' AND version = '%(version)s';" %query)
                        records = self.cursor.fetchone()
                        if records == None: 
                            query['min'] =  minMaxD['min'];  query['max'] =  minMaxD['max']    
                            self.cursor.execute("INSERT INTO process.processparamSetMinMax (rootprocid, subprocid, version, paramid, parent, element, minval, maxval) VALUES \
                            ('%(rootprocid)s','%(subprocid)s','%(version)s','%(paramid)s','%(parent)s','%(element)s',%(min)s,%(max)s);" %query)
                            self.conn.commit()
 

