'''
Created on 22 feb. 2018

@author: thomasgumbricht
'''
from geoimagine.postgresdb import PGsession
from base64 import b64encode
import netrc

from geoimagine.support.karttur_dt import Today

class SelectLayout(PGsession):
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
        self.session = PGsession.__init__(self,query)
        self.session.name = 'SelectLayout'
        
    def _SelectStratum(self,query):
        self.cursor.execute("SELECT minuserstratum FROM process.subprocesses WHERE subprocid = '%(subprocid)s';" %query)
        record = self.cursor.fetchone()
        return record
        
    def _SelectRootProcess(self,query):  
        print ("SELECT rootprocid FROM process.subprocesses WHERE subprocid = '%(subprocid)s';" %query)
        self.cursor.execute("SELECT rootprocid FROM process.subprocesses WHERE subprocid = '%(subprocid)s';" %query)
        record = self.cursor.fetchone()
        return record
    
    def _SelectProcessTagAttr(self,subprocess,parent,tag):
        query = {'sub':subprocess, 'par':parent, 'tag':tag}
        self.cursor.execute("SELECT tagorattr, paramid, paramtyp, required, defaultvalue FROM process.processparams WHERE subprocid = '%(sub)s' AND parent = '%(par)s' AND element = '%(tag)s';" %query)
        records = self.cursor.fetchall()
        return records
    
    def _SelectCompBands(self,subprocess,parent,tag):
            query = {'sub':subprocess, 'par':parent, 'tag':tag}
            self.cursor.execute("SELECT bandid FROM process.processparams WHERE subprocid = '%(sub)s' AND parent = '%(par)s' AND element = '%(tag)s' AND paramid = 'band';" %query)
            records = self.cursor.fetchall()
            if len(records) == 0:
                print ("SELECT bandid FROM process.processparams WHERE subprocid = '%(sub)s' AND parent = '%(par)s' AND element = '%(tag)s' AND paramid = 'band';" %query)
            return records
        
    def _SelectProcessAdditionalTagAttr(self,subprocess,parent):
        query = {'sub':subprocess, 'par':parent}
        self.cursor.execute("SELECT DISTINCT ON (element) element FROM process.processparams WHERE subprocid = '%(sub)s' AND parent = '%(par)s';" %query)
        records = self.cursor.fetchall()
        return records

class ManageLayout(PGsession):
    '''
    DB support for setting up processes
    '''

    def __init__(self):
        """The constructor connects to the database"""
        HOST = 'managelayout'
        secrets = netrc.netrc()
        username, account, password = secrets.authenticators( HOST )
        pswd = b64encode(password.encode())
        #create a query dictionary for connecting to the Postgres server
        query = {'db':'postgres','user':username,'pswd':pswd}
        #Connect to the Postgres Server
        self.session = PGsession.__init__(self,query)   
        
    def _ManageRasterPalette(self,process,palette,querys):
        query = {'pal':palette,'owner':process.proj.userid,'compid':process.parameters.compid}
        #print ('query',query)
        self.cursor.execute("SELECT owner FROM layout.rasterpalettes WHERE palette = '%(pal)s';" %query)
        rec = self.cursor.fetchone()
        if rec != None:
            if rec[0] != process.proj.userid:
                warnstr = 'Skipping palette management. Your user is not the owner of the palette %/pal)s' %query
                print (warnstr)
                return
        elif not process.delete:
            self.cursor.execute("INSERT INTO layout.rasterpalettes (palette, compid, owner) VALUES (%s, %s, %s)",
                        (palette, process.parameters.compid, process.proj.userid))
            self.conn.commit()
        self.cursor.execute("SELECT owner FROM layout.rasterpalettes WHERE compid = '%(compid)s' AND owner = '%(owner)s';" %query)
        rec = self.cursor.fetchone()
        if rec != None:
            warnstr = 'Skipping create management. Each user can only create one (1) palette for each composition (%(compid)s)' %query
            print (warnstr)
            return
        if process.delete or process.overwrite:
            self.cursor.execute("DELETE FROM layout.rasterpalcolors WHERE palette = '%(pal)s';" %query)
            if process.delete:
                return
        for key in querys:
            query = {'pal':palette, 'val':key}
            self.cursor.execute("SELECT * FROM layout.rasterpalcolors WHERE palette = '%(pal)s' AND value = %(val)s;" %query)
            rec = self.cursor.fetchone()
            if rec == None:
                cD = querys[key]
                self.cursor.execute("INSERT INTO layout.rasterpalcolors (palette, value, red, green, blue, alpha, label, hint) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                        (palette, cD['value'], cD['red'], cD['green'], cD['blue'], cD['alpha'], cD['label'], cD['hint']))
                self.conn.commit()
