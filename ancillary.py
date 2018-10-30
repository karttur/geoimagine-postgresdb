'''
Created on 8 mars 2018

@author: thomasgumbricht
'''

from geoimagine.postgresdb import PGsession
from geoimagine.postgresdb.compositions import InsertCompDef, InsertCompProd, InsertLayer
from base64 import b64encode
import netrc

from geoimagine.support.karttur_dt import Today

class ManageAncillary(PGsession):
    '''
    DB support for managing regions
    '''
    def __init__(self):
        """The constructor connects to the database"""
        HOST = 'manageancillary'
        secrets = netrc.netrc()
        username, account, password = secrets.authenticators( HOST )
        pswd = b64encode(password.encode())
        #create a query dictionary for connecting to the Postgres server
        query = {'db':'postgres','user':username,'pswd':pswd}
        #Connect to the Postgres Server
        self.session = PGsession.__init__(self,query,'ManageAncillary')
  
        
    def _SelectDefaultRegion(self,defregid):
        query = {'regionid':defregid}
        self.cursor.execute("SELECT regioncat FROM system.defregions WHERE regionid = '%(regionid)s';" %query)
        rec = self.cursor.fetchone()
        return rec
    
    def _ManageAncilDS(self, system, paramsD, dsid, overwrite, delete): 

        self.cursor.execute("SELECT * FROM system.regions WHERE regionid = '%(regionid)s';" %paramsD)
        rec = self.cursor.fetchone()
        if rec == None:
            print ("SELECT * FROM system.regions WHERE regionid = '%(id)s' AND regioncat = '%(cat)s';" %paramsD)
            exitstr = 'EXITING: Can not find region and regioncat for DS: %s, %s' %(ancilDS.regionid,ancilDS.regioncat)
            exit(exitstr)
        query = {'system':system,'dsid':dsid}
        print ("SELECT * FROM %(system)s.datasets WHERE dsid = '%(dsid)s'" %query)
        self.cursor.execute("SELECT * FROM %(system)s.datasets WHERE dsid = '%(dsid)s'" %query) 
        rec = self.cursor.fetchone()
                
        if rec == None and not delete:
            '''
            query = {'system':system, 'instid':parameters.dsinst, 'dsname':parameters.dsname, 
                     'regionid':parameters.regionid,  
                     'title':parameters.title, 'label':parameters.label,'dataurl':parameters.dataurl, 
                     'metaurl':parameters.metaurl, 'metapath':parameters.metapath, 'dsid':dsid, 'dsversion':parameters.dsversion, 'accessdate':parameters.accessdate}
            '''
            query = {**query, **paramsD}
            print (query)

            print ("INSERT INTO %(system)s.datasets (instid, dsname, regionid, title, label, dataurl, metaurl, dsid, accessdate, dsversion, copyright) VALUES \
                ('%(instid)s', '%(dsname)s', '%(regionid)s', '%(title)s', '%(label)s', '%(dataurl)s', '%(metaurl)s','%(dsid)s', '%(accessdate)s', '%(dsversion)s', '%(copyright)s')" %query)
            self.cursor.execute("INSERT INTO %(system)s.datasets (instid, dsname, regionid, title, label, dataurl, metaurl, dsid, accessdate, dsversion, copyright) VALUES \
                ('%(instid)s', '%(dsname)s', '%(regionid)s', '%(title)s', '%(label)s', '%(dataurl)s', '%(metaurl)s','%(dsid)s', '%(accessdate)s', '%(dsversion)s', '%(copyright)s')" %query)
        elif rec != None and overwrite:
            self.cursor.execute("DELETE FROM %(system)s.datasets  WHERE dsid = '%(dsid)s';" %query )
            self.conn.commit()
            self.cursor.execute("INSERT INTO %(system)s.datasets (instid, dsname, regionid, title, label,dataurl, metaurl, metapath, datadir, datafile, dsid) VALUES ('%(instid)s', '%(dsname)s', '%(regionid)s', '%(title)s', '%(label)s', '%(dataurl)s', '%(metaurl)s', '%(metapath)s', '%(datadir)s', '%(datafile)s', '%(dsid)s')" %query)
        elif delete:
            self.cursor.execute("DELETE FROM %(system)s.datasets  WHERE dsid = '%(dsid)s';" %query )
            self.conn.commit()

    def _InsertCompDef(self,comp,title,label):
        InsertCompDef(self,comp, title, label)
        
    def _InsertCompProd(self, comp):
        InsertCompProd(self, comp)
        
    def _InsertLayer(self,layer,overwrite,delete):
        InsertLayer(self,layer,overwrite,delete)
        
    def _LinkDsCompid(self,dsid,compid,overwrite,delete):  
        query ={'dsid':dsid,'compid':compid} 
        self.cursor.execute("SELECT * FROM ancillary.dscompid WHERE dsid = '%(dsid)s' AND compid = '%(compid)s';" %query)
        recs = self.cursor.fetchone()
        if recs == None and not delete:
            self.cursor.execute("INSERT INTO ancillary.dscompid (dsid, compid) VALUES ('%(dsid)s', '%(compid)s' )" %query)           
            self.conn.commit()

    def _SelectComp(self, compQ):
        querystem = 'SELECT C.source, C.product, B.folder, B.band, B.prefix, C.suffix, C.masked, C.cellnull, C.celltype, B.measure, B.scalefac, B.offsetadd, B.dataunit '   
        query ='FROM %(system)s.compdefs AS B ' %compQ
        querystem = '%s %s ' %(querystem, query)
        query ='INNER JOIN %(system)s.compprod AS C ON (B.compid = C.compid)' %compQ
        querystem = '%s %s ' %(querystem, query)
        #query = {'system':system,'id':compid}
        querypart = "WHERE B.folder = '%(folder)s' AND B.band = '%(band)s'" %compQ
        querystem = '%s %s' %(querystem, querypart)
        #print ('querystem',querystem)
        self.cursor.execute(querystem)
        records = self.cursor.fetchall()
        params = ['source', 'product', 'folder', 'band', 'prefix', 'suffix', 'masked', 'cellnull', 'celltype', 'measure', 'scalefac', 'offsetadd', 'dataunit']
    
        if len(records) == 1:
            return dict(zip(params,records[0]))
        else:
            print ('querystem',querystem)
            ERRORCHECK
        
    def _InsertClimateIndex(self,queryL):
        self.cursor.execute("DELETE FROM climateindex.climindex WHERE index = '%(index)s';" %queryL[0])
        for query in queryL:
            self.cursor.execute("INSERT INTO climateindex.climindex (index, acqdate, acqdatestr, value) VALUES ('%(index)s', '%(acqdate)s', '%(acqdatestr)s', %(value)s);" %query)
            self.conn.commit()                    
                                
                                