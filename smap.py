'''
Created on 10 Oct 2018

@author: thomasgumbricht
'''

from geoimagine.postgresdb import PGsession
from geoimagine.postgresdb.compositions import InsertCompDef, InsertCompProd, InsertLayer, SelectComp
from base64 import b64encode
import netrc

#from geoimagine.support.karttur_dt import Today

class ManageSMAP(PGsession):
    '''
    DB support for setting up processes
    '''

    def __init__(self):
        """The constructor connects to the database"""
        HOST = 'managesmap'
        secrets = netrc.netrc()
        username, account, password = secrets.authenticators( HOST )
        pswd = b64encode(password.encode())
        #create a query dictionary for connecting to the Postgres server
        query = {'db':'postgres','user':username,'pswd':pswd}
        #Connect to the Postgres Server
        self.session = PGsession.__init__(self,query,'ManageSMAP')
        
    def _InsertSmapData(self,queryD):
        '''
        '''
        rec = self._CheckInsertSingleRecord(queryD,'smap', 'daacdata', [('smapid',),('product',),('version',)])

    def _SelectSmapData(self,period,params,statusD):
        '''
        '''
        queryD = {}

        queryD['product'] = {'val':params.product, 'op':'=' }
        queryD['version'] = {'val':params.version, 'op':'=' }

        for status in statusD:
            queryD[status] = {'val':statusD[status], 'op':'=' }
        queryD['acqdate'] = {'val':period.startdate, 'op':'>=' }
        queryD['#acqdate'] = {'val':period.enddate, 'op':'<=' }
        if period.enddoy > 0 and period.enddoy > period.startdoy:
            queryD['doy'] = {'val':period.startdoy, 'op':'>=' }
            queryD['#doy'] = {'val':period.enddoy, 'op':'<=' }

        wherestr = self._DictToSelect(queryD)

        query = "SELECT smapfilename, smapid, source, product, version, folder, acqdate FROM smap.daacdata \
                %s;" %(wherestr)
        print (query)
        self.cursor.execute(query)
        return self.cursor.fetchall()
    
    def _UpdateSmapStatus(self, queryD):
        query = "UPDATE smap.daacdata SET %(column)s = '%(status)s' WHERE smapid = '%(smapid)s'" %queryD
        self.cursor.execute(query)
        self.conn.commit()
        
    def _SelectTemplateLayersOnSource(self,query,paramL):
        return self._MultiSearch(query, paramL, 'smap', 'template')
    
    def _SelectSingleSMAPDaacTile(self, queryD, paramL):
        return self._SingleSearch(queryD, paramL, 'smap','daacdata')
    
    def _SelectTemplateLayersOnGrid(self,query,paramL):
        return self._SingleSearch(query, paramL, 'smap', 'template')
    
    def _SelectSMAPTemplate(self,queryD,paramL):
        return self._MultiSearch(queryD,paramL,'smap','template')
        
    def _InsertLayer(self,layer,overwrite,delete):
        InsertLayer(self,layer,overwrite,delete)
        
    def _SelectComp(self, compQ):
        return SelectComp(self, compQ)
    
    
        
    def _SelectCompOld(self, system, compQ):
        
        
        '''This is identical to came def in ancillary - mshould be joined
        '''
        querystem = 'SELECT C.source, C.product, B.folder, B.band, B.prefix, C.suffix, C.masked, C.cellnull, C.celltype, B.measure, B.scalefac, B.offsetadd, B.dataunit '   
        query ='FROM %(system)s.compdefs AS B ' %compQ
        querystem = '%s %s ' %(querystem, query)
        query ='INNER JOIN %(system)s.compprod AS C ON (B.compid = C.compid)' %compQ
        querystem = '%s %s ' %(querystem, query)
        #query = {'system':system,'id':compid}
        querypart = "WHERE B.folder = '%(folder)s' AND B.band = '%(band)s'" %compQ
        querystem = '%s %s' %(querystem, querypart)
        print ('querystem',querystem)
        self.cursor.execute(querystem)
        records = self.cursor.fetchall()
        params = ['source', 'product', 'folder', 'band', 'prefix', 'suffix', 'masked', 'cellnull', 'celltype', 'measure', 'scalefac', 'offsetadd', 'dataunit']
    
        if len(records) == 1:
            return dict(zip(params,records[0]))
        elif len(records) > 1:
            querypart = "AND C.suffix = '%(suffix)s'" %compQ
            querystem = '%s %s' %(querystem, querypart)
            print ('querystem',querystem)
            self.cursor.execute(querystem)
            records = self.cursor.fetchall()
            if len(records) == 1: 
                return dict(zip(params,records[0]))
            else:
                print ('querystem',querystem)
                ERRORINANCILLARY    
        else:
            print ('querystem',querystem)
            ERRORINANCILLARY