'''
Created on 23 feb. 2018

@author: thomasgumbricht
'''

from geoimagine.postgresdb import PGsession
from base64 import b64encode
import netrc

#from geoimagine.support.karttur_dt import Today

class ManageMODIS(PGsession):
    '''
    DB support for setting up processes
    '''

    def __init__(self):
        """The constructor connects to the database"""
        HOST = 'managemodis'
        secrets = netrc.netrc()
        username, account, password = secrets.authenticators( HOST )
        pswd = b64encode(password.encode())
        #create a query dictionary for connecting to the Postgres server
        query = {'db':'postgres','user':username,'pswd':pswd}
        #Connect to the Postgres Server
        self.session = PGsession.__init__(self,query,'ManageMODIS')
        
    def _InsertModisTileCoord(self,hvtile,h,v,ulxsin,ulysin,lrxsin,lrysin,ullat,ullon,lrlon,lrlat,urlon,urlat,lllon,lllat):
        query = {'hvtile':hvtile}
        #source, product, folder, band, prefix, suffix, fileext, celltype, dataunit, compid, hdfgrid, hdffolder, scalefactor, offsetadd, cellnull, retrieve, ecode
        self.cursor.execute("SELECT * FROM modis.tilecoords WHERE hvtile = '%(hvtile)s';" %query)
        record = self.cursor.fetchone()
        if record == None:
            self.cursor.execute("INSERT INTO modis.tilecoords (hvtile,h,v,minxsin,maxysin,maxxsin,minysin,ullat,ullon,lrlon,lrlat,urlon,urlat,lllon,lllat) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ;", 
                                (hvtile,h,v,ulxsin,ulysin,lrxsin,lrysin,ullat,ullon,lrlon,lrlat,urlon,urlat,lllon,lllat))
            self.conn.commit()
     
    def _SelectModisTileCoords(self, searchD):
        #construct where statement - LATER
        query = {}
        self.cursor.execute("SELECT hvtile,h,v,minxsin,minysin,maxxsin,maxysin,ullat,ullon,lrlat,lrlon,urlat,urlon,lllat,lllon FROM modis.tilecoords;" %query)
        records = self.cursor.fetchall()
        return records        
                      
    def _InsertModisRegionTile(self, query):
        '''
        print ("SELECT * FROM %(system)s.regions WHERE regionid = '%(regionid)s';"  %query)
        print (self.name)
        print ('query',query)
        print ("SELECT * FROM %(system)s.regions WHERE regionid = '%(regionid)s';"  %query)
        '''
        self.cursor.execute("SELECT * FROM %(system)s.regions WHERE regionid = '%(regionid)s';"  %query)
        record = self.cursor.fetchone()
        if record == None:
            #print "SELECT * FROM regions WHERE regions.regionid = '%(regid)s' AND regioncat = '%(cat)s' AND type = '%(typ)s';"  %query
            warnstr = 'WARNING can not add tile to region %(regionid)s, no such region at category %(category)s and type %(type)s' %query
            print (warnstr)
            return
        self.cursor.execute("SELECT * FROM modis.regions WHERE htile = %(h)s AND vtile = %(v)s AND regiontype = '%(regiontype)s' AND regionid = '%(regionid)s';" %query)
        record = self.cursor.fetchone()
        
        if record == None and not query['delete']:
            ##print aD['senssat'],aD['typeid'],aD['subtype'], filecat, tD['pattern'], tD['folder'], tD['band'], tD['prefix'],suffix, tD['celltype'],  tD['fileext']
            self.cursor.execute("INSERT INTO modis.regions (regionid, regiontype, htile, vtile) VALUES (%s, %s, %s, %s)",
                    (query['regionid'], query['regiontype'],query['h'], query['v']))
            self.conn.commit()
        elif record and query['delete']:
            self.cursor.execute("DELETE FROM modis.regions WHERE htile = '%(h)s' AND vtile = '%(v)s' AND regiontype = '%(regiontype)s'AND regionid = '%(regionid)s';" %query)
            self.conn.commit()
    
    def _SelectModisRegionTiles(self,query):
        print ("SELECT path, row from modis.regions WHERE regionid = '%(regionid)s'" %query)
        self.cursor.execute("SELECT path, row from modis.regions WHERE regionid = '%(regionid)s'" %query)
        records = self.cursor.fetchall()
        return records
    
    def _DeleteBulkTiles(self,params,acqdate):
        query = {'prod':params.product, 'v':params.version, 'acqdate':acqdate}
        self.cursor.execute("DELETE FROM modis.datapooltiles WHERE product= '%(prod)s' AND version = '%(v)s' AND acqdate = '%(acqdate)s';" %query)     
        self.conn.commit()
        
    def _LoadBulkTiles(self,params,acqdate,tmpFPN,headL):
        self._DeleteBulkTiles(params,acqdate)
        query = {'tmpFPN':tmpFPN, 'items': ",".join(headL)}
        print ("COPY modis.datapooltiles (%(items)s) FROM '%(tmpFPN)s' DELIMITER ',' CSV HEADER;" %query)
        #self.cursor.execute("COPY modis.datapooltiles (%(items)s) FROM '%(tmpFPN)s' DELIMITER ',' CSV HEADER;" %query)
        #print ("%(tmpFPN)s, 'modis.datapooltiles', columns= (%(items)s), sep=','; "%query)
        with open(tmpFPN, 'r') as f:
            next(f)  # Skip the header row.
            self.cursor.copy_from(f, 'modis.datapooltiles', sep=',')
            self.conn.commit()
            #cur.copy_from(f, 'test', columns=('col1', 'col2'), sep=",")
                
    
        
        