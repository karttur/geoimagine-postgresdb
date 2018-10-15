'''
Created on 7 Oct 2018

@author: thomasgumbricht
'''

from geoimagine.postgresdb import PGsession
from geoimagine.postgresdb.compositions import InsertCompDef, InsertCompProd, InsertLayer, SelectComp
from base64 import b64encode
import netrc

from geoimagine.support.karttur_dt import Today

class ManageLandsat(PGsession):
    '''
    DB support for setting up processes
    '''

    def __init__(self):
        """The constructor connects to the database"""
        HOST = 'managelandsat'
        secrets = netrc.netrc()
        
        username, account, password = secrets.authenticators( HOST )

        pswd = b64encode(password.encode())
        #create a query dictionary for connecting to the Postgres server
        query = {'db':'postgres','user':username,'pswd':pswd}
        #Connect to the Postgres Server
        self.session = PGsession.__init__(self,query,'ManageSentinel')
                       
    def _SelectComp(self,system,comp):
        comp['system'] = system
        return SelectComp(self, comp)
    
    def _InsertTileCoords(self,query):
        #rec = self._SingleSearch(query,'sentinel', 'vectorsearches')
        self.cursor.execute("SELECT * FROM landsat.tilecoords WHERE path = '%(path)s' AND row = '%(row)s' AND dir = '%(dir)s' AND wrs = '%(wrs)s';" %query)
        record = self.cursor.fetchone()
        if record == None:
            self._InsertRecord(query, 'landsat', 'tilecoords')

    def _SearchTilesFromWSEN(self, west, south, east, north):
        query = {'west':west, 'south':south,'east':east,'north':north}
        #self.cursor.execute("SELECT mgrs,west,south,east,north,ullon,ullat,urlon,urlat,lrlon,lrlat,lllon,lllat, minx, miny, maxx, maxy FROM sentinel.tilecoords WHERE centerlon > %(west)s AND centerlon < %(east)s AND centerlat > %(south)s AND centerlat < %(north)s;" %query)
        self.cursor.execute("SELECT wrs,dir,path,row,west,south,east,north,ullon,ullat,urlon,urlat,lrlon,lrlat,lllon,lllat, minx, miny, maxx, maxy FROM landsat.tilecoords WHERE east > %(west)s AND west < %(east)s AND north > %(south)s AND south < %(north)s;" %query)
        #print ("SELECT wrs,dir,path,row,west,south,east,north,ullon,ullat,urlon,urlat,lrlon,lrlat,lllon,lllat, minx, miny, maxx, maxy FROM landsat.tilecoords WHERE east > %(west)s AND west < %(east)s AND north > %(south)s AND south < %(north)s;" %query)

        records = self.cursor.fetchall()
        return records
    
    def _InsertSingleLandsatRegion(self,queryD):
        '''
        '''
        tabkeys = (['regionid'],['prstr'],['dir'],['wrs'])
        #regionid,mgrs
        self._CheckInsertSingleRecord(queryD, 'landsat', 'regions', tabkeys) 
           
    def _SelectLandsatTileCoords(self, searchD):
        #construct where statement - LATER
        query = {}
        self.cursor.execute("SELECT wrs,dir,path,row,minx,miny,maxx,maxy,ullat,ullon,lrlat,lrlon,urlat,urlon,lllat,lllon FROM landsat.tilecoords;" %query)
        records = self.cursor.fetchall()
        return records
    
    def _SelectLandsatTileCoordsNoSentinelRegion(self):
        #construct where statement - LATER
        query = {}
        self.cursor.execute("SELECT wrs,dir,path,row,minx,miny,maxx,maxy,ullat,ullon,lrlat,lrlon,urlat,urlon,lllat,lllon FROM landsat.tilecoords;" %query)
        records = self.cursor.fetchall()
        return records