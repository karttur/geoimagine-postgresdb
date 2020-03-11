'''
Created on 8 juni 2018

@author: thomasgumbricht
'''

from geoimagine.postgresdb import PGsession
from geoimagine.postgresdb.compositions import InsertCompDef, InsertCompProd, InsertLayer, SelectComp, SelectLayer
from base64 import b64encode
import netrc

from geoimagine.support.karttur_dt import Today

class ManageSentinel(PGsession):
    '''
    DB support for setting up processes
    '''

    def __init__(self):
        """The constructor connects to the database"""
        HOST = 'managesentinel'
        HOST = 'karttur'
        secrets = netrc.netrc()

        username, account, password = secrets.authenticators( HOST )

        pswd = b64encode(password.encode())
        #create a query dictionary for connecting to the Postgres server
        query = {'db':'postgres','user':username,'pswd':pswd}
        #Connect to the Postgres Server
        self.session = PGsession.__init__(self,query,'ManageSentinel')



    def _InsertSentinelMODISTile(self, query):

        self.cursor.execute("SELECT * FROM sentinel.regions WHERE mgrs = '%(mgrs)s' AND regiontype = '%(regiontype)s' AND regionid = '%(regionid)s';" %query)
        record = self.cursor.fetchone()
        if record == None and not query['delete']:
            ##print aD['senssat'],aD['typeid'],aD['subtype'], filecat, tD['pattern'], tD['folder'], tD['band'], tD['prefix'],suffix, tD['celltype'],  tD['fileext']
            self.cursor.execute("INSERT INTO sentinel.regions (regionid, regiontype, mgrs, mgrsid, utm) VALUES (%s, %s, %s, %s, %s)",
                    (query['regionid'], query['regiontype'],query['mgrs'], query['mgrsid'], query['utmzone']))
            self.conn.commit()
        elif record and query['delete']:
            self.cursor.execute("DELETE FROM modis.regions WHERE mgrs = '%(mgrss' AND regiontype = '%(regiontype)s'AND regionid = '%(regionid)s';" %query)
            self.conn.commit()

    def _InsertSentinelRegionTile(self, query):
        self.cursor.execute("SELECT * FROM %(system)s.regions WHERE regionid = '%(regionid)s';"  %query)
        record = self.cursor.fetchone()
        if record == None:
            #print "SELECT * FROM regions WHERE regions.regionid = '%(regid)s' AND regioncat = '%(cat)s' AND type = '%(typ)s';"  %query
            warnstr = 'WARNING can not add tile to region %(regionid)s, no such region at category %(category)s and type %(type)s' %query
            print (warnstr)
            return
        self.cursor.execute("SELECT * FROM sentinel.regions WHERE mgrs = '%(mgrs)s' AND regiontype = '%(regiontype)s' AND regionid = '%(regionid)s';" %query)
        record = self.cursor.fetchone()

        if record == None and not query['delete']:
            ##print aD['senssat'],aD['typeid'],aD['subtype'], filecat, tD['pattern'], tD['folder'], tD['band'], tD['prefix'],suffix, tD['celltype'],  tD['fileext']
            self.cursor.execute("INSERT INTO sentinel.regions (regionid, regiontype, mgrs, mgrsid, utm) VALUES (%s, %s, %s, %s, %s)",
                    (query['regionid'], query['regiontype'],query['mgrs'], query['mgrsid'], query['utmzone']))
            self.conn.commit()
        elif record and query['delete']:
            self.cursor.execute("DELETE FROM modis.regions WHERE mgrs = '%(mgrss' AND regiontype = '%(regiontype)s'AND regionid = '%(regionid)s';" %query)
            self.conn.commit()

    def _SelectSentinelRegionTiles(self,query):
        #print ("SELECT path, row from regions.sentinel WHERE regionid = '%(regionid)s'" %query)
        self.cursor.execute("SELECT path, row from regions.sentinel WHERE regionid = '%(regionid)s'" %query)
        records = self.cursor.fetchall()
        return records

    def _GetMetaTranslator(self):
        #print (self.name)
        self.cursor.execute("SELECT * FROM sentinel.metatranslate")
        records = self.cursor.fetchall()
        recD = {}
        for row in records:
            recD[row[0]] = {'dst':row[1],'tab':row[2], 'typ':row[3]}
        return recD

    def _SelectComp(self,system,comp):
        comp['system'] = system
        return SelectComp(self, comp)

    def _SelectLayer(self,system,queryD,paramL):
        return self._SingleSearch(queryD,paramL,system,'layers',True)

    def _SelectLayerOld(self,system,comp):
        comp['system'] = system
        return SelectLayer(self, comp)

    def _SelectLayerOnLocus(self,system,queryD,paramL):
        rec = self._SingleSearch(queryD, paramL, system,' layers')
        return dict(zip(paramL,rec))

    def _InstertTileMeta(self,queryD):
        rec = self._CheckInsertSingleRecord(queryD,'sentinel', 'tilemeta', [('tileid',)])

    def _InsertGranuleMeta(self,queryD):
        rec = self._CheckInsertSingleRecord(queryD,'sentinel', 'granulemeta', [('granuleid',)])

    def _InstertTile(self,queryD):
        rec = self._CheckInsertSingleRecord(queryD,'sentinel', 'tiles', [('tileid',)])
        if rec != None:
            if rec[2] != queryD['mgrs']:
                print (rec)
                print (queryD)
                print (queryD['mgrs'],rec[2])
                BALLE

    def _InstertGranule(self,queryD):
        rec = self._CheckInsertSingleRecord(queryD,'sentinel', 'granules', [('granuleid',)])

    def _InsertVectorSearch(self,queryD):
        self._CheckInsertSingleRecord(queryD,'sentinel', 'vectorsearches')

    def _SelectVectorSearch(self,queryD,paramL):
        rec = self._SingleSearch(queryD,paramL,'sentinel', 'vectorsearches')
        return rec

    def _UpdateTileStatus(self, queryD):
        query = "UPDATE sentinel.tiles SET %(column)s = '%(status)s' WHERE tileid = '%(tileid)s'" %queryD
        self.cursor.execute(query)
        self.conn.commit()

    def _UpdateGranuleStatus(self, queryD):
        query = "UPDATE sentinel.granules SET %(column)s = '%(status)s' WHERE granuleid = '%(granuleid)s'" %queryD
        self.cursor.execute(query)
        self.conn.commit()

    def _SelectSentinelGranules(self,params, period, statusD):
        queryD = {}
        queryD['platformname'] = {'val':params.platformname, 'op':'=' }
        queryD['product'] = {'val':params.prodtype, 'op':'=' }
        if 'cloudcover' in statusD:
            queryD['cloudcover'] = {'val':params.cloudmax, 'op':'<=' }
        for status in statusD:
            queryD[status] = {'val':statusD[status], 'op':'=' }
        if period:
            datumkey = period.datumL[0]

            startdate = period.datumD[datumkey]['startdate']
            queryD['acqdate'] = {'val':startdate, 'op':'>=' }
            enddate = period.datumD[datumkey]['enddate']
            queryD['#acqdate'] = {'val':enddate, 'op':'<=' }
            if period.datumD[datumkey]['enddoy'] > 0:

                startdoy = period.datumD[datumkey]['startdoy']
                queryD['doy'] = {'val':startdoy, 'op':'>=' }
                enddoy = period.datumD[datumkey]['enddoy']
                queryD['#doy'] = {'val':enddoy, 'op':'<=' }

        #if params.orbitdirection.upper() != 'B':
        #    pass
        wherestr = self._DictToSelect(queryD)
        query = "SELECT uuid, granuleid, source, product, folder, acqdate, orbitid FROM sentinel.granulemeta \
                JOIN sentinel.granules USING (granuleid, product) \
                %s;" %(wherestr)
        print (query)
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def _SelectSentinelTiles(self,params, period, statusD):
        queryD = {}
        queryD['m.platformname'] = {'val':params.platformname, 'op':'=' }
        queryD['t.product'] = {'val':params.prodtype, 'op':'=' }

        for status in statusD:
            queryD[status] = {'val':statusD[status], 'op':'=' }
        if 'cloudcover' in statusD:
            #overwrites and cloudcover from above
            queryD['t.cloudcover'] = {'val':params.cloudmax, 'op':'<=' }
        datumkey = period.datumL[0]

        startdate = period.datumD[datumkey]['startdate']
        queryD['t.acqdate'] = {'val':startdate, 'op':'>=' }
        enddate = period.datumD[datumkey]['enddate']
        queryD['#t.acqdate'] = {'val':enddate, 'op':'<=' }
        if period.datumD[datumkey]['enddoy'] > 0:

            startdoy = period.datumD[datumkey]['startdoy']
            queryD['t.doy'] = {'val':startdoy, 'op':'>=' }
            enddoy = period.datumD[datumkey]['enddoy']
            queryD['#t.doy'] = {'val':enddoy, 'op':'<=' }

        if params.orbitdirection.upper() != 'B':
            BALLE
        wherestr = self._DictToSelect(queryD)

        query = "SELECT DISTINCT ON (m.uuid) m.uuid, t.tileid, t.source, t.product, t.folder, t.acqdate, t.orbitid, t.utm, t.mgrsid, t.mgrs \
            FROM sentinel.tilemeta AS M \
            INNER JOIN sentinel.tiles AS T ON (M.tileid = T.tileid) "
        if 'r.regionid' in statusD:
            query += "INNER JOIN sentinel.regions AS R ON (T.mgrs = R.mgrs) "
        query += wherestr
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def _SelectSentinelTemplate(self,queryD,paramL):
        #return self._SingleSearch(queryD,'modis','template',paramL)
        return self._MultiSearch(queryD,paramL,'sentinel','template')

    def _InsertLayer(self,layer, overwrite = False, delete = False):
        InsertLayer(self,layer,overwrite, delete)

    def _InsertTileCoords(self,query):
        '''
        #rec = self._SingleSearch(query,'sentinel', 'vectorsearches')
        '''
        self.cursor.execute("SELECT * FROM sentinel.tilecoords WHERE mgrs = '%(mgrs)s';" %query)
        record = self.cursor.fetchone()
        if record == None:
            self._InsertRecord(query, 'sentinel', 'tilecoords')
        else:
            search = {'mgrs':query['mgrs']}
            query.pop('mgrs')
            self._UpdateRecord(query, 'sentinel', 'tilecoords', search)

    def _SelectSentinelTile(self,query):
        self.cursor.execute("SELECT * FROM sentinel.tilecoords WHERE mgrs = '%(mgrs)s';" %query)
        return self.cursor.fetchone()

    def _SelectSentinelTileCoords(self, searchD):
        #construct where statement - LATER
        query = {}
        self.cursor.execute("SELECT epsg, mgrs,utmzone,mgrsid,minx,miny,maxx,maxy,ullat,ullon,lrlat,lrlon,urlat,urlon,lllat,lllon FROM sentinel.tilecoords;" %query)
        records = self.cursor.fetchall()
        return records

    def _SelectAllDefRegionsOld(self,wherestatement):
        print ('wherestatement',wherestatement)
        return SelectAllDefRegions(self,'sentinel','regions',wherestatement)


    def _SelectAllDefRegions(self, wherestatement = '' ):
        query = {'schema': 'sentinel', 'table':'regions', 'where':wherestatement}
        if wherestatement == '':
            self.cursor.execute("SELECT regioncat, regionid FROM system.defregions;" %query)

        else:
            #print ("SELECT DISTINCT R.regioncat, R.regionid FROM system.defregions R LEFT JOIN %(schema)s.%(table)s M ON (R.regionid = M.regionid) WHERE %(where)s;" %query)
            print ("SELECT DISTINCT R.regioncat, R.regionid FROM system.defregions R LEFT JOIN %(schema)s.%(table)s M ON (R.regionid = M.regionid) %(where)s;" %query)
            self.cursor.execute("SELECT DISTINCT R.regioncat, R.regionid FROM system.defregions R LEFT JOIN %(schema)s.%(table)s M ON (R.regionid = M.regionid)  %(where)s;" %query)
        return self.cursor.fetchall()
        #return SelectAllDefRegions(self,'modis','regions',wherestatement)

    def _InsertMGRSCoords(self,query):
        #rec = self._SingleSearch(query,'sentinel', 'vectorsearches')
        self.cursor.execute("SELECT * FROM sentinel.mgrscoords WHERE mgrs = '%(mgrs)s';" %query)
        record = self.cursor.fetchone()
        if record == None:
            self._InsertRecord(query, 'sentinel', 'mgrscoords')
        else:
            search = {'mgrs':query['mgrs']}
            query.pop('mgrs')
            self._UpdateRecord(query, 'sentinel', 'mgrscoords', search)

    def _InsertSentinelTileCoordOld(self,hvtile,h,v,ulxsin,ulysin,lrxsin,lrysin,ullat,ullon,lrlon,lrlat,urlon,urlat,lllon,lllat):
        query = {'hvtile':hvtile}
        #source, product, folder, band, prefix, suffix, fileext, celltype, dataunit, compid, hdfgrid, hdffolder, scalefactor, offsetadd, cellnull, retrieve, ecode
        self.cursor.execute("SELECT * FROM sentinel.tilecoords WHERE hvtile = '%(hvtile)s';" %query)
        record = self.cursor.fetchone()
        if record == None:
            self.cursor.execute("INSERT INTO sentinel.tilecoords (hvtile,h,v,minxsin,maxysin,maxxsin,minysin,ullat,ullon,lrlon,lrlat,urlon,urlat,lllon,lllat) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ;",
                                (hvtile,h,v,ulxsin,ulysin,lrxsin,lrysin,ullat,ullon,lrlon,lrlat,urlon,urlat,lllon,lllat))
            self.conn.commit()

    def _SearchMGRSfromCentroid(self,lon,lat):
        query = {'lon':lon, 'lat':lat}
        self.cursor.execute("SELECT mgrs,west,south,east,north,ullon,ullat,urlon,urlat,lrlon,lrlat,lllon,lllat FROM sentinel.tilecoords WHERE %(lon)s > west AND %(lon)s < east AND %(lat)s > south and %(lat)s < north;" %query)
        records = self.cursor.fetchall()
        return records

    def _SearchTilesFromWSEN(self, west, south, east, north):
        query = {'west':west, 'south':south,'east':east,'north':north}
        #self.cursor.execute("SELECT mgrs,west,south,east,north,ullon,ullat,urlon,urlat,lrlon,lrlat,lllon,lllat, minx, miny, maxx, maxy FROM sentinel.tilecoords WHERE centerlon > %(west)s AND centerlon < %(east)s AND centerlat > %(south)s AND centerlat < %(north)s;" %query)
        self.cursor.execute("SELECT mgrs,west,south,east,north,ullon,ullat,urlon,urlat,lrlon,lrlat,lllon,lllat, minx, miny, maxx, maxy FROM sentinel.tilecoords WHERE east > %(west)s AND west < %(east)s AND north > %(south)s AND south < %(north)s;" %query)
        #self.cursor.execute("SELECT epsg, mgrs,utmzone,mgrsid,minx,miny,maxx,maxy,ullat,ullon,lrlat,lrlon,urlat,urlon,lllat,lllon FROM sentinel.tilecoords;" %query)
        records = self.cursor.fetchall()
        return records

    def _SearchMGRSFromWSEN(self, west, south, east, north, sentinel):
        query = {'west':west, 'south':south,'east':east,'north':north,'sentinel':sentinel}
        if sentinel:
            self.cursor.execute("SELECT mgrs,west,south,east,north FROM sentinel.mgrscoords WHERE east > %(west)s AND west < %(east)s AND north > %(south)s AND south < %(north)s AND sentinel = '%(sentinel)s';" %query)
        else:
            self.cursor.execute("SELECT mgrs,west,south,east,north FROM sentinel.mgrscoords WHERE east > %(west)s AND west < %(east)s AND north > %(south)s AND south < %(north)s;" %query)
        records = self.cursor.fetchall()
        return records

    def _InsertGranuleTiles(self, granuleid, tileD):
        query = {'granuleid':granuleid}
        self.cursor.execute("SELECT mgrs FROM sentinel.granuletiles WHERE granuleid = '%(granuleid)s';" %query)
        records = self.cursor.fetchall()
        mgrsL = [item[0] for item in records]
        for tile in tileD:
            if tile not in mgrsL:
                query['tile'] = tile
                query['overlap'] = tileD[tile]
                self.cursor.execute("INSERT INTO sentinel.granuletiles (granuleid, mgrs, overlap) VALUES ('%(granuleid)s', '%(tile)s', %(overlap)s)" %query)
                self.conn.commit()

    def _SelectGranuleTiles(self, granuleid,overlap):
        #query = {'granuleid':granuleid}
        query = {'granuleid':granuleid,'overlap':overlap}
        self.cursor.execute("SELECT mgrs FROM sentinel.granuletiles WHERE granuleid = '%(granuleid)s' and overlap >= %(overlap)s;" %query)
        records = self.cursor.fetchall()
        mgrsL = [item[0] for item in records]
        return mgrsL

    def _GetGranuleMeta(self, granuleid):
        query = {'granuleid':granuleid}
        #print ("SELECT product, proclevel, orbitnr, orbitdir, cloudcover, sensopmode, s2datatakeid, procbase, platformid, platformname, instrument \
        #FROM sentinel.granulemeta WHERE granuleid = '%(granuleid)s';" %query)
        self.cursor.execute("SELECT product, proclevel, orbitnr, orbitdir, cloudcover, sensopmode, s2datatakeid, procbase, platformid, platformname, instrument \
        FROM sentinel.granulemeta WHERE granuleid = '%(granuleid)s';" %query)
        record = self.cursor.fetchone()
        return record

    def _GetGranuleTile(self, granuleid):
        query = {'granuleid':granuleid}
        #print ("SELECT orbitid, acqdate, acqtime, sunazimuth, sunelevation, doy, source, product, folder, filetype, filename, downloaded, organized, exploded, deleted, declouded, maskstatus, metacheck, tgnote \
        #FROM sentinel.granules WHERE granuleid = '%(granuleid)s';" %query)
        self.cursor.execute("SELECT orbitid, acqdate, acqtime, sunazimuth, sunelevation, doy, source, product, folder, filetype, filename, downloaded, organized, exploded, deleted, declouded, maskstatus, metacheck, tgnote \
        FROM sentinel.granules WHERE granuleid = '%(granuleid)s';" %query)
        record = self.cursor.fetchone()
        return record

    def _SelectMGRS(self,mgrs):
        query = {'mgrs':mgrs}
        self.cursor.execute("SELECT utmzone,mgrsid,proj4,minx,miny,maxx,maxy,refsize,refcols,reflins FROM sentinel.tilecoords WHERE mgrs = '%(mgrs)s'" %query)
        record = self.cursor.fetchone()
        if record == None:
            print ("SELECT utmzone,mgrsid,proj4,minx,miny,maxx,maxy,refsize,refcols,reflins FROM sentinel.tilecoords WHERE mgrs = '%(mgrs)s'" %query)
        return record

    def _InsertSingleSentinelRegion(self,queryD):
        '''
        '''
        tabkeys = (['regionid'],['mgrs'])
        #regionid,mgrs
        self._CheckInsertSingleRecord(queryD, 'sentinel', 'regions', tabkeys)

    def _SelectRegionTiles(self,queryD):
        '''
        '''
        #print ("SELECT mgrs, utm, mgrsid FROM sentinel.regions WHERE regionid = '%(regionid)s' and regiontype = '%(regiontype)s';" %queryD)
        self.cursor.execute("SELECT mgrs, utm, mgrsid FROM sentinel.regions WHERE regionid = '%(regionid)s' and regiontype = '%(regiontype)s';" %queryD)
        records = self.cursor.fetchall()
        return (records)

    def _SelectUniqueRegionTiles(self,queryD):
        '''
        '''
        #print ("SELECT mgrs, utm, mgrsid FROM sentinel.regions WHERE regionid = '%(regionid)s' and regiontype = '%(regiontype)s';" %queryD)
        self.cursor.execute("SELECT mgrs, utm, mgrsid FROM sentinel.regions WHERE regionid = '%(regionid)s' and regiontype = '%(regiontype)s';" %queryD)
        records = self.cursor.fetchall()
        return (records)

    def _SelectSentineRegions(self,queryD):
        '''
        '''
        self.cursor.execute("SELECT DISTINCT ON (regionid) regionid FROM sentinel.regions WHERE regiontype = '%(regiontype)s';" %queryD)
        records = self.cursor.fetchall()
        regionidL = [item[0] for item in records]
        return regionidL
