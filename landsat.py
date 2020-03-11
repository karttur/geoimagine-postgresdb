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
        HOST = 'karttur'
        secrets = netrc.netrc()

        username, account, password = secrets.authenticators( HOST )

        pswd = b64encode(password.encode())
        #create a query dictionary for connecting to the Postgres server
        query = {'db':'postgres','user':username,'pswd':pswd}
        #Connect to the Postgres Server
        self.session = PGsession.__init__(self,query,'ManageLandsat')

    def _ManageMetaDb(self,process,colD):
        '''Manage USGS metadata definitions for Landsat data'''
        sensor = process.params.sensor
        query = {'sens':sensor,'col':colD['column']}
        self.cursor.execute("SELECT * FROM landsat.meta_links WHERE sensor = '%(sens)s' AND metacolumn = '%(col)s';" %query)
        record = self.cursor.fetchone()
        if colD['required']: req = 'Y'
        else: req = 'N'
        if record == None and not process.delete:
            self.cursor.execute("INSERT INTO landsat.meta_links (sensor,csvheader,metatable,metacolumn,metatype,metalength,required,defaultvalue) VALUES (%s,%s, %s, %s, %s, %s, %s, %s)",
                           (sensor,colD['csv'],colD['table'],colD['column'],colD['type'],colD['length'],req,colD['default']))
            self.conn.commit()
        elif record and process.overwrite:
            self.cursor.execute("UPDATE landsat.meta_links SET (csvheader,metatable,metatype,metalength,required,defaultvalue) = (%s, %s, %s, %s, %s, %s) WHERE sensor = %s and metacolumn = %s",
                           (colD['csv'],colD['table'],colD['type'],colD['length'],req,colD['default'],sensor,colD['column']))
            self.conn.commit()
        elif process.delete:
            self.cursor.execute("DELETE FROM landsat.meta_links WHERE sensor = %s and metacolumns = %s",
                           (sensor,colD['column']))
        self.conn.commit()
        table_name = 'meta_%s' %(colD['table'])
        table_name = table_name.lower()
        query = {'schema': 'landsat', 'tab':table_name, 'col':colD['column'].lower()}
        self.cursor.execute("SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='%(schema)s' AND table_name='%(tab)s' AND column_name='%(col)s');" %query)
        rec = self.cursor.fetchone()
        if rec[0]:
            pass
        else:
            if not process.delete:
                #create the column
                query = "ALTER TABLE landsat.%s ADD COLUMN %s %s" %(table_name,colD['column'].lower(),colD['type'])
                if colD['type'] in ['char', 'varchar']:
                    query = '%s(%s)' %(query,colD['length'])
                query = '%s;' %(query)
                print (query)
                #self.cursor.execute(query)
                #self.conn.commit()

    def _ManageBulkMetaUrl(self,queryD):
        rec = self._CheckInsertSingleRecord(queryD,'landsat', 'usgs_bulkmeta', [('collection','level','sensor')])

    def _SelectBulkMetaUrl(self,queryD,paramL):
        return self._SingleSearch(queryD, paramL, 'landsat', 'usgs_bulkmeta')

    def _SelectMetaTranslate(self,filetype,query):

        if filetype == 'csv':
            self.cursor.execute("SELECT csvheader, metatable, metacolumn, required, defaultvalue FROM landsat.meta_links WHERE sensor = '%(sensor)s';" %query)
        else:
            NOTDONE
            self.cursor.execute("SELECT xmlparent, xmlelement,  metatable, metacolumn, required, defaultvalue FROM landsat.meta_links WHERE sensor = '%(sensor)s';" %query)
        records = self.cursor.fetchall()
        return records

    def _CopyBulkMeta(self, metaFPND, headerD, copyfile):
        for key in metaFPND:
            #if key in ['main','url','geo','sub']:
            #    continue

            query = {'tab':key, 'c':copyfile, 'f':metaFPND[key]}
            printstr = '    Deleting all %(c)s data from table landsat.meta_%(tab)s' %query
            print (printstr)
            print ("DELETE FROM landsat.meta_%(tab)s WHERE copyfile = '%(c)s';" %query)
            self.cursor.execute("DELETE FROM landsat.meta_%(tab)s WHERE copyfile = '%(c)s';" %query)

            tab = 'landsat.meta_%(tab)s' %query
            printstr = '    Copying %(c)s csv data (%(f)s) to table landsat.meta_%(tab)s' %query
            print (printstr)
            f = open(metaFPND[key], 'r')
            next(f)
            #'sceneid', 'satelnr', 'sensorid', 'acqdate', 'update', 'path', 'row', 'datatypel1', 'acqdoy'
            #self.cursor.copy_from(f, tab, sep=',', columns=('sceneid', 'satelnr', 'sensorid', 'acqdate', 'update', 'path', 'row', 'datatypel1', 'acqdoy') )
            print (metaFPND[key], tab, headerD[key])
            self.cursor.copy_from(f, tab, sep=',', columns=(headerD[key]) )
            self.conn.commit()
            f.close()

    def ManageBulkMeta(self, metaFPND):
        from os import path, makedirs, remove
        from shutil import move

        #set the schema
        schema = 'landsat'
        #increase buffer size
        self.cursor.execute("SET temp_buffers = '2GB';")
        #create tmp folder
        tmpFP = path.split(metaFPND['main'])[0]
        tmpFP = path.join(tmpFP,'tmp')
        if not path.exists(tmpFP):
            makedirs(tmpFP)
        #Step 1, csv files into temporary tables
        for key in metaFPND:
            #set the table name
            table = 'meta_%(key)s' %{'key':key}
            headerL, tableKeyL, origtab, tmptab = self.BulkMetaInitial(metaFPND[key], schema, table)
            #ADD NEW RECORDS
            self.BulkMetaAddNew(origtab,tmptab,headerL,tmpFP,key)
            #Only update if the input represents a higher collection, not pre collection
            #if collection in ['01','02','03','04']:
            #UPDATE EXISTING RECORDS
            self.BulkMetaUpdate(schema,table,tmptab,headerL,tableKeyL,tmpFP,key)
            if key == 'main':
                self.BulkMetaMainUpdate(schema,table,tmptab,headerL,tableKeyL,tmpFP,key)
        #move the bulk metafile
        #Update the setdate for the metafile
        query = {'regdate':regdate, 'srcFPN':srcFPN}
        self.cursor.execute("UPDATE landsat.usgs_bulkmeta SET latestdate = '%(regdate)s' WHERE csvlocal = '%(srcFPN)s';" %query)
        self.conn.commit()
        #If the process reahced here the file is OK; put in the 'retired' folder
        tarFP,srcFN = path.split(srcFPN)
        tarFP = path.join(tarFP,'retired')
        if not path.exists(tarFP):
            makedirs(tarFP)
        tarFN,srcFext = path.splitext(srcFN)
        tarFN = '%(t)s_%(d)s%(e)s' %{'t':tarFN,'d':mj_dt.DateToStrDate(regdate),'e':srcFext}
        tarFPN = path.join(tarFP,tarFN)
        if path.isfile(tarFPN):
            #remove existing retired file and add the present (only happens with playing around)
            remove(tarFPN)
        print ('moving to retired', srcFPN, tarFPN)
        move(srcFPN, tarFPN)

    def _SelectUSGSLandsatScenes(self, period, statusD, paramL):

        statusD['M.acqdate'] = {'val':period.startdate, 'op':'>=' }
        statusD['#M.acqdate'] = {'val':period.enddate, 'op':'<=' }
        if period.enddoy > 0 and period.enddoy > period.startdoy:
            statusD['M.acqdoy'] = {'val':period.startdoy, 'op':'>=' }
            statusD['#M.acqdoy'] = {'val':period.enddoy, 'op':'<=' }
        if len(paramL) == 1:
            cols = paramL[0]
        else:
            cols =  ','.join(paramL)

        wherestr = self._DictToSelect(statusD)

        if statusD['l.downloaded']['val'] == 'N':
            #If only finding tiles not previosly downloaded

            qD = {'cols':cols,'where':wherestr}
            #qD = {'cols':cols,'where':wherestr}
            query = "SELECT %(cols)s FROM landsat.meta_main M\
                    LEFT JOIN landsat.meta_sub S USING (sceneid)\
                    LEFT JOIN landsat.meta_coll C USING (sceneid)\
                    LEFT JOIN landsat.meta_geo G USING (sceneid)\
                    LEFT JOIN landsat.scenes L USING (lsatprodid)\
                    %(where)s ORDER BY m.acqdate;" %qD

            self.cursor.execute(query)
            recs = self.cursor.fetchall()

            #Reset statusD['L.downloaded'] to find null (unregistered local scenes)
            statusD['l.downloaded'] = {'val':'NULL', 'op':'IS' }

            wherestr = self._DictToSelect(statusD)
            wherestr = wherestr.replace("'NULL'", "NULL")
            qD = {'cols':cols,'where':wherestr}
            query = "SELECT %(cols)s FROM landsat.meta_main M\
                    LEFT JOIN landsat.meta_sub S USING (sceneid)\
                    LEFT JOIN landsat.meta_coll C USING (sceneid)\
                    LEFT JOIN landsat.meta_geo G USING (sceneid)\
                    LEFT JOIN landsat.scenes L USING (lsatprodid)\
                    %(where)s ORDER BY m.acqdate;" %qD
            self.cursor.execute(query)
            recs2 = self.cursor.fetchall()
            recs.extend(recs2)
        else:
            qD = {'cols':cols,'where':wherestr}
            #qD = {'cols':cols,'where':wherestr}
            query = "SELECT %(cols)s FROM landsat.meta_main M\
                    LEFT JOIN landsat.meta_sub S USING (sceneid)\
                    LEFT JOIN landsat.meta_coll C USING (sceneid)\
                    LEFT JOIN landsat.meta_geo G USING (sceneid)\
                    LEFT JOIN landsat.scenes L USING (lsatprodid)\
                    %(where)s ORDER BY m.acqdate;" %qD
            self.cursor.execute(query)
            recs = self.cursor.fetchall()

        return recs

    def _SelectLocalLandsatScenes(self, period, statusD, paramL, verbose=False):

        statusD['acqdate'] = {'val':period.startdate, 'op':'>=' }
        statusD['#acqdate'] = {'val':period.enddate, 'op':'<=' }
        if period.enddoy > 0 and period.enddoy > period.startdoy:
            statusD['acqdoy'] = {'val':period.startdoy, 'op':'>=' }
            statusD['#acqdoy'] = {'val':period.enddoy, 'op':'<=' }
        if len(paramL) == 1:
            cols = paramL[0]
        else:
            cols =  ','.join(paramL)

        wherestr = self._DictToSelect(statusD)

        qD = {'cols':cols,'where':wherestr}

        query = "SELECT %(cols)s FROM landsat.scenes\
                %(where)s ORDER BY acqdate;" %qD
        if verbose:
            print (query)
        self.cursor.execute(query)
        recs = self.cursor.fetchall()
        if len(recs) == 0:
            print (query)
        return recs

    def _InsertSceneTemplate(self, queryD, overwrite, delete):
        self._CheckInsertSingleRecord(queryD, 'landsat', 'templatescenes', ([]), overwrite, delete)

    def _InsertLayerTemplate(self, queryD, overwrite, delete):
        self._CheckInsertSingleRecord(queryD, 'landsat', 'templatelayers', ([]), overwrite, delete)

    def _InsertBandDos(self, queryD, overwrite, delete):
        self._CheckInsertSingleRecord(queryD, 'landsat', 'banddos', ([]), overwrite, delete)

    def _SelectBandDos(self,queryD,paramL):
        recs = self._MultiSearch(queryD, paramL, 'landsat', 'banddos', True)
        return recs

    def _InsertSceneDos(self, queryD, overwrite, delete):
        self._CheckInsertSingleRecord(queryD, 'landsat', 'scenedos', ([]), overwrite, delete)

    def _SelectSceneDos(self,queryD,paramL):
        rec = self._SingleSearch(queryD, paramL, 'landsat', 'scenedos', False)
        return rec

    def _InsertDOStoSRFItrans(self, queryD, overwrite, delete):
        self._CheckInsertSingleRecord(queryD, 'landsat', 'dossrfi', (['lsatprodid'],['suffix']), overwrite, delete)

    def _SelectSceneTemplate(self,queryD,paramL):
        recs = self._MultiSearch(queryD, paramL, 'landsat', 'templatelayers',True)
        return recs

    def _SelectSceneBand(self,queryD,paramL):
        rec = self._SingleSearch(queryD, paramL, 'landsat', 'bands',True)
        return rec

    def _SelectCompOld(self,system,comp):
        comp['system'] = system
        return SelectComp(self, comp)

    def _InsertTileCoordsOld(self,query):
        #rec = self._SingleSearch(query,'sentinel', 'vectorsearches')
        self.cursor.execute("SELECT * FROM landsat.tilecoords WHERE path = '%(path)s' AND row = '%(row)s' AND dir = '%(dir)s' AND wrs = '%(wrs)s';" %query)
        record = self.cursor.fetchone()
        if record == None:
            self._InsertRecord(query, 'landsat', 'tilecoords')

    def _SearchTilesFromWSENOld(self, west, south, east, north):
        query = {'west':west, 'south':south,'east':east,'north':north}
        #self.cursor.execute("SELECT mgrs,west,south,east,north,ullon,ullat,urlon,urlat,lrlon,lrlat,lllon,lllat, minx, miny, maxx, maxy FROM sentinel.tilecoords WHERE centerlon > %(west)s AND centerlon < %(east)s AND centerlat > %(south)s AND centerlat < %(north)s;" %query)
        self.cursor.execute("SELECT wrs,dir,path,row,west,south,east,north,ullon,ullat,urlon,urlat,lrlon,lrlat,lllon,lllat, minx, miny, maxx, maxy FROM landsat.tilecoords WHERE east > %(west)s AND west < %(east)s AND north > %(south)s AND south < %(north)s;" %query)
        #print ("SELECT wrs,dir,path,row,west,south,east,north,ullon,ullat,urlon,urlat,lrlon,lrlat,lllon,lllat, minx, miny, maxx, maxy FROM landsat.tilecoords WHERE east > %(west)s AND west < %(east)s AND north > %(south)s AND south < %(north)s;" %query)

        records = self.cursor.fetchall()
        return records

    def _InsertSingleLandsatRegionOld(self,queryD):
        '''
        '''
        tabkeys = (['regionid'],['prstr'],['dir'],['wrs'])
        #regionid,mgrs
        self._CheckInsertSingleRecord(queryD, 'landsat', 'regions', tabkeys)

    def _SelectLandsatTileCoordsOld(self, searchD):
        #construct where statement - LATER
        query = {}
        self.cursor.execute("SELECT wrs,dir,path,row,minx,miny,maxx,maxy,ullat,ullon,lrlat,lrlon,urlat,urlon,lllat,lllon FROM landsat.tilecoords;" %query)
        records = self.cursor.fetchall()
        return records

    def _SelectLandsatTileCoordsNoSentinelRegionOld(self):
        #construct where statement - LATER
        query = {}
        self.cursor.execute("SELECT wrs,dir,path,row,minx,miny,maxx,maxy,ullat,ullon,lrlat,lrlon,urlat,urlon,lllat,lllon FROM landsat.tilecoords;" %query)
        records = self.cursor.fetchall()
        return records

    def _InsertScene(self,queryD):
        rec = self._CheckInsertSingleRecord(queryD,'landsat', 'scenes', [('lsatprodid',)])

    def _InsertReflectanceCalibration(self,queryD):
        rec = self._CheckInsertSingleRecord(queryD,'landsat', 'dnreflcal', [('lsatprodid','band')])

    def _InsertEmissivityCalibration(self,queryD):
        rec = self._CheckInsertSingleRecord(queryD,'landsat', 'dnemiscal', [('lsatprodid','band')])

    def _InsertImageAttributes(self,queryD):
        rec = self._CheckInsertSingleRecord(queryD,'landsat', 'imgattr', [('lsatprodid',)])

    def _SelectReflectanceCalibration(self, queryD, paramL):
        recs = self._MultiSearch(queryD, paramL, 'landsat', 'dnreflcal', True)
        return recs

    def _SelectEmissivityCalibration(self, queryD, paramL):
        recs = self._MultiSearch(queryD, paramL, 'landsat', 'dnemiscal', True)
        return recs

    def _SelectBandWaveLengths(self, queryD, paramL):
        recs = self._MultiSearch(queryD, paramL, 'landsat', 'wavelengths', True)
        return recs

    def _SelectBandsFromProdId(self,queryD,paramL):
        recs = self._MultiSearch(queryD, paramL, 'landsat', 'bands', True)
        return recs

    def _SelectImageAttributes(self, queryD, paramL):

        recs = self._SingleSearch(queryD, paramL, 'landsat', 'imgattr', True)
        return recs

    def _UpdateSceneStatus(self, queryD):
        query = "UPDATE landsat.scenes SET %(column)s = '%(status)s' WHERE lsatprodid = '%(lsatprodid)s'" %queryD
        self.cursor.execute(query)
        self.conn.commit()

    def _InsertBand(self,queryD):
        rec = self._CheckInsertSingleRecord(queryD,'landsat', 'bands', [('lsatprodid','folder','band','suffix')])

    def _InsertLayer(self,layer,overwrite,delete):
        InsertLayer(self,layer,overwrite,delete)
