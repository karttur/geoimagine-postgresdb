'''
Created on 8 mars 2018

@author: thomasgumbricht
'''

from geoimagine.postgresdb import PGsession
from geoimagine.postgresdb.compositions import InsertCompDef, InsertCompProd, InsertLayer, SelectComp
from base64 import b64encode
import netrc

from geoimagine.support.karttur_dt import Today

class ManageAncillary(PGsession):
    '''
    DB support for managing regions
    '''
    def __init__(self):
        """The constructor connects to the database"""
        #HOST = 'manageancillary'
        HOST = 'karttur'
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
            print ("SELECT * FROM system.regions WHERE regionid = '%(regionid)s';" %paramsD)
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
        return SelectComp(self, compQ)

    def _InsertClimateIndex(self,queryL):
        self.cursor.execute("DELETE FROM climateindex.climindex WHERE index = '%(index)s';" %queryL[0])
        for query in queryL:
            self.cursor.execute("INSERT INTO climateindex.climindex (index, acqdate, acqdatestr, value) VALUES ('%(index)s', '%(acqdate)s', '%(acqdatestr)s', %(value)s);" %query)
            self.conn.commit()

    def _SelectRegionExtent(self, queryD, paramL):
        return self._SingleSearch(queryD, paramL, 'system', 'regions')

    def _SelectClimateIndex(self,period,index):
        query = {'index':index, 'sdate': period.startdate, 'edate':period.enddate}
        self.cursor.execute("SELECT acqdate, value FROM climateindex.climindex WHERE \
            index = '%(index)s' AND acqdate >= '%(sdate)s' AND acqdate <= '%(edate)s' ORDER BY acqdate" %query )
        recs = self.cursor.fetchall()
        if len (recs) == 0:
            print ("SELECT acqdate, value FROM climateindex.climindex WHERE \
            index = '%(index)s' AND acqdate >= '%(sdate)s' AND acqdate <= '%(edate)s' ORDER BY acqdate" %query )
        return recs

    def _DeleteSRTMBulkTiles(self,params):
        query = {'prod':params.product, 'v':params.version}
        self.cursor.execute("DELETE FROM ancillary.srtmdptiles WHERE product= '%(prod)s' AND version = '%(v)s';" %query)
        self.conn.commit()

    def _LoadSRTMBulkTiles(self,params,tmpFPN,headL):
        self._DeleteSRTMBulkTiles(params)
        #query = {'tmpFPN':tmpFPN, 'items': ",".join(headL)}
        #print ("COPY modis.datapooltiles (%(items)s) FROM '%(tmpFPN)s' DELIMITER ',' CSV HEADER;" %query)
        #self.cursor.execute("COPY modis.datapooltiles (%(items)s) FROM '%(tmpFPN)s' DELIMITER ',' CSV HEADER;" %query)
        #print ("%(tmpFPN)s, 'modis.datapooltiles', columns= (%(items)s), sep=','; "%query)
        print ('Copying from',tmpFPN)
        with open(tmpFPN, 'r') as f:
            next(f)  # Skip the header row.
            self.cursor.copy_from(f, 'ancillary.srtmdptiles', sep=',')
            self.conn.commit()
            #cur.copy_from(f, 'test', columns=('col1', 'col2'), sep=",")

    def _SelectSRTMdatapooltilesOntile(self,queryD,paramL):
        rec = self._SingleSearch(queryD, paramL, 'ancillary', 'srtmdptiles', True)
        return rec

    def _InsertSRTMtileNOT(self,query):
        self.cursor.execute("SELECT * FROM ancillary.tiles WHERE tileid = '%(tileid)s';"  %query)
        record = self.cursor.fetchone()
        if record == None:
            cols = query.keys()
            values = query.values()
            values =["'{}'".format(str(x)) for x in values]
            query = {'cols':",".join(cols), 'values':",".join(values)}
            self.cursor.execute ("INSERT INTO modis.tiles (%(cols)s) VALUES (%(values)s);" %query)
            self.conn.commit()

    def _SelectDefRegionExtent(self,queryD, paramL):
        rec = self._SingleSearch(queryD, paramL, 'system', 'regions', True)
        return rec

    def _Select1degSquareTiles(self, queryD, paramL):
        """
        """


        print ("SELECT L.lltile FROM system.regions as R \
        JOIN system.defregions as D USING (regionid)\
        JOIN ancillary.srtmdptiles as L ON (R.regionid = L.lltile) \
        WHERE title = '1degsquare' AND lrlon > %(ullon)s AND ullon < %(lrlon)s AND ullat > %(lrlat)s AND lrlat < %(ullat)s;" %queryD)

        self.cursor.execute ("SELECT L.lltile FROM system.regions as R \
        JOIN system.defregions as D USING (regionid)\
        JOIN ancillary.srtmdptiles as L ON (R.regionid = L.lltile) \
        WHERE title = '1degsquare' AND lrlon > %(ullon)s AND ullon < %(lrlon)s AND ullat > %(lrlat)s AND lrlat < %(ullat)s;" %queryD)

        recs = self.cursor.fetchall()
        return recs
