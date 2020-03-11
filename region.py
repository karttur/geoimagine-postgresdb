'''
Created on 23 feb. 2018

@author: thomasgumbricht
'''


from geoimagine.postgresdb import PGsession
from geoimagine.postgresdb.compositions import InsertCompDef, InsertCompProd, InsertLayer, SelectComp
from base64 import b64encode
import netrc

#from geoimagine.support.karttur_dt import Today

class ManageRegion(PGsession):
    '''
    DB support for managing regions
    '''
    def __init__(self):
        """The constructor connects to the database"""
        HOST = 'manageregion'
        HOST = 'karttur'
        secrets = netrc.netrc()
        username, account, password = secrets.authenticators( HOST )
        pswd = b64encode(password.encode())
        #create a query dictionary for connecting to the Postgres server
        query = {'db':'postgres','user':username,'pswd':pswd}
        #Connect to the Postgres Server
        self.session = PGsession.__init__(self,query,'ManageRegion')

    def _InsertRegionCat(self,process,region):
        #Get the parentid from all cats except tracts and global
        if region['stratum'] == 0:
            exit ('Stratum 0 can not be altered')

        if region['parentcat'] == '*' and region['stratum'] > 11:
            catrec = True
        else:
            #query = {'parentcat': region.parentcat}

            self.cursor.execute("SELECT regioncat FROM system.regioncats WHERE regioncat = '%(parentcat)s';" %region)
            catrec = self.cursor.fetchone()
        if catrec != None:
            #check for the regioncat itself
            #query = {'cat': region.regioncat}
            self.cursor.execute("SELECT * FROM system.regioncats WHERE regioncat = '%(regioncat)s';" %region)
            record = self.cursor.fetchone()
            if record == None:
                print ('region',region)
                self.cursor.execute('INSERT INTO system.regioncats (regioncat, parentcat, stratum, title, label) VALUES (%s, %s, %s,%s,%s)',
                                    (region['regioncat'], region['parentcat'], region['stratum'], region['title'], region['label']))
                self.conn.commit()
            elif process.proc.delete:
                self.cursor.execute("DELETE FROM system.regioncats WHERE regioncat'%(regioncat)s';" %region)
                self.conn.commit()
            elif process.proc.overwrite:
                pass
        else:
            #print ("SELECT regioncat FROM system.regions WHERE regionid = '%(parentid)s';" %region)
            exitstr = 'The parentcat region %s for region %s does not exists, it must be added proir to the region' %(region['parentid'],region['regioncat'])
            exit(exitstr)


    def _Insert1DegDefRegion(self, query):
        '''
        '''
        self._CheckInsertSingleRecord(query,'system','defregions')


    def _InsertDefRegion(self, process, layer, query, bounds, llD, overwrite, delete):
        '''
        '''
        if overwrite or delete:
            self.cursor.execute("DELETE FROM system.defregions WHERE regionid = '%(regionid)s' AND regioncat ='%(parentcat)s' AND parentid ='%(parentid)s'  ;" %query)
            self.conn.commit()
            if delete:
                self._InsertRegion(query, bounds, llD, overwrite, delete)
                return
        #Check that the regioncat is correctly set
        self.cursor.execute("SELECT * FROM system.regioncats WHERE regioncat = '%(regioncat)s';" %query)
        record = self.cursor.fetchone()
        if record == None:
            exitstr = 'the regioncat %(regioncat)s does not exist in the regioncats table' %query
            exit(exitstr)

        #Check that the parent regions is set
        self.cursor.execute("SELECT * FROM system.defregions WHERE regionid = '%(parentid)s' AND regioncat ='%(parentcat)s' ;" %query)
        record = self.cursor.fetchone()
        if record == None:
            if query['parentid'] in ['south-america','antarctica'] and query['parentcat'] == 'subcontinent':
                xquery = {'parentid':query['parentid'], 'parentcat':'continent'}
                self.cursor.execute("SELECT * FROM system.defregions WHERE regionid = '%(parentid)s' AND regioncat ='%(parentcat)s' ;" %xquery)
                record = self.cursor.fetchone()
                if record == None:
                    print ("SELECT * FROM system.defregions WHERE regionid = '%(parentid)s' AND regioncat ='%(parentcat)s' ;" %xquery)
                    FISKA
                    exitstr = 'the parentid region "%s" of regioncat "%s" does not exist in the defregions table' %(query['parentid'], query['parentcat'])
                    exit(exitstr)
            else:

                exitstr = 'the parentid region "%s" of regioncat "%s" does not exist in the defregions table' %(query['parentid'], query['parentcat'])
                print ("SELECT * FROM system.defregions WHERE regionid = '%(parentid)s' AND regioncat ='%(parentcat)s' ;" %query)

                exit(exitstr)

        #Check if the region itself already exists
        #query = {'id': layer.location.regionid}
        self.cursor.execute("SELECT regioncat FROM system.defregions WHERE regionid = '%(regionid)s';" %query)
        record = self.cursor.fetchone()
        if record == None:
            self.cursor.execute('INSERT INTO system.defregions (regioncat, regionid, regionname, parentid, title, label) VALUES (%s, %s, %s, %s, %s, %s)',
                                (query['regioncat'], query['regionid'], query['regionname'], query['parentid'], query['title'], query['label']))
            self.conn.commit()

        else:
            if query['regioncat'] != record[0]:
                if layer.locus.locus in ['antarctica','south-america']:
                    query2 = {'id': layer.locus.locus,'cat':query['regioncat']}
                    self.cursor.execute("SELECT regioncat FROM system.defregions WHERE regionid = '%(id)s' and regioncat = '%(cat)s';" %query2)
                    record = self.cursor.fetchone()
                    if record == None:
                        self.cursor.execute('INSERT INTO system.defregions (regioncat, regionid, regionname, parentid, title, label) VALUES (%s, %s, %s, %s, %s, %s)',
                                            (query['regioncat'], query['regionid'], query['regionname'], query['parentid'], query['title'], query['label']))
                        self.conn.commit()
                else:
                    pass

        query['system'] = 'system'
        query['regiontype'] = 'D'
        self._InsertRegion(query, bounds, llD, overwrite, delete)

        InsertCompDef(self,layer.comp)
        InsertCompProd(self,layer.comp)
        #InsertCompProd(self,process.system,process.system,layer.comp)
        InsertLayer(self, layer, process.proc.overwrite, process.proc.delete)

    def _InsertRegion(self, query, bounds, llD, overwrite, delete):
        #query = {'id': region.regionid}
        if overwrite or delete:
            self.cursor.execute("DELETE FROM %(system)s.regions WHERE regionid = '%(regionid)s';" %query)
            self.conn.commit()
            if delete:
                return

        self.cursor.execute("SELECT * FROM %(system)s.regions WHERE regionid = '%(regionid)s';" %query)
        record = self.cursor.fetchone()
        if record == None:
            #print ("SELECT * FROM %(system)s.regions WHERE regionid = '%(regionid)s';" %query)
            self.cursor.execute("INSERT INTO %(system)s.regions (regionid, regioncat, regiontype) VALUES \
                    ('%(regionid)s', '%(regioncat)s', '%(regiontype)s');" %query)

            self.conn.commit()
            query['minx'] = bounds[0]
            query['miny'] = bounds[1]
            query['maxx'] = bounds[2]
            query['maxy'] = bounds[3]
            self.cursor.execute("UPDATE %(system)s.regions SET (epsg, minx, miny, maxx, maxy) = \
                    (%(epsg)s, %(minx)s, %(miny)s, %(maxx)s, %(maxy)s) WHERE regionid = '%(regionid)s';" %query)

            self.conn.commit()
            for key in llD:
                query[key] = llD[key]

            self.cursor.execute("UPDATE %(system)s.regions SET (ullon,ullat,urlon,urlat,lrlon,lrlat,lllon,lllat) = \
                    (%(ullon)s,%(ullat)s,%(urlon)s,%(urlat)s,%(lrlon)s,%(lrlat)s,%(lllon)s,%(lllat)s) WHERE regionid = '%(regionid)s';" %query)
            self.conn.commit()
        elif record[0] != query['regioncat']:
            if query['regionid'] in ['antarctica','south-america']:
                pass
            else:
                exitstr = 'Duplicate categories (%s %s) for regionid %s' %(record[0],query['regioncat'], query['regionid'])
                exit(exitstr)
        #TGTODO duplicate name for tract but different user???, delete and overwrite

    def _SelectComp(self,comp):
        #comp['system'] = system
        return SelectComp(self, comp)


    def _LoadBulkDefregions(self,tmpFPN):
        #self._DeleteSRTMBulkTiles(params)
        #query = {'tmpFPN':tmpFPN, 'items': ",".join(headL)}
        #print ("COPY modis.datapooltiles (%(items)s) FROM '%(tmpFPN)s' DELIMITER ',' CSV HEADER;" %query)
        #self.cursor.execute("COPY modis.datapooltiles (%(items)s) FROM '%(tmpFPN)s' DELIMITER ',' CSV HEADER;" %query)
        #print ("%(tmpFPN)s, 'modis.datapooltiles', columns= (%(items)s), sep=','; "%query)
        print ('Copying from',tmpFPN)
        with open(tmpFPN, 'r') as f:
            next(f)  # Skip the header row.
            self.cursor.copy_from(f, 'system.defregions', sep=',')
            self.conn.commit()
            #cur.copy_from(f, 'test', columns=('col1', 'col2'), sep=",")

    def _LoadBulkRegions(self,tmpFPN):

        #query = {'tmpFPN':tmpFPN, 'items': ",".join(headL)}
        #print ("COPY modis.datapooltiles (%(items)s) FROM '%(tmpFPN)s' DELIMITER ',' CSV HEADER;" %query)
        #self.cursor.execute("COPY modis.datapooltiles (%(items)s) FROM '%(tmpFPN)s' DELIMITER ',' CSV HEADER;" %query)
        #print ("%(tmpFPN)s, 'modis.datapooltiles', columns= (%(items)s), sep=','; "%query)
        print ('Copying from',tmpFPN)
        with open(tmpFPN, 'r') as f:
            next(f)  # Skip the header row.
            self.cursor.copy_from(f, 'system.regions', sep=',')
            self.conn.commit()
