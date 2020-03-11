'''
Created on 21 Nov 2018

@author: thomasgumbricht
'''

from geoimagine.postgresdb import PGsession
from geoimagine.postgresdb.compositions import InsertCompDef, InsertCompProd, InsertLayer, SelectComp
from base64 import b64encode
import netrc
from geoimagine.support import Today

class ManageUserProj(PGsession):
    '''
    DB support for setting up processes
    '''
    def __init__(self):
        """The constructor connects to the database"""
        HOST = 'ManageUserProj'
        HOST = 'karttur'
        secrets = netrc.netrc()
        username, account, password = secrets.authenticators( HOST )
        pswd = b64encode(password.encode())
        #create a query dictionary for connecting to the Postgres server
        query = {'db':'postgres','user':username,'pswd':pswd}
        #Connect to the Postgres Server
        self.session = PGsession.__init__(self,query,'UserLocale')

    def _ManageDefRegProjTractSite(self,queryD):
        '''
        '''
        userStatus = self._SelectUserStatus(queryD)

        if userStatus == None:
            exit('SHOULD NOT HAPPEN')
        self.cursor.execute("SELECT * FROM system.regions WHERE regionid = '%(defaultregion)s';" %queryD)
        defRegion = self.cursor.fetchone()
        if defRegion == None:
            print ("SELECT * FROM system.regions WHERE regionid = '%(defaultregion)s';" %queryD)
            exitstr = 'the default parent region %(defaultregion)s does not exist' %queryD
            exit(exitstr)
            return False
        userid, usercat, stratum, userProjs, userTracts, userSites = userStatus
        '''
        userProjL = [item[0] for item in userProjs]
        userTractL = [item[0] for item in userTracts]
        userSiteL = [item[0] for item in userSites]
        #print (userid, usercat, stratum, userProjs, userTracts, userSites)
        #Check that the proj, tract and site to create does not exist

        if queryD['projid'] in userProjL:
            exitstr = 'Your user project with projid = %(projid)s already exists' %queryD
            exit(exitstr)
        if queryD['tractid'] in userTractL:
            exitstr = 'Your user tract with tractid = %(tractid)s already exists' %queryD
            exit(exitstr)
        if queryD['siteid'] in userSiteL:
            exitstr = 'Your user tract with tractid = %(tractid)s already exists' %queryD
            exit(exitstr)
        self._CheckProjTractSite(queryD)
        '''
        self._ManageProj(queryD, False, False)
        self._ManageTract(queryD, False, False)
        self._ManageSite(queryD, False, False)
        self._ManageProjTractLink(queryD, False, False)


    def _ManageTractProjSite(self,queryD):
        '''
        '''
        userStatus = self._SelectUserStatus(queryD)

        if userStatus == None:
            exit('SHOULD NOT HAPPEN')
        self.cursor.execute("SELECT * FROM system.regions WHERE regionid = '%(defaultregion)s';" %queryD)
        defRegion = self.cursor.fetchone()
        if defRegion == None:
            print ("SELECT * FROM system.regions WHERE regionid = '%(defaultregion)s';" %queryD)
            exitstr = 'the default parent region %(defaultregion)s does not exist' %queryD
            exit(exitstr)
            return False
        userid, usercat, stratum, userProjs, userTracts, userSites = userStatus

        userProjL = [item[0] for item in userProjs]
        userTractL = [item[0] for item in userTracts]
        userSiteL = [item[0] for item in userSites]
        #print (userid, usercat, stratum, userProjs, userTracts, userSites)
        #Check that the proj, tract and site to create does not exist
        '''
        if queryD['projid'] in userProjL:
            exitstr = 'Your user project with projid = %(projid)s already exists' %queryD
            exit(exitstr)
        if queryD['tractid'] in userTractL:
            exitstr = 'Your user tract with tractid = %(tractid)s already exists' %queryD
            exit(exitstr)
        if queryD['siteid'] in userSiteL:
            exitstr = 'Your user site with siteid = %(siteid)s already exists' %queryD
            exit(exitstr)

        self._CheckProjTractSite(queryD)
        '''
        self._ManageProj(queryD, False, False)
        self._ManageTract(queryD, False, False)
        self._ManageSite(queryD, False, False)
        self._ManageProjTractLink(queryD, False, False)

    _ManageTractProjSite

    def _ManageProj(self, qD, overwrite, delete):
        '''
        '''
        if overwrite:
            pass
        elif delete:
            pass
        else:
            queryD = {'projid':qD['projid'],'projname':qD['projname'],'owner':qD['userid'],
                      'creator':qD['userid'],'title':qD['projtitle'],'label':qD['projlabel'],
                      'createdate':Today()}
            self._CheckInsertSingleRecord(queryD,'userlocale','userprojects',(['projid']))

    def _ManageTract(self, qD, overwrite, delete):
        '''
        '''
        if overwrite:
            pass
        elif delete:
            pass
        else:
            print ('qD',qD)
            queryD = {'parentid':qD['defaultregion'],'tractid':qD['tractid'],'tractname':qD['tractname'],'owner':qD['userid'],
                      'creator':qD['userid'],'title':qD['tracttitle'],'label':qD['tractlabel'], 'regiontype':qD['regiontype'],
                      'createdate':Today()}
            self._CheckInsertSingleRecord(queryD,'regions','tracts',(['tractid']))

    def _ManageSite(self, qD, overwrite, delete):
        '''
        '''
        if overwrite:
            pass
        elif delete:
            pass
        else:
            queryD = {'tractid':qD['tractid'], 'siteid':qD['siteid'],'sitename':qD['sitename'],'owner':qD['userid'],
                      'title':qD['sitetitle'],'label':qD['sitelabel'],
                      'createdate':Today()}
            self._CheckInsertSingleRecord(queryD,'regions','sites',(['siteid']))

    def _ManageProjTractLink(self,queryD,overwrite,delete):

        self.cursor.execute("SELECT owner FROM regions.tracts WHERE tractid = '%(tractid)s';" %queryD)
        recown = self.cursor.fetchone()
        if recown == None:
            warnstr = 'Unregistered tract %(tractid)s can not link to project' %queryD
            print (warnstr)
            return False
        elif recown[0] != queryD['userid']:
            warnstr = 'User %(userid)s does not owner of tract %(tractid)s and can not link to project' %queryD
            print (warnstr)
            return False
        self.cursor.execute("SELECT owner FROM userlocale.userprojects WHERE projid = '%(projid)s';" %queryD)
        recown = self.cursor.fetchone()
        if recown == None:
            warnstr = ('Unregistered project %(projid)s can not link to project' %queryD)
            print (warnstr)
            return False
        elif recown[0] != queryD['userid']:
            warnstr = 'User %(userid)s does not owner of project %(projid)s and can not link to project' %queryD
            print (warnstr)
            return False
        #link the project and the region
        self.cursor.execute("SELECT * FROM userlocale.linkprojregion WHERE regionid = '%(tractid)s' and projid = '%(projid)s';" %queryD)
        record = self.cursor.fetchone()
        if record == None:
            self.cursor.execute('INSERT INTO userlocale.linkprojregion (regionid, projid) VALUES (%s, %s)',
                                (queryD['tractid'], queryD['projid']))
            self.conn.commit()

    def _SelectUserStatus(self,queryD):
        '''
        '''
        self.cursor.execute("SELECT userid,usercat,stratum FROM userlocale.users WHERE userid = '%(userid)s';" %queryD)
        rec = self.cursor.fetchone()
        if rec == None:
            return rec
        self.cursor.execute("SELECT projid FROM userlocale.userprojects WHERE owner = '%(userid)s';" %queryD)
        userProjs = self.cursor.fetchall()
        self.cursor.execute("SELECT tractid FROM regions.tracts WHERE owner = '%(userid)s';" %queryD)
        userTracts = self.cursor.fetchall()
        self.cursor.execute("SELECT siteid FROM regions.sites WHERE owner = '%(userid)s';" %queryD)
        userSites = self.cursor.fetchall()
        return (rec[0], rec[1], rec[2], userProjs, userTracts, userSites)

    def _CheckProjTractSite(self,queryD):
        #Check that the projid, tractid and siteid does not exists with other user
        self.cursor.execute("SELECT projid FROM userlocale.userprojects WHERE projid = '%(projid)s';" %queryD)
        userProj = self.cursor.fetchone()
        if userProj != None:
            exitstr = 'A project with projid = %(projid)s already exists' %queryD
            exit(exitstr)

        self.cursor.execute("SELECT tractid FROM regions.tracts WHERE tractid = '%(tractid)s';" %queryD)
        userTract = self.cursor.fetchone()
        if userTract != None:
            exitstr = 'A tract with tractid = %(tractid)s already exists' %queryD
            exit(exitstr)

        self.cursor.execute("SELECT siteid FROM regions.sites WHERE siteid = '%(siteid)s';" %queryD)
        userSite = self.cursor.fetchone()
        if userSite != None:
            exitstr = 'A site with siteid = %(siteid)s already exists' %queryD
            exit(exitstr)

    def _SelectComp(self,system,comp):
        comp['system'] = system
        return SelectComp(self, comp)

    def _SelectParentRegion(self,queryD):
        '''
        '''
        self.cursor.execute("SELECT regionid, regioncat FROM ancillary.layers \
            JOIN system.regions USING (regionid) \
            WHERE compid = '%(compid)s';" %queryD)

        rec = self.cursor.fetchone()
        if rec == None:
            exitstr = 'no region founds' %queryD
            exit(exitstr)
        return rec

    def _InsertTractRegion(self, process, layer, query, bounds, llD):
        '''
        '''
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
                print ("SELECT * FROM system.defregions WHERE regionid = '%(parentid)s' AND regioncat ='%(parentcat)s' ;" %query)
                FISKA
                exitstr = 'the parentid region "%s" of regioncat "%s" does not exist in the defregions table' %(query['parentid'], query['parentcat'])
                exit(exitstr)

        #Check if the region itself already exists

        self.cursor.execute("SELECT regioncat FROM regions.defregions WHERE regionid = '%(regionid)s';" %query)
        record = self.cursor.fetchone()
        if record == None:
            self.cursor.execute('INSERT INTO regions.defregions (regioncat, regionid, regionname, parentid, title, label) VALUES (%s, %s, %s, %s, %s, %s)',
                                (query['regioncat'], query['regionid'], query['regionname'], query['parentid'], query['title'], query['label']))
            self.conn.commit()

        query['system'] = 'regions'
        query['regiontype'] = 'T'
        self._InsertRegion(query, bounds, llD)
        print (layer.comp.system)
        InsertCompDef(self,layer.comp)
        InsertCompProd(self,layer.comp)
        #InsertCompProd(self,process.system,process.system,layer.comp)
        InsertLayer(self, layer, process.proc.overwrite, process.proc.delete)


    def _InsertRegion(self, query, bounds, llD):
        '''DUPLICATE FROM REGION
        '''
        #query = {'id': region.regionid}
        self.cursor.execute("SELECT * FROM %(system)s.regions WHERE regionid = '%(regionid)s';" %query)
        record = self.cursor.fetchone()
        print ('record',record)
        if record == None:

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
            exitstr = 'Duplicate categories (%s %s) for regionid %s' %(record[0],query['regioncat'], query['regionid'])
            exit(exitstr)


    '''The following should be put together with MODIS
    '''
    def _SelectModisTileCoords(self, searchD):

        query = {}
        self.cursor.execute("SELECT hvtile,h,v,minxsin,minysin,maxxsin,maxysin,ullat,ullon,lrlat,lrlon,urlat,urlon,lllat,lllon FROM modis.tilecoords;" %query)
        records = self.cursor.fetchall()
        return records

    def _SearchTilesFromWSEN(self, west, south, east, north):
        query = {'west':west, 'south':south,'east':east,'north':north}
        #self.cursor.execute("SELECT mgrs,west,south,east,north,ullon,ullat,urlon,urlat,lrlon,lrlat,lllon,lllat, minx, miny, maxx, maxy FROM sentinel.tilecoords WHERE centerlon > %(west)s AND centerlon < %(east)s AND centerlat > %(south)s AND centerlat < %(north)s;" %query)
        self.cursor.execute("SELECT hvtile,h,v,west,south,east,north,ullon,ullat,urlon,urlat,lrlon,lrlat,lllon,lllat, minxsin, minysin, maxxsin, maxysin FROM modis.tilecoords WHERE east > %(west)s AND west < %(east)s AND north > %(south)s AND south < %(north)s;" %query)

        #print ("SELECT mgrs,west,south,east,north,ullon,ullat,urlon,urlat,lrlon,lrlat,lllon,lllat FROM sentinel.tilecoords WHERE centerlon > %(west)s AND centerlon < %(east)s AND centerlat > %(south)s AND centerlat < %(north)s;" %query)
        records = self.cursor.fetchall()
        return records

    def _InsertModisRegionTile(self, query):
        '''
        '''
        if query['table'] == 'regions':
            self.cursor.execute("SELECT * FROM %(system)s.%(table)s WHERE regionid = '%(regionid)s';"  %query)
        elif query['table'] == 'tracts':
            self.cursor.execute("SELECT * FROM %(system)s.%(table)s WHERE tractid = '%(regionid)s';"  %query)

        record = self.cursor.fetchone()
        if record == None:
            print ("SELECT * FROM %(system)s.regions WHERE regionid = '%(regionid)s';"  %query)
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
