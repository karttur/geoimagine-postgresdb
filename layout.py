'''
Created on 22 feb. 2018

@author: thomasgumbricht
'''
from geoimagine.postgresdb import PGsession
from base64 import b64encode
import netrc


class ManageLayout(PGsession):
    '''
    DB support for setting up processes
    '''

    def __init__(self):
        """The constructor connects to the database"""
        #HOST = 'managelayout'
        HOST = 'karttur'
        secrets = netrc.netrc()
        username, account, password = secrets.authenticators( HOST )
        pswd = b64encode(password.encode())
        #create a query dictionary for connecting to the Postgres server
        query = {'db':'postgres','user':username,'pswd':pswd}
        #Connect to the Postgres Server
        self.session = PGsession.__init__(self,query)

    def _ManageRasterPalette(self,userId, params,setcolorD,overwrite,delete):

        if params.default:
            #set this palette as the default for the given compid
            query = {'compid':params.compid,'palette':params.palette}

        query = {'compid':params.compid, 'palette':params.palette, 'owner':userId, 'access':params.access[0]}
        self._CheckInsertSingleRecord(query,'layout','rasterpalettes', [['palette']])
        #If overwrite or delete all colors all deleted
        if overwrite or delete:
            self.cursor.execute("DELETE FROM layout.rasterpalcolors WHERE palette = '%(palette)s';" %query)
            self.conn.commit()

        if not delete:
            for col in setcolorD:
                c = setcolorD[col]
                query = {'palette':params.palette, 'value':col, 'red':c['red'], 'green':c['green'], 'blue':c['blue'], 'alpha':c['alpha'],
                         'label':c['label'], 'hint':c['hint']}
                self._CheckInsertSingleRecord(query,'layout','rasterpalcolors', [['palette'],['value']])

    def _ManageRasterLegend(self,paramsD,comp,overwrite,delete):
        compid = '%(f)s_%(b)s' %{'f':comp['folder'].lower(),'b':comp['band'].lower()}
        paramsD['compid'] = compid
        paramsD['source'] = comp['source']
        paramsD['product'] = comp['product']
        paramsD['suffix'] = comp['suffix']

        booleans = ['label','compresslabels','matrix','two51','two52','two53','two54','two55']
        for b in booleans:
            if paramsD[b]:
                paramsD[b] = 'Y'
            else:
                paramsD[b] = 'N'

        #If overwrite or delete all colors all deleted
        if overwrite or delete:
            self.cursor.execute("DELETE FROM layout.legend WHERE \
            compid = '%(compid)s' AND source = '%(source)s' AND product = '%(product)s' AND suffix = '%(suffix)s';" %paramsD)
            self.conn.commit()
        if not delete:
            self._CheckInsertSingleRecord(paramsD,'layout','legend', [['compid'],['source'],['product'],['suffix']])

    def _ManageRasterScaling(self,paramsD,comp,overwrite,delete):
        compid = '%(f)s_%(b)s' %{'f':comp['folder'].lower(),'b':comp['band'].lower()}
        paramsD['compid'] = compid
        paramsD['source'] = comp['source']
        paramsD['product'] = comp['product']
        paramsD['suffix'] = comp['suffix']

        #If overwrite or delete all colors all deleted
        if overwrite or delete:
            self.cursor.execute("DELETE FROM layout.scaling WHERE \
            compid = '%(compid)s' AND source = '%(source)s' AND product = '%(product)s' AND suffix = '%(suffix)s';" %paramsD)
            self.conn.commit()
        if not delete:
            self._CheckInsertSingleRecord(paramsD,'layout','scaling', [['compid'],['source'],['product'],['suffix']])

    def _GetCompFormat(self, query):
        BALLE

    def _SelectPaletteColors(self,query,paramL):
        query['cols'] = ",".join(paramL)
        self.cursor.execute("SELECT  %(cols)s FROM layout.rasterpalcolors \
        WHERE palette = '%(palette)s';" %query)
        recs = self.cursor.fetchall()
        return (recs)

    def _SelectCompDefaultPalette(self,query):
        self.cursor.execute("SELECT palette FROM layout.defaultpalette \
        WHERE compid = '%(compid)s';" %query)
        recs = self.cursor.fetchone()
        return (recs)

    def _ManageMovieClock(self,query,overwrite,delete):
        query.pop('today')

        booleans = ['textatclock','textinvideo']
        for b in booleans:
            if query[b]:
                query[b] = 'Y'
            else:
                query[b] = 'N'

        if overwrite or delete:
            self.cursor.execute("DELETE FROM layout.movieclock WHERE \
            name = '%(name)s';" %query)
            self.conn.commit()
        if not delete:
            self._CheckInsertSingleRecord(query,'layout','movieclock', [['name']])
