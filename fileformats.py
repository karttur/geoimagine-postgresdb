'''
Created on 23 feb. 2018

@author: thomasgumbricht
'''
from geoimagine.postgresdb import PGsession
from base64 import b64encode
import netrc



class SelectFileFormats(PGsession):
    '''
    DB support for setting up processes
    '''

    def __init__(self):
        """The constructor connects to the database"""
        HOST = 'formatread'
        HOST = 'karttur'
        secrets = netrc.netrc()
        username, account, password = secrets.authenticators( HOST )
        pswd = b64encode(password.encode())
        #create a query dictionary for connecting to the Postgres server
        query = {'db':'postgres','user':username,'pswd':pswd}
        #Connect to the Postgres Server
        PGsession.__init__(self,query,'SelectFileFormats')


    def _SelectCellTypes(self):
        self.cursor.execute("SELECT gdal, arr, np, usgs FROM process.celltypes")
        records = self.cursor.fetchall()
        return records

    def _SelectGDALof(self):
        self.cursor.execute("SELECT hdr, dat, gdaldriver FROM process.gdalformat")
        records = self.cursor.fetchall()
        return records
