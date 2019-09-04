'''
Created on 1 apr. 2019

@author: thomasgumbricht
'''

from geoimagine.postgresdb import PGsession
from geoimagine.postgresdb.compositions import InsertCompDef, InsertCompProd, InsertLayer, SelectComp
from base64 import b64encode
import netrc

#from geoimagine.support.karttur_dt import Today

class ManageSqlDumps(PGsession):
    '''
    DB support for setting up processes
    '''

    def __init__(self):
        """The constructor connects to the database"""
        HOST = 'managesql'
        secrets = netrc.netrc()
        username, account, password = secrets.authenticators( HOST )
        pswd = b64encode(password.encode())
        #create a query dictionary for connecting to the Postgres server
        query = {'db':'postgres','user':username,'pswd':pswd}
        #Connect to the Postgres Server
        self.session = PGsession.__init__(self,query,'ManageSMAP')
        
    def _SelectAllTableRecs(self,query):
        '''
        '''
        #print ("SELECT %(items)s FROM %(schematab)s;" %query)
        #recs = self.session._SelectAllTableRecs(query)
        self.cursor.execute("SELECT %(items)s FROM %(schematab)s;" %query)
        return self.cursor.fetchall()
    
    def _SelectUserSecrets(self):
        HOST = 'managesql'
        secrets = netrc.netrc()
        username, account, password = secrets.authenticators( HOST )
        return (username, account, password)
    