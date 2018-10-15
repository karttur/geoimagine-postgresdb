'''
Created on 8 mars 2018

@author: thomasgumbricht
'''

from geoimagine.support.karttur_dt import Today
        
def InsertCompDef(session, comp, title=False, label=False):
    '''
    '''

    def CheckCompDef():

        query = {'id':comp.compid,'system':comp.system}
        session.cursor.execute("SELECT folder, band, scalefac, offsetadd, dataunit, measure FROM %(system)s.compdefs WHERE compid = '%(id)s';" %query)
        record = session.cursor.fetchone()
        if record == None:
            return True
        if record[5] == 'N': #Nominal data, no chekc
            return False
        inT = (comp.folder, comp.band, comp.scalefac, comp.offsetadd, comp.dataunit, comp.measure)
        diff = [x for x,y in zip(inT,record) if x != y]
        if len(diff) != 0:
            exitstr = 'EXITING: duplicate compid %s, you must change the band name %s %s' %(comp.compid, inT,record)
            print (exitstr)
            itemL = ['folder','band','scalefac','offsetadd','dataunit','measure']
            for n in range(len(inT)):
                if inT[n] != record[n]:
                    print ('    Error at',itemL[n], inT[n], record[n])
            exit(exitstr)
        else:
            return False
        #end CheckCompDef  
    addrec = CheckCompDef()
    if addrec:
        tableschema = '%(schema)s.compdefs' %{'schema':comp.system}
        query ={'table':tableschema, 'compid':comp.compid,'folder':comp.folder,'band':comp.band,'prefix':comp.prefix,'measure':comp.measure,'dataunit':comp.dataunit,'scalefac':comp.scalefac,'offsetadd':comp.offsetadd}
        
        session.cursor.execute("INSERT INTO %(table)s (compid, folder, band, prefix, measure, dataunit, scalefac, offsetadd) VALUES ('%(compid)s','%(folder)s','%(band)s','%(prefix)s','%(measure)s','%(dataunit)s',%(scalefac)s, %(offsetadd)s)" %query)
        session.conn.commit()
        if title:
            query['title'] = title
            #print ('query',query)
            #print ("UPDATE %(table)s SET title = '%(title)s' WHERE compid = '%(compid)s';" %query)
            session.cursor.execute("UPDATE %(table)s SET title = '%(title)s' WHERE compid = '%(compid)s';" %query)
            session.conn.commit()
        if label:
            query['label'] = label
            session.cursor.execute("UPDATE %(table)s SET label = '%(label)s' WHERE compid = '%(compid)s';" %query)
            session.conn.commit()
                    
def InsertCompProd(session, comp):
        '''
        '''
        def CheckCompProd():
            #query = {'system':system,'id':comp.compid, 'src':comp.source, 'prod':comp.product, 'suf':comp.suffix}
            session.cursor.execute("SELECT cellnull, celltype FROM %(system)s.compprod WHERE compid = '%(compid)s' AND source = '%(source)s' AND product = '%(product)s' AND suffix = '%(suffix)s';" %query)
            record = session.cursor.fetchone()
            if record != None:
                inT = (comp.cellnull, comp.celltype)
                diff = [x for x,y in zip(inT,record) if x != y]
                if len(diff) != 0:
                    exitstr = 'EXITING: duplicate compprod %s, you must change the band name %s %s' %(comp.compid, inT,record)
    
                    exit(exitstr)
                    itemL = ['cellnull','celltype']
                    for n in range(len(inT)):
                        if inT[n] != record[n]:
                            print ('    Error at',itemL[n], inT[n], record[n])
                    exit(exitstr)
                else:
                    return False
            elif record == None:
                return True
            #end CheckCompProd
        if comp.cellnull < -32768:
            comp.cellnull = -32768 
        elif comp.cellnull > 32767:
            comp.cellnull = 32768
        #cast the comp class to a dict
        query = comp.__dict__

        addrec = CheckCompProd()
        if addrec:
            #query = {'system':system,'srcsystem':system,'compid':comp.compid,'source':comp.source,'product':comp.product,'suffix':comp.suffix,'cellnull':comp.cellnull,'celltype':comp.celltype}
            #print ("INSERT INTO %(system)s.compprod (compid, system, source, product, suffix, cellnull, celltype) VALUES ('%(compid)s','%(system)s','%(source)s','%(product)s','%(suffix)s',%(cellnull)s,'%(celltype)s');" %query)
            session.cursor.execute("INSERT INTO %(system)s.compprod (compid, system, source, product, suffix, cellnull, celltype) VALUES ('%(compid)s','%(system)s','%(source)s','%(product)s','%(suffix)s',%(cellnull)s,'%(celltype)s');" %query)
            session.conn.commit()
            
def InsertLayer(session,layer,overwrite,delete): 
        
    def InsertRegionLayer():

        query = {'system':layer.comp.system, 'compid': layer.comp.compid, 'source':layer.comp.source ,'product':layer.comp.product, 'suffix':layer.comp.suffix, 'acqdatestr':layer.datum.acqdatestr, 'regionid':layer.locus.locus, 'today':Today()}
        session.cursor.execute("SELECT * FROM %(system)s.layers WHERE compid = '%(compid)s' AND product = '%(product)s' AND suffix = '%(suffix)s' AND regionid = '%(regionid)s' AND acqdatestr = '%(acqdatestr)s';" %query)
        record = session.cursor.fetchone()
        if record != None and (delete or overwrite):
            session.cursor.execute("DELETE FROM %(system)s.layers WHERE compid = '%(compid)s' AND product = '%(product)s' AND suffix = '%(suffix)s' AND regionid = '%(regionid)s' AND acqdatestr = '%(acqdatestr)s';" %query)
            if delete:
                return
        if record == None or overwrite:
            session.cursor.execute("INSERT INTO %(system)s.layers (compid, source, product, suffix, regionid, acqdatestr, createdate) VALUES ('%(compid)s', '%(source)s', '%(product)s', '%(suffix)s', '%(regionid)s', '%(acqdatestr)s', '%(today)s')" %query)

            session.conn.commit()
            if layer.datum.acqdate:
                query['acqdate'] = layer.datum.acqdate
                query['doy'] = layer.datum.doy
                session.cursor.execute("UPDATE %(system)s.layers SET (acqdate, doy) = ('%(acqdate)s', %(doy)d) WHERE compid = '%(compid)s' AND product = '%(product)s' AND suffix = '%(suffix)s' AND regionid = '%(regionid)s' AND acqdatestr = '%(acqdatestr)s';" %query)
                session.conn.commit()
        #end InsertRegionLayer
        
    def InsertSentinelLayer():

        #query = {'system':layer.comp.system, 'compid': layer.comp.compid, 'source':layer.comp.source ,'product':layer.comp.product, 'suffix':layer.comp.suffix, 'acqdatestr':layer.datum.acqdatestr, 'utm':layer.locus.utm, 'mgrsid':layer.locus.mgrsid, 'today':Today()}
        queryD = {'compid': layer.comp.compid, 'source':layer.comp.source ,'product':layer.comp.product, 'suffix':layer.comp.suffix, 
                  'acqdatestr':layer.datum.acqdatestr, 'utm':layer.locus.utm, 'mgrsid':layer.locus.mgrsid, 'orbitid':layer.locus.orbitid}
        if layer.datum.acqdate:
            queryD['acqdate'] = layer.datum.acqdate
            queryD['doy'] = layer.datum.doy    
        session._CheckInsertSingleRecord(queryD, layer.comp.system, 'layers')
        ''''
        BALLE
        print ("SELECT * FROM %(system)s.layers WHERE compid = '%(compid)s' AND product = '%(product)s' AND suffix = '%(suffix)s' AND utm = '%(utm)s' AND acqdatestr = '%(acqdatestr)s';" %query)
        session.cursor.execute("SELECT * FROM %(system)s.layers WHERE compid = '%(compid)s' AND product = '%(product)s' AND suffix = '%(suffix)s' AND mgrs = '%(mgrs)s' AND acqdatestr = '%(acqdatestr)s';" %query)
        record = session.cursor.fetchone()
        if record != None and (delete or overwrite):
            session.cursor.execute("DELETE FROM %(system)s.layers WHERE compid = '%(compid)s' AND product = '%(product)s' AND suffix = '%(suffix)s' AND mgrs = '%(mgrs)s' AND acqdatestr = '%(acqdatestr)s';" %query)
            if delete:
                return
        if record == None or overwrite:
            session.cursor.execute("INSERT INTO %(system)s.layers (compid, source, product, suffix, mgrs, acqdatestr, createdate) VALUES ('%(compid)s', '%(source)s', '%(product)s', '%(suffix)s', '%(mgrs)s', '%(acqdatestr)s', '%(today)s')" %query)

            session.conn.commit()
            if layer.datum.acqdate:
                query['acqdate'] = layer.datum.acqdate
                query['doy'] = layer.datum.doy
                session.cursor.execute("UPDATE %(system)s.layers SET (acqdate, doy) = ('%(acqdate)s', %(doy)d) WHERE compid = '%(compid)s' AND product = '%(product)s' AND suffix = '%(suffix)s' AND mgrs = '%(mgrs)s' AND acqdatestr = '%(acqdatestr)s';" %query)
                session.conn.commit()
        '''
        #end InsertRegionLayer
        
    
    InsertCompDef(session, layer.comp)
    InsertCompProd(session, layer.comp)
        
    if layer.comp.system == 'system':
        InsertRegionLayer()    
    elif layer.comp.system == 'regions':
        InsertRegionLayer()
    elif layer.comp.system == 'ancillary':
        InsertRegionLayer()
    elif layer.comp.system == 'specimen':
        InsertSpecimenLayer(layer.comp.system,layer)
    elif layer.comp.system == 'landsat':
        InsertLandsatLayer(layer.comp.system,layer)
    elif layer.comp.system == 'modis':
        InsertMODISLayer(layer.comp.system,layer)
    elif layer.comp.system == 'sentinel':
        InsertSentinelLayer()
    else:
        exitstr = 'unknown system (compositions.py: InsertLayer): %s' %(system)
        exit(exitstr)
               
def SelectCompAlt(session,compQ,inclL):
    #self.process.compinD,self.process.period.startdate,self.process.period.enddate,location,self.process.proj.system
    #Quert that looks for input data
    #querysys = {'system':system}
    querystem = 'SELECT C.source, C.product, B.folder, B.band, B.prefix, C.suffix, C.masked, C.cellnull, C.celltype, B.measure, B.scalefac, B.offsetadd, B.dataunit '   
    query ='FROM %(system)s.compdefs AS B ' %compQ
    querystem = '%s %s ' %(querystem, query)
    query ='INNER JOIN %(system)s.compprod AS C ON (B.compid = C.compid)' %compQ
    querystem = '%s %s ' %(querystem, query)
    #query = {'system':system,'id':compid}
    
    selectQuery = {}
    for item in inclL:
        selectQuery[item] = {'col': item, 'op':'=', 'val': compQ[item]}
        
    print ('selectQuery',selectQuery)
    wherestatement = session._DictToSelect(selectQuery)
    print ('wherestatement',wherestatement) 
    
    
    # for item in inclL:
    #    querypart = "WHERE B.%(item)s = '%(folder)s' AND B.band = '%(band)s'" %compQ
    querystem = '%s %s;' %(querystem, wherestatement)

    session.cursor.execute(querystem)
    records = session.cursor.fetchall()
    return records

    params = ['source', 'product', 'folder', 'band', 'prefix', 'suffix', 'masked', 'cellnull', 'celltype', 'measure', 'scalefac', 'offsetadd', 'dataunit']

    if len(records) == 1:
        return dict(zip(params,records[0]))
    else:
        print ('querystem',querystem)
        print ('query',compQ)
        BALLE
        
def SelectComp(session, compQ):
    params = ['source', 'product', 'folder', 'band', 'prefix', 'suffix', 'masked', 'cellnull', 'celltype', 'measure', 'scalefac', 'offsetadd', 'dataunit']

    inclL = ['folder','band']
    records = SelectCompAlt(session,compQ,inclL)
    if len(records) == 1:
        return dict(zip(params,records[0]))
    else:
        inclL = ['folder','band', 'suffix']
        records = SelectCompAlt(session,compQ,inclL)
        if len(records) == 1:
            return dict(zip(params,records[0]))
        else:
            print ('records', records)
            print ('query',compQ)
            BALLE
    '''    
    #self.process.compinD,self.process.period.startdate,self.process.period.enddate,location,self.process.proj.system
    #Quert that looks for input data
    #querysys = {'system':system}
    querystem = 'SELECT C.source, C.product, B.folder, B.band, B.prefix, C.suffix, C.masked, C.cellnull, C.celltype, B.measure, B.scalefac, B.offsetadd, B.dataunit '   
    query ='FROM %(system)s.compdefs AS B ' %compQ
    querystem = '%s %s ' %(querystem, query)
    query ='INNER JOIN %(system)s.compprod AS C ON (B.compid = C.compid)' %compQ
    querystem = '%s %s ' %(querystem, query)
    #query = {'system':system,'id':compid}
    querypart = "WHERE B.folder = '%(folder)s' AND B.band = '%(band)s'" %compQ
    querystem = '%s %s' %(querystem, querypart)

    session.cursor.execute(querystem)
    records = session.cursor.fetchall()
    
    if len(records) == 1:
        return dict(zip(params,records[0]))
    else:
        print ('querystem',querystem)
    '''   
    