import mysql.connector

mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    passwd="",
    database="scrapydb"
)

myac = mydb.cursor()

source = 'Merlot'

def getUrlOpenCulture(lstDownloads):
    for d in lstDownloads:
        download = d[1]
        if download.find('www.oercommons.org') >= 0:
            return download
    return None

def saveTriple(s, sa, p, o, repository):
    if o == 'None' or o == '' or o == None:
        pass
    else:
        sql = "INSERT INTO scrapydb.cleantriple(subject, subject_alternative, predicate, object,repository) VALUES(%s,%s,%s,%s,%s)"
        values = (s, sa, p, o, repository)
        myac.execute(sql, values)
        mydb.commit()


<<<<<<< HEAD
#------------------- TO get Row from DB ---------------------
#Los predicados son las columnas de la tabla OER
myac.execute('desc merlot')
=======
# ------------------- TO get Row from DB ---------------------
# Los predicados son las columnas de la tabla OER
myac.execute('desc oer')
>>>>>>> c2e31550fb680944e4f6c3e5c88e581e64deae4a
predicates = myac.fetchall()

#query = 'select * from oer o join oer_pages op on o.pages_id = op.id where op.name = "{}";'.format(source)
query = 'select * from merlot limit 100;'
myac.execute(query)
oer = myac.fetchall()
totalOer = len(oer)
for o in oer:
    
    idOer = o[0]
    SUBJECT = o[2]

<<<<<<< HEAD
    #Descargas
    '''
    query = 'select od.name, od.url from oer_downloads od join oer o on od.oer_id = o.id where o.id ={};'.format(idOer)
    myac.execute(query)
    dowloadsLink = myac.fetchall()
    '''
    
    #Definiendo el sujeto

    subject_aux = ''
    '''

    if len(str(SUBJECT).strip()) == 0:
        testUrlToSubject = getUrlOpenCulture(dowloadsLink)
        if testUrlToSubject == None:
            SUBJECT = '{0}-{1}'.format(source.replace(' ', ''), idOer)
        else:
            SUBJECT = testUrlToSubject
    else:
        subject_aux = ''
    '''

    saveTriple(SUBJECT,subject_aux,'repository_oer',source,source)
=======
    # Definiendo el sujeto
    idOer = o[0]
    SUBJECT = o[8]
    subject_aux = '{0}-{1}'.format(source.replace(' ', ''), idOer)
    saveTriple(SUBJECT, subject_aux, 'repository_oer', source, source)
>>>>>>> c2e31550fb680944e4f6c3e5c88e581e64deae4a

    
    # Valores de Llaves Foraneas
    # tabla Categoria de OER
    '''
    query = 'select name from oer_categories where id={}'.format(int(o[12]))
    myac.execute(query)
    objCategoryOer = myac.fetchall()
    objCategoryOer = objCategoryOer[0][0]
    saveTriple(SUBJECT, subject_aux, 'category_oer', objCategoryOer, source)

    # tabla Tipo de OER
    query = 'select name from oer_types where id={}'.format(int(o[10]))
    myac.execute(query)
    objTypeOer = myac.fetchall()
    objTypeOer = objTypeOer[0][0]
    saveTriple(SUBJECT, subject_aux, 'type_oer', objTypeOer, source)
<<<<<<< HEAD
    '''
=======
>>>>>>> c2e31550fb680944e4f6c3e5c88e581e64deae4a

    for p in predicates:
        '''
        idxPredicate = predicates.index(p)
        if idxPredicate == 10 or idxPredicate == 11 or idxPredicate == 12:
            pass
        else:
            # recorriendo todos las columnas de la tabla para un OER"
            PREDICATE = p[0]
            OBJECT = str(o[idxPredicate]).strip()
            saveTriple(SUBJECT, subject_aux, PREDICATE, OBJECT, source)
<<<<<<< HEAD
        '''
        idxPredicate = predicates.index(p)
        PREDICATE = p[0]
        OBJECT = str(o[idxPredicate]).strip()
        saveTriple(SUBJECT, subject_aux, PREDICATE, OBJECT, source)
    #Descargas
    '''
=======
    # Descargas
    query = 'select od.name, od.url from oer_downloads od join oer o on od.oer_id = o.id where o.id ={}'.format(idOer)
    myac.execute(query)
    dowloadsLink = myac.fetchall()
>>>>>>> c2e31550fb680944e4f6c3e5c88e581e64deae4a
    nroDownloads = len(dowloadsLink)
    if nroDownloads > 0:
        obj_subj = 'download-{0}-{1}'.format(source.replace(' ', ''), idOer)
        saveTriple(SUBJECT, subject_aux, 'downloads', obj_subj, source)

    for d in dowloadsLink:
        # Falta un triple mas de download
        subjectDownload = 'download-{}'.format(dowloadsLink.index(d))
        saveTriple(obj_subj, '', 'has', subjectDownload, source)

<<<<<<< HEAD
        saveTriple(subjectDownload, '','name-download',d[0],source)
        saveTriple(subjectDownload, '','url-download',d[1],source)
    '''
=======
        saveTriple(subjectDownload, '', 'name-download', d[0], source)
        saveTriple(subjectDownload, '', 'url-download', d[1], source)
>>>>>>> c2e31550fb680944e4f6c3e5c88e581e64deae4a

    print("{0} / {1}".format(oer.index(o), totalOer))
print('Total OER procesados {0} de {1}'.format(totalOer, source))


