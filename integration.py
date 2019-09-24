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
        sql = "INSERT INTO cleantriple(subject, subject_alternative, predicate, object,repository) VALUES(%s,%s,%s,%s,%s)"
        values = (s, sa, p, o, repository)
        myac.execute(sql, values)
        mydb.commit()


#------------------- TO get Row from DB ---------------------
#Los predicados son las columnas de la tabla OER
myac.execute('desc merlot')
predicates = myac.fetchall()

#query = 'select * from oer o join oer_pages op on o.pages_id = op.id where op.name = "{}";'.format(source)
query = 'select * from merlot;'
myac.execute(query)
oer = myac.fetchall()
totalOer = len(oer)
for o in oer:
    
    idOer = o[0]
    SUBJECT = o[2]

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
    '''

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
        '''
        idxPredicate = predicates.index(p)
        PREDICATE = p[0]
        OBJECT = str(o[idxPredicate]).strip()
        saveTriple(SUBJECT, subject_aux, PREDICATE, OBJECT, source)

    #MERLOT - Foreing Keys
    query = f'select * from merlot_link_resource where merlot_id = {idOer} and url != "" limit 1;'
    myac.execute(query)
    merlotLinkResource = myac.fetchall()
    for mlr in merlotLinkResource:
        saveTriple(SUBJECT, subject_aux, 'url_external', mlr[1], source)
        saveTriple(SUBJECT, subject_aux, 'url_external_status',mlr[2], source)

    query = f'select * from merlot_info_meta where merlot_id = {idOer};'
    myac.execute(query)
    metas = myac.fetchall()
    for m in metas:
        obj_subj = f'meta-{m[0]}'
        saveTriple(SUBJECT,subject_aux,'hasInformationMeta',obj_subj,source)

        saveTriple(obj_subj,subject_aux,'attr_name',m[1],source)
        saveTriple(obj_subj, subject_aux, 'attr_value', m[2], source)
        saveTriple(obj_subj, subject_aux, 'content', m[3], source)

    query = f'select id, author,organization,email from merlot_author where material ="{SUBJECT}";'
    myac.execute(query)
    autores = myac.fetchall()
    for a in autores:
        obj_subj = f'author-{a[0]}'
        saveTriple(SUBJECT, subject_aux, 'hasAutor', obj_subj, source)

        saveTriple(obj_subj, subject_aux, 'author_name', a[1], source)
        saveTriple(obj_subj, subject_aux, 'author_organization', a[2], source)
        saveTriple(obj_subj, subject_aux, 'author_email', a[3], source)

    query = f'select id, category,category_link from merlot_category where material ="{SUBJECT}";'
    myac.execute(query)
    categories = myac.fetchall()
    for c in categories:
        obj_subj = f'category-{c[0]}'
        saveTriple(SUBJECT, subject_aux, 'hasCategory', obj_subj, source)

        saveTriple(obj_subj, subject_aux, 'category_name', c[1], source)
        saveTriple(obj_subj, subject_aux, 'category_link', c[2], source)

    #Descargas
    '''
    # Descargas
    query = 'select od.name, od.url from oer_downloads od join oer o on od.oer_id = o.id where o.id ={}'.format(idOer)
    myac.execute(query)
    dowloadsLink = myac.fetchall()
    nroDownloads = len(dowloadsLink)
    if nroDownloads > 0:
        obj_subj = 'download-{0}-{1}'.format(source.replace(' ', ''), idOer)
        saveTriple(SUBJECT, subject_aux, 'downloads', obj_subj, source)

    for d in dowloadsLink:
        # Falta un triple mas de download
        subjectDownload = 'download-{}'.format(dowloadsLink.index(d))
        saveTriple(obj_subj, '', 'has', subjectDownload, source)

        saveTriple(subjectDownload, '','name-download',d[0],source)
        saveTriple(subjectDownload, '','url-download',d[1],source)
    '''

    print("{0} / {1}".format(oer.index(o), totalOer))
print('Total OER procesados {0} de {1}'.format(totalOer, source))


