from _curses import savetty

import mysql.connector

mydb=mysql.connector.connect(
    host="localhost",
    user="root",
    passwd="",
    database="scrapydb"
    )

myac=mydb.cursor()

source = 'OpenCulture'


def saveTriple(s,sa,p,o,repository):
    if  o == 'None' or o == '' or o == None:
        pass
    else:
        sql = "INSERT INTO oerintegrationdb.cleantriple(subject, subject_aux, predicate, object,repository) VALUES(%s,%s,%s,%s,%s)"
        values = (s, sa, p, o, repository)
        myac.execute(sql, values)
        mydb.commit()


#------------------- TO get Row from DB ---------------------
#Los predicados son las columnas de la tabla OER
myac.execute('desc oer')
predicates = myac.fetchall()

query = 'select * from oer o join oer_pages op on o.pages_id = op.id where op.name = "{}";'.format(source)
myac.execute(query)
oer = myac.fetchall()
totalOer = len(oer)
for o in oer:
    # Valores de Llaves Foraneas

    #Definiendo el sujeto
    idOer = o[0]
    SUBJECT = o[8]
    subject_aux = '{0}-{1}'.format(source.replace(' ', ''), idOer)
    saveTriple(SUBJECT,subject_aux,'repository_oer',source,source)

    # tabla Categoria de OER
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


    for p in predicates:

        idxPredicate = predicates.index(p)
        if idxPredicate == 10 or idxPredicate == 11 or idxPredicate == 12:
            pass
        else:
            #recorriendo todos las columnas de la tabla para un OER"
            PREDICATE = p[0]
            OBJECT = str(o[idxPredicate]).strip()
            saveTriple(SUBJECT, subject_aux, PREDICATE, OBJECT, source)
    #Descargas
    query = 'select od.name, od.url from oer_downloads od join oer o on od.oer_id = o.id where o.id ={}'.format(idOer)
    myac.execute(query)
    dowloadsLink = myac.fetchall()
    nroDownloads = len(dowloadsLink)
    if nroDownloads > 0:
        obj_subj = 'download-{0}-{1}'.format(source.replace(' ',''),idOer)
        saveTriple(SUBJECT,subject_aux,'downloads', obj_subj,source)
    
    for d in dowloadsLink:
        #Falta un triple mas de download
        subjectDownload = 'download-{}'.format(dowloadsLink.index(d))
        saveTriple(obj_subj, '','has',subjectDownload,source)

        saveTriple(subjectDownload, '','name-download',d[0],source)
        saveTriple(subjectDownload, '','url-download',d[1],source)

    print("{0} / {1}".format(oer.index(o),totalOer))
print('Total OER procesados {0} de {1}'.format(totalOer,source))


