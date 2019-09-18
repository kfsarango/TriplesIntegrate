from _curses import savetty

import mysql.connector

mydb=mysql.connector.connect(
    host="localhost",
    user="root",
    passwd="",
    database="scrapydb"
    )

myac=mydb.cursor()

source = 'TextBook Revolution'


def saveTriple(s,p,o,repository):
    print('s->{} __ p->{} __ o->{}'.format(s,p,o))
    if o != '':
        sql = "INSERT INTO cleantriple(subject, predicate, object,repository) VALUES(%s,%s,%s,%s)"
        values = (s, p, o, repository)
        myac.execute(sql, values)
        mydb.commit()


#------------------- TO get Row from DB ---------------------
#Los predicados son las columnas de la tabla OER
myac.execute('desc oer')
predicates = myac.fetchall()

query = 'select * from oer o join oer_pages op on o.pages_id = op.id where op.name = "{}" limit 10;'.format(source)
myac.execute(query)
oer = myac.fetchall()

for o in oer:
    # Valores de Llaves Foraneas

    # tabla Repositorio de OER
    query = 'select name from oer_pages where id={}'.format(int(o[11]))
    myac.execute(query)
    objPageOer = myac.fetchall()
    objPageOer = objPageOer[0][0]
    print(objPageOer)

    #Definiendo el sujeto
    idOer = o[0]
    SUBJECT = '{0}-{1}'.format(objPageOer.replace(' ', ''), idOer)
    saveTriple(SUBJECT,'repository_oer',objPageOer,objPageOer)

    # tabla Categoria de OER
    query = 'select name from oer_categories where id={}'.format(int(o[12]))
    myac.execute(query)
    objCategoryOer = myac.fetchall()
    objCategoryOer = objCategoryOer[0][0]
    print(objCategoryOer)
    saveTriple(SUBJECT, 'category_oer', objCategoryOer, objPageOer)

    # tabla Tipo de OER
    query = 'select name from oer_types where id={}'.format(int(o[10]))
    myac.execute(query)
    objTypeOer = myac.fetchall()
    objTypeOer = objTypeOer[0][0]
    print(objTypeOer)
    saveTriple(SUBJECT, 'type_oer', objTypeOer, objPageOer)


    for p in predicates:

        idxPredicate = predicates.index(p)
        #recorriendo todos las columnas de la tabla para un OER
        PREDICATE = p[0]
        OBJECT = str(o[idxPredicate]).strip()
        saveTriple(SUBJECT, PREDICATE, OBJECT, objPageOer)

    break
g