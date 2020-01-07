import mysql.connector

mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    passwd="",
    database="scrapydb"
)

myac = mydb.cursor()

source = 'Feedbooks'

def saveTriple(s, sa, p, o, repository):
    if o == 'None' or o == '' or o == None:
        pass
    else:
        sql = "INSERT INTO cleantriple(subject, subject_alternative, predicate, object,repository) VALUES(%s,%s,%s,%s,%s)"
        values = (s, sa, p, o, repository)
        myac.execute(sql, values)
        mydb.commit()


#------------------- TO get Row from DB ---------------------
#Los predicados son las columnas de la tabla ted
myac.execute('desc ted_talks')
predicates = myac.fetchall()

#lista de enlaces de los OER
query = "SELECT distinct(source) FROM oerintegrationdb.cleantriple where predicate = 'hasBook' and subject = 'feedbooks';"
myac.execute(query)
oer = myac.fetchall()
totalOer = len(oer)
for o in oer:
    SUBJECT = o[0]
    subject_aux = ''

    #consultando metadatos de los OER
    query = f"select * from oerintegrationdb.cleantriple where source ='{SUBJECT}' and predicate != 'hasBook';"
    myac.execute(query)
    oer_meta = myac.fetchall()
    for p in oer_meta:
        saveTriple(SUBJECT, subject_aux, p[2], p[3], source)

    print("{0} / {1}".format(oer.index(o),totalOer))