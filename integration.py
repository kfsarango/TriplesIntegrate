import mysql.connector

mydb=mysql.connector.connect(
    host="localhost",
    user="root",
    passwd="",
    database="scrapydb"
    )

myac=mydb.cursor()

source = 'TextBook Revolution'

#------------------- TO get Row from DB ---------------------
#Los predicados son las columnas de la tabla OER
myac.execute('desc oer')
predicates = myac.fetchall()

query = 'select * from oer o join oer_pages op on o.pages_id = op.id where op.name = "{}" limit 10;'.format(source)
myac.execute(query)
oer = myac.fetchall()

for o in oer:
    print(o)
    for p in predicates:
        break
        idxPredicate = predicates.index(p)
        #recorriendo todos las columnas de la tabla para un OER
        pre = p
        obj = o[23]
        print('Predicate: {0} Object: {1}'.format(pre,obj))
        
    
#Se completo hasta el id 53228