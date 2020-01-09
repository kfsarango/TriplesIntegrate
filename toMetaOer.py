import mysql.connector


dataDb = mysql.connector.connect(
    host="localhost",
    user="root",
    passwd="root",
    database="metaoerdb"
)

mydb = dataDb.cursor()



def saveTriple(s, p, o, identifier):
    if o == 'None' or o == '' or o == None:
        pass
    else:
        sql = "INSERT INTO Triples(subject, predicate, object,identifier_id) VALUES(%s,%s,%s,%s)"
        mydb.execute(sql, (s, p, o, identifier))
        dataDb.commit()

def saveIdentifier(s, sa, rep):
    sql = "INSERT INTO Identifiers(subject, subject_alternative, repository_id) VALUES(%s,%s,%s)"
    mydb.execute(sql, (s, sa, rep))
    dataDb.commit()

def saveRepository(name):
    sql = "INSERT INTO Repositories(name) VALUES(%s)"
    mydb.execute(sql, (name,))
    dataDb.commit()

repositories = [
    {'id':10,'r':'OpenCulture','s':'repository_oer'},
    {'id':11,'r':'Galileo Open','s':'url_native'},
    {'id':1,'r':'OER Commons','s':'repository_oer'},
    {'id':2,'r':'Merlot','s':'id'},
    #{'id':3,'r':'Ted Talks'},
    #{'id':4,'r':'Oapen'},
    {'id':5,'r':'Orange Grove','s':'collection'},
    {'id':6,'r':'Skill Commons','s':'title'},
    {'id':7,'r':'OpenTextBooks','s':'hasTitle'},
    {'id':8,'r':'OpenBCcampus','s':'hasTitle'},
    {'id':9,'r':'Feedbooks','s':'hasTitle'}
    ]

def getIndexOfRespositories(dataList,separador):
    newList = []
    for idx, data in enumerate(dataList):
        if data[2] == separador:
            newList.append({'data':data,'idx':idx})
    return newList

contIdentifier = 72
for r in repositories[1:2]:
    repository = r['r']
    #consultando todas las tripletas de repository
    query = f"SELECT * FROM oerintegrationdb.cleantriple where repository = '{repository}';"
    mydb.execute(query)
    allDataRepository = mydb.fetchall()
    dataIndices = getIndexOfRespositories(allDataRepository,r['s'])
    dataIndices = dataIndices[:40]
    ledDataInx = len(dataIndices)
    for idx, di in enumerate(dataIndices):
        identifier = di['data'][0]
        # insert identifier
        saveIdentifier(identifier,'',r['id'])

        startIdx = di['idx']
        try:
            endIdx = dataIndices[idx + 1]['idx']
            # recorriendo triples
            for triple in allDataRepository[startIdx:endIdx]:
                tSubject = triple[0]
                if identifier == tSubject:
                    tSubject = str(contIdentifier)
                tPredicate = triple[2]
                tObject = triple[3]
                saveTriple(tSubject,tPredicate,tObject,contIdentifier)

        except Exception as ex:
            print('Execption')
            print(ex)
            for triple in allDataRepository[startIdx:]:
                tSubject = triple[0]
                if identifier == tSubject:
                    tSubject = str(contIdentifier)
                tPredicate = triple[2]
                tObject = triple[3]
                saveTriple(tSubject, tPredicate, tObject, contIdentifier)
                print(f'saved triple: identifier{contIdentifier}')
        print(f'{repository}: {idx}/{ledDataInx}')
        contIdentifier+=1