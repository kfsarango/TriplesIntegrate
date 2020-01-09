import csv
import mysql.connector


dataDb = mysql.connector.connect(
    host="localhost",
    user="root",
    passwd="root",
    database="metaoerdb"
)

repositories = [
    #{'id': 1, 'r': 'Merlot', 's': 'id'},
    {'id': 2, 'r': 'Galileo Open', 's': 'url_native'},
    #{'id': 3,'r':'OpenCulture','s':'repository_oer'},
    ]

def getTriplesByIdentifier(allData, identifier):

    newList = []
    idx = 0
    try:
        while allData[idx][3] == identifier:
            newList.append(allData[idx])
            allData.pop(idx)
    except Exception as ex:
        print(ex)
    return newList

mydb = dataDb.cursor()

for r in repositories:
    query = f'SELECT t.subject, t.predicate,t.object,  i.subject, i.subject_alternative FROM Triples t join Identifiers i on t.identifier_id = i.id where i.repository_id = {r["id"]};'
    mydb.execute(query)
    metaTriples = mydb.fetchall()

    query = f'SELECT distinct (t.predicate) FROM Triples t join Identifiers i on t.identifier_id = i.id where i.repository_id ={r["id"]};'
    mydb.execute(query)
    predicates = mydb.fetchall()

    f = open(f'dt/{r["r"].replace(" ","")}MetaData.csv', 'w')
    with f:
        listHeaders = ["identifier"]
        [listHeaders.append(p[0]) for p in predicates]
        writer = csv.DictWriter(f, fieldnames=listHeaders)
        writer.writeheader()
        lenData = len(metaTriples)
        while lenData != 0:
            identifier = metaTriples[0][3]
            dataByOer = getTriplesByIdentifier(metaTriples, identifier)
            dict = {"identifier": identifier}
            for d in dataByOer:
                dict[f"{d[1]}"] = d[2]
            writer.writerow(dict)
            lenData = len(metaTriples)
