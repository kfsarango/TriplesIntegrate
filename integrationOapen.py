import csv
import mysql.connector

mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    passwd="",
    database="scrapydb"
)

myac = mydb.cursor()


def saveTriple(s, sa, p, o, repository):
    if o == 'None' or o == '' or o == None:
        pass
    else:
        sql = "INSERT INTO cleantriple(subject, subject_alternative, predicate, object,repository) VALUES(%s,%s,%s,%s,%s)"
        values = (s, sa, p, o, repository)
        myac.execute(sql, values)
        mydb.commit()

source = 'Oapen'

with open('dt/oapen.csv') as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')
    line_count = 0
    firstRow = True
    headers = None
    cont = 0
    for row in csv_reader:
        if firstRow:
            headers = []
            for x in row:
                headers.append(x.lower())
            firstRow = False
        else:
            SUBJECT = row[22].split('|')[0]
            subject_aux = row[21]
            for h in headers:
                idx = headers.index(h)
                valueField = row[idx]
                if h == 'oapen_url':
                    valueField = valueField.split('|')
                    saveTriple(SUBJECT,subject_aux,'url_native',valueField[0],source)
                    for ud in valueField[1:]:
                        saveTriple(SUBJECT,subject_aux,'url_download',ud,source)
                else:
                    valueField = valueField.split('|')
                    for data in valueField:
                        saveTriple(SUBJECT,subject_aux,h,data,source)
        cont+=1
        print(cont)
    print(f'Processed {line_count} lines.')