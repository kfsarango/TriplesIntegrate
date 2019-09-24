import mysql.connector

mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    passwd="",
    database="scrapydb"
)

myac = mydb.cursor()

source = 'Ted Talks'

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

query = 'select * from ted_talks;'
myac.execute(query)
teds = myac.fetchall()
totalTed = len(teds)
for t in teds:
    idTalkTable = t[0]
    SUBJECT = t[21]
    subject_aux = f'TalkId-{t[1]}'
    for p in predicates:
        idxPredicate = predicates.index(p)
        PREDICATE = p[0]
        OBJECT = str(t[idxPredicate]).strip()
        saveTriple(SUBJECT, subject_aux, PREDICATE, OBJECT, source)

    #Llaves Foraneas
    query = f'select * from ted_related_talks where talks_id = {idTalkTable};'
    myac.execute(query)
    related_talks = myac.fetchall()
    for rt in related_talks:
        obj_subj = rt[1]
        saveTriple(SUBJECT, subject_aux, 'hasRelatedTalk', obj_subj, source)

        saveTriple(obj_subj, subject_aux, 'talk_id', rt[1], source)
        saveTriple(obj_subj, subject_aux, 'title', rt[2], source)
        saveTriple(obj_subj, subject_aux, 'speaker', rt[3], source)
        saveTriple(obj_subj, subject_aux, 'duration', rt[4], source)
        saveTriple(obj_subj, subject_aux, 'slug', rt[5], source)


    query = f'select ttl.languageName, ttl.endonym, ttl.languageCode, ttl.ianaCode from scrapydb.ted_talk_languages ttl join ted_talks_has_languages middle on ttl.id = middle.languages_id join ted_talks tt on middle.talks_id = tt.id where tt.id = {idTalkTable};'
    myac.execute(query)
    languages = myac.fetchall()
    for l in languages:
        obj_subj = l[0]
        saveTriple(SUBJECT, subject_aux, 'hasLanguage', obj_subj, source)

        saveTriple(obj_subj, subject_aux, 'languageName', l[1], source)
        saveTriple(obj_subj, subject_aux, 'endonym', l[2], source)
        saveTriple(obj_subj, subject_aux, 'ianaCode', l[3], source)

    query = f'select * from scrapydb.ted_native_dowloads where talks_id = {idTalkTable};'
    myac.execute(query)
    native_dowloads = myac.fetchall()
    for nd in native_dowloads:
        obj_subj = f'nativeDownload-{nd[0]}'
        saveTriple(SUBJECT, subject_aux, 'hasNativeDownload', obj_subj, source)

        saveTriple(obj_subj, subject_aux, 'low', nd[1], source)
        saveTriple(obj_subj, subject_aux, 'medium', nd[2], source)
        saveTriple(obj_subj, subject_aux, 'high', nd[3], source)
        saveTriple(obj_subj, subject_aux, 'audiodownload', nd[4], source)

    query = f'select tr.name from scrapydb.ted_ratings tr join ted_talks_has_ratings middle on tr.id = middle.ratings_id join ted_talks tt on middle.talks_id = tt.id where tt.id = {idTalkTable};'
    myac.execute(query)
    ratings = myac.fetchall()
    for r in ratings:
        saveTriple(SUBJECT, subject_aux, 'hasRating', r[0], source)

    query = f'select ttag.name from scrapydb.ted_tags ttag join ted_talks_has_tags middle on ttag.id = middle.tags_id join ted_talks tt on middle.talks_id = tt.id where tt.id = {idTalkTable};'
    myac.execute(query)
    tags = myac.fetchall()
    for tag in tags:
        saveTriple(SUBJECT, subject_aux, 'hasTag', tag[0], source)

    query = f'select te.id, te.speaker_id, te.slug,te.is_published, te.firstname, te.lastname,te.middleinitial,te.title,te.description,te.photo_url,te.whatotherssay,te.whotheyare,te.whylisten from scrapydb.ted_speakers te join ted_talks_has_speakers middle on te.id = middle.speakers_id join ted_talks tt on middle.talks_id = tt.id where tt.id = {idTalkTable};'
    myac.execute(query)
    speakers = myac.fetchall()
    for s in speakers:
        obj_subj = s[1]
        saveTriple(SUBJECT, subject_aux, 'hasSpeaker', obj_subj, source)

        saveTriple(obj_subj, subject_aux, 'id', s[0], source)
        saveTriple(obj_subj, subject_aux, 'speaker_id', s[1], source)
        saveTriple(obj_subj, subject_aux, 'slug', s[2], source)
        saveTriple(obj_subj, subject_aux, 'is_published', s[3], source)
        saveTriple(obj_subj, subject_aux, 'firstname', s[4], source)
        saveTriple(obj_subj, subject_aux, 'lastname', s[5], source)
        saveTriple(obj_subj, subject_aux, 'middleinitial', s[6], source)
        saveTriple(obj_subj, subject_aux, 'title', s[7], source)
        saveTriple(obj_subj, subject_aux, 'description', s[8], source)
        saveTriple(obj_subj, subject_aux, 'photo_url', s[9], source)
        saveTriple(obj_subj, subject_aux, 'whatotherssay', s[10], source)
        saveTriple(obj_subj, subject_aux, 'whotheyare', s[11], source)
        saveTriple(obj_subj, subject_aux, 'whylisten', s[12], source)

    query = f'select * from scrapydb.ted_subtitled_downloads where talks_id = {idTalkTable};'
    myac.execute(query)
    subtitle_dowloads = myac.fetchall()
    for sd in subtitle_dowloads:
        obj_subj = f'subtitledDownload-{sd[0]}'
        saveTriple(SUBJECT, subject_aux, 'hasSubtitledDownload', obj_subj, source)

        saveTriple(obj_subj, subject_aux, 'name', sd[1], source)
        saveTriple(obj_subj, subject_aux, 'low', sd[2], source)
        saveTriple(obj_subj, subject_aux, 'high', sd[3], source)

    print("{0} / {1}".format(teds.index(t),totalTed))


