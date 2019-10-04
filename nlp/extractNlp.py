from nltk.corpus import wordnet as wn
import pandas as pd
from sqlalchemy import create_engine
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import sent_tokenize, word_tokenize
from nltk.tokenize import RegexpTokenizer
from nltk.stem import WordNetLemmatizer
from nltk.tag import StanfordNERTagger

import hashlib

stanford_ner_tagger = StanfordNERTagger(
    'src/stanford_ner/english.muc.7class.distsim.crf.ser.gz',
    'src/stanford_ner/stanford-ner-3.9.2.jar'
)

# Init the Wordnet Lemmatizer
lemmatizer = WordNetLemmatizer()


# Función tokenizar y eliminar stop words
def preprocess(sentence):
    sentence = sentence.lower()
    tokenizer = RegexpTokenizer(r'\w+')
    #Tokenizar
    tokens = tokenizer.tokenize(sentence)
    # Eliminar stop words
    filtered_words = [w for w in tokens if not w in stopwords.words('english')]
    return filtered_words

def saveData(s,sa,p,o):
    data = {'subject': s,
            'subject_alternative': sa,
            'predicate': p,
            'object': o}
    df = pd.DataFrame([data], columns=['subject',
                                       'subject_alternative',
                                       'predicate',
                                       'object'])
    # almacenar en base de datos
    df.to_sql(
        name='nlp_triples',
        con=engine,
        index=False,
        if_exists='append'
    )
stringDB = "mysql+pymysql://root:@localhost:3306/oerintegrationdb"
# Conexion a la BD
engine = create_engine(stringDB)
query = "SELECT * FROM cleantriple where predicate = 'title' or predicate = 'description' limit 2;"
df = pd.read_sql(query, engine)
totalRows = len(df.values)
contValues = 1
for d in df.values:
    SUBJECT = d[0]
    predicate = d[2]
    if predicate == 'title':
        title = d[3]
        hs = hashlib.md5(title.encode())
        title_md5 = hs.hexdigest()
        obj_subj_Nlp = f'title_NLP_{title_md5}'

        saveData(SUBJECT,'','hasNltkData',obj_subj_Nlp)
        saveData(obj_subj_Nlp,SUBJECT, 'hasTitleValue', title)

        res_preprocesamiento = preprocess(title)

        for word in nltk.pos_tag(res_preprocesamiento):
            token = word[0]
            saveData(obj_subj_Nlp,SUBJECT,'hasToken',token)
            #Para encontrar la palabra raiz
            lema = lemmatizer.lemmatize(token)
            saveData(token,SUBJECT,'hasLema',lema)

            posTag = word[1]
            saveData(token,SUBJECT,'hasPosTag',posTag)

            #synsets
            synss = wn.synsets(token)
            for syn in synss:
                positionSyn = synss.index(syn)
                synName = syn.name()
                saveData(token,SUBJECT,'hasSynset',synName)

                definition = syn.definition()
                saveData(synName,SUBJECT,'hasSynsetDefinition',definition)



                for e in syn.examples():
                    saveData(synName,SUBJECT,'hasSynsetExample',e)

                for l in syn.lemmas():
                    synonym = l.name()
                    saveData(synName,SUBJECT,'hasSynsetSynonym',synonym)
                    if l.antonyms():
                        antonym = l.antonyms()[0].name()
                        saveData(synName,SUBJECT,'hasSynsetAntonym',antonym)

                for h in syn.hypernyms():
                    hyper = h.name()
                    saveData(synName,SUBJECT,'hasSynsetHyperonym',hyper)

                for h in syn.hyponyms():
                    hypo = h.name()
                    saveData(synName,SUBJECT,'hasSynsetHyponym',hypo)
    else:
        description = d[3]
        hs = hashlib.md5(description.encode())
        description_md5 = hs.hexdigest()
        obj_subj_Nlp = f'description_NLP_{description_md5}'

        saveData(SUBJECT, '', 'hasNltkData', obj_subj_Nlp)
        saveData(obj_subj_Nlp, SUBJECT, 'hasDescriptionValue', description)

        # calcular frecuencia de palabras
        res_preprocesamiento = preprocess(description)
        freq = nltk.FreqDist(res_preprocesamiento)
        mostRelevants = freq.most_common(5)
        contImportant = 1
        for val, cou in mostRelevants:
            o_s_Important = f'importantWord_{contImportant}_{description_md5}'
            saveData(obj_subj_Nlp,SUBJECT,'hasImportantWord',o_s_Important)
            saveData(o_s_Important,SUBJECT,'hasWordValue',val)
            saveData(o_s_Important, SUBJECT, 'hasWordCount', cou)
            contImportant += 1

        #NER
        results = stanford_ner_tagger.tag(description.split())
        cont = 1
        for result in results:
            tag_value = result[0]
            tag_type = result[1]
            if tag_type != 'O':
                o_s_Entity = f'entity-{cont}_{description_md5}'
                saveData(obj_subj_Nlp,SUBJECT,'hasEntity',o_s_Entity)
                saveData(o_s_Entity,SUBJECT,'hasEntityType',tag_type)
                saveData(o_s_Entity,SUBJECT,'hasEntityValue',tag_value)
                cont += 1
    print(f'{contValues} / {totalRows}')
    contValues += 1