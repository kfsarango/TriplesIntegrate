from nltk import word_tokenize, pos_tag
from nltk.corpus import wordnet as wn
import pandas as pd
from sqlalchemy import create_engine
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import sent_tokenize, word_tokenize
from nltk.tokenize import RegexpTokenizer
from nltk.stem import WordNetLemmatizer
from nltk.tag import StanfordNERTagger

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

def saveData(data):
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
query = "SELECT * FROM cleantriple where predicate = 'title' or predicate = 'description' limit 4;"
df = pd.read_sql(query, engine)
#totalRows = len(df.values())
for d in df.values:
    SUBJECT = d[0]
    predicate = d[2]
    if predicate == 'title':
        title = d[3]
        obj_subj_Nlp = 'titleNLP'
        dt = {'subject': SUBJECT, 'subject_alternative': '','predicate':'hasNltkData','object': obj_subj_Nlp}
        saveData(dt)

        res_preprocesamiento = preprocess(title)

        for word in nltk.pos_tag(res_preprocesamiento):
            token = word[0]
            dt = {'subject': obj_subj_Nlp, 'subject_alternative': SUBJECT, 'predicate': 'hasToken', 'object': token}
            saveData(dt)
            #Para encontrar la palabra raiz
            lema = lemmatizer.lemmatize(token)
            dt = {'subject': token, 'subject_alternative': SUBJECT, 'predicate': 'hasLema', 'object': lema}
            saveData(dt)

            posTag = word[1]
            dt = {'subject': token, 'subject_alternative': SUBJECT, 'predicate': 'hasPosTag', 'object': posTag}
            saveData(dt)

            #synsets
            for syn in wn.synsets(token):
                synName = syn.name()
                dt = {'subject': token, 'subject_alternative': SUBJECT, 'predicate': 'hasSynset', 'object': synName}
                saveData(dt)

                definition = syn.definition()
                dt = {'subject': synName, 'subject_alternative': SUBJECT, 'predicate': 'hasSynsetDefinition', 'object': definition}
                saveData(dt)



                for e in syn.examples():
                    dt = {'subject': synName, 'subject_alternative': SUBJECT, 'predicate': 'hasSynsetExample','object': e}
                    saveData(dt)

                for l in syn.lemmas():
                    synonym = l.name()
                    dt = {'subject': synName, 'subject_alternative': SUBJECT, 'predicate': 'hasSynsetSynonym','object': synonym}
                    saveData(dt)
                    if l.antonyms():
                        antonym = l.antonyms()[0].name()
                        dt = {'subject': synName, 'subject_alternative': SUBJECT, 'predicate': 'hasSynsetAntonym',
                              'object': antonym}
                        saveData(dt)

                for h in syn.hypernyms():
                    hyper = h.name()
                    dt = {'subject': synName, 'subject_alternative': SUBJECT, 'predicate': 'hasSynsetHyperonym',
                          'object': hyper}
                    saveData(dt)

                for h in syn.hyponyms():
                    hypo = h.name()
                    dt = {'subject': synName, 'subject_alternative': SUBJECT, 'predicate': 'hasSynsetHyponym',
                          'object': hypo}
                    saveData(dt)
    else:
        description = d[3]
        obj_subj_Nlp = 'descriptionNLP'
        dt = {'subject': SUBJECT, 'subject_alternative': '', 'predicate': 'hasNltkData', 'object': obj_subj_Nlp}
        saveData(dt)

        # calcular frecuencia de palabras
        res_preprocesamiento = preprocess(description)
        freq = nltk.FreqDist(res_preprocesamiento)
        mostRelevants = freq.most_common(5)
        contImportant = 1
        for key, val in mostRelevants:
            o_s_Important = f'important-{contImportant}'
            dt = {'subject': obj_subj_Nlp, 'subject_alternative': SUBJECT, 'predicate': 'hasImportantWord', 'object': o_s_Important}
            saveData(dt)

            dt = {'subject': o_s_Important, 'subject_alternative': SUBJECT, 'predicate': 'hasWord',
                  'object': key}
            saveData(dt)
            dt = {'subject': o_s_Important, 'subject_alternative': SUBJECT, 'predicate': 'hasWordCount',
                  'object': val}
            saveData(dt)
            contImportant += 1

        #NER
        results = stanford_ner_tagger.tag(description.split())
        cont = 1
        for result in results:
            tag_value = result[0]
            tag_type = result[1]
            if tag_type != 'O':
                o_s_Entity = f'entity-{cont}'
                dt = {'subject': obj_subj_Nlp, 'subject_alternative': SUBJECT, 'predicate': 'hasEntity',
                      'object': o_s_Entity}
                saveData(dt)

                dt = {'subject': o_s_Entity, 'subject_alternative': SUBJECT, 'predicate': 'hasEntityType',
                      'object': tag_type}
                saveData(dt)
                dt = {'subject': o_s_Entity, 'subject_alternative': SUBJECT, 'predicate': 'hasEntityValue',
                      'object': tag_value}
                saveData(dt)
                cont += 1
    #print(f'{df.values.__index__(d)} / {totalRows}')