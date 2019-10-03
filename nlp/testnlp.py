from nltk import word_tokenize, pos_tag
from nltk.corpus import wordnet as wn
import pandas as pd
from sqlalchemy import create_engine
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import sent_tokenize, word_tokenize
from nltk.tokenize import RegexpTokenizer
from nltk.stem import WordNetLemmatizer

# Init the Wordnet Lemmatizer
lemmatizer = WordNetLemmatizer()
synonyms = []


# Función tokenizar y eliminar stop words
def preprocess(sentence):
	sentence = sentence.lower()
	tokenizer = RegexpTokenizer(r'\w+')
	#Tokenizar
	tokens = tokenizer.tokenize(sentence)
	# Eliminar stop words
	filtered_words = [w for w in tokens if not w in stopwords.words('english')]
	return filtered_words


stringDB = "mysql+pymysql://root:@localhost:3306/oerintegrationdb"
# Conexion a la BD
engine = create_engine(stringDB)
query = "SELECT * FROM cleantriple where predicate = 'title' limit 5;"
df = pd.read_sql(query, engine)


for d in df.values:
    title = d[3]
    print(f'\n\t\tTitle: {title}')
    res_preprocesamiento = preprocess(title)
    for word in nltk.pos_tag(res_preprocesamiento):
        lema = lemmatizer.lemmatize(word[0])
        data = {
            'token': word[0],
            'pos_tag': word[1],
            'lema': lema
        }
        print(data)
        print(f'Wordnet DEF: {data["token"]}')
        for syn in wn.synsets(word[0]):
            print(f'- {syn.definition()}')
        print('-------- * -------- * -------- * -------- * --------')