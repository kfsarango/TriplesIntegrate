import math
import pandas as pd
from functools import reduce

corpus =  """
Simple example with Cats and Mouse
Another simple example with dogs and cats
Another simple example with mouse and cheese
""".split("\n")[1:-1]

# clearing and tokenizing
l_A = corpus[0].lower().split()
l_B = corpus[1].lower().split()
l_C = corpus[2].lower().split()
print(l_A)
print(l_B)
print(l_C)

# Calculating bag of words
word_set = set(l_A).union(set(l_B)).union(set(l_C))
print(word_set)

word_dict_A = dict.fromkeys(word_set, 0)
word_dict_B = dict.fromkeys(word_set, 0)
word_dict_C = dict.fromkeys(word_set, 0)

for word in l_A:
    word_dict_A[word] += 1

for word in l_B:
    word_dict_B[word] += 1

for word in l_C:
    word_dict_C[word] += 1

print(pd.DataFrame([word_dict_A, word_dict_B, word_dict_C]))

def compute_tf(word_dict, l):
    tf = {}
    sum_nk = len(l)
    for word, count in word_dict.items():
        tf[word] = count / sum_nk
    return tf


tf_A = compute_tf(word_dict_A, l_A)
tf_B = compute_tf(word_dict_B, l_B)
tf_C = compute_tf(word_dict_C, l_C)


def compute_idf(strings_list):
    n = len(strings_list)
    idf = dict.fromkeys(strings_list[0].keys(), 0)
    for l in strings_list:
        for word, count in l.items():
            if count > 0:
                idf[word] += 1

    for word, v in idf.items():
        idf[word] = math.log(n / float(v))
    return idf


idf = compute_idf([word_dict_A, word_dict_B, word_dict_C])


def compute_tf_idf(tf, idf):
    tf_idf = dict.fromkeys(tf.keys(), 0)
    for word, v in tf.items():
        tf_idf[word] = v * idf[word]
    return tf_idf


tf_idf_A = compute_tf_idf(tf_A, idf)
tf_idf_B = compute_tf_idf(tf_B, idf)
tf_idf_C = compute_tf_idf(tf_C, idf)

print(pd.DataFrame([tf_idf_A, tf_idf_B, tf_idf_C]))
