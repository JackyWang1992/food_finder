"""
index.py
author: Jiaqi Wang
E-mail: wangjiaqi2017@brandeis.edu
his is one of the two main files for this project. It builds the various shelf files
that the information retrieval system uses. Running it takes about 30 seconds.
"""

import json
import math
from collections import defaultdict
import shelve
from nltk.corpus import stopwords
from nltk import word_tokenize
from nltk import PorterStemmer

# the dictionary from term to list of ids
tm_dct = defaultdict(list)
# the dictionary from id to another dictionary of movie data
doc_data_dct = dict()
# the dictionary from token to its frequency: to analyze which word should be added to stop word list
# token_dct = dict()  # you can uncomment here if you like to see how I see the frequencies of tokens
# the nested dictionary from film ids to all of the terms in that film to those terms’ length-normalized tf-idf values
vector_dct = dict()
# the dictionary from film ids to its doc length
doc_len_dct = dict()
# the dictionary of terms to log inverse document frequencies
idf_dct = dict()

# the stemmer from nltk package
ps = PorterStemmer()
# the stop word list which construct from nltk default stop list and from our word frequency analysis
stop_lst = stopwords.words('english')
# this extended list is from word frequency analysis, I analyzed top 50 words below the the word I picked
# stop_lst.extend(['film', 'released', 'cast', 'also', 'release', 'directed', 'production', 'references',
#                  'links', 'external', 'imdb', 'festival', 'based', 'story', 'movie', 'stars', 'films', 'plot',
#                  'reviews', 'written', 'rating', 'director', 'may'])


# read the corpus data
def read_corpus(corpus_file):
    f = open(corpus_file, 'r')
    cps = json.load(f)
    f.close()
    return cps


# read the corpus file to build the term index dict and doc_data dict
def build_idx(cps):
    # get the num of film files
    num_of_fms = len(cps)
    print(num_of_fms)
    for fm in cps.keys():
        doc_data_dct[fm] = cps[fm]
        word = cps[fm]['Title'][0] + ' ' + cps[fm]['Text']
        all_terms = build_term(word)
        freq_v = dict()
        for tm in set(all_terms):
            tm_dct[tm].append(fm)
            tm_freq = all_terms.count(tm)
            freq_v[tm] = 1 + math.log10(tm_freq)
            idf_dct[tm] = idf_dct.get(tm, 0) + 1.0
        vector_dct[fm] = freq_v

    for tm in idf_dct:
        idf_dct[tm] = math.log10(num_of_fms / idf_dct[tm])

    for fm in vector_dct:
        freq_square_sum = 0
        for tm in vector_dct[fm]:
            freq_square_sum += (vector_dct[fm][tm] * idf_dct[tm]) ** 2
        doc_len_dct[fm] = math.sqrt(freq_square_sum)

    for fm in vector_dct:
        for tm in vector_dct[fm]:
            vector_dct[fm][tm] *= idf_dct[tm]
            vector_dct[fm][tm] /= doc_len_dct[fm]


# the function which take a word string as input, and convert it to a list of terms
def build_term(word):
    # Spider-Man => Spider Man
    if '-' in word:
        word = word.replace("-", " ")
    # U.S.A => USA
    if '.' in word:
        word = word.replace(".", "")

    tokenized_list = [token.lower() for token in word_tokenize(word)
                      if (token.lower() not in stop_lst) and token.isalpha()]
    term_list = [ps.stem(token) for token in tokenized_list]
    # the code snippet which help us to analyze word frequency
    # for tk in tokenized_list:
    #     token_dct[tk] = 1 if tk not in token_dct else token_dct[tk] + 1
    return term_list


# the function which store the various dictionaries into disk
def store_index():
    idx_sh = shelve.open('inverted_index', writeback=False)
    for term in tm_dct.keys():
        # we sort our posting_lst to help us to do intersect from shortest list
        posting_lst = sorted([int(i) for i in tm_dct[term]])
        idx_sh[term] = [str(num) for num in posting_lst]
    idx_sh.close()

    doc_sh = shelve.open('film_doc', writeback=False)
    doc_sh.update(doc_data_dct)
    doc_sh.close()

    vct_sh = shelve.open('film_v', writeback=False)
    vct_sh.update(vector_dct)
    vct_sh.close()

    idf_sh = shelve.open('film_idf', writeback=False)
    idf_sh.update(idf_dct)
    idf_sh.close()

    len_sh = shelve.open('film_doc_len', writeback=False)
    len_sh.update(doc_len_dct)
    len_sh.close()


# the word frequency analysis function, you can uncomment this function to compute word_frequency
# def word_frequency():
#     pq = queue.PriorityQueue()
#     for token in token_dct.keys():
#         pq.put((-token_dct[token], str(token)))
#     for i in range(50):
#         print(pq.get()[1])

# the function which get the stop_word from word_str given
def get_stop_word(word_str):
    # Spider-Man => Spider Man
    if '-' in word_str:
        word_str = word_str.replace("-", " ")
    # U.S.A => USA
    if '.' in word_str:
        word_str = word_str.replace(".", "")

    stop_word_lst = [t.lower() for t in word_tokenize(word_str)
                     if (t.lower() in stop_lst) or (not t.isalpha())]
    return set(stop_word_lst)


if __name__ == '__main__':
    # read our corpus
    # corp = read_corpus('test_corpus.json')
    corp = read_corpus('2018_movies.json')
    # build two dictionaries from corp
    build_idx(corp)
    # store two dictionaries into disk
    store_index()
    # the function to analyze the word frequency
    # word_frequency()
