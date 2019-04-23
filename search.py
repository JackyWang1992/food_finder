"""
search.py
author: Jiaqi Wang
E-mail: wangjiaqi2017@brandeis.edu

This module takes a search query and finds movies matching that
query. It processes the query using boolean_terms (the same module vs_index used
to process the corpus, ensuring like terms will be alike) and then accesses term_index
to get lists of movies each of those terms are in—postings lists. Then union the res ids to
present disjunction Search ("OR" query)
"""
import math

import index
import shelve
import heapq

# read data from the shelves file we store in index.py and put it into our dict
tm_dct = shelve.open('inverted_index', flag='r', writeback=False)
doc_data = shelve.open('film_doc', flag='r', writeback=False)
idf_dct = shelve.open('film_idf', flag='r', writeback=False)
vct_dct = shelve.open('film_v', flag='r', writeback=False)
len_dct = shelve.open('film_doc_len', flag='r', writeback=False)


# this function which implement boolean search
def dummy_search(query):
    """Return a list of movie ids that match the query."""
    res = []
    term_lst = preprocess_query(query)

    if len(term_lst) == 0:
        return res

    # put all of posting lists into our priority queue, ordered by posting list length
    for tm in term_lst:
        posting_list = tm_dct[str(tm)]
        res.extend(posting_list)
    res = list(set(res))
    return res


# find the missing terms if there are any non-noise query terms that do not occur in this article
def find_missing_terms(fm, q_terms):
    res = []
    for tm in q_terms:
        posting_list = tm_dct[tm]
        if fm not in posting_list:
            res.append(tm)
    return sorted(res)


# this function compute the cosine similarity score for all documents and find the top k scoring documents using a heap
def vs_search(query, fms, k):
    res = []

    q_terms = preprocess_query(query)
    if len(q_terms) == 0:
        return res

    q_v = compute_vct(q_terms)

    for fm in fms:
        score = compute_score(fm, q_v)
        missing_term = find_missing_terms(fm, q_terms)
        if k == 0:
            heapq.heappushpop(res, (score, fm, missing_term))
        else:
            heapq.heappush(res, (score, fm, missing_term))
            k -= 1
    return sorted(res, reverse=True)


# compute the cosine similarity score for single film over query terms
def compute_score(fm, query_vct):
    score = 0
    for tm in query_vct:
        score += query_vct[tm] * vct_dct[fm].get(tm, 0)
    return score


# compute weight query terms using logarithmic tf*idf formula without length normalization
def compute_vct(q_terms):
    vct = dict()
    for tm in set(q_terms):
        freq = q_terms.count(tm)
        vct[tm] = (1 + math.log10(freq)) * idf_dct[tm]
    return vct


def dummy_movie_data(doc_id):
    """
    Return data fields for a movie.
    Your code should use the doc_id as the key to access the shelf entry for the movie doc_data.
    You can decide which fields to display, but include at least title and text.
    """
    # since we need a bigger title in HTML file, we process the title field separately
    fm = doc_data[doc_id]
    title = fm['Title'][0]
    # this is other fields we want to display
    fields = ['director', 'starring', 'country', 'location', 'text']
    data = list()
    # process these fields one by one
    for fld in fields:
        val = fm[fld.title()]
        if len(val) > 0:
            if isinstance(val, list):
                val = ', '.join(val)
        data.append((fld, val))
    return title, data


def dummy_movie_snippet(doc_id):
    """
    Return a snippet for the results page.
    Needs to include a title and a short description.
    Your snippet does not have to include any query terms, but you may want to think about implementing
    that feature. Consider the effect of normalization of index terms (e.g., stemming), which will affect
    the ease of matching query terms to words in the text.
    """
    fm = doc_data[doc_id]
    title = fm['Title'][0]
    text = fm['Text']
    if len(text) > 200:
        text = text[0:200]
    return doc_id, title, text


# make sure remove stop words and unknown words
def preprocess_query(query):
    q_terms = index.build_term(query)
    # when we got a empty list(maybe all of stop words... ), we just return empty list
    if len(q_terms) == 0:
        return []
    # next we need to check whether all of terms are in our term dictionary, if not, return empty list
    for tm in q_terms:
        if tm not in tm_dct.keys():
            return []
    return q_terms


# this function is to return the terms which cannot found in term dictionary, we need them to display in Web UI
# Interface
def get_ovv(term_lst):
    oov_lst = [tm for tm in term_lst if tm not in tm_dct.keys()]
    return set(oov_lst)
