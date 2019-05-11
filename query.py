"""
This module implements a (partial, sample) query interface for elasticsearch movie search. 
You will need to rewrite and expand sections to support the types of queries over the fields in your UI.

Documentation for elasticsearch query DSL:
https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl.html

For python version of DSL:
https://elasticsearch-dsl.readthedocs.io/en/latest/

Search DSL:
https://elasticsearch-dsl.readthedocs.io/en/latest/search_dsl.html
"""

import re, string
from flask import *
from nltk.corpus import stopwords
import nltk

from index import Restaurant
from pprint import pprint
from elasticsearch_dsl import Q
from elasticsearch_dsl.utils import AttrList
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search

app = Flask(__name__)

# Initialize global variables for rendering page
tmp_text = ""
tmp_city = ""
tmp_state = ""
gresults = {}
mode = "text conjunctive"
stop_lst = set(stopwords.words('english'))


# display query page
@app.route("/")
def search():
    return render_template('page_query.html')


# display results page for first set of results and "next" sets.
@app.route("/results", defaults={'page': 1}, methods=['GET', 'POST'])
@app.route("/results/<page>", methods=['GET', 'POST'])
def results(page):
    global tmp_text, phrases  # to store phrases like "philip roth"
    global tmp_min_star
    global tmp_max_star
    global tmp_state
    global tmp_city
    global gresults
    global mode  # field to store whether we use "Conjunctive" or "Disjunctive" search for text field search

    # convert the <page> parameter in url to integer.
    if type(page) is not int:
        page = int(page.encode('utf-8'))
        # if the method of request is post (for initial query), store query in local global variables
    # if the method of request is get (for "next" results), extract query contents from client's global variables  
    if request.method == 'POST':
        text_query = request.form['query']
        phrases = re.findall(r"\"(.*?)\"", text_query)  # get specific phrases from query text
        # remove all " in the query text
        if "\"" in text_query:
            text_query = text_query.replace("\"", "")

        city_query = request.form['city']


        # update global variable template data
        tmp_text = text_query
        tmp_city = city_query
    else:
        # use the current values stored in global variables.
        text_query = tmp_text
        city_query = tmp_city

    # store query values to display in search boxes in UI
    shows = {}
    shows['query'] = text_query
    shows['city'] = city_query

    # Create a search object to query our index 
    search = Search(index='sample_restaurant_index')

    # Build up your elasticsearch query in piecemeal fashion based on the user's parameters passed in.
    # The search API is "chainable".
    # Each call to search.query method adds criteria to our growing elasticsearch query.
    # You will change this section based on how you want to process the query data input into your interface.

    # fuzzy search on cities field
    if len(city_query) > 0:
        s = search.query('fuzzy', city={'value': city_query, 'transpositions': True})
    else:
        s = search
    temp_s = s

    # Conjunctive search over multiple fields (title and text) using the text_query passed in
    if len(text_query) > 0:
        if "\"" in text_query:
            phrase_query = re.findall(r'"(.*?)"', text_query)
            remaining_text = text_query
            remaining_text = (re.sub(r'"(.*?)"', '', remaining_text)).strip()
            for ph in phrase_query:
                if len(ph) > 0:
                    s = s.query('multi_match', query=ph, type='phrase', fields=['name', 'review'], operator='and')
            if len(remaining_text) > 0:
                s = s.query('multi_match', query=remaining_text, type='cross_fields', fields=['name', 'review'], operator='and')
        else:
            s = s.query('multi_match', query=text_query, type='cross_fields', fields=['name', 'review'], operator='and')

    start = 0 + (page - 1) * 10
    end = 10 + (page - 1) * 10

    # highlight
    s = s.highlight_options(pre_tags='<mark>', post_tags='</mark>')
    s = s.highlight('name', fragment_size=999999999, number_of_fragments=1)
    s = s.highlight('review', fragment_size=999999999, number_of_fragments=1)
    s = s.highlight('city', fragment_size=999999999, number_of_fragments=1)

    response = s[start:end].execute()

    # insert data into response
    resultList = {}
    for hit in response.hits:
        result = {}
        result['score'] = hit.meta.score
        if 'highlight' in hit.meta:
            if 'name' in hit.meta.highlight:
                result['name'] = hit.meta.highlight.name[0]
            else:
                result['name'] = hit.name

            if 'city' in hit.meta.highlight:
                result['city'] = hit.meta.highlight.city[0]
            else:
                result['city'] = hit.city

            if 'star' in hit.meta.highlight:
                result['star'] = hit.meta.highlight.star[0]
            else:
                result['star'] = hit.star

            # used for sentimental analysis
            text = nltk.Text(hit.review.split())
            findconcordance(text, text_query)

            if 'review' in hit.meta.highlight:
                result['review'] = hit.meta.highlight.review[0]
            else:
                result['review'] = hit.review
        else:
            result['name'] = hit.name
            result['city'] = hit.city
            result['star'] = hit.star
            result['review'] = hit.review

        resultList[hit.meta.id] = result

    # make the result list available globally
    gresults = resultList

    # get the total number of matching results
    result_num = response.hits.total

    # if we find the results, extract title and text information from doc_data, else do nothing
    if result_num > 0:
        return render_template('page_SERP.html', mode=mode, results=resultList, res_num=result_num, page_num=page,
                               queries=shows)
    else:
        message = []
        if len(text_query) > 0:
            tokens = text_query.split()
            unknowns = []
            for i in range(len(tokens)):
                if tokens[i] in stop_lst:
                    # store the stop words in query text
                    message.append('Contains Stop Words: ' + tokens[i] + '\n')
                else:
                    # store the actual unknown words in query text
                    unknowns.append(tokens[i])
            if len(unknowns) > 0:
                message.append('Unknown search term: ' + ", ".join(unknowns))

        return render_template('page_SERP.html', mode=mode, results=message, res_num=result_num, page_num=page,
                               queries=shows)


def findconcordance(text, text_query):
    for i in text.concordance_list(text_query,lines=10000):
        surrounding = []
        if i.left is not None:
            for w in i.left:
                if w and w not in stop_lst and w not in string.punctuation:
                    w = re.sub('(\.|\?|\,|\;|\!|\(|\))+', '', w)
                    surrounding.append(w.lower())
        if i.right is not None:
            for w in i.right:
                if w and w not in stop_lst and w not in string.punctuation:
                    w = re.sub('(\.|\?|\,|\;|\!|\(|\))+', '', w)
                    surrounding.append(w.lower())
        print(surrounding)



# display a particular document given a result number
@app.route("/documents/<res>", methods=['GET'])
def documents(res):
    global gresults
    restaurant = gresults[res]
    restaurant_name = restaurant['name']

    for term in restaurant:
        if type(restaurant[term]) is AttrList:
            s = "\n"
            for item in restaurant[term]:
                s += item + ",\n "
            restaurant[term] = s
    return render_template('page_targetArticle.html', restaurant=restaurant, title=restaurant_name)


if __name__ == '__main__':
    app.run(debug=True)
