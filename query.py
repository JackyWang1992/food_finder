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

import re
from flask import *
from nltk.corpus import stopwords
import nltk
from nltk.corpus import PlaintextCorpusReader

from index import Restaurant
from pprint import pprint
from elasticsearch_dsl import Q
from elasticsearch_dsl.utils import AttrList
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search

app = Flask(__name__)

# Initialize global variables for rendering page
tmp_text = ""
tmp_address = ""
tmp_max_star = ""
tmp_min_star = ""
tmp_city = ""
tmp_min_review_count = ""
tmp_max_review_count = ""
tmp_min_useful = ""
tmp_max_useful = ""
tmp_min_cool = ""
tmp_max_cool = ""
tmp_min_funny = ""
tmp_max_funny = ""
tmp_state = ""
tmp_review_count = ""
tmp_date = ""
gresults = {}
mode = "text conjunctive"
stop_lst = stopwords.words('english')


# display query page
@app.route("/")
# @app.route("/")
def search():
    # global tmp_text
    # if request.method == 'POST':
    #     text_query = request.form['query']
    #     search = Search(index='sample_restaurant_index')
    #     s = search.suggest('auto_complete', text_query, completion={'field': 'suggest'})
    #     response = s.execute()
    #     suggest = response.suggest.auto_complete[0].options
    #     return render_template('page_query.html', suggest)
    # else:
    return render_template('page_query.html')


# display results page for first set of results and "next" sets.
@app.route("/results", defaults={'page': 1}, methods=['GET', 'POST'])
@app.route("/results/<page>", methods=['GET', 'POST'])
def results(page):
    global tmp_text, phrases  # to store phrases like "philip roth"
    global tmp_address
    global tmp_min_star
    global tmp_max_star
    global tmp_state
    global tmp_review_count
    global tmp_date
    global tmp_min_review_count
    global tmp_max_review_count
    global tmp_min_cool
    global tmp_max_cool
    global tmp_min_useful
    global tmp_max_useful
    global tmp_min_funny
    global tmp_max_funny
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
        # address_query = request.form['address']
        # min_star_query = request.form['min_star']
        # if len(min_star_query) is 0:
        #     min_star = 0
        # else:
        #     min_star = float(min_star_query)
        # max_star_query = request.form['max_star']
        # if len(max_star_query) is 0:
        #     max_star = 5
        # else:
        #     max_star = float(max_star_query)
        #
        # min_review_count_query = request.form['min_review']
        # if len(min_review_count_query) is 0:
        #     min_review_count = 0
        # else:
        #     min_review_count = int(min_review_count_query)
        # max_review_count_query = request.form['max_review']
        # if len(max_review_count_query) is 0:
        #     max_review_count = 99999
        # else:
        #     max_review_count = int(max_review_count_query)
        #
        # min_useful_query = request.form['min_useful']
        # if len(min_useful_query) is 0:
        #     min_useful = 0
        # else:
        #     min_useful = int(min_useful_query)
        # max_useful_query = request.form['max_useful']
        # if len(max_useful_query) is 0:
        #     max_useful = 99999
        # else:
        #     max_useful = int(max_useful_query)
        #
        # min_cool_query = request.form['min_cool']
        # if len(min_cool_query) is 0:
        #     min_cool = 0
        # else:
        #     min_cool = int(min_cool_query)
        # max_cool_query = request.form['max_cool']
        # if len(max_cool_query) is 0:
        #     max_cool = 99999
        # else:
        #     max_cool = int(max_cool_query)
        #
        # min_funny_query = request.form['min_funny']
        # if len(min_funny_query) is 0:
        #     min_funny = 0
        # else:
        #     min_funny = int(min_funny_query)
        # max_funny_query = request.form['max_funny']
        # if len(max_funny_query) is 0:
        #     max_funny = 99999
        # else:
        #     max_funny = int(max_funny_query)
        city_query = request.form['city']
        # state_query = request.form['state']
        # date_query = request.form['date']

        # update global variable template data
        tmp_text = text_query
        # tmp_address = address_query
        # tmp_min_star = min_star
        # tmp_max_star = max_star
        # tmp_min_review_count = min_review_count
        # tmp_max_review_count = max_review_count
        # tmp_min_useful = min_useful
        # tmp_max_useful = max_useful
        # tmp_min_cool = min_cool
        # tmp_max_cool = max_cool
        # tmp_min_funny = min_funny
        # tmp_max_funny = max_funny
        # tmp_state = state_query
        tmp_city = city_query
        # tmp_date = date_query
    else:
        # use the current values stored in global variables.
        text_query = tmp_text
        # address_query = tmp_address
        # min_star = tmp_min_star
        # if tmp_min_star > 0:
        #     min_star_query = tmp_min_star
        # else:
        #     min_star_query = ""
        # max_star = tmp_max_star
        # if tmp_max_star < 5:
        #     max_star_query = tmp_max_star
        # else:
        #     max_star_query = ""
        #
        # min_review_count = tmp_min_review_count
        # if tmp_min_review_count > 0:
        #     min_review_count_query = tmp_min_review_count
        # else:
        #     min_review_count_query = ""
        # max_review_count = tmp_max_review_count
        # if tmp_max_review_count < 99999:
        #     max_review_count_query = tmp_max_review_count
        # else:
        #     max_review_count_query = ""
        #
        # min_useful = tmp_min_useful
        # if tmp_min_useful > 0:
        #     min_useful_query = tmp_min_useful
        # else:
        #     min_useful_query = ""
        # max_useful = tmp_max_useful
        # if tmp_max_useful < 99999:
        #     max_useful_query = tmp_max_useful
        # else:
        #     max_useful_query = ""
        #
        # min_cool = tmp_min_cool
        # if tmp_min_cool > 0:
        #     min_cool_query = tmp_min_cool
        # else:
        #     min_cool_query = ""
        # max_cool = tmp_max_cool
        # if tmp_max_cool < 99999:
        #     max_cool_query = tmp_max_cool
        # else:
        #     max_cool_query = ""
        #
        # min_funny = tmp_min_funny
        # if tmp_min_funny > 0:
        #     min_funny_query = tmp_min_funny
        # else:
        #     min_funny_query = ""
        # max_funny = tmp_max_funny
        # if tmp_max_funny < 99999:
        #     max_funny_query = tmp_max_funny
        # else:
        #     max_funny_query = ""
        city_query = tmp_city
        # state_query = tmp_state
        # date_query = tmp_date

    # store query values to display in search boxes in UI
    shows = {}
    shows['query'] = text_query
    # shows['address'] = address_query
    shows['city'] = city_query
    # shows['state'] = state_query
    # shows['min_review'] = min_review_count_query
    # shows['max_review'] = max_review_count_query
    # shows['min_star'] = min_star_query
    # shows['max_star'] = max_star_query
    # shows['min_useful'] = min_useful_query
    # shows['max_useful'] = max_useful_query
    # shows['min_cool'] = min_cool_query
    # shows['max_cool'] = max_cool_query
    # shows['min_funny'] = min_funny_query
    # shows['max_funny'] = max_funny_query
    # shows['date'] = date_query

    # Create a search object to query our index 
    search = Search(index='sample_restaurant_index')

    # Build up your elasticsearch query in piecemeal fashion based on the user's parameters passed in.
    # The search API is "chainable".
    # Each call to search.query method adds criteria to our growing elasticsearch query.
    # You will change this section based on how you want to process the query data input into your interface.

    # search for runtime using a range query
    # s = search.query('range', star={'gte': min_star, 'lte': max_star})
    # s = s.query('range', review_count={'gte': min_review_count, 'lte': max_review_count})
    # s = s.query('range', useful={'gte': min_useful, 'lte': max_useful})
    # s = s.query('range', cool={'gte': min_cool, 'lte': max_cool})
    # s = s.query('range', funny={'gte': min_funny, 'lte': max_funny})

    # search for matching stars
    # You should support multiple values (list)
    # if len(address_query) > 0:
    #     s = s.query('match', address=address_query)

    # fuzzy search on cities field
    if len(city_query) > 0:
        s = search.query('fuzzy', city={'value': city_query, 'transpositions': True})
    else:
        s = search

    # if len(state_query) > 0:
    #     s = s.query('match', state=state_query)
    #
    # if len(date_query) > 0:
    #     s = s.query('match', date=date_query)

    # store the temporary search result before we do text query
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
            print(text_query)
            # print(hit.text)


    # # use "phrase" type search to find specific phrases
    # for ph in phrases:
    #     s = s.query('multi_match', query=ph, type='phrase', fields=['name', 'review'], operator='and')
    #     text_query = text_query.replace(ph, '')
    #     print(ph)
    # # use "phrase" type search to find other single word one by one
    # for token in text_query.split():
    #     s = s.query('multi_match', query=token, type='phrase', fields=['name', 'review'], operator='and')
    #     print(token)
    # text_query = text_query.strip()
    # # Conjunctive search over multiple fields (title and text) using the text_query passed in
    # if len(text_query) > 0:
    #     s = s.query('multi_match', query=text_query, type='cross_fields', fields=['name', 'review'], operator='and')

    # determine the subset of results to display (based on current <page> value)
    start = 0 + (page - 1) * 10
    end = 10 + (page - 1) * 10

    # execute search and return results in specified range.
    # response = s[start:end].execute()
    # execute temporary search without text query to compare with actual search
    # temp_response = temp_s[start:end].execute()
    # if actual search result is 0 but temporary result before text search is not 0, we change text query to
    # "disjunctive"
    # if response.hits.total == 0 and temp_response.hits.total != 0:
    #     mode = "disjunctive"
    #     s = s.query('multi_match', query=text_query, type='cross_fields', fields=['name', 'review'], operator='or')
    # else:
    #     mode = "conjunctive"

    # highlight
    s = s.highlight_options(pre_tags='<mark>', post_tags='</mark>')
    s = s.highlight('name', fragment_size=999999999, number_of_fragments=1)
    s = s.highlight('review', fragment_size=999999999, number_of_fragments=1)
    # s = s.highlight('address', fragment_size=999999999, number_of_fragments=1)
    s = s.highlight('city', fragment_size=999999999, number_of_fragments=1)
    # s = s.highlight('state', fragment_size=999999999, number_of_fragments=1)
    # s = s.highlight('star', fragment_size=999999999, number_of_fragments=1)
    # s = s.highlight('cool', fragment_size=999999999, number_of_fragments=1)
    # s = s.highlight('funny', fragment_size=999999999, number_of_fragments=1)
    # s = s.highlight('useful', fragment_size=999999999, number_of_fragments=1)
    # s = s.highlight('date', fragment_size=999999999, number_of_fragments=1)
    # s = s.highlight('review_count', fragment_size=999999999, number_of_fragments=1)
    # print(len(s))
    response = s[start:end].execute()
    # print(response)
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

            text = nltk.Text(hit.review.split())
            print(text)
            print(type(text))
            findconcordance(text, text_query)

            # match = text.concordance(text_query)
            # print([text1.tokens[offset + 1] for offset in c.offsets('monstrous')])

            if 'review' in hit.meta.highlight:
                result['review'] = hit.meta.highlight.review[0]

            else:
                result['review'] = hit.review

            # if 'star' in hit.meta.highlight:
            #     result['star'] = hit.meta.highlight.star[0]
            # else:
            #     result['star'] = hit.star
            #
            # if 'useful' in hit.meta.highlight:
            #     result['useful'] = hit.meta.highlight.useful[0]
            # else:
            #     result['useful'] = hit.useful
            #
            # if 'review_count' in hit.meta.highlight:
            #     result['review_count'] = hit.meta.highlight.review_count[0]
            # else:
            #     result['review_count'] = hit.review_count
            #
            # if 'cool' in hit.meta.highlight:
            #     result['cool'] = hit.meta.highlight.cool[0]
            # else:
            #     result['cool'] = hit.cool
            #
            # if 'funny' in hit.meta.highlight:
            #     result['funny'] = hit.meta.highlight.funny[0]
            # else:
            #     result['funny'] = hit.funny

            if 'city' in hit.meta.highlight:
                result['city'] = hit.meta.highlight.city[0]
            else:
                result['city'] = hit.city

            # if 'state' in hit.meta.highlight:
            #     result['state'] = hit.meta.highlight.state[0]
            # else:
            #     result['state'] = hit.state
            #
            # if 'date' in hit.meta.highlight:
            #     result['date'] = hit.meta.highlight.date[0]
            # else:
            #     result['date'] = hit.date
        else:
            result['name'] = hit.name
            result['review'] = hit.review
            # result['star'] = hit.star
            # result['review_count'] = hit.review_count
            # result['useful'] = hit.useful
            # result['cool'] = hit.cool
            # result['funny'] = hit.funny
            result['city'] = hit.city
            # result['date'] = hit.date
            # result['funny'] = hit.funny

        resultList[hit.meta.id] = result

    # make the result list available globally
    gresults = resultList

    # get the total number of matching results
    result_num = response.hits.total
    # print(result_num)
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
        #
        # if len(address_query) > 0:
        #     message.append('Cannot find address: ' + address_query)

        return render_template('page_SERP.html', mode=mode, results=message, res_num=result_num, page_num=page,
                               queries=shows)


def findconcordance(text, text_query):
    # print(text)
    # # print(text.concordance_list)
    # # print(text.find_con)
    for i in text.concordance_list(text_query,lines=100):
        surrounding = i.left.extend(i.right)
        # print(i.left)
        # print(type(i.left))
        # print(i.right)

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
    # fetch the movie from the elasticsearch index using its id
    rest = Restaurant.get(id=res, index='sample_restaurant_index')
    rest_dic = rest.to_dict()
    restaurant['star'] = str(rest_dic['star'])
    restaurant['cool'] = str(rest_dic['cool'])
    restaurant['review_count'] = str(rest_dic['review_count'])
    restaurant['useful'] = str(rest_dic['useful'])
    restaurant['funny'] = str(rest_dic['funny'])

    return render_template('page_targetArticle.html', restaurant=restaurant, title=restaurant_name)


if __name__ == '__main__':
    app.run(debug=True)
