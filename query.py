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

from index import Movie
from pprint import pprint
from elasticsearch_dsl import Q
from elasticsearch_dsl.utils import AttrList
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search

app = Flask(__name__)

# Initialize global variables for rendering page
tmp_text = ""
tmp_title = ""
tmp_star = ""
tmp_min = ""
tmp_max = ""
tmp_director = ""
tmp_language = ""
tmp_country = ""
tmp_location = ""
tmp_time = ""
tmp_categories = ""
gresults = {}
mode = "text conjunctive"
stop_lst = stopwords.words('english')


# display query page
@app.route("/")
def search():
    return render_template('page_query.html')


# display results page for first set of results and "next" sets.
@app.route("/results", defaults={'page': 1}, methods=['GET', 'POST'])
@app.route("/results/<page>", methods=['GET', 'POST'])
def results(page):
    global tmp_text, phrases  # to store phrases like "philip roth"
    global tmp_title
    global tmp_star
    global tmp_min
    global tmp_max
    global tmp_director
    global tmp_language
    global tmp_country
    global tmp_location
    global tmp_time
    global tmp_categories
    global gresults
    global mode   # field to store whether we use "Conjunctive" or "Disjunctive" search for text field search

    # convert the <page> parameter in url to integer.
    if type(page) is not int:
        page = int(page.encode('utf-8'))
        # if the method of request is post (for initial query), store query in local global variables
    # if the method of request is get (for "next" results), extract query contents from client's global variables  
    if request.method == 'POST':
        text_query = request.form['query']
        phrases = re.findall(r"\"(.*?)\"", text_query)   # get specific phrases from query text
        # remove all " in the query text
        if "\"" in text_query:
            text_query = text_query.replace("\"", "")
        star_query = request.form['starring']
        mintime_query = request.form['mintime']
        if len(mintime_query) is 0:
            mintime = 0
        else:
            mintime = int(mintime_query)
        maxtime_query = request.form['maxtime']
        if len(maxtime_query) is 0:
            maxtime = 99999
        else:
            maxtime = int(maxtime_query)
        director_query = request.form['director']
        language_query = request.form['language']
        country_query = request.form['country']
        location_query = request.form['location']
        time_query = request.form['time']
        categories_query = request.form['categories']
        # update global variable template data
        tmp_text = text_query
        tmp_star = star_query
        tmp_min = mintime
        tmp_max = maxtime
        tmp_director = director_query
        tmp_language = language_query
        tmp_country = country_query
        tmp_location = location_query
        tmp_time = time_query
        tmp_categories = categories_query
    else:
        # use the current values stored in global variables.
        text_query = tmp_text
        star_query = tmp_star
        mintime = tmp_min
        if tmp_min > 0:
            mintime_query = tmp_min
        else:
            mintime_query = ""
        maxtime = tmp_max
        if tmp_max < 99999:
            maxtime_query = tmp_max
        else:
            maxtime_query = ""
        director_query = tmp_director
        language_query = tmp_language
        country_query = tmp_country
        location_query = tmp_location
        time_query = tmp_time
        categories_query = tmp_categories

    # store query values to display in search boxes in UI
    shows = {}
    shows['text'] = text_query
    shows['starring'] = star_query
    shows['director'] = director_query
    shows['country'] = country_query
    shows['language'] = language_query
    shows['time'] = time_query
    shows['location'] = location_query
    shows['categories'] = categories_query
    shows['maxtime'] = maxtime_query
    shows['mintime'] = mintime_query

    # Create a search object to query our index 
    search = Search(index='sample_film_index')

    # Build up your elasticsearch query in piecemeal fashion based on the user's parameters passed in.
    # The search API is "chainable".
    # Each call to search.query method adds criteria to our growing elasticsearch query.
    # You will change this section based on how you want to process the query data input into your interface.

    # search for runtime using a range query
    s = search.query('range', runtime={'gte': mintime, 'lte': maxtime})

    # search for matching stars
    # You should support multiple values (list)
    if len(star_query) > 0:
        s = s.query('match', starring=star_query)

    if len(director_query) > 0:
        s = s.query('match', director=director_query)

    if len(location_query) > 0:
        s = s.query('match', location=location_query)

    if len(language_query) > 0:
        s = s.query('match', language=language_query)

    if len(country_query) > 0:
        s = s.query('match', country=country_query)

    if len(categories_query) > 0:
        s = s.query('match', categories=categories_query)
    # store the temporary search result before we do text query
    temp_s = s
    # use "phrase" type search to find specific phrases
    for ph in phrases:
        s = s.query('multi_match', query=ph, type='phrase', fields=['title', 'text'], operator='and')
        text_query = text_query.replace(ph, '')
    # use "phrase" type search to find other single word one by one
    for token in text_query.split():
        s = s.query('multi_match', query=token, type='phrase', fields=['title', 'text'], operator='and')
    text_query = text_query.strip()
    # Conjunctive search over multiple fields (title and text) using the text_query passed in
    if len(text_query) > 0:
        s = s.query('multi_match', query=text_query, type='cross_fields', fields=['title', 'text'], operator='and')

    # determine the subset of results to display (based on current <page> value)
    start = 0 + (page - 1) * 10
    end = 10 + (page - 1) * 10

    # execute search and return results in specified range.
    response = s[start:end].execute()
    # execute temporary search without text query to compare with actual search
    temp_response = temp_s[start:end].execute()
    # if actual search result is 0 but temporary result before text search is not 0, we change text query to
    # "disjunctive"
    if response.hits.total == 0 and temp_response.hits.total != 0:
        mode = "disjunctive"
        s = temp_s.query('multi_match', query=text_query, type='cross_fields', fields=['title', 'text'], operator='or')
    else:
        mode = "conjunctive"

    # highlight
    s = s.highlight_options(pre_tags='<mark>', post_tags='</mark>')
    s = s.highlight('text', fragment_size=999999999, number_of_fragments=1)
    s = s.highlight('title', fragment_size=999999999, number_of_fragments=1)
    s = s.highlight('starring', fragment_size=999999999, number_of_fragments=1)
    s = s.highlight('country', fragment_size=999999999, number_of_fragments=1)
    s = s.highlight('language', fragment_size=999999999, number_of_fragments=1)
    s = s.highlight('location', fragment_size=999999999, number_of_fragments=1)
    s = s.highlight('categories', fragment_size=999999999, number_of_fragments=1)
    s = s.highlight('runtime', fragment_size=999999999, number_of_fragments=1)
    s = s.highlight('director', fragment_size=999999999, number_of_fragments=1)

    response = s[start:end].execute()
    # insert data into response
    resultList = {}
    for hit in response.hits:
        result = {}
        result['score'] = hit.meta.score

        if 'highlight' in hit.meta:
            if 'title' in hit.meta.highlight:
                result['title'] = hit.meta.highlight.title[0]
            else:
                result['title'] = hit.title

            if 'text' in hit.meta.highlight:
                result['text'] = hit.meta.highlight.text[0]
            else:
                result['text'] = hit.text

            if 'starring' in hit.meta.highlight:
                result['starring'] = hit.meta.highlight.starring[0]
            else:
                result['starring'] = hit.starring

            if 'director' in hit.meta.highlight:
                result['director'] = hit.meta.highlight.director[0]
            else:
                result['director'] = hit.director

            if 'country' in hit.meta.highlight:
                result['country'] = hit.meta.highlight.country[0]
            else:
                result['country'] = hit.country

            if 'language' in hit.meta.highlight:
                result['language'] = hit.meta.highlight.language[0]
            else:
                result['language'] = hit.language

            if 'location' in hit.meta.highlight:
                result['location'] = hit.meta.highlight.location[0]
            else:
                result['location'] = hit.location

            if 'categories' in hit.meta.highlight:
                result['categories'] = hit.meta.highlight.categories[0]
            else:
                result['categories'] = hit.categories

            if 'time' in hit.meta.highlight:
                result['time'] = hit.meta.highlight.time[0]
            else:
                result['time'] = hit.time
        else:
            result['title'] = hit.title
            result['text'] = hit.text
            result['starring'] = hit.starring
            result['time'] = hit.time
            result['categories'] = hit.categories
            result['location'] = hit.location
            result['language'] = hit.language
            result['country'] = hit.country
            result['director'] = hit.director

        resultList[hit.meta.id] = result

    # make the result list available globally
    gresults = resultList

    # get the total number of matching results
    result_num = response.hits.total

    # if we find the results, extract title and text information from doc_data, else do nothing
    if result_num > 0:
        return render_template('page_SERP.html', mode=mode, results=resultList, res_num=result_num, page_num=page, queries=shows)
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

        if len(star_query) > 0:
            message.append('Cannot find star: ' + star_query)

        return render_template('page_SERP.html', mode=mode, results=message, res_num=result_num, page_num=page, queries=shows)


# display a particular document given a result number
@app.route("/documents/<res>", methods=['GET'])
def documents(res):
    global gresults
    film = gresults[res]
    filmtitle = film['title']
    for term in film:
        if type(film[term]) is AttrList:
            s = "\n"
            for item in film[term]:
                s += item + ",\n "
            film[term] = s
    # fetch the movie from the elasticsearch index using its id
    movie = Movie.get(id=res, index='sample_film_index')
    filmdic = movie.to_dict()
    film['runtime'] = str(filmdic['runtime']) + " min"
    return render_template('page_targetArticle.html', film=film, title=filmtitle)


if __name__ == "__main__":
    app.run()
