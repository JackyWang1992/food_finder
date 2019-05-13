# Naive Bayes Classifier for yelp_reviews on surrounding words of keyword

import re, nltk, json, string
import pickle
import numpy as np
from collections import defaultdict
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize


class NaiveBayes(object):
    def __init__(self):
        self.class_dict = {0: 'neg', 1: 'pos'}

        # use Hu and Liu's sentimental lexicon as feature, but it takes too long, so we use a 180words dictionary
        # f = open('feature.txt')
        # s = f.read()
        # l = s.split('\n')
        # self.feature_dict = {}
        # for i in range(len(l)):
        #     self.feature_dict[i] = l[i]

        self.vocabulary = set()
        self.doc_voc = defaultdict(list)
        self.stopword = set(stopwords.words('english'))

        # 100-words dictionary extracted from Hu and Liu's lexicon
        self.feature_dict = {0: 'good', 1: 'ideal', 2: 'love', 3: 'like', 4: 'impressed',
                             5: 'joy', 6: 'amaze', 7: 'amazed', 8: 'amazing', 9: 'lovely', 10: 'delicate',
                             11: 'appealing', 12: 'magnificent', 13: 'awesome', 14: 'beautiful', 15: 'best',
                             16: 'recommend', 17: 'bravo', 18: 'brilliant', 19: 'nice', 20: 'recommended',
                             21: 'creative', 22: 'cute', 23: 'delight', 24: 'delicious', 25: 'superb', 26: 'wonderful',
                             27: 'worthy', 28: 'perfect', 29: 'tasty', 30: 'pleasure', 31: 'excellent', 32: 'excited',
                             33: 'extraordinary', 34: 'success', 35: 'fabulous', 36: 'fancy', 37: 'bright',
                             38: 'fantastic', 39: 'praised', 40: 'palatable', 41: 'favorite', 42: 'fine',
                             43: 'gifted', 44: 'great', 45: 'happiness', 46: 'happy', 47: 'chill',
                             48: 'yummy', 49: 'toothsome',
                             50: 'bullshit', 51: 'awful', 52: 'complain', 53: 'damn',
                             54: 'disappointed', 55: 'disappointing', 56: 'dislike', 57: 'diss', 58: 'fail',
                             59: 'failure', 60: 'hate', 61: 'junk', 62: 'imperfect', 63: 'hell', 64: 'noisy',
                             65: 'nightmare', 66: 'nasty', 67: 'messed', 68: 'mess', 69: 'mediocre',
                             70: 'poor', 71: 'problem', 72: 'questionable', 73: 'ridiculous', 74: 'rubbish',
                             75: 'expensive', 76: 'creepy', 77: 'detest', 78: 'unacceptable', 79: 'sucks',
                             80: 'ruin', 81: 'rude', 82: 'suck', 83: 'terrible', 84: 'trash',
                             85: 'ugly', 86: 'wasteful', 87: 'weak', 88: 'worse', 89: 'worst', 90: 'wrong',
                             91: 'absurd', 92: 'angry', 93: 'annoyed', 94: 'unhappy', 95: 'vomit', 96: 'ashamed',
                             97: 'upset', 98: 'bad', 99: 'disgusting'}

        self.prior = np.zeros(len(self.class_dict))
        self.likelihood = np.zeros((len(self.class_dict), len(self.feature_dict)))


    '''
    Trains a multinomial Naive Bayes classifier on a training set.
    Training set: reviews for restaurant in states other than AZ.
    For review stars > 3, consider as positive review, for review stars <= 3, consider as negative review.
    self.prior = prior probability of each class, pos/neg.
    self.likelihood = the probability of that feature word appears in each class.
    '''
    def train(self):
        # iterate over training documents
        num_res = defaultdict(int)
        with open('nb_trainset.json', 'r', encoding='utf-8') as data_file:
            restaurants = json.load(data_file)
            porter = nltk.PorterStemmer()
            # collect pos/neg class count and feature counts
            for i in range(len(restaurants)):
                content = restaurants[str(i)]
                review = content['review'].lower()
                review = re.sub('-', ' ', review)
                review = re.sub('\.', '', review)
                tokens = word_tokenize(review)
                tokens = [w for w in tokens if w and w not in self.stopword and w not in string.punctuation ]
                # build vocabulary
                self.vocabulary = self.vocabulary | set(tokens)
                if content['stars'] == 'neg':
                    num_res[0] += 1
                    self.doc_voc[0].extend(tokens)
                elif content['stars'] == 'pos':
                    num_res[1] += 1
                    self.doc_voc[1].extend(tokens)
            # calculate likelihood
            for i in self.feature_dict:
                for j in self.class_dict:
                    wcount = self.doc_voc[j].count(self.feature_dict[i])
                    self.likelihood[j][i] = np.log((wcount + 1) / (len(self.doc_voc[j]) + len(self.vocabulary)))
            # calculate prior probability
            self.prior[0] = np.log(num_res[0] / (num_res[0] + num_res[1]))
            self.prior[1] = np.log(num_res[1] / (num_res[0] + num_res[1]))


    '''
    Use the classifier trained to predict the sentiment of keywords-surrounding words.
    '''
    def predict(self, tokens):
        # iterate over the review tokens
        feature = np.zeros((len(self.feature_dict), 1))
        for i in range(0, len(self.feature_dict)):
            wcount = tokens.count(self.feature_dict[i])
            feature[i] = wcount
        # use numpy matrix production
        prob = np.matmul(self.likelihood, feature)
        for i in range(len(self.prior)):
            prob[i][0] = prob[i][0] + self.prior[i]
        # calculate the probability between 0-1, 0 as neg and 1 as pos
        return np.argmax(prob)


if __name__ == '__main__':
    from naivebayes import NaiveBayes
    # so that it is using the NaiveBayes class defined above rather __main__.NaiveBayes
    nb = NaiveBayes()
    nb.train()
    # save the trained classifier so that in query.py we don't need to train it from beginning
    nb_pickle = pickle.dump(nb, open("nb_pickle", 'wb'))