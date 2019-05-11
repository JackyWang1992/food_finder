# Naive Bayes Classifier for yelp_reviews on surrounding words of keyword

import re, nltk, json, string
import numpy as np
from collections import defaultdict
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize




class NaiveBayes():
    def __init__(self):
        self.class_dict = {0: 'neg', 1: 'pos'}
        # use Hu and Liu's sentimental lexicon as feature.
        f = open('feature.txt')
        s = f.read()
        l = s.split('\n')
        self.feature_dict = {}
        self.vocabulary = set()
        self.doc_voc = defaultdict(list)
        self.stopword = set(stopwords.words('english'))

        for i in range(len(l)):
            self.feature_dict[i] = l[i]

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
        with open('trainset4.json', 'r', encoding='utf-8') as data_file:
            restaurants = json.load(data_file)
            porter = nltk.PorterStemmer()
            # collect pos/neg class count and feature counts
            for i in range(len(restaurants)):
                content = restaurants[str(i)]
                review = content['review'].lower()
                review = re.sub('-', ' ', review)
                review = re.sub('\.', '', review)
                tokens = word_tokenize(review)
                tokens = [porter.stem(w) for w in tokens if w and w not in self.stopword and w not in string.punctuation ]
                self.vocabulary = self.vocabulary | set(tokens)
                if content['stars'] == 'neg':
                    num_res[0] += 1
                    self.doc_voc[0].extend(tokens)
                elif content['stars'] == 'pos':
                    num_res[1] += 1
                    self.doc_voc[1].extend(tokens)
            print(self.vocabulary)
            for i in self.feature_dict:
                for j in self.class_dict:
                    wcount = self.doc_voc[j].count(self.feature_dict[i])
                    self.likelihood[j][i] = np.log((wcount + 1) / (len(self.doc_voc[j]) + len(self.vocabulary)))
            self.prior[0] = np.log(num_res[0] / (num_res[0] + num_res[1]))
            self.prior[1] = np.log(num_res[1] / (num_res[0] + num_res[1]))



    '''
    Use the classifier trained to predict the sentiment of keywords-surrounding words.
    '''
    def predict(self, tokens):
        # iterate over testing documents
        feature = np.zeros((len(self.feature_dict), 1))
        for i in range(0, len(self.feature_dict)):
            wcount = tokens.count(self.feature_dict[i])
            feature[i] = wcount
        prob = np.matmul(self.likelihood, feature)
        for i in range(len(self.prior)):
            prob[i][0] = prob[i][0] + self.prior[i]
        # calculate the probability between 0-1, 0 as neg and 1 as pos
        print(np.argmax(prob))
        return np.argmax(prob)


if __name__ == '__main__':
    nb = NaiveBayes()
    nb.train()
    print(nb.prior)
    print(nb.doc_voc)
    nb.predict(['25', 'year', 'arizona', 'I', 'dare', 'find', 'better', 'the', 'mani', 'differ', 'choic', 'cream', 'wonder', 'make', 'ole', 'everyth', 'bagel', 'new', 'time', 'It', 'realli', 'hard', 'pick'])
    nb.predict(['magic', 'own', 'veri', 'fresh', 'bagelsfriendli', 'staffseat', 'insid', 'outsid', 'I', 'sesam', 'bagel', 'egg', 'bacon', 'fresh', 'delici', 'they', 'also', 'bake', 'good', 'well', 'say', 'oasi'])


