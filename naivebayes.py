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
        # use Hu and Liu's sentimental lexicon as feature.
        # f = open('feature.txt')
        # s = f.read()
        # l = s.split('\n')
        # self.feature_dict = {}
        self.vocabulary = set()
        self.doc_voc = defaultdict(list)
        self.stopword = set(stopwords.words('english'))

        self.feature_dict = {0: 'accomplished', 1: 'achievement', 2: 'admire', 3: 'admiring', 4: 'advantage',
                             5: 'affectionate', 6: 'amaze', 7: 'amazed', 8: 'amazing', 9: 'amuse', 10: 'amusing',
                             11: 'appealing', 12: 'attractive', 13: 'awesome', 14: 'beautiful', 15: 'best', 16: 'bonus',
                             17: 'bravo', 18: 'brilliant', 19: 'catchy', 20: 'charming', 21: 'creative', 22: 'cute',
                             23: 'delight', 24: 'elegant', 25: 'enchanted', 26: 'enjoy', 27: 'enlighten',
                             28: 'entertain', 29: 'entertaining', 30: 'enthusiastic', 31: 'excellent', 32: 'excited',
                             33: 'extraordinary', 34: 'eye-catching', 35: 'fabulous', 36: 'fancy', 37: 'fans',
                             38: 'fantastic', 39: 'fascinate', 40: 'fascinating', 41: 'favorite', 42: 'fine',
                             43: 'gifted', 44: 'great', 45: 'happiness', 46: 'happy', 47: 'heartwarming',
                             48: 'honorable', 49: 'honored', 50: 'humor', 51: 'humorous', 52: 'ideal', 53: 'important',
                             54: 'impressed', 55: 'impressive', 56: 'innovative', 57: 'insightful', 58: 'interesting',
                             59: 'interest', 60: 'joy', 61: 'joyful', 62: 'like', 63: 'love', 64: 'lovely',
                             65: 'magical', 66: 'magnificent', 67: 'marvelous', 68: 'masterpiece', 69: 'meaningful',
                             70: 'nice', 71: 'perfect', 72: 'phenomenal', 73: 'pleased', 74: 'pleasure',
                             75: 'recommend', 76: 'recommended', 77: 'spectacular', 78: 'success', 79: 'successful',
                             80: 'superb', 81: 'talent', 82: 'talented', 83: 'terrific', 84: 'thrilling',
                             85: 'unforgettable', 86: 'vivid', 87: 'win', 88: 'wonderful', 89: 'worthy', 90: '',
                             91: 'absurd', 92: 'angry', 93: 'annoyed', 94: 'annoying', 95: 'arrogant', 96: 'ashamed',
                             97: 'awful', 98: 'bad', 99: 'bias', 100: 'biased', 101: 'bored', 102: 'boring',
                             103: 'bother', 104: 'bug', 105: 'buggy', 106: 'bullshit', 107: 'cliche', 108: 'complain',
                             109: 'confused', 110: 'creepy', 111: 'cringe', 112: 'critical', 113: 'damn', 114: 'detest',
                             115: 'detesting', 116: 'disappointed', 117: 'disappointing', 118: 'dislike', 119: 'diss',
                             120: 'dissatisfied', 121: 'doom', 122: 'doomed', 123: 'embarrass', 124: 'embarrassing',
                             125: 'fail', 126: 'failure', 127: 'flaw', 128: 'fool', 129: 'harm', 130: 'hate',
                             131: 'hell', 132: 'ignore', 133: 'imperfect', 134: 'inappropriate', 135: 'insulting',
                             136: 'junk', 137: 'meaningless', 138: 'mediocre', 139: 'mess', 140: 'messed', 141: 'nasty',
                             142: 'nightmare', 143: 'noisy', 144: 'nonsense', 145: 'offensive', 146: 'painful',
                             147: 'poor', 148: 'problem', 149: 'questionable', 150: 'racist', 151: 'redundant',
                             152: 'ridiculous', 153: 'risk', 154: 'rubbish', 155: 'rude', 156: 'ruin', 157: 'screwed',
                             158: 'shame', 159: 'sick', 160: 'silly', 161: 'suck', 162: 'sucks', 163: 'tedious',
                             164: 'terrible', 165: 'thumb-down', 166: 'thumbs-down', 167: 'tired', 168: 'trash',
                             169: 'ugly', 170: 'unacceptable', 171: 'unhappy', 172: 'upset', 173: 'vomit', 174: 'waste',
                             175: 'wasteful', 176: 'weak', 177: 'worse', 178: 'worst', 179: 'worthless', 180: 'wrong'}
        # for i in range(len(l)):
        #     self.feature_dict[i] = l[i]

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
                self.vocabulary = self.vocabulary | set(tokens)
                if content['stars'] == 'neg':
                    num_res[0] += 1
                    self.doc_voc[0].extend(tokens)
                elif content['stars'] == 'pos':
                    num_res[1] += 1
                    self.doc_voc[1].extend(tokens)
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
        return np.argmax(prob)


if __name__ == '__main__':
    from naivebayes import NaiveBayes
    # so that it is using the NaiveBayes class defined above rather __main__.NaiveBayes
    nb = NaiveBayes()
    nb.train()
    nb_pickle = pickle.dump(nb, open("nb_pickle", 'wb'))