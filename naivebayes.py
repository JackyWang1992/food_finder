# Naive Bayes Classifier for reviews

import os
import re
from collections import defaultdict

import numpy as np


class NaiveBayes():
    def __init__(self):
        self.class_dict = {0: 'neg', 1: 'pos'}
        # self.feature_dict = {0: 'great', 1: 'poor', 2: 'long')
        # use Hu and Liu's sentimental lexicon as feature.
        f = open('feature.txt')
        s = f.read()
        l = s.split('\n')
        self.feature_dict = {}
        for i in range(len(l)):
            self.feature_dict[i] = l[i]

        """self.feature_gain = {0: 'shrek', 1: '&nbsp', 2: 'mulan', 3: 'gattaca', 4: 'flynt', 5: 'guido', 6: 'jolie',
                             7: 'ordell', 8: 'seagal', 9: 'leila', 10: 'sweetback', 11: 'taran', 12: 'homer',
                             13: 'mallory', 14: 'donkey', 15: 'brenner', 16: 'argento', 17: '1900', 18: 'rounders',
                             19: "truman's", 20: 'pokemon', 21: '8mm', 22: 'lebowski', 23: 'giles', 24: 'fei-hong',
                             25: 'farquaad', 26: 'silverman', 27: 'bruckheimer', 28: 'carver', 29: 'memphis',
                             30: 'tango', 31: 'psychlo', 32: 'osment', 33: "flynt's", 34: 'atrocious', 35: 'sethe',
                             36: 'supergirl', 37: 'psychlos', 38: "mulan's", 39: 'lumumba', 40: 'babysitter',
                             41: 'lambeau', 42: 'chickens', 43: 'reza', 44: 'hen', 45: 'carlito', 46: 'camille',
                             47: 'symbol', 48: 'farrellys', 49: 'tomb', 50: 'mandingo', 51: 'raider', 52: 'bilko',
                             53: 'religion', 54: 'anna', 55: 'neo', 56: "argento's", 57: 'coens', 58: 'maximus',
                             59: 'jo', 60: 'shyamalan', 61: 'tibbs', 62: 'cal', 63: 'humbert', 64: 'stephane',
                             65: 'capone', 66: 'fashioned', 67: 'sphere', 68: 'hush', 69: 'eszterhas', 70: 'damon',
                             71: 'stephens', 72: '3000', 73: 'incoherent', 74: 'jill', 75: 'ideals', 76: 'soviet',
                             77: 'angelina', 78: 'hilary', 79: 'apostle', 80: 'justin', 81: 'wrestlers', 82: 'cisco',
                             83: 'jude', 84: 'crowe', 85: 'cauldron', 86: 'schumacher', 87: 'dead-bang', 88: 'mushu',
                             89: 'macdonald', 90: '+2', 91: 'tunney', 92: 'flanders', 93: 'motta', 94: 'dolores',
                             95: "son's", 96: 'croft', 97: 'thematic', 98: 'bischoff', 99: 'hawk'}"""

        """self.feature_likelihood = {0: "&nbsp", 1: "shrek", 2: "mulan", 3: "flynt", 4: "jolie", 5: "gattaca",
                                   6: "seagal", 7: "guido", 8: "ordell", 9: "leila", 10: "8mm", 11: "sweetback",
                                   12: "brenner", 13: "taran", 14: "1900", 15: "homer", 16: "pokemon", 17: "mallory",
                                   18: "lebowski", 19: "donkey", 20: "bruckheimer", 21: "silverman", 22: "rounders",
                                   23: "argento", 24: "tango", 25: "psychlos", 26: "supergirl", 27: "babysitter",
                                   28: "atrocious", 29: "memphis", 30: "psychlo", 31: "schumacher", 32: 'truman"s',
                                   33: "damon", 34: "farrellys", 35: "mandingo", 36: "tomb", 37: "bilko",
                                   38: "raider", 39: "fei - hong", 40: "giles", 41: "religion", 42: "sphere",
                                   43: "hush", 44: "eszterhas", 45: "anna", 46: "farquaad", 47: "godzilla",
                                   48: "werewolf", 49: "jakob", 50: "justin", 51: "angelina", 52: "incoherent",
                                   53: "3000", 54: "wrestlers", 55: "cisco", 56: "jill", 57: "osment", 58: "sethe",
                                   59: "carver", 60: 'flynt"s', 61: "lumumba", 62: 'mulan"s', 63: "lambeau",
                                   64: "outstanding", 65: "cauldron", 66: "magoo", 67: "jawbreaker", 68: "chickens",
                                   69: "camille", 70: "symbol", 71: "carlito", 72: "reza", 73: "hen", 74: "musketeer",
                                   75: "silverstone", 76: "dead-bang", 77: "bischoff", 78: "terl", 79: "macdonald",
                                   80: "croft", 81: "tunney", 82: "dud", 83: "hawk", 84: "crowe", 85: "jude",
                                   86: "wrestling", 87: "hudson", 88: 'charlie"s', 89: "cal", 90: "maximus",
                                   91: "coens", 92: "shyamalan", 93: 'argento"s', 94: "stephane", 95: "tibbs", 96: "jo",
                                   97: "neo", 98: "capone", 99: "humbert"}"""

        self.prior = np.zeros(len(self.class_dict))
        self.likelihood = np.zeros((len(self.class_dict), len(self.feature_dict)))

    # Attempt to use information gain to generate features.
    def getFeature_infogain(self, train_set):
        # iterate over training documents
        num_doc = defaultdict(int)
        self.vocabulary = set()
        self.bigdoc = defaultdict(list)
        self.feature = {}
        for root, dirs, files in os.walk(train_set):
            for name in files:
                with open(os.path.join(root, name)) as f:
                    # collect class counts and feature counts
                    tokens = [w for w in re.split('\s+', f.read()) if w]
                    self.vocabulary = self.vocabulary | set(tokens)
                    if str(root)[-3:] == 'neg':
                        num_doc[0] += 1
                        self.bigdoc[0].extend(tokens)
                    elif str(root)[-3:] == 'pos':
                        num_doc[1] += 1
                        self.bigdoc[1].extend(tokens)
        self.prior[0] = np.log(num_doc[0] / (num_doc[0] + num_doc[1]))
        self.prior[1] = np.log(num_doc[1] / (num_doc[0] + num_doc[1]))
        voc_dict = list(self.vocabulary)
        for i in range(len(voc_dict)):
            wcount = 0
            class_wordcount = []
            for j in self.class_dict:
                cw_count = self.bigdoc[j].count(voc_dict[i])
                class_wordcount.append(cw_count)
                wcount += cw_count
            # p(w)
            pw = wcount / len(self.bigdoc[0]) + len(self.bigdoc[1])
            # use formula to calcuate info gain
            gain = -1 * (self.prior[0] * (num_doc[0] / (num_doc[0] + num_doc[1])) + self.prior[1] * num_doc[1] / (num_doc[0] + num_doc[1])) \
                   + pw * ((class_wordcount[0] + 1)/(wcount + 2)) * np.log(((class_wordcount[0] + 1)/(wcount + 2))) \
                   + pw * ((class_wordcount[1] + 1)/(wcount + 2)) * np.log(((class_wordcount[1] + 1)/(wcount + 2)))\
                   + (1 - pw) * (1- ((class_wordcount[0] + 1)/(wcount + 2))) * np.log(1 - ((class_wordcount[0] + 1)/(wcount + 2))) \
                   + (1 - pw) * (1- ((class_wordcount[1] + 1)/(wcount + 2))) * np.log(1 - ((class_wordcount[1] + 1)/(wcount + 2)))
            self.feature[voc_dict[i]] = gain
        self.feature = sorted(self.feature.items(), key=lambda kv: kv[1], reverse=True)

    # Attempts to use likelihood method
    def getFeature_likelihood(self, train_set):
        # iterate over training documents
        num_doc = defaultdict(int)
        self.vocabulary = set()
        self.bigdoc = defaultdict(list)
        self.likelihood_ratio = defaultdict(float)
        for root, dirs, files in os.walk(train_set):
            for name in files:
                with open(os.path.join(root, name)) as f:
                    # collect class counts and feature counts
                    tokens = [w for w in re.split('\s+', f.read()) if w]
                    self.vocabulary = self.vocabulary | set(tokens)
                    if str(root)[-3:] == 'neg':
                        num_doc[0] += 1
                        self.bigdoc[0].extend(tokens)
                    elif str(root)[-3:] == 'pos':
                        num_doc[1] += 1
                        self.bigdoc[1].extend(tokens)
                    self.bigdoc[3].extend(tokens)
        voc_dict = list(self.vocabulary)
        llikelihood = np.zeros((len(self.class_dict), len(voc_dict)))
        for i in range(len(voc_dict)):
            for j in self.class_dict:
                wcount = self.bigdoc[j].count(voc_dict[i])
                llikelihood[j][i] = np.log((wcount + 1)/(len(self.bigdoc[j]) + len(self.vocabulary)))
            self.likelihood_ratio[voc_dict[i]] = max(llikelihood[0][i]/llikelihood[1][i], llikelihood[1][i]/llikelihood[0][i])
            # print(self.likelihood_ratio[i])
        self.likelihood_ratio = sorted(self.likelihood_ratio.items(), key=lambda kv: kv[1], reverse=True)
        print(self.likelihood_ratio[:100])

    '''
    Trains a multinomial Naive Bayes classifier on a training set.
    Specifically, fills in self.prior and self.likelihood such that:
    self.prior[class] = log(P(class))
    self.likelihood[class][feature] = log(P(feature|class))
    '''
    def train(self, train_set):
        # iterate over training documents
        num_doc = defaultdict(int)
        self.vocabulary = set()
        self.bigdoc = defaultdict(list)
        for root, dirs, files in os.walk(train_set):
            for name in files:
                with open(os.path.join(root, name)) as f:
                    # collect class counts and feature counts
                    tokens = [w for w in re.split('\s+', f.read()) if w]
                    self.vocabulary = self.vocabulary | set(tokens)
                    if str(root)[-3:] == 'neg':
                        num_doc[0] += 1
                        self.bigdoc[0].extend(tokens)
                    elif str(root)[-3:] == 'pos':
                        num_doc[1] += 1
                        self.bigdoc[1].extend(tokens)
        for i in self.feature_dict:
            for j in self.class_dict:
                wcount = self.bigdoc[j].count(self.feature_dict[i])
                self.likelihood[j][i] = np.log((wcount + 1)/(len(self.bigdoc[j]) + len(self.vocabulary)))
        self.prior[0] = np.log(num_doc[0] / (num_doc[0] + num_doc[1]))
        self.prior[1] = np.log(num_doc[1] / (num_doc[0] + num_doc[1]))


    '''
    Tests the classifier on a development or test set.
    Returns a dictionary of filenames mapped to their correct and predicted
    classes such that:
    results[filename]['correct'] = correct class
    results[filename]['predicted'] = predicted class
    '''
    def test(self, dev_set):
        results = defaultdict(dict)
        # iterate over testing documents
        for root, dirs, files in os.walk(dev_set):
            for name in files:
                with open(os.path.join(root, name)) as f:
                    # create feature vectors for each document
                    feature = np.zeros((len(self.feature_dict), 1))
                    tokens = [w for w in re.split('\s+', f.read()) if w]
                    for i in range(0, len(self.feature_dict)):
                        wcount = tokens.count(self.feature_dict[i])
                        feature[i] = wcount
                    prob = np.matmul(self.likelihood, feature)
                    for i in range(len(self.prior)):
                        prob[i][0] = prob[i][0] + self.prior[i]
                    # get most likely class
                    results[name] = {'correct': 0 if str(root)[-3:] == 'neg' else 1, 'predicted': np.argmax(prob)}
        return results

    '''
    Given results, calculates the following:
    Precision, Recall, F1 for each class
    Accuracy overall
    Also, prints evaluation metrics in readable format.
    '''
    def evaluate(self, results):
        # you may find this helpful
        confusion_matrix = np.zeros((len(self.class_dict), len(self.class_dict)))
        for val in results.values():
            confusion_matrix[val['predicted']][val['correct']] += 1

        for i in range(len(self.class_dict)):
            precision = confusion_matrix[i][i] / np.sum(confusion_matrix, axis=1)[i]
            recall = confusion_matrix[i][i] / np.sum(confusion_matrix, axis = 0)[i]
            f1 = 2 * precision * recall / (precision + recall)
            print("Results based on " + self.class_dict[i] + " class:")
            print("Precision:  ", precision)
            print("Recall:  ", recall)
            print("F1:  ", f1)
            print()

        accuracy = np.trace(confusion_matrix, offset=0) / np.sum(confusion_matrix, axis=None)
        print("Overall Accuracy:  ", accuracy)


if __name__ == '__main__':
    nb = NaiveBayes()
    # make sure these point to the right directories
    nb.train('movie_reviews/train')
    results = nb.test('movie_reviews/dev')
    nb.evaluate(results)


