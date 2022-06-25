import numpy as np
import pandas as pd
from constants import COEFFICIENTS_KEY, INTERCEPTS_KEY
from sklearn.neural_network import MLPClassifier
from sklearn.metrics import confusion_matrix, classification_report
from sklearn.model_selection import train_test_split
from functools import reduce
from sklearn.datasets import load_digits


def make_digit_datasets(
        number_of_workers,
        test_size=0.2,
        initial_samples_per_class=1):
    digits = load_digits()
    # create a Pandas DataFrame out of Bunch object
    digits_df = pd \
        .DataFrame(
            data=np.c_[digits['data'], digits['target']],
            columns=digits['feature_names'] + ['target'])
    # take one sample from each class i.e. target
    initial_df = digits_df\
        .groupby('target')\
        .head(initial_samples_per_class)
    # remove the previously taken samples
    digits_df = pd \
        .merge(
            digits_df,
            initial_df,
            indicator=True,
            how='outer') \
        .query('_merge=="left_only"')\
        .drop('_merge', axis=1)

    y_initial = pd.DataFrame(initial_df['target']).to_numpy()
    X_initial = initial_df.drop(columns='target').to_numpy()

    y = pd.DataFrame(digits_df['target']).to_numpy()
    X = digits_df.drop(columns='target').to_numpy()
    # split test and training sets
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=test_size)
    X_train, y_train = cutoff_dataset(
        X_train, y_train, number_of_workers)

    return X_train, X_test, X_initial,\
           y_train, y_test, y_initial


def cutoff_dataset(X, y, number_of_workers):
    new_length = len(X) - len(X) % number_of_workers
    return X[:new_length], y[:new_length]


def aggregate_parameters(parameters):
    parameter_sum = np.asarray(reduce(sum_parameters, parameters), dtype=object)
    return np.divide(parameter_sum, len(parameters))


def sum_parameters(parameters_a, parameters_b):
    if len(parameters_a) != len(parameters_b):
        raise ValueError('The parameter lists must be of the same length! (Check hidden layer counts)')

    parameter_sum = []
    for i in range(len(parameters_a)):
        parameter_sum.append(np.add(parameters_a[i], parameters_b[i]))
    return parameter_sum


def serialize_parameters(coefficients, intercepts):
    return {
        COEFFICIENTS_KEY: [array.tolist() for array in coefficients],
        INTERCEPTS_KEY: [array.tolist() for array in intercepts]
    }


def deserialize_parameters(parameter_dictionary):
    coefficients = [np.array(array) for array in parameter_dictionary[COEFFICIENTS_KEY]]
    intercepts = [np.array(array) for array in parameter_dictionary[INTERCEPTS_KEY]]
    return coefficients, intercepts


class IrisClassifier:

    def __init__(self, verbose=False):
        self.model = MLPClassifier(
            hidden_layer_sizes=(3, 4),
            activation='relu',
            solver='adam',
            alpha=0.0001,
            learning_rate='constant',
            learning_rate_init=0.1,
            verbose=verbose)

    def fit(self, X, y):
        self.model.fit(X, y)

    def partial_fit(self, X, y):
        self.model.partial_fit(X, y)

    def predict(self, X):
        return self.model.predict(X)

    def score(self, X, y):
        return self.model.score(X, y)

    def confusion_matrix(self, X, y_true):
        y_predicted = self.predict(X)
        return confusion_matrix(y_true, y_predicted)

    def get_coefficients(self):
        return self.model.coefs_

    def set_coefficients(self, coefficients):
        self.model.coefs_ = coefficients

    def get_intercepts(self):
        return self.model.intercepts_

    def set_intercepts(self, intercepts):
        self.model.intercepts_ = intercepts

    def get_number_of_layers(self):
        return len(self.model.coefs_)


class DigitClassifier():

    def __init__(self):
        self.model = MLPClassifier(
            hidden_layer_sizes=(40,),
            activation='logistic',
            solver='sgd',
            alpha=1e-4,
            learning_rate='constant',
            learning_rate_init=0.2)

    def fit(self, X, y):
        self.model.fit(X, y)

    def partial_fit(self, X, y):
        self.model.partial_fit(X, y)

    def predict(self, X):
        return self.model.predict(X)

    def score(self, X, y):
        return self.model.score(X, y)

    def confusion_matrix(self, X, y_true):
        y_predicted = self.predict(X)
        return confusion_matrix(y_true, y_predicted)

    def get_coefficients(self):
        return self.model.coefs_

    def set_coefficients(self, coefficients):
        self.model.coefs_ = coefficients

    def get_intercepts(self):
        return self.model.intercepts_

    def set_intercepts(self, intercepts):
        self.model.intercepts_ = intercepts

    def get_number_of_layers(self):
        return len(self.model.coefs_)

    def get_classification_report(self, X_test, y_test):
        y_pred = self.predict(X_test)
        return classification_report(y_test, y_pred)
