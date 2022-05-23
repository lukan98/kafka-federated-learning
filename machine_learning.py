import numpy as np
from constants import COEFFICIENTS_KEY, INTERCEPTS_KEY
from sklearn.neural_network import MLPClassifier
from sklearn.datasets import load_iris
from sklearn.model_selection import train_test_split
from sklearn.metrics import confusion_matrix
from functools import reduce


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


if __name__ == '__main__':
    X, y = load_iris(return_X_y=True)
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)

    classifier = IrisClassifier()
    classifier.fit(X_train, y_train)

    print(classifier.score(X_test, y_test))
    print(classifier.confusion_matrix(X_test, y_test))