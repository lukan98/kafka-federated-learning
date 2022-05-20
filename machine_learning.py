from sklearn.neural_network import MLPClassifier
from sklearn.datasets import load_iris
from sklearn.model_selection import train_test_split


class IrisClassifier:

    def __init__(self, verbose):
        self.model = MLPClassifier(
            hidden_layer_sizes=(3, 3),
            activation='relu',
            solver='sgd',
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

    def get_parameters(self):
        return self.model.get_params()

    def set_parameters(self, parameters):
        self.model.set_params(parameters)


if __name__ == '__main__':
    X, y = load_iris(return_X_y=True)

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)

    classifier = IrisClassifier(verbose=True)
    classifier.fit(X_train, y_train)

    print(classifier.score(X_test, y_test))
