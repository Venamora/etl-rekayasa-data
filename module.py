import numpy as np
from sklearn.metrics import classification_report, accuracy_score

class MultinomialNBClassifier:
    def __init__(self, alpha):
        self.alpha = alpha  # Smoothing parameter
        self.class_priors = {}
        self.feature_probs = {}
        self.classes = []

    def fit(self, X, y):
        self.classes = np.unique(y)
        n_features = X.shape[1]

        # Calculate class priors and feature likelihoods
        for c in self.classes:
            X_c = X[y == c]
            self.class_priors[c] = len(X_c) / len(X)
            
            # Count the occurrences of each feature in the class
            feature_counts = X_c.sum(axis=0)
            total_count = feature_counts.sum()

            # Calculate likelihood with Laplace smoothing
            self.feature_probs[c] = (feature_counts + self.alpha) / (total_count + self.alpha * n_features)

    def _calculate_posterior(self, x):
        posteriors = {}
        for c in self.classes:
            # Log of the prior for the class
            prior = np.log(self.class_priors[c])
            # Sum log-likelihoods of features
            likelihood = np.sum(x * np.log(self.feature_probs[c]))
            posteriors[c] = prior + likelihood
        return posteriors

    def predict(self, X):
        y_pred = []
        for x in X:
            posteriors = self._calculate_posterior(x)
            # Select class with highest posterior
            y_pred.append(max(posteriors, key=posteriors.get))
        return np.array(y_pred)
    
    def score(self, X, y):
        y_pred = self.predict(X)
        # Return the accuracy score
        return np.mean(y_pred == y)
    
    def info(self):
        print(f'class priors: {self.class_priors}')
        print(f'feature probabilities: {self.feature_probs}')

    def show_process(self, X):
        for i, x in enumerate(X):
            print(f"\nInstance {i + 1}:")
            print(f"Features: {x}")
            posteriors = self._calculate_posterior(x)
            
            # Display scores for each class
            for c in self.classes:
                print(f"  Class '{c}': Score = {posteriors[c]}")
            
            # Determine and display the predicted class
            predicted_class = max(posteriors, key=posteriors.get)
            print(f"Predicted class: {predicted_class}")

def CrossValidation(X_train_np, y_train_np, k_folds, classifier):
    scores = []
    for train_index, val_index in k_folds.split(X_train_np, y_train_np):
        X_train_fold, X_val_fold = X_train_np[train_index], X_train_np[val_index]
        y_train_fold, y_val_fold = y_train_np[train_index], y_train_np[val_index]

        classifier.fit(X_train_fold, y_train_fold)

        accuracy = classifier.score(X_val_fold, y_val_fold)
        scores.append(accuracy)
    return scores

def RandomizedSearchCV(X_train_np, y_train_np, iter, k_folds, alpha_values):
    best_params = None
    best_score = 0

    for i in range(iter):
        fold_scores = []

        for trainidx, validx in k_folds.split(X_train_np, y_train_np):
            X_train_fold, X_val_fold = X_train_np[trainidx], X_train_np[validx]
            y_train_fold, y_val_fold = y_train_np[trainidx], y_train_np[validx]

            model = MultinomialNBClassifier(alpha = alpha_values[i])
            model.fit(X_train_fold, y_train_fold)

            y_val_pred = model.predict(X_val_fold)
            fold_accuracy = accuracy_score(y_val_fold, y_val_pred)
            fold_scores.append(fold_accuracy)
        
        avg_score = np.mean(fold_scores)
        print(f"Alpha: {alpha_values[i]}, Average CV Accuracy: {avg_score}")

        if avg_score > best_score:
            best_score = avg_score
            best_params = {'alpha': alpha_values[i]}

    return best_score, best_params