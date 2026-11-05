from sklearn.svm import SVR
import numpy as np
import matplotlib.pyplot as plt

# Données synthétiques
X = np.sort(5 * np.random.rand(100, 1), axis=0)
y = np.sin(X).ravel() + np.random.normal(0, 0.1, X.shape[0])  # bruit ajouté

# Modèle SVR
svr_rbf = SVR(kernel='rbf', C=100, gamma=0.1, epsilon=0.1)
svr_rbf.fit(X, y)

# Prédiction
X_test = np.linspace(0, 5, 100)[:, np.newaxis]
y_pred = svr_rbf.predict(X_test)

# Visualisation
plt.scatter(X, y, color='blue', label='Données')
plt.plot(X_test, y_pred, color='red', label='SVR (RBF)')
plt.legend()
plt.show()