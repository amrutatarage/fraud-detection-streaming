import numpy as np
import joblib
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler

np.random.seed(42)
n = 10000
amounts = np.random.uniform(10, 20000, n)
velocities = np.random.randint(1, 20, n)
country_risk = np.random.randint(0, 3, n)

labels = ((amounts > 5000) & (velocities > 5) & (country_risk == 2)).astype(int)
X = np.column_stack([amounts, velocities, country_risk])

X_train, X_test, y_train, y_test = train_test_split(X, labels, test_size=0.2)

scaler = StandardScaler()
X_train = scaler.fit_transform(X_train)
X_test = scaler.transform(X_test)

model = LogisticRegression()
model.fit(X_train, y_train)
print(f'Accuracy: {model.score(X_test, y_test):.2%}')

joblib.dump({'model': model, 'scaler': scaler}, 'fraud_model.pkl')
print('Saved to fraud_model.pkl')