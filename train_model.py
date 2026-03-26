import pandas as pd
import random
from sklearn.ensemble import RandomForestClassifier
import joblib

data = []

for _ in range(5000):
    amount = random.uniform(10, 20000)
    country = random.choice([0, 1])  # 0 = IN, 1 = foreign
    rapid = random.choice([0, 1])

    fraud = 1 if (amount > 5000 or country == 1 or rapid == 1) else 0

    data.append([amount, country, rapid, fraud])

df = pd.DataFrame(data, columns=["amount", "country", "rapid", "fraud"])

X = df[["amount", "country", "rapid"]]
y = df["fraud"]

model = RandomForestClassifier()
model.fit(X, y)

joblib.dump(model, "fraud_model.pkl")

print("Model trained and saved")