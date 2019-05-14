training_data = None
model_name = None

import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.svm import SVC
from sklearn import preprocessing
from sklearn.model_selection import GridSearchCV
from sklearn.preprocessing import normalize
from sklearn.metrics import accuracy_score,make_scorer
import numpy as np
from joblib import dump, load
import sys

import requests
symbol = "GOOG"
date = 20190430

l = []
for i in range(30):
  url = 'https://investors-exchange-iex-trading.p.rapidapi.com/stock/' + symbol +'/chart/date/' + str(date - i)
  response = requests.get(url,
    headers={
      "X-RapidAPI-Host": "investors-exchange-iex-trading.p.rapidapi.com",
      "X-RapidAPI-Key": "cd4641fc4emshec67ca2f66d3209p108dd9jsn9b428d096e8b"
      }
  )
  # price = response.json()

  price = response.json()
  l += price



def svm():
    gammas, Cs = [], []
    for a in np.r_[-15: 4][::4]:
        gamma = pow(float(2), a)
        gammas.append(gamma)
    for b in np.r_[-5: 16][::4]:
        c = pow(float(2), b)
        Cs.append(c)
    # Set the parameters by cross-validation
    tuned_parameters = {'kernel': ['sigmoid'], 'gamma': gammas,  'C': Cs}
    clf = GridSearchCV(SVC(probability = False), tuned_parameters, cv = 5,
                  scoring = 'accuracy')
    return clf
def get_xy(data):
    price = data
    x = []
    y = []
    for i in range(len(price)-30):
        if i+10 >= len(price):
            break
        y.append(1 if price[i]["average"] < price[i+10]["average"] else 0)
        past30 = [price[j]["average"] for j in range(30)]
        x.append([np.mean(past30), np.var(past30), np.polyfit(past30, np.r_[0:30], 1)[0]])
    return normalize(x, axis=1), y





if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: train.py [model_name]")
        exit(1)

    model_name = sys.argv[1]
    x, y = get_xy(l)
    # print(x)
    print(y)

    print("finish processed the data")
    clf = svm()
    clf.fit(x, y)
    print("finish training")
    dump(clf, model_name + '.joblib')
    print(clf.predict(x))
    print("Saved model to " + model_name + '.joblib')