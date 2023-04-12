from sklearn.ensemble import IsolationForest
import numpy as np

def isolation_forest_labels(data):
    clf = IsolationForest(n_estimators=500, random_state=101)
    X = np.array(data.close).reshape(len(data), 1)
    clf.fit(X)
    return clf.predict(X) == 1

def quantiles(data):    
    df = data.sort_values(by='close', ascending=True).reset_index()

    Q1 = round(0.25 * len(df))
    Q3 = round(0.75 * len(df))

    Q1 = df.iloc[Q1].close
    Q3 = df.iloc[Q3].close

    iqr = Q3 - Q1
    upper = Q3 + (1.5 * iqr)
    lower = Q1 - (1.5 * iqr)

    X = np.array(df.close).reshape(len(df), 1)

    labs = []
    for pt in X:
        if (pt > upper) or (pt < lower):
            labs.append(True)
            continue

        labs.append(False) 
    
    return np.array(labs)