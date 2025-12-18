#%%
import pandas as pd
import numpy as np

# Paramètres
start_date = "2024-01-01"
end_date = "2024-12-31"

# Génération du calendrier
dates = pd.date_range(start=start_date, end=end_date, freq="D")

df = pd.DataFrame({"date": dates})

# Variables calendaires utiles
df["weekday"] = df["date"].dt.weekday        # 0=lundi, 6=dimanche
df["week"] = df["date"].dt.isocalendar().week
df["month"] = df["date"].dt.month

# Jour ON / OFF (exemple : OFF le week-end)
df["is_on"] = df["weekday"] < 5

# Facteurs journaliers
np.random.seed(42)

df["factor_a"] = np.random.normal(loc=1.0, scale=0.1, size=len(df))
df["factor_b"] = np.random.choice([0.8, 1.0, 1.2], size=len(df))

# Montant journalier (logique métier simulée)
base_amount = 100.0

df["amount_eur"] = (
    base_amount
    * df["factor_a"]
    * df["factor_b"]
    * df["is_on"].astype(int)
    + np.random.normal(0, 5, size=len(df))
)

