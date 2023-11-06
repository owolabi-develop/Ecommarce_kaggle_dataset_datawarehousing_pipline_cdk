import pandas as pd



df = pd.read_csv('olist_order_reviews_dataset.csv')

df['review_creation_date'] = pd.to_datetime(df['review_creation_date'])

df.to_csv("olist_order_reviews_dataset.csv",index=False)
