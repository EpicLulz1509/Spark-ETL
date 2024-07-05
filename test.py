import pandas as pd

df = pd.read_csv('tgtdata\customer.csv')
cust_group_by_location = df.groupby('LocationId', as_index=False).count()
cust_group_by_location = cust_group_by_location.drop(['CustomerKey'], axis=1)
# cust_group_by_location = cust_group_by_location.rename(index={'CustomerId', 'CustomerCount'})
print(cust_group_by_location.head(10))
print(cust_group_by_location.columns)
print(cust_group_by_location.index)


cust_group_by_location.to_csv('trials.csv')