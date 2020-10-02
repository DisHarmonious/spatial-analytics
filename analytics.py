import pandas as pd
import numpy
import timeit

###find the ids of the ships that spend most time at fishing areas
data = pd.read_csv('/home/alex/Desktop/doulke_eksamhniaia/data/[P1] AIS Data/nari_dynamic.csv')
ids=pd.read_csv('/home/alex/Desktop/doulke_eksamhniaia/apotelesmata/Breaches_everything.txt', sep=" ")

pliades=data.iloc[:,6:8]
area_id=ids.iloc[:,1]
pliada=ids.iloc[:,0]
boat_ids=data.iloc[pliada.values,0]
unique_boat_ids=boat_ids.drop_duplicates()

boat_id=[]
counter=[]
t1=timeit.default_timer()
for i in unique_boat_ids:
    temp=i
    boat_id.append(temp)
    temp2=sum(boat_ids==temp)
    counter.append(temp2)
    
    
a=pd.DataFrame({'boat_ids':boat_id , 'counter':counter })
b=a.drop_duplicates()
result=b.sort_values('counter', ascending=False)[:5]
print("Top 5 ship ids that spend most time in fishing areas:\n%s")%result.iloc[:,0].values

###find what month the ships prefer to visit fishing areas
t=data.iloc[pliada.values,8]
a=pd.to_datetime(t, unit='s')

months=[]
for i in range(0,len(pliada)):
    temp=a.iloc[i].month
    months.append(temp)

unique_months=list(set(months))

result = {i:months.count(i) for i in unique_months}
print("\nMonthly Distribution of ships that area inside fishing areas:\n%s")%result


###find for each month, best fishing area
t=data.iloc[pliada.values,8]
a=pd.to_datetime(t, unit='s')

mon=[]
for i in range(0,len(pliada)):
    temp=a.iloc[i].month
    mon.append(temp)
ar=[]
for i in range(0,len(pliada)):
    temp=area_id[i]
    ar.append(temp)
am=[]
for i in range(0,len(pliada)):
    am.append(1)

a=pd.DataFrame({'month':mon, 'area':ar, 'amount':am })
a['Total'] = a.groupby(['month', 'area'])['amount'].transform('sum')
b=a[['month','area','Total']]
c=b.drop_duplicates()
d=c.groupby('month', sort = False).max()
result=d.iloc[:,:-1]
print("\nBest fishing area during each month:\n%s")%result
