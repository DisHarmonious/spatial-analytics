import geopandas as gp
from shapely.geometry import Point, Polygon
import pandas as pd
import timeit
from pyproj import Proj, transform
from functools import partial
import pyproj
import pyspark
from pyspark.sql import SQLContext
from pyspark.sql.functions import spark_partition_id
from pyspark.sql.types import *
from pyspark.sql.functions import col, count, rand, collect_list, explode, struct, count, lit
from pyspark.sql.functions import pandas_udf, PandasUDFType
from itertools import chain
import numpy 
import random
import pyarrow
from scipy import spatial



###read csv with pliades
data = pd.read_csv('/home/alex/Desktop/doulke_eksamhniaia/data/[P1] AIS Data/nari_dynamic.csv')
pliades=data.iloc[:,6:8] #take the columns with lon and lat

###geocoords
shp = gp.GeoDataFrame.from_file('/home/alex/Desktop/doulke_eksamhniaia/data/[C4] Fishing Areas (European commission)/v_recode_fish_area_clean.shp')
shp1=shp['geometry']


################################## kdTREE ########################################
#methodology:
#1. For every polygon, for every POINT, add the point to kdtree
#2. find for one point, the nearest kdtree.point
#3. return the polygon of the kdtree.point
#4. check if point is inside returned polygon

all_xs=[]
all_ys=[]
which_poly=[]
for i in range(0,len(shp1)):
	if shp1[i].type=='Polygon':
		temp1, temp2=shp1[i].exterior.coords.xy
		all_xs.append(temp1)
		all_ys.append(temp2)
		temp=list(numpy.repeat(i, len(shp1[i].exterior.coords.xy[0])))
		which_poly.append(temp)
	elif shp1[i].type=='MultiPolygon':
		allparts = [p.buffer(0) for p in shp1[i]]
		for k in range(0,len(allparts)):
			temp1=list(allparts[k].exterior.coords.xy[0])
			temp2=list(allparts[k].exterior.coords.xy[1])
			all_xs.append(temp1)	
			all_ys.append(temp2)
			temp=list(numpy.repeat(i, len(allparts[k].exterior.coords.xy[0])))
			which_poly.append(temp)

all_xs=numpy.concatenate(all_xs).ravel()
all_ys=numpy.concatenate(all_ys).ravel()
which_poly=numpy.concatenate(which_poly).ravel()

#build tree
tree = spatial.KDTree(zip(all_xs, all_ys))


#build helping function
def inside_kd(i):
	distances, indices = tree.query((pliades.iloc[i,0], pliades.iloc[i,1]), k=1)
	poly_to_check=which_poly[indices]
	point=Point(pliades.iloc[i,0], pliades.iloc[i,1])
	breaches=numpy.array([])
	if point.within(shp1[poly_to_check]):
		breaches=numpy.append(breaches, poly_to_check)
	if len(breaches)>0:
		breaches=numpy.append(breaches, i)
		return breaches

'''
###rdd
test=sc.parallelize(range(0,2000)).map(lambda x:inside_kd(x)).collect()
'''

###Python
result=[]
timer=[]
t1=timeit.default_timer()
for i in range(0,len(pliades)):
	temp=inside_kd(i)
	result.append(temp)
	if i%500000==0:	
		t2=timeit.default_timer()
		timetemp=t2-t1
		timer.append(timetemp)
		print(i,timetemp)

test=result
result=[]
for i in range(0,len(test)):
	if test[i] is not None:
		temp=[test[i][1], test[i][0]]
		result.append(temp)

result_df=pd.DataFrame(result, columns=["area", "point_id"])
numpy.savetxt(r'/home/alex/Desktop/Breaches_whole_dataset.txt', result_df.values, fmt='%d')
numpy.savetxt(r'/home/alex/Desktop/Time_whole_dataset.txt', timer, fmt='%d')






























