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



###read csv with pliades
data = pd.read_csv('/home/alex/Desktop/doulke_eksamhniaia/data/[P1] AIS Data/nari_dynamic.csv')
pliades=data.iloc[:,6:8] #take the columns with lon and lat

###geocoords
shp = gp.GeoDataFrame.from_file('/home/alex/Desktop/doulke_eksamhniaia/data/[C4] Fishing Areas (European commission)/v_recode_fish_area_clean.shp')
shp1=shp['geometry']

'''
###create bbox of fishing shapefile, for faster maths
temp=[]
for i in range(0, len(shp1)):
	x_min=shp1[i].bounds[0]
	y_min=shp1[i].bounds[1]
	x_max=shp1[i].bounds[2]
	y_max=shp1[i].bounds[3]
	p1 = Point(x_min, y_min)
	p2 = Point(x_max, y_min)
	p3 = Point(x_max, y_max)
	p4 = Point(x_min, y_max)
	np1 = (p1.coords.xy[0][0], p1.coords.xy[1][0])
	np2 = (p2.coords.xy[0][0], p2.coords.xy[1][0])
	np3 = (p3.coords.xy[0][0], p3.coords.xy[1][0])
	np4 = (p4.coords.xy[0][0], p4.coords.xy[1][0])
	polygon = Polygon([np1, np2, np3, np4])
	temp.append(polygon)

shp1=temp
'''

###create bbox of pliades
pliadesx=pliades.iloc[:,0]
pliadesy=pliades.iloc[:,1]

x_min=min(pliadesx)
y_min=min(pliadesy)
x_max=max(pliadesx)
y_max=max(pliadesy)

p1 = Point(x_min, y_min)
p2 = Point(x_max, y_min)
p3 = Point(x_max, y_max)
p4 = Point(x_min, y_max)

np1 = (p1.coords.xy[0][0], p1.coords.xy[1][0])
np2 = (p2.coords.xy[0][0], p2.coords.xy[1][0])
np3 = (p3.coords.xy[0][0], p3.coords.xy[1][0])
np4 = (p4.coords.xy[0][0], p4.coords.xy[1][0])
bb_polygon = Polygon([np1, np2, np3, np4])

#create grid
temp1=x_max-x_min
temp2=y_max-y_min

step_x = temp1/5
step_y = temp2/5

grid_boundaries = []

for i in xrange(0,5):
    lower_boundary_x = x_min +step_x*i
    upper_boundary_x = x_min +step_x*(i + 1)
    for j in range(0,5):
        lower_boundary_y = y_min +step_y*j
        upper_boundary_y = y_min +step_y*(j + 1)
        grid_boundaries.append([lower_boundary_x,lower_boundary_y,upper_boundary_x,upper_boundary_y])

#Find, for each cell, which fishing_areas are inside it
t5=timeit.default_timer()
fishing_area_grid=[]
for i in range(0,25):
    p1 = Point(grid_boundaries[i][0], grid_boundaries[i][1])
    p2 = Point(grid_boundaries[i][2], grid_boundaries[i][1])
    p3 = Point(grid_boundaries[i][2], grid_boundaries[i][3])
    p4 = Point(grid_boundaries[i][0], grid_boundaries[i][3])
    np1 = (p1.coords.xy[0][0], p1.coords.xy[1][0])
    np2 = (p2.coords.xy[0][0], p2.coords.xy[1][0])
    np3 = (p3.coords.xy[0][0], p3.coords.xy[1][0])
    np4 = (p4.coords.xy[0][0], p4.coords.xy[1][0])
    poly=Polygon([np1,np2,np3,np4])
    fishing_area_grid.append([])
    for j in range(0, len(shp1)):
	if poly.intersects(shp1[j]):
	    fishing_area_grid[i].append(j)

t6=timeit.default_timer()      
print("Time to place protected areas in grid:\n%s") %(t6-t5)

#for each cell, find how many fishing areas are inside it
counter=[]
for i in range(0,25):
    temp=len(fishing_area_grid[i])
    counter.append(temp)

#make polygons of grid cells
cell_poly=[]
for b in range(0,25):
	p1 = Point(grid_boundaries[b][0], grid_boundaries[b][1])
	p2 = Point(grid_boundaries[b][2], grid_boundaries[b][1])
	p3 = Point(grid_boundaries[b][2], grid_boundaries[b][3])
	p4 = Point(grid_boundaries[b][0], grid_boundaries[b][3])
	np1 = (p1.coords.xy[0][0], p1.coords.xy[1][0])
	np2 = (p2.coords.xy[0][0], p2.coords.xy[1][0])
	np3 = (p3.coords.xy[0][0], p3.coords.xy[1][0])
	np4 = (p4.coords.xy[0][0], p4.coords.xy[1][0])
	poly=Polygon([np1,np2,np3,np4])
	cell_poly.append(poly)


############################### INVOLVE SPARK #################################
def inside(a):
	point=Point(pliades.iloc[a,0], pliades.iloc[a,1])
	breaches=numpy.array([], dtype=long)
    	for b in range(0,25):
		poly=cell_poly[b]
		if point.intersects(poly):
	    		cell=b
			break
	for c in fishing_area_grid[cell]: 
		if point.intersects(shp1[c]):
			breaches=numpy.append(breaches, c)
	if len(breaches)>0:
		breaches=numpy.append(breaches, a)
		return breaches


###WITH RDD###
t1=timeit.deafult_timer()
test=sc.parallelize(range(0,200)).map(lambda x:inside(x)).collect()

#fix and save the results
result=[]
for i in range(0,len(test)):
	if test[i] is not None:
		temp=[test[i][0], test[i][1]]
		result.append(temp)

result_df=pd.DataFrame(result, columns=["area", "point_id"])
#numpy.savetxt(r'/home/alex/Desktop/Breaches_small.txt', result_df.values, fmt='%d')


'''
############################  GROUPED_MAP UDF  ###################################
#https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/1281142885375883/2174302049319883/7729323681064935/latest.html


small=pliades.iloc[0:20000,:] #take small portion of dataset
ids=range(0,len(small),1)
rand=random.sample(list(range(0,20000000)),len(small))
small['area']=rand #add integers (that will become fishing areas)
small['point_id']=ids #add ids
small_df=sqlContext.createDataFrame(small)
df2 = small_df.select('lon', 'lat','point_id','area')

group_column = 'point_id'
y_column='area'
x_columns=['lon','lat']
schema=df2.select(group_column, y_column).schema
@pandas_udf(schema, PandasUDFType.GROUPED_MAP)
def ols(pdf):
	group_key=pdf[group_column]
	x=pdf['lon']
	y=pdf['lat']
	i=pdf['point_id']
	point=Point(x, y)
	breaches=numpy.array([], dtype=long)
    	for b in range(0,25):
		poly=cell_poly[b]
		if point.intersects(poly):
	    		cell=b
			break
	for c in fishing_area_grid[cell]: 
		if point.intersects(shp1[c]):
			breaches=numpy.append(breaches, c)
	if len(breaches)>0:
		a=numpy.repeat(i, len(breaches))
		data=pd.DataFrame({'point_id':a, 'area':breaches})
		return data
	

beta = df2.groupby(group_column).apply(ols).show()
#beta.collect()
'''




