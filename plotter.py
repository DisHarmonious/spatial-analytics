import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

#Execution time using bbox
time=[484,947,1409,1864,2326,2556,3010,
3424,3836,4245,4661,5118,5578,5996,
6419,6834,7255,7887, 8310]

num_iterations=['1',  '2',  '3',  '4',  '5',  '6',  '7', 
 '8',  '9',  '10',  '11',  '12', '13',  '14',  '15',
'16',  '17',  '18',  '19']

plt.plot(time, color='blue')
plt.xticks(arange(19), num_iterations)
plt.title('Execution Time using BBOX of fishing areas')
plt.xlabel("Millions of iterations")
plt.ylabel("Time(s)")
plt.show()


#Execution time using kd_tree,whole fishing area
a=pd.read_csv("/home/alex/Desktop/doulke_eksamhniaia/apotelesmata/time_everything.txt", header=None)
a=a/3600 #convert secs to hours
num_iterations=['1',  '2',  '3',  '4',  '5',  '6',  '7', 
 '8',  '9',  '10',  '11',  '12', '13',  '14',  '15',
'16',  '17',  '18',  '19']
plt.plot(a, color='red')
plt.yticks(np.arange(0, 95, step=5))
plt.xticks(np.arange(19), num_iterations)
plt.title('Execution Time using the whole fishing areas')
plt.xlabel("Millions of iterations")
plt.ylabel("Time(hours)")
plt.show()


#Monthly Distribution of ships inside fishing areas
s = pd.Series(
    [335562, 558591, 423759, 1456, 275789, 284857, 263625],
    index = ['Jan', 'Feb', 'Mar', 'Sep', 'Oct', 'Nov', 'Dec']
)
plt.title("Monthly Distribution of ships inside fishing areas")
plt.ylabel('Frequency')
plt.xlabel('Month')
ax = plt.gca()
ax.tick_params(axis='x', colors='blue')
ax.tick_params(axis='y', colors='red')
my_colors = 'brgbrg'  #red, green, blue, black, etc
s.plot( 
    kind='bar', 
    color=my_colors,
    rot=0
)
plt.show()


#Execution time for 3 different methods

#Monthly Distribution of ships inside fishing areas
s = pd.Series(
    [288, 250, 16],
    index = ['Brute Force', 'Inside', 'Inside_kd']
)
plt.title("Mean Execution time for different methods, for 1 iteration")
plt.ylabel('Time(msec)')
plt.xlabel('Method')
ax = plt.gca()
ax.tick_params(axis='x', colors='blue')
ax.tick_params(axis='y', colors='red')
my_colors = 'brgbrg'  #red, green, blue, black, etc
s.plot( 
    kind='bar', 
    color=my_colors,
    rot=0
)
plt.show()







