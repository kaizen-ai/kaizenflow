from moviepy.editor import VideoFileClip
import cv2
import numpy as np
import time
from scipy.io import savemat
import rethinkdb as r
import json
import pandas as pd
rethink = r.RethinkDB()
rethink.connect('localhost', 28015).repl()
frames = []

def split(array):
    array = np.array(array)
    red = array[:, :, 0:1]
    green = array[:, :, 1:2]
    blue = array[:, :, 2:3]
    nb = []
    nr = []
    ng = []
    for i in red:
        tp = [item for sublist in i for item in sublist]
        tp = [int(x) for x in tp]
        nr.append(tp)
        i = None
    red = None
    for j in green:
        tp = [item for sublist in j for item in sublist]
        tp = [int(x) for x in tp]
        ng.append(tp)
        j = None
    green = None
    for k in blue:
        tp = [item for sublist in k for item in sublist]
        tp = [int(x) for x in tp]
        nb.append(tp)
        k = None
    blue = None
    red = None
    green = None
    return nr, ng, nb

cap = cv2.VideoCapture('/home/ke/Desktop/database/testing/Data605Project/Python Codes/Rickroll.mp4')
length = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
'''for i in range(length):
    new = []
    cap.set(cv2.CAP_PROP_POS_FRAMES, i)
    ret, frame = cap.read()
    frame = np.array(frame)
    red, green, blue = split(frame)
    name = "frame_"+str(i)
    rethink.db('Project').table_create(name).run()
    for j in range(len(red)):
        new.append({"red":red[j], "green":green[j], "blue":blue[j]})
    rethink.db('Project').table(name).insert(new).run()
    print(i/length)'''
    
###Avalible Code###
#Everything Above has been tested and will work    
    
#rethink.db("test").table_create("array"run()
#print(frame.ndim)
print(length)
#new = frames
'''jsondata = json.dumps(frames)
with open("/home/ke/Desktop/database/testing/Data605Project/Python Codes/test2.json", "w") as json_file:
    json_file.write(jsondata)'''
# Serialize the list to JSON
#rethink.db("ke-B660M-AORUS-PRO-AX-DDR4").table
#rethink.connect('localhost', 28015).repl()
#print(loaded_arrays[1].ndim)
# Write the array to disk
#print(frames[12].ndim)
#newarray = np.stack(frames)
#print(newarray.shape)

###Line of Submission###

'''new = []
im = cv2.imread("/home/ke/Desktop/database/testing/Data605Project/Python Codes/earth.jpg")
im = np.array(im)
red, green, blue = split(im)
for i in range(len(red)):
    new.append({"red":red[i], "green":green[i], "blue":blue[i]})
    #print(i)
rethink.db('Project').table('picture').insert(new).run()
'''


'''
red = red.tolist()
green = im[:, :, 1:2]
green = green.tolist()
blue = im[:, :, 2:3]
blue = blue.tolist()
nb = []
for k in blue:
    nb.append([item for sublist in k for item in sublist])'''
#rethink.db('Project').table_create('picture').run()
'''for i in range(len(red)):
    frames.append({"red":red[i], "green":green[i], "blue":blue[i]})
    #print(i)
print(len(red[2]))
#rethink.db('Project').table_create('NewArray').run()
#rethink.db('Project').table('Array').insert(frames).run()
array = rethink.db('Project').table('Array').pluck('red').run()
for document in array:
    new.append(document)
x = list(new[4].values())
print(x[0][4][0])'''