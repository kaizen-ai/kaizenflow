from moviepy.editor import VideoFileClip
import cv2
import numpy as np
import time
from scipy.io import savemat
frames = []
cap = cv2.VideoCapture('/Users/xukexukexuke/Data605Project/Python Codes/Rickroll.mp4')
length = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
print( length )
fps = cap.get(cv2.CAP_PROP_FPS)
print(fps)
#for i in range(length):
frame_count = 0

#cap.set(cv2.CAP_PROP_POS_FRAMES, 12)
ret, frame = cap.read()
#y=np.array(frame)
print(frame.shape)
#frames.append(y)
#print(i/length)
# Write the array to disk
#print(frames[12].ndim)
#newarray = np.stack(frames)
#print(newarray.shape)