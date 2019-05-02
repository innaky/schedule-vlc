#!/usr/bin/python3

import mysql.connector
import numpy as np
from moviepy.editor import VideoFileClip
import subprocess
import os
import signal
import time

processed_clip = []

def processed_p(file_path):
    if file_path in processed_clip:
        return True
    else:
        return False

while True:
    con = mysql.connector.connect(user='root', database='music', password='123')
    cursor = con.cursor()
    cursor.execute("SELECT clipname FROM schedule limit 10")
    result = cursor.fetchall()
    list_files = np.array(result).tolist()

    for i in range(len(list_files)):
        file_name = list_files[i][0]

        if not processed_p(file_name):
            clip = VideoFileClip(file_name)
            duration_time = clip.duration
            clip.close()
            command = "vlc " + file_name
            # kill process from subprocess
            process = subprocess.Popen(command, shell=True, preexec_fn=os.setsid)
            time.sleep(duration_time)
            os.killpg(os.getpgid(process.pid), signal.SIGTERM)
            processed_clip.append(file_name)
        else:
            continue
