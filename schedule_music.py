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
    processed_temp = []
    for i in range(len(processed_clip)):
        processed_temp.append(processed_clip[i][0])
    if file_path in processed_temp:
        return True
    else:
        return False

def imagefile_p(file_name):
    if file_name.split(".")[1] == "jpg" or file_name.split(".")[1] == "png" or file_name.split(".")[1] == "jpeg":
        return True
    else:
        return False

while True:
    con = mysql.connector.connect(user='root', database='music', password='123')
    cursor = con.cursor()
    drop = "DROP TABLE IF EXISTS schedule;"
    create = "CREATE TABLE schedule (clipname text, clipstdate date NOT NULL, clipenddate date NOT NULL, cweight int(2) NOT NULL, SYSTIMESTAMP datetime NOT NULL, SYSHR int(2) DEFAULT NULL, SYSWKDAY int(1) DEFAULT NULL, id int(11) DEFAULT NULL,  datestamp timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP) ENGINE=InnoDB DEFAULT CHARSET=latin1;"
    select =  "SELECT clipname FROM schedule limit 10;"
    cursor.execute(drop)
    cursor.execute(create)
    time.sleep(2)
    cursor.execute(select)
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
            if imagefile_p(file_name):
                time.sleep(10)
            else:
                time.sleep(duration_time)
            os.killpg(os.getpgid(process.pid), signal.SIGTERM)
            processed_clip.append([file_name, time.strftime("%Y-%m-%d %H:%M:%S")])
        else:
            continue
