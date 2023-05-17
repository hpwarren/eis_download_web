import os
import sys
import pickle
import subprocess
import re
import glob
import time
import random

from queue import Queue
from threading import Thread

num_threads = 10
task_queue = Queue()

def curl_download():
  # the worker checks the queue until it is empty
  while not(task_queue.empty()):
    input = task_queue.get()
    local_file = input[0]
    remote_file = input[1]

    # download the file via a spawn to curl; check file first
    # what happens if the spawn crashes?
    if not(os.path.isfile(local_file)):
      com = 'curl --insecure -s --fail --max-time 120 -o ' + local_file + ' ' + remote_file
      subprocess.run(com, shell=True)
      print(remote_file + ' -> ' + local_file)
    else:
      print(' file exists ' + local_file)

    # mark the processed task as done
    task_queue.task_done()

def check_paths(file_list):
  paths = []
  for input in file_list:
    local_file = input[0]
    path, filename = os.path.split(local_file)
    paths.append(path)

  paths = set(paths)
  
  for path in paths:
    if not(os.path.exists(path)):
      print(' creating ' + path)
      os.makedirs(path)

def threaded_download(input):
  
  if type(input) is str:
    f = open(input, 'rb')
    file_list = pickle.load(f)
    f.close()
  elif type(input) is list:
    file_list = input
  else:
    sys.exit('the input to threaded_download must be a pickle file or file list')
  n_files = len(file_list)
  if n_files == 0: return

  # create all of the paths serially to avoid race conditions
  check_paths(file_list)

  start_time = time.time()
  
  # create the worker threads
  print('   num_files = {:d}'.format(n_files))
  print(' num_threads = {:d}'.format(num_threads))
  threads = [Thread(target=curl_download) for _ in range(num_threads)]

  # add the files to the task queue
  [task_queue.put(item) for item in file_list]

  # start all the workers
  [thread.start() for thread in threads]

  # wait for all the tasks in the queue to be processed
  task_queue.join()

  end_time = time.time()

  dt = end_time-start_time
  print('       Total time = {:6.2f}s'.format(dt))
  print(' Files downloaded = {:d}'.format(n_files))
  print('    Time per file = {:6.2f}s'.format(dt/n_files))

  
