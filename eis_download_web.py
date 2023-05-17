#!/usr/bin/env python

import sys
import os
import re
import time
import subprocess
import ssl

from urllib.request import urlopen
from datetime import datetime
from datetime import timedelta
from bs4 import BeautifulSoup
from threaded_download import threaded_download

class aia_download_web:
    def __init__(self):
        self.file_pattern = r'eis_\d{8}_\d{6}.(data|head)[.]h5'
        self.url_top = 'https://eis.nrl.navy.mil/level1/hdf5' # WHERE YOU ARE GETTING THE DATA
        self.local_top = 'local_data' # WHERE THE DATA WILL GO
        self.current_date_dir = ''
        self.current_hour_dir = ''
        self.current_remote_url = ''
        self.current_remote_filenames = []
        
    def date_range(self, date_start, date_end):
        t1 = datetime.strptime(date_start,"%d-%b-%Y")
        t2 = datetime.strptime(date_end,"%d-%b-%Y")
        dt = t2 - t1
        dates = []
        for t in range(dt.days+1):
            dates.append( t1 + timedelta(days=t) )
        return dates

    def date2dir(self, date):
        self.current_date_dir = date.strftime("%Y/%m/%d")

    def full_remote_url(self):
        self.current_remote_url = os.path.join(self.url_top, self.current_date_dir)

    def local_filename(self, filename):
        str = filename.replace('.fits','')
        wave_dir = (str.split('_'))[2]
        local_path = os.path.join(self.local_top, self.current_date_dir)
        local_path = os.path.join(local_path, wave_dir)
        local_file = os.path.join(local_path, filename)
        return local_path, local_file        

    def get_remote_filenames(self):
        print('reading '+self.current_remote_url)

        completed = False
        for i in range(6):
            try:
                context = ssl._create_unverified_context()
                response = urlopen(self.current_remote_url, context=context) 
                s = BeautifulSoup(response.read(), 'html.parser')
                completed = True
            except:
                print(' problem with query, waiting 10s')
                time.sleep(10)
        if not(completed):
            print(' query aborted, no files found, returning')
            self.current_remote_filenames = []
            return
        
        files = s.findAll('a')
        files = filter(lambda f: re.search(self.file_pattern, f.get('href')), files)
        files = list(files)
        if len(files) > 0:
            remote_filenames = []
            for f in files:
                t = ( f.get('href'), os.path.join(self.current_remote_url, f.get('href')) )
                remote_filenames.append(t)
            self.current_remote_filenames = remote_filenames
                
    def download_remote_files(self):
        for f in self.current_remote_filenames:
            local_path, local_file = self.local_filename(f[0])
            if not(os.path.isfile(local_file)):
                print('')
                print(' remote file -> '+f[1])
                print(' file does not exist locally -> '+local_file)
                if not(os.path.exists(local_path)):
                    print('  local path does not exist '+local_path)
                    os.makedirs(local_path)
                com  = 'curl --fail --max-time 120 -o '
                com  = com + local_file + ' ' + f[1]
                subprocess.call(com, shell=True)
            else:
                print(' file exists locally, not downloading '+local_file)

    def download_remote_files_threaded(self):
        print(' files to check : {:d}'.format(len(self.current_remote_filenames)))
        file_list = []
        for f in self.current_remote_filenames:
            local_path, local_file = self.local_filename(f[0])
            if not(os.path.isfile(local_file)):
                print('')
                print('remote file -> '+f[1])
                print('file does not exist locally -> '+local_file)
                if not(os.path.exists(local_path)):
                    print('local path does not exist '+local_path)
                    os.makedirs(local_path)
                file_list.append((local_file, f[1]))
            else:
                print('file exists locally, not downloading '+local_file)
        if len(file_list) > 0:
            threaded_download(file_list)

# --------------------------------------------------------------------------------------

if __name__ == '__main__':

    w = aia_download_web()

    if '--test' in sys.argv:
        # For testing
        t1 = datetime(2023, 4, 12)
        t2 = datetime(2023, 4, 12)
    else:
        # the usual way, get the last 30 days
        t2 = datetime.now()
        t1 = t2 - timedelta(days=30)

    dates = w.date_range(t1.strftime('%d-%b-%Y'), t2.strftime('%d-%b-%Y'))
    
    for d in dates:
        w.date2dir(d)
        w.full_remote_url()
        w.get_remote_filenames()
        w.download_remote_files_threaded()
