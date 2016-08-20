#!/usr/bin/env python3

import pymysql
import os
import sys
import logging
import logging.config

##
# Configuration variables
##

# Remote
remote_host = '192.168.1.1'  # IP Address of remote host
remote_user = 'root'  # Remote username
source_path = '/source/'  # Source copy from
dest_path = '/dest/'  # Destination copy to

# Define log
logger = logging.getLogger('sync')
hdlr = logging.FileHandler('/tmp/sync.log')
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr)
logger.setLevel(logging.WARNING)

# Other
max_home_dir_size = 36 * 1024 * 1024 * 1024  # Max directory size, in bytes

##
# Functions
##

# Check pid exists
pid = str(os.getpid())
pidfile = "/tmp/sync.pid"

if os.path.isfile(pidfile):
    print('%s already exists, exiting' % pidfile)
    sys.exit()
else:
    with open(pidfile, 'w') as f:
        f.write(pid)

# Open database connection
MyHost = '192.168.1.2'
MyUser = 'dbadmin'
MyPass = '123456'
MyDB = 'test'
db = pymysql.connect(host=MyHost, user=MyUser, password=MyPass, db=MyDB)

# Prepare a cursor object using cursor() method
cursor = db.cursor()

# Get lastest id
sql_max_id = "SELECT max(id) from admin_content_belong_publisher"
cursor.execute(sql_max_id)
ids_max = cursor.fetchone()

for id_max in ids_max:
    pass

# Get olds media_id
# This will create a new file or overwrite an existing file.
try:
    with open('/root/sync/id_max.txt', 'w') as f:
        f.write(str(id_max))  # Write a string to a file
except IOError:
    print('File id_max.txt not found')


try:
    with open('/root/sync/id_old.txt') as f:
        id_old = int(f.read())
except IOError:
    print('File id_old.txt not found')

# Get media list
while id_old <= id_max:
    id_old = id_old + 1
    cursor.execute("SELECT file_path FROM admin_content_belong_publisher where id = %s", id_old)
    list_media = cursor.fetchall()
    for path_media in list_media:
        source_file_media = source_path + path_media[0]
        dest_file_media = dest_path + path_media[0]
        if not os.path.exists(dest_file_media):
            folder_media = os.path.dirname(dest_file_media)
            if os.path.exists(folder_media):
                print('Folder exist')
                os.system('scp "%s:%s" "%s"' % (remote_host, source_file_media, dest_file_media))
                logger.info(dest_file_media)
            else:
                os.makedirs(folder_media)
                os.system('scp "%s:%s" "%s"' % (remote_host, source_file_media, dest_file_media))
                logger.info(dest_file_media)
        else:
            print('File %s exist', dest_file_media)

# Update id_old from id_max
os.system('cat /root/sync/id_max.txt > /root/sync/id_old.txt')
db.close()

# Delete pid
os.system('rm -f "/tmp/sync.pid"')
print('Backup Completed')
