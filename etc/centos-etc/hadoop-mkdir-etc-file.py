'''
Created on 2015-8-26

@author: xie
'''

import os,commands

if __name__ == '__main__':
    data_count = 0
    listfile = os.listdir('/data')
    for line in listfile: 
        if line.startswith('data'):
            data_count += 1
    commands.getoutput(' mkdir -p /home/hadoop/logs')
    commands.getoutput(' chown -R hadoop:hadoop /home/hadoop/logs')
    commands.getoutput(' mkdir -p /home/hadoop/pids')
    commands.getoutput(' chown -R hadoop:hadoop /home/hadoop/pids')
    for index in range(0,data_count):
        data_dir = '/data/data'+str(index+1)+'/hadoop/tmp'
        commands.getoutput(' mkdir -p '+data_dir)
        commands.getoutput(' mkdir -p '+data_dir+'/io/local')
        commands.getoutput(' mkdir -p '+data_dir+'/dfs/name')
        commands.getoutput(' mkdir -p '+data_dir+'/dfs/data')
        commands.getoutput(' mkdir -p '+data_dir+'/dfs/namesecondary')
        commands.getoutput(' mkdir -p '+data_dir+'/mapred/local')
        commands.getoutput(' mkdir -p '+data_dir+'/mapred/system')
        commands.getoutput(' mkdir -p '+data_dir+'/mapred/staging')
        commands.getoutput(' mkdir -p '+data_dir+'/mapred/temp')
        commands.getoutput(' mkdir -p '+data_dir+'/yarn/system/rmstore')
        commands.getoutput(' mkdir -p '+data_dir+'/nm-local-dir')
        commands.getoutput(' mkdir -p '+data_dir+'/logs')
        commands.getoutput(' mkdir -p /data/data'+str(index+1)+'/spark')
        commands.getoutput(' chown -R hadoop:hadoop /data/data'+str(index+1))
