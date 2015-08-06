# -*- coding: utf-8 -*-
"""
Created on Wed Apr  8 14:22:26 2015

@author: prillard
"""

import numpy as np
from multiprocessing import Process
import os
import time
import commands
import app 
import linecache

def get_cluster(network_ip_regex, cluster_size_max=5):
    ipAdress = commands.getoutput("hostname -i")
    cluster = commands.getoutput("nmap -sn " + ipAdress + "/24 | awk \'{print $5}\' | grep -o " + network_ip_regex)
    cluster = np.array(cluster.split('\n')[:cluster_size_max])
    print 'cluster : ', len(cluster), cluster
    return cluster

def get_cluster_classrom(exclude, start, end, classrom, size_max=None):
    cluster = []
    for worker in range(start, end):
        if size_max != None and len(cluster) >= size_max:
            break
        if worker not in exclude:
            if worker < 10:
                cluster.append(str(classrom) + '-0' + str(worker) + '.enst.fr')
            else:
                cluster.append(str(classrom) + '-' + str(worker) + '.enst.fr')
    return cluster
    
def launch_script_worker(ip, script, args=None, other_args=None):
    args_str = ''
    other_args_str = ''
    if args!=None:
        args_str = ' ' + ' '.join(args)
    if other_args!=None:
        other_args_str 
        for arg in other_args:
            other_args_str += ' ' + str(arg) + '=' + str(other_args[arg])
    res = commands.getoutput('ssh ' + str(ip) + ' python ' + script + args_str + other_args_str)  

    print res
    return res
    
def get_cores(cluster, keep_core_alive=0):
	operational_cores = {}
	for ip in cluster:
		cores_nb = launch_script_worker(ip, app.script_get_cores)
		try:
			cores_nb = int(cores_nb)
			if (cores_nb > 0):
				operational_cores[ip] = cores_nb - keep_core_alive
		except:
			pass
	print 'operational_cores : ', operational_cores
	return operational_cores
    
def chunks(l, n):
    start = 0
    for i in xrange(0, n):
        if i < (len(l) % n):
            k = len(l)/n +1
        else:
            k = len(l)/n 
        yield l[start:start +k]
        start += k

def apply_cores_function(script, X_path, cluster, output_file, other_args=None, user_name='prillard'):
    # get cores cluster
    cores_cluster = get_cores(cluster)
    cores = 0
    for core in cores_cluster:
        cores += cores_cluster[core]
    print 'total cores : ', cores    
    
    # load file
    total_size = int(commands.getoutput('wc -l ' + X_path).split(' ')[0])
    chunks_list = np.array(list(chunks(range(0, total_size), cores)))
    print 'Chunks : ', chunks_list.shape 

    # check if the file is already splitted
    split = True
    try:
        cmd = 'ls ' + X_path + '_part* | wc -l'
        print cmd
        nb_part = int(commands.getoutput(cmd))
        if nb_part == cores:
            split = False
    except:
        pass
    tasks = []
    n = 0
    for ip in cores_cluster:
        for core in range(0, cores_cluster[ip]):
            core = core + n
            start = chunks_list[core][0]
            stop = chunks_list[core][-1] + 1
            # create chunk
            X_path_splited = X_path + '_part' + str(core) + '.csv'
            if split:
                chunk = np.array(commands.getoutput('sed -n ' + str(start+1) + ',' + str(stop+1) + 'p ' + X_path).split('\n'))       
                np.savetxt(X_path_splited, chunk, delimiter=";", fmt="%s")    
            # merge with scp
            commands.getoutput('scp ' + X_path_splited + ' ' + user_name + '@' + ip + ':' + X_path_splited)
            print 'ip : ', str(ip), ' [', str(start), ':', str(stop), ']'
            p = Process(target=launch_script_worker, args=(str(ip), str(script), [str(X_path_splited), str(0), str(stop-start), str(output_file + '_part' + str(core) + '.csv')], other_args))
            tasks.append(p)
        n += cores_cluster[ip]
    startTime = time.time()
    # start threads
    for task in tasks:
        task.start()
    # join threads
    for task in tasks:
        task.join()
    # merge result files
    merge_result_files(output_file, cores_cluster, user_name)
    #calculate the total time it took to complete the work
    print 'finish in ', str(time.time() - startTime), ' seconds'

    
def merge_result_files(output_file, cores_cluster, user_name='prillard'):
    # new file
    try:
        os.remove(output_file)
    except:
        pass
    fout=open(output_file, 'a')
    n = 0
    for ip in cores_cluster:
        for core in range(0, cores_cluster[ip]):
            core = core + n
            part_file = output_file + '_part' + str(core) + '.csv'
            # merge with scp
            commands.getoutput('scp ' + user_name + '@' + ip + ':' + part_file + ' ' + part_file)
            for line in open(part_file):
                 fout.write(line)
            # remove temp files
            os.remove(part_file)
        n += cores_cluster[ip]
    fout.close()
    print 'file available : ', output_file
    
def kill_all_process(cluster, user_name):
    for ip in cluster:
        cmd = 'ssh ' + str(ip) + ' pkill -U ' + str(user_name)
        commands.getoutput(cmd) 
        print 'ip : ', str(ip), ' process killed' 
        time.sleep(0.5)