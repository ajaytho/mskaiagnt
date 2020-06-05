#!/usr/bin/python3

import sys
engineip=sys.argv[1]

def add_header_records():	
	f = open('/home/ubuntu/WSL/mskaiagnt/output/jobexeclist.csv', "w")
	f.write("{},{},{},{},{},{},{},{}\n".format("jobid","jobname","jobmaxmemory","reservememory","environmentid","environmentname","ip_address","jobstatus"))
	f.close()
	print("Added header record")
	return

def add_records(engineip):	
	for x in range(30, 40):
		f = open('/home/ubuntu/WSL/mskaiagnt/output/jobexeclist.csv', "a")
		f.write("{},{},{},{},{},{},{},{}\n".format(x,"mskjob{}".format(x),"5120","0","2","mskdevenv",engineip,"RUNNING"))
		f.close()
	print("10 jobs of 5GB each are mocked as ruuning on Engine {}".format(engineip))
	return

#print(len(sys.argv))
if len(sys.argv) == 3:
	add_header_records()
	add_records(engineip)
else:
	add_records(engineip)
