#!/usr/bin/python3

import csv
import json
import boto3
import configparser

def init_session():
	config = configparser.RawConfigParser()
	config.read('/home/ubuntu/.aws/credentials')
	keyid = config.get('default','aws_access_key_id')
	keysecret = config.get('default','aws_secret_access_key')
	session = \
		boto3.session.Session(aws_access_key_id=keyid,
							  aws_secret_access_key=keysecret, 
							  region_name='us-east-1')
	return session.resource('ec2')

def write_records():
	f = open('/home/ubuntu/WSL/mskaiagnt/output/enginelist.csv', "w")
	f.write("{},{},{}\n".format("mskengine01","64","10"))
	f.write("{},{},{}\n".format("ajaydlpx6pri.dcenter.delphix.com","64","10"))
	f.write("{},{},{}\n".format("ajaydlpx53pri.dcenter.delphix.com","64","10"))
	f.write("{},{},{}\n".format("ajayedsidlpx.dcenter.delphix.com","64","10"))
	f.write("{},{},{}\n".format("ajayedsidlpx.dlpxdc.co","64","10"))
	f.write("{},{},{}\n".format("rp6021m.dlpxdc.co","64","10"))
	f.write("{},{},{}\n".format("rp6030m.dlpxdc.co","64","10"))
	f.close()
	return

def write_dxtools_header_record():
	f = open(dxtools_csvfile, "w")
	f.write("{},{},{},{},{},{},{}\n".format('protocol','ip_address','password','port','username','default','hostname'))
	f.write("{},{},{},{},{},{},{}\n".format('http','mskengine01','delphix','80','admin','true','mskengine01'))
	f.write("{},{},{},{},{},{},{}\n".format('http','ajaydlpx6pri.dcenter.delphix.com','delphix','80','admin','true','ajaydlpx6pri.dcenter.delphix.com'))
	f.write("{},{},{},{},{},{},{}\n".format('http','ajaydlpx53pri.dcenter.delphix.com','delphix','80','admin','true','ajaydlpx53pri.dcenter.delphix.com'))
	f.write("{},{},{},{},{},{},{}\n".format('http','ajayedsidlpx.dcenter.delphix.com','delphix','80','admin','true','ajayedsidlpx.dcenter.delphix.com'))
	f.write("{},{},{},{},{},{},{}\n".format('http','ajayedsidlpx.dlpxdc.co','delphix','80','admin','true','ajayedsidlpx.dlpxdc.co'))
	f.write("{},{},{},{},{},{},{}\n".format('http','rp6021m.dlpxdc.co','delphix','80','admin','true','rp6021m.dlpxdc.co'))
	f.write("{},{},{},{},{},{},{}\n".format('http','rp6030m.dlpxdc.co','delphix','80','admin','true','rp6030m.dlpxdc.co'))
	f.close()
	return

def add_records_dxtools(engineip):
	f = open(dxtools_csvfile, "a")
	f.write("{},{},{},{},{},{},{}\n".format('http',engineip,'delphix','80','admin','true',engineip))
	f.close()
	return

def add_records(engineip):
	f = open('/home/ubuntu/WSL/mskaiagnt/output/enginelist.csv', "a")
	f.write("{},{},{}\n".format(engineip,"64","10"))
	f.close()
	return

dxtools_conffile = '/home/ubuntu/WSL/mskaiagnt/dxtools.conf'
dxtools_csvfile = '/home/ubuntu/WSL/mskaiagnt/output/dxtools.csv'
#write_records()
write_dxtools_header_record()

ec2 = init_session()
filters = [ { 'Name' : 'tag:' + 'ClassName', 'Values' : [ 'at3msk' ] } ]
for instance in ec2.instances.filter(Filters=filters):
	for tag in instance.tags:
		if tag['Key'] == 'Name' and tag['Value'] == 'me-sd60v2_1':
			add_records(instance.public_ip_address)
			add_records_dxtools(instance.public_ip_address)
			print ("Files updated with instance")


# Write conf file
csvfile = open(dxtools_csvfile, 'r')
reader = csv.DictReader(csvfile)
fieldnames = ('protocol','ip_address','password','port','username','default','hostname')
output = []
for each in reader:
	row = {}
	for field in fieldnames:
		row[field] = each[field]
	output.append(row)

outputdict = { "data" : output }
with open(dxtools_conffile, 'w') as outfile:
     json.dump(outputdict, outfile, sort_keys = True, indent = 4, ensure_ascii = False)
outfile.close()