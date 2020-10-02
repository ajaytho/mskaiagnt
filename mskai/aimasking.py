import collections
import csv
import json
import os
import sys
import datetime
import pickle
from collections import Counter
from csv import DictReader
from sys import exit

import requests
from termcolor import colored, cprint

import mskai.globals as globals
from mskai.DxLogging import print_debug
from mskai.banner import banner

from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

class dotdict(dict):
    """dot.notation access to dictionary attributes"""
    __getattr__ = dict.get
    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__
#selfreg = { "envname" : "mskdevenv", "jobname" : "mskjob30" , "jobexeclistfile" : "/home/ubuntu/WSL/mskaiagnt/output/jobexeclist.csv"}
#self = dotdict(selfreg)
#print(self.envname)  
class aimasking():
    def __init__(self, config, **kwargs):
        self.scriptname = os.path.basename(__file__)
        self.scriptdir = getattr(sys, '_MEIPASS', os.path.dirname(os.path.abspath(__file__)))
        # self.scriptdir = os.path.dirname(os.path.abspath(__file__))
        self.enginelistfile = globals.enginelistfile
        self.joblistfile = globals.joblistfile
        self.jobexeclistfile = globals.jobexeclistfile
        self.qualifiedengineslistfile = globals.qualifiedengineslistfile
        self.enginecpulistfile = globals.enginecpulistfile
        self.config = config
        self.src_dummy_conn_app = "COMMON_OTF_MSKJOB_SRC_CONN_APP"
        self.src_dummy_conn_env = "COMMON_OTF_MSKJOB_SRC_CONN_ENV"

        #if not os.path.exists(self.enginelistfile):
        #    with open(self.enginelistfile, mode='a'): pass
        #if not os.path.exists(self.joblistfile):
        #    with open(self.joblistfile, mode='a'): pass
        #if not os.path.exists(self.jobexeclistfile):
        #    with open(self.jobexeclistfile, mode='a'): pass
        #if not os.path.exists(self.enginecpulistfile):
        #    with open(self.enginecpulistfile, mode='a'): pass

        if "mock" in kwargs.keys():
            self.mock = kwargs['mock']
        if "jobname" in kwargs.keys():
            self.jobname = kwargs['jobname']            
        if "envname" in kwargs.keys():
            self.envname = kwargs['envname']
        if "run" in kwargs.keys():
            self.run = kwargs['run']
        if "username" in kwargs.keys():
            self.username = kwargs['username']
        if "password" in kwargs.keys():
            self.password = kwargs['password']
        if "mskengname" in kwargs.keys():
            self.mskengname = kwargs['mskengname']
        if "totalgb" in kwargs.keys():
            self.totalgb = kwargs['totalgb']
        if "systemgb" in kwargs.keys():
            self.systemgb = kwargs['systemgb']
        if "srcmskengname" in kwargs.keys():
            self.srcmskengname = kwargs['srcmskengname']
        if "srcenvname" in kwargs.keys():
            self.srcenvname = kwargs['srcenvname']
        if "srcjobname" in kwargs.keys():
            self.srcjobname = kwargs['srcjobname']            
        if "tgtmskengname" in kwargs.keys():
            self.tgtmskengname = kwargs['tgtmskengname']
        if "tgtenvname" in kwargs.keys():
            self.tgtenvname = kwargs['tgtenvname']
        if "globalobjsync" in kwargs.keys():
            self.globalobjsync = kwargs['globalobjsync']
        if "protocol" in kwargs.keys():
            self.protocol = kwargs['protocol']
        else:
            self.protocol = "http"
        if "backup_dir" in kwargs.keys():
            self.backup_dir = kwargs['backup_dir']
        self.outputdir = os.path.join(self.scriptdir, 'output')
        self.outputfilename = 'output.txt'
        self.report_output = os.path.join(self.scriptdir, 'output', self.outputfilename)
        try:
            os.stat(self.outputdir)
        except:
            os.mkdir(self.outputdir)
            if self.config.debug:
                print_debug("Created directory {}".format(self.outputdir))
        
    def create_dictobj(self, filename):
        with open(filename, 'r') as read_obj:
            reader = DictReader(read_obj)
            dictobj = list(reader)
            return dictobj

    def unqlist(self, mydict, ignore_field):
        return [dict(data) for data in
                sorted(set(tuple((key, value) for key, value in row.items() if key != ignore_field) for row in mydict))]

    def gen_dxtools_csv_file(self, protocol="http"):
        if protocol == "https":
            port = 443
        else:
            port = 80
        f = open(globals.dxtools_file_csv, "w")
        f.write("{},{},{},{},{},{},{}\n".format('protocol','ip_address','password','port','username','default','hostname'))
        engine_list = self.create_dictobj(self.enginelistfile)
        for engine in engine_list:
            f.write("{},{},{},{},{},{},{}\n".format(protocol,engine['ip_address'],'delphix',port,'admin','true',engine['ip_address']))
        f.close()
        return

    def gen_dxtools_conf(self):
        protocol = self.protocol        
        # Write csv conf file
        self.gen_dxtools_csv_file(protocol)
        # Generate json conf file
        csvfile = open(globals.dxtools_file_csv, 'r')
        reader = csv.DictReader(csvfile)
        fieldnames = ('protocol','ip_address','password','port','username','default','hostname')
        output = []
        for each in reader:
            row = {}
            for field in fieldnames:
                row[field] = each[field]
            output.append(row)

        outputdict = { "data" : output }
        with open(globals.dxtools_file, 'w') as outfile:
            json.dump(outputdict, outfile, sort_keys = True, indent = 4, ensure_ascii = False)
        outfile.close()
        print("{} file generated successfully".format(globals.dxtools_file))

    def get_jobreqlist(self, mydictname, myjobname, myenvname):
        filtereddatafinal1 = filter(lambda row: (myjobname == row['jobname'] and myenvname == row['environmentname']),mydictname)
        filtereddataQ = filtereddatafinal1
        return list(filtereddataQ)

    def join_dict1(self, dict1, dict2, fieldname):
        answer = {}
        for item in dict2:
            answer[item[fieldname]] = item
        for item in dict1:
            key = item[fieldname]
            if key in answer.keys():
                del item[fieldname]
                answer[key].update(item)
        return answer.values()

    def join_dict(self, dict1, dict2, fieldname, emptyfield):
        mergedictlist = []
        emptymem = collections.OrderedDict([('totalusedmemory', '0')])
        emptycpu = collections.OrderedDict([('cpu', '0')])
        for item1 in dict1:
            key = item1[fieldname]
            i = 0
            for item2 in dict2:
                if fieldname in item2.keys():
                    if key == item2[fieldname]:
                        res = {**item1, **item2}
                        mergedictlist.append(res)
                        i = i + 1
            
            if i == 0:
                if emptyfield == 'cpu':
                    res = {**item1, **emptycpu}
                    mergedictlist.append(res)
                elif emptyfield == 'memcpu':
                    restmp = {**item1, **emptymem}
                    res = {**restmp, **emptycpu}
                    mergedictlist.append(res)
        return mergedictlist

    def get_unqualified_qualified_engines(self, dict1):
        qualified_engines = []
        unqualified_engines = []
        for item in dict1:
            if int(item['availablemb']) > 0:
                qualified_engines.append(item)
            else:
                unqualified_engines.append(item)
        #print(qualified_engines)
        #print(unqualified_engines)
        return qualified_engines, unqualified_engines

    def get_max_free_mem_engine(self, dict1):
        freemem = 0
        winner_engine = {}
        for item in dict1:
            if int(item['availablemb']) > freemem:
                winner_engine = item
                freemem = int(item['availablemb'])
        return winner_engine

    def group_job_mem_usage(self, key, sumcol, mydictname):
        try:
            aggregate_list = []
            c = Counter()
            for v in mydictname:
                if v['jobstatus'] == 'RUNNING':
                    c[v[key]] += int(v[sumcol])

            aggregate_list = [{key: key1, 'totalusedmemory': sumcol1} for key1, sumcol1 in c.items()]
            if aggregate_list is None:
                print_debug("Returned None for aggregate job usage data")
                return None
            elif aggregate_list == []:
                print_debug("Returned [] for aggregate job usage data")
                return None
            else:
                return aggregate_list
        except Exception as e:
            print_debug("ERROR : Unable to aggregate job usage data")
            print_debug(e)
            return None

    def convert_ordered_dict_to_dict(self, ordered_dict):
        simple_dict = {}
        for key, value in ordered_dict.items():
            if isinstance(value, collections.OrderedDict):
                simple_dict[key] = self.convert_dict_to_ordereddict(value)
            else:
                simple_dict[key] = value
        return simple_dict

    def convert_dict_to_ordereddict(self, mydict):
        ordered_dict = {}
        for key, value in mydict.items():
            if isinstance(value, dict):
                ordered_dict[key] = self.convert_ordered_dict_to_dict(value)
            else:
                ordered_dict[key] = value
        return ordered_dict

    def read_data_from_file(self, filename):
        rc = []
        with open(filename) as f:
            records = csv.DictReader(f)
            for row in records:
                rc.append(row)
        return rc

    def add_engine(self):
        #import pdb
        #pdb.set_trace()
        print_debug("self.enginelistfile = {}".format(self.enginelistfile))
        try:
            if os.path.exists(self.enginelistfile):
                engine_list = self.create_dictobj(self.enginelistfile)
                for engine in engine_list:
                    if self.mskengname == engine['ip_address']:
                        print("Engine {} already exists in pool".format(self.mskengname))
                        print("Please use upd-engine OR del-engine and add-engine module")
                        exit()
                f = open(self.enginelistfile, "a")
                f.write("{},{},{}\n".format(self.mskengname, self.totalgb, self.systemgb))
                f.close()
            else:
                #print("filename = {}".format(self.enginelistfile))
                f = open(self.enginelistfile, "w")
                f.write("{},{},{}\n".format("ip_address", "totalgb", "systemgb"))
                f.write("{},{},{}\n".format(self.mskengname, self.totalgb, self.systemgb))
                f.close()
            print("Engine {} successfully added to pool".format(self.mskengname))
        except Exception as e:
            print_debug(str(e))
            print_debug("Error adding engine {} to file {}".format(self.mskengname, self.enginelistfile))

    def list_engine(self):
        csvdir = self.outputdir
        try:
            if os.path.exists(self.enginelistfile):
                engine_list = self.create_dictobj(self.enginelistfile)
                print('{0:>1}{1:<35}{2:>20}{3:>20}'.format(" ", "EngineName", "Total Memory(GB)", "System Memory(GB)"))
                for engine in engine_list:
                    print('{0:>1}{1:<35}{2:>20}{3:>20}'.format(" ", engine['ip_address'], engine['totalgb'],
                                                               engine['systemgb']))
                print(" ")
            else:
                print("No Engine found in pool")
        except Exception as e:
            print_debug(str(e))
            print_debug("Not able to open file {}".format(self.enginelistfile))

    def del_engine(self):
        newenginelist = []
        try:
            i = 0
            if os.path.exists(self.enginelistfile):
                engine_list = self.create_dictobj(self.enginelistfile)
                for engine in engine_list:
                    if self.mskengname != engine['ip_address']:
                        newenginelist.append(engine)
                    else:
                        i = 1
                        print("Engine {} deleted from pool".format(self.mskengname))

                if i == 1:
                    f = open(self.enginelistfile, "w")
                    f.write("{},{},{}\n".format("ip_address", "totalgb", "systemgb"))
                    f.close()
                    f = open(self.enginelistfile, "a")
                    for engine in newenginelist:
                        f.write("{},{},{}\n".format(engine['ip_address'], engine['totalgb'], engine['systemgb']))
                    f.close()
                else:
                    print("Engine {} does not exists in pool".format(self.mskengname))
            else:
                print("File {} does not exists".format(self.enginelistfile))
        except Exception as e:
            print_debug(str(e))
            print_debug("Error deleting engine {} from file {}".format(self.mskengname, self.enginelistfile))

    def get_auth_key(self, ip_address, port=80):
        protocol = self.protocol        
        if protocol == "https":
            port = 443
        api_url_base = '{}://{}:{}/masking/api/'.format(protocol, ip_address, port)
        headers = {'Content-Type': 'application/json'}
        api_url = '{0}login'.format(api_url_base)
        print_debug("api_url = {}".format(api_url))
        credentials = {"username": self.username, "password": self.password}
        # print_debug('{},{},{},{},{},{}'.format(ip_address,port,api_url_base,headers,api_url,credentials))
        try:
            response = requests.post(api_url, headers=headers, json=credentials, verify=False)
            if response.status_code == 200:
                data = json.loads(response.content.decode('utf-8'))
                # print_debug (data['Authorization'])
                return data['Authorization']
            else:
                print_debug("Error generating key {}".format(ip_address))
                return None
        except:
            print_debug("Error connecting engine {}".format(ip_address))
            return None

    def get_api_response(self, ip_address, api_token, apicall, port=80):
        protocol = self.protocol
        if protocol == "https":
            port = 443
        api_url_base = '{}://{}:{}/masking/api/'.format(protocol,ip_address, port)
        headers = {'Content-Type': 'application/json', 'Authorization': '{0}'.format(api_token)}
        api_url = '{0}{1}'.format(api_url_base, apicall)
        response = requests.get(api_url, headers=headers, verify=False)
        if response.status_code == 200:
            data = json.loads(response.content.decode('utf-8'))
            return data
        else:
            print_debug(response.content.decode('utf-8'))
            return None

    def del_api_response(self, ip_address, api_token, apicall, port=80):
        protocol = self.protocol
        if protocol == "https":
            port = 443
        api_url_base = '{}://{}:{}/masking/api/'.format(protocol,ip_address, port)
        headers = {'Content-Type': 'application/json', 'Authorization': '{0}'.format(api_token)}
        api_url = '{0}{1}'.format(api_url_base, apicall)
        response = requests.delete(api_url, headers=headers, verify=False)
        if response.status_code == 200:
            data = response.content.decode('utf-8')
            return data
        else:
            print_debug(response.content.decode('utf-8'))
            print(response.content.decode('utf-8'))
            return None

    def post_api_response(self, ip_address, api_token, apicall, body, port=80):
        protocol = self.protocol
        if protocol == "https":
            port = 443
        api_url_base = '{}://{}:{}/masking/api/'.format(protocol,ip_address, port)
        headers = {'Content-Type': 'application/json', 'Authorization': '{0}'.format(api_token)}
        api_url = '{0}{1}'.format(api_url_base, apicall)
        response = requests.post(api_url, headers=headers, json=body, verify=False)
        #print(response)
        #data = json.loads(response.content.decode('utf-8'))
        #print(data)
        #print("=====")
        if response.status_code == 200:
            data = json.loads(response.content.decode('utf-8'))
            return data
        else:
            print_debug(response.content.decode('utf-8'))
            return None

    def post_api_response1(self, ip_address, api_token, apicall, body, port=80):
        protocol = self.protocol
        if protocol == "https":
            port = 443
        api_url_base = '{}://{}:{}/masking/api/'.format(protocol, ip_address, port)
        
        headers = {'Content-Type': 'application/json', 'Accept': 'application/json', 'Authorization': '{0}'.format(api_token)}
        api_url = '{0}{1}'.format(api_url_base, apicall)
        print_debug("api_url: {}".format(api_url))
        response = requests.post(api_url, headers=headers, json=body, verify=False)
        #print(response)
        #data = json.loads(response.content.decode('utf-8'))
        if response.status_code == 200:
            data = json.loads(response.content.decode('utf-8'))
            return data
        elif response.status_code == 409:
            data = json.loads(response.content.decode('utf-8'))
            return data            
        else:
            print(" {}".format(response.content.decode('utf-8')))
            return None

    def put_api_response(self, ip_address, api_token, apicall, body, port=80):
        protocol = self.protocol
        if protocol == "https":
            port = 443
        api_url_base = '{}://{}:{}/masking/api/'.format(protocol, ip_address, port)
        
        headers = {'Content-Type': 'application/json', 'Accept': 'application/json', 'Authorization': '{0}'.format(api_token)}
        api_url = '{0}{1}'.format(api_url_base, apicall)
        print_debug("api_url: {}".format(api_url))
        response = requests.put(api_url, headers=headers, json=body, verify=False)
        #print(response)
        #data = json.loads(response.content.decode('utf-8'))
        if response.status_code == 200:
            data = json.loads(response.content.decode('utf-8'))
            return data
        elif response.status_code == 409:
            data = json.loads(response.content.decode('utf-8'))
            return data            
        else:
            print(" {}".format(response.content.decode('utf-8')))
            return None

    def exec_job(self, ip_address, api_token, jobid):
        jobpayload = {"jobId": jobid}
        data = self.post_api_response(ip_address, api_token, 'executions', jobpayload)
        return data

    def chk_job_running(self):
        envname = self.envname
        jobname = self.jobname
        filepath = self.jobexeclistfile
        reqjobspec = "{},{}".format(envname,jobname)
        r = 0    
        with open(filepath) as fp:
            line = fp.readline()
            cnt = 1
            while line:
                print_debug("Line {}: {}".format(cnt, line.strip()))
                env_job = []
                env_job = line.strip().split(",")
                filejobspec = "{},{}".format(env_job[5],env_job[1])
                if filejobspec == reqjobspec:
                    r = 1
                    return r
                    break
                line = fp.readline()
                cnt += 1
        return r

    def add_debugspace(self):
        print_debug(" ")
        print_debug(" ")

    # @track
    def run_job(self):
        if self.config.debug:
            print_debug("Parameter List:")
            print_debug("  jobname = {}".format(self.jobname))
            print_debug("  envname = {}".format(self.envname))
            print_debug("  run     = {}".format(self.run))
        # print_debug("  password= {}".format(self.password))
        # on windows
        # os.system('color')
        
        if not self.mock:
            # Run this if its not mock run for demos
            self.pull_jobexeclist()
        engine_list = self.create_dictobj(self.enginelistfile)
        job_list = self.create_dictobj(self.joblistfile)
        jobexec_list = self.create_dictobj(self.jobexeclistfile)
        enginecpu_list = self.create_dictobj(self.enginecpulistfile)

        self.add_debugspace()
        print_debug("enginecpu_list:{}".format(enginecpu_list))
        self.add_debugspace()

        engine_list = self.create_dictobj(self.enginelistfile)
        print_debug("engine_list:\n{}".format(engine_list))

        enginelist = []
        for engine in engine_list:
            engine_list_dict = collections.OrderedDict(ip_address=engine['ip_address'],
                                                       totalmb=int(engine['totalgb']) * 1024,
                                                       systemmb=int(engine['systemgb']) * 1024)
            enginelist.append(engine_list_dict)
        print_debug("engine_list:\n{}".format(engine_list))
        print_debug("enginelist:\n{}".format(enginelist))
        engine_list = enginelist

        joblistunq = self.unqlist(job_list, 'ip_address')
        print_debug("joblistunq:{}".format(joblistunq))
        jobreqlist = self.get_jobreqlist(joblistunq, self.jobname, self.envname)
        print_debug("jobreqlist:{}".format(jobreqlist))
        if len(jobreqlist) == 0:
            print_red_on_white = lambda x: cprint(x, 'red', 'on_white')            
            print_red_on_white('Job : {} in Environment: {} does not exists on any masking server. Please recheck job name / environment and resubmit.'.format(self.jobname, self.envname))
            sys.exit(1)
        engine_pool_for_job = self.get_jobreqlist(job_list, self.jobname, self.envname)
        print_debug("engine_pool_for_job:\n{}\n".format(engine_pool_for_job))
        for job in engine_pool_for_job:
            print_debug(job)

        bannertext = banner()
        print(" ")
        print((colored(bannertext.banner_sl_box(text="Requirements:"), 'yellow')))
        print(' Jobname   = {}'.format(self.jobname))
        print(' Env       = {}'.format(self.envname))
        print(' MaxMB     = {} MB'.format(jobreqlist[0]['jobmaxmemory']))
        print(' ReserveMB = {} MB'.format(jobreqlist[0]['reservememory']))
        print(' Total     = {} MB'.format(int(jobreqlist[0]['jobmaxmemory']) + int(jobreqlist[0]['reservememory'])))

        if self.config.verbose or self.config.debug:
            print((colored(bannertext.banner_sl_box(text="Available Engine Pool:"), 'yellow')))
            print('{0:>1}{1:<35}{2:>20}{3:>20}'.format("", "Engine Name", "Total Memory(MB)", "System Memory(MB)"))
            for ind in engine_list:
                print('{0:>1}{1:<35}{2:>20}{3:>20}'.format(" ", ind['ip_address'], ind['totalmb'], ind['systemmb']))

        if self.config.verbose or self.config.debug:
            print((colored(bannertext.banner_sl_box(text="CPU Usage:"), 'yellow')))
            print('{0:>1}{1:<35}{2:>20}'.format("", "Engine Name", "Used CPU(%)"))
            for ind in enginecpu_list:
                print('{0:>1}{1:<35}{2:>20}'.format(" ", ind['ip_address'], ind['cpu']))

        print_debug('jobexec_list = \n{}'.format(jobexec_list))
        engineusage = self.group_job_mem_usage('ip_address', 'jobmaxmemory', jobexec_list)
        print_debug('engineusage = \n{}'.format(engineusage))
        if engineusage is None:
            print_debug("Creating empty list.")
            engineusage_od = []
            temporddict = {}
            for ind in engine_list:
                temporddict = collections.OrderedDict(ip_address=ind['ip_address'], totalusedmemory=0)
                engineusage_od.append(temporddict)
            print_debug(engineusage_od)
        else:
            engineusage_od = []
            temporddict = {}
            for row in engineusage:
                engineusage_od.append(collections.OrderedDict(row))

            # Add empty list for remaining engines [ not in jobexeclist ]
            print_debug('engine_list = \n{}'.format(engine_list))
            for ind in engine_list:
                i = 0
                for ind1 in engineusage:
                    if ind['ip_address'] == ind1['ip_address']:
                        i = 1
                if i == 0:
                    temporddict = collections.OrderedDict(ip_address=ind['ip_address'], totalusedmemory=0)
                    engineusage_od.append(temporddict)                

        print_debug('engineusage_od = \n{}'.format(engineusage_od))

        if self.config.verbose or self.config.debug:
            print((colored(bannertext.banner_sl_box(text="Memory Usage:"), 'yellow')))
            print('{0:>1}{1:<35}{2:>20}'.format("", "Engine Name", "Used Memory(MB)"))
            for ind in engineusage_od:
                print('{0:>1}{1:<35}{2:>20}'.format(" ", ind['ip_address'], ind['totalusedmemory']))

        if self.config.verbose or self.config.debug:
            print((colored(bannertext.banner_sl_box(text="Engine Current Usage:"), 'yellow')))
            print('{0:>1}{1:<35}{2:>20}{3:>20}'.format("", "Engine Name", "Used Memory(MB)", "Used CPU(%)"))

        if len(enginecpu_list) != 0:
            engineusage = self.join_dict(engineusage_od, enginecpu_list, 'ip_address', 'cpu')
            self.add_debugspace()
            print_debug("engineusage:{}".format(engineusage))
            self.add_debugspace()
            if self.config.verbose or self.config.debug:
                for ind in engineusage:
                    print('{0:>1}{1:<35}{2:>20}{3:>20}'.format(" ", ind['ip_address'], ind['totalusedmemory'],
                                                               ind['cpu']))
        else:
            print("Handle this situation")


        self.add_debugspace()
        print_debug("enginecpu_list:{}".format(enginecpu_list))
        self.add_debugspace()
        print_debug('engineusage_od = \n{}\n'.format(engineusage_od))
        print_debug('enginecpu_list = \n{}\n'.format(enginecpu_list))
        print_debug('engineusage = \n{}\n'.format(engineusage))

        if self.config.verbose or self.config.debug:
            print((colored(bannertext.banner_sl_box(text="Shortlisted Engines for running Job:"), 'yellow')))
            print('{0:>1}{1:<35}{2:>20}{3:>20}'.format("", "Engine Name", "Job ID", "Env Name"))

        if self.config.verbose or self.config.debug:
            for row in engine_pool_for_job:
                print(
                    '{0:>1}{1:<35}{2:>20}{3:>20}'.format(" ", row['ip_address'], row['jobid'], row['environmentname']))

        jpd1 = self.join_dict(engine_pool_for_job, engine_list, 'ip_address', 'dummy')
        print_debug('jpd1 = \n{}\n'.format(jpd1))
        jpd2 = self.join_dict(jpd1, engineusage, 'ip_address', 'totalusedmemory')
        #jpd2 = self.join_dict(jpd1, engineusage, 'ip_address', 'dummy')
        print_debug('jpd2 = \n{}\n'.format(jpd2))

        tempjpd = []
        for jpd in jpd2:
            availablemb = int(jpd['totalmb']) - int(jpd['systemmb']) - int(jpd['totalusedmemory']) - int(
                jobreqlist[0]['jobmaxmemory']) - int(jobreqlist[0]['reservememory'])
            jpd['availablemb'] = availablemb
            tempjpd.append(jpd)

        jpd2 = tempjpd
        print_debug(jpd2)

        qualified_engines, unqualified_engines = self.get_unqualified_qualified_engines(jpd2)
        print_debug('qualified_engines = \n{}\n'.format(qualified_engines))
        print_debug('unqualified_engines = \n{}\n'.format(unqualified_engines))

        if len(qualified_engines) == 0:
            redcandidate = []
            for item in unqualified_engines:
                item.update({"maxavailablememory": (
                            float(item['availablemb']) + float(jobreqlist[0]['jobmaxmemory']) + float(
                        jobreqlist[0]['reservememory']))})
                redcandidate.append(item)

            if self.config.verbose or self.config.debug:
                print((colored(bannertext.banner_sl_box(text="Red Engines:"), 'yellow')))
                print('{0:>1}{1:<35}{2:>20}{3:>20}'.format("", "Engine Name", "Available Memory(MB)", "Used CPU(%)"))
                for ind in redcandidate:
                    print(colored('{0:>1}{1:<35}{2:>20}{3:>20}'.format(" ", ind['ip_address'],
                                                                       round(int(ind['maxavailablememory'])),
                                                                       ind['cpu']), 'red'))
            print(" ")
            print(" All engines are busy. Running job {} of environment {} may cause issues.".format(self.jobname,
                                                                                                       self.envname))
            print(" Existing jobs may complete after sometime and create additional capacity to execute new job.")
            print(" Please retry later.")
            print(" ")
            print("",colored("Recommendation: 1",color='green',attrs=['reverse', 'blink', 'bold']))
            print(" Please retry later.")
            print(" ")
            print("",colored("Recommendation: 2",color='green',attrs=['reverse', 'blink', 'bold']))
            print(" Add job to following engines using sync_eng/sync_env/sync_job module")
            print(" python ./mskaiagnt.py sync-eng --srcmskengname <SRC_IP> --tgtmskengname <TGT_IP> -g --username admin --password xxxxxx")
            print(" OR")
            print(" python ./mskaiagnt.py sync-env --srcmskengname <SRC_IP> --srcenvname {} --tgtmskengname <TGT_IP> --tgtenvname {} -g --username admin --password xxxxxx".format(self.envname,self.envname))
            print(" ")
            print(" Job can be added to following engines")
            idx = 0
            for engine in engine_list:
                i = 0
                for red in redcandidate:
                    if engine['ip_address'] == red['ip_address']:
                        i = 1
                if i == 0:
                    idx = idx + 1
                    print(" {}) {}".format(idx,engine['ip_address']))
            print(" ")
        else:
            redcandidate = []
            for item in unqualified_engines:
                item.update({"maxavailablememory": (
                            float(item['availablemb']) + float(jobreqlist[0]['jobmaxmemory']) + float(
                        jobreqlist[0]['reservememory']))})
                redcandidate.append(item)

            if self.config.verbose or self.config.debug:
                print((colored(bannertext.banner_sl_box(text="Red Engines:"), 'yellow')))
                print('{0:>1}{1:<35}{2:>20}{3:>20}'.format("", "Engine Name", "Available Memory(MB)", "Used CPU(%)"))
                for ind in redcandidate:
                    print(colored('{0:>1}{1:<35}{2:>20}{3:>20}'.format(" ", ind['ip_address'],
                                                                       round(int(ind['maxavailablememory'])),
                                                                       ind['cpu']), 'red'))

            bestcandidatedetails = []
            for item in qualified_engines:
                item.update({"maxavailablememory": (
                            float(item['availablemb']) + float(jobreqlist[0]['jobmaxmemory']) + float(
                        jobreqlist[0]['reservememory']))})
                bestcandidatedetails.append(item)
            #print(qualified_engines)
            #print(bestcandidatedetails)
            if self.config.verbose or self.config.debug:
                print((colored(bannertext.banner_sl_box(text="Green Engines:"), 'yellow')))
                print('{0:>1}{1:<35}{2:>20}{3:>20}'.format("", "Engine Name", "Available Memory(MB)", "Used CPU(%)"))
                for ind in bestcandidatedetails:
                    print(colored('{0:>1}{1:<35}{2:>20}{3:>20}'.format(" ", ind['ip_address'],
                                                                       round(int(ind['maxavailablememory'])),
                                                                       ind['cpu']), 'green'))

            print((colored(bannertext.banner_sl_box(text="Best Candidate:"), 'yellow')))
            print(" ")
            winner_engine = self.get_max_free_mem_engine(bestcandidatedetails)
            engine_name = winner_engine['ip_address']
            engine_mem = winner_engine['maxavailablememory']
            engine_cpu = winner_engine['cpu']
            print(colored(" Engine : {} , Available Memory : {} MB ,  Available CPU : {}% ".format(engine_name, engine_mem,
                            (100 - float(engine_cpu))), color='green',attrs=['reverse', 'blink', 'bold']))

            if self.run:
                apikey = self.get_auth_key(engine_name)
                # print(apikey)
                jobid = self.find_job_id(self.jobname, self.envname, engine_name)
                chk_status = self.chk_job_running()
                if chk_status == 0:
                    job_exec_response = self.exec_job(engine_name, apikey, jobid)
                    if job_exec_response is not None:
                        if job_exec_response['status'] == 'RUNNING':
                            executionId = job_exec_response['executionId']
                            print_green_on_white = lambda x: cprint(x, 'blue', 'on_white')
                            print_green_on_white(
                                " Execution of Masking job# {} with execution ID {} on Engine {} is in progress".format(
                                    jobid, executionId, engine_name))
                        else:
                            print_red_on_white = lambda x: cprint(x, 'red', 'on_white')
                            print_red_on_white(
                                " Execution of Masking job# {} on Engine {} failed".format(jobid, engine_name))
                    else:
                        print_red_on_white = lambda x: cprint(x, 'red', 'on_white')
                        print_red_on_white(
                            " Execution of Masking job# {} on Engine {} failed".format(jobid, engine_name))
                else:
                    print_red_on_white(
                            " Job {} on Env {} is already running on engine {}. Please retry later".format(self.jobname, self.envname, chk_status))
            print(" ")

    def pull_joblist(self):
        if self.mskengname == 'all':

            try:
                if os.path.exists(self.joblistfile):
                    os.remove(self.joblistfile)
                    f = open(self.joblistfile, "w")
                    f.write("{},{},{},{},{},{},{}\n".format("jobid", "jobname", "jobmaxmemory", "reservememory",
                                                            "environmentid", "environmentname", "ip_address"))
                    f.close()
                else:
                    f = open(self.joblistfile, "w")
                    f.write("{},{},{},{},{},{},{}\n".format("jobid", "jobname", "jobmaxmemory", "reservememory",
                                                            "environmentid", "environmentname", "ip_address"))
                    f.close()
            except:
                print_debug("Error deleting file ", self.joblistfile)

            engine_list = self.create_dictobj(self.enginelistfile)
            for engine in engine_list:
                engine_name = engine['ip_address']
                apikey = self.get_auth_key(engine_name)
                #print("apikey:{}".format(apikey))
                if apikey is not None:
                    apicall = "environments?page_number=1&page_size=999"
                    envlist_response = self.get_api_response(engine_name, apikey, apicall)

                    f = open(self.joblistfile, "a")

                    for envname in envlist_response['responseList']:
                        jobapicall = "masking-jobs?page_number=1&page_size=999&environment_id={}".format(envname['environmentId'])
                        joblist_response = self.get_api_response(engine_name, apikey, jobapicall)
                        joblist_responselist = joblist_response['responseList']
                        for joblist in joblist_responselist:
                            f.write("{},{},{},{},{},{},{}\n".format(joblist['maskingJobId'], joblist['jobName'],
                                                                    joblist['maxMemory'], '0', envname['environmentId'],
                                                                    envname['environmentName'], engine_name))
                    f.close()
                    print("File {} successfully updated with jobs from {}".format(self.joblistfile, engine_name))

        else:
            # Delete existing jobs for particular engine
            newjoblist = []
            try:
                i = 0
                if os.path.exists(self.joblistfile):
                    job_list = self.create_dictobj(self.joblistfile)
                    for job in job_list:
                        if self.mskengname != job['ip_address']:
                            newjoblist.append(job)
                        else:
                            i = 1
                            print("Existing Job {} deleted for engine {}".format(job['jobname'], self.mskengname))

                    if i == 1:

                        try:
                            if os.path.exists(self.joblistfile):
                                os.remove(self.joblistfile)
                                f = open(self.joblistfile, "w")
                                f.write(
                                    "{},{},{},{},{},{},{}\n".format("jobid", "jobname", "jobmaxmemory", "reservememory",
                                                                    "environmentid", "environmentname", "ip_address"))
                                f.close()
                        except:
                            print_debug("Error deleting file ", self.joblistfile)

                        f = open(self.joblistfile, "a")
                        for job in newjoblist:
                            f.write("{},{},{},{},{},{},{}\n".format(job['jobid'], job['jobname'], job['jobmaxmemory'],
                                                                    job['reservememory'], job['environmentid'],
                                                                    job['environmentname'], job['ip_address']))
                        f.close()
                    else:
                        print("No existing jobs found for Engine {} in pool".format(self.mskengname))
                else:
                    print("File {} does not exists. Creating it".format(self.joblistfile))
                    f = open(self.joblistfile, "w")
                    f.write("{},{},{},{},{},{},{}\n".format("jobid", "jobname", "jobmaxmemory", "reservememory",
                                                            "environmentid", "environmentname", "ip_address"))
                    f.close()
            except Exception as e:
                print_debug(str(e))
                print_debug("Error deleting jobs for engine {} in file {}".format(self.mskengname, self.joblistfile))

            # Pull New List
            engine_name = self.mskengname
            apikey = self.get_auth_key(engine_name)
            if apikey is not None:
                apicall = "environments?page_number=1&page_size=999"
                envlist_response = self.get_api_response(engine_name, apikey, apicall)
                f = open(self.joblistfile, "a")
                for envname in envlist_response['responseList']:
                    jobapicall = "masking-jobs?page_number=1&page_size=999&environment_id={}".format(envname['environmentId'])
                    joblist_response = self.get_api_response(engine_name, apikey, jobapicall)
                    joblist_responselist = joblist_response['responseList']
                    for joblist in joblist_responselist:
                        f.write("{},{},{},{},{},{},{}\n".format(joblist['maskingJobId'], joblist['jobName'],
                                                                joblist['maxMemory'], '0', envname['environmentId'],
                                                                envname['environmentName'], engine_name))
                f.close()
                print("Job list for engine {} successfully generated in file {}".format(self.mskengname,
                                                                                        self.joblistfile))

    def pull_jobexeclist(self):

        try:
            if os.path.exists(self.jobexeclistfile):
                os.remove(self.jobexeclistfile)
                fe = open(self.jobexeclistfile, "w")
                fe.write("{},{},{},{},{},{},{},{}\n".format("jobid", "jobname", "jobmaxmemory", "reservememory",
                                                            "environmentid", "environmentname", "ip_address",
                                                            "jobstatus"))
                fe.close()
            else:
                fe = open(self.jobexeclistfile, "w")
                fe.write("{},{},{},{},{},{},{},{}\n".format("jobid", "jobname", "jobmaxmemory", "reservememory",
                                                            "environmentid", "environmentname", "ip_address",
                                                            "jobstatus"))
                fe.close()
        except:
            print_debug("Error while deleting file ", self.jobexeclistfile)

        engine_list = self.create_dictobj(self.enginelistfile)
        for engine in engine_list:
            print_debug("Engine : {}".format(engine))
            engine_name = engine['ip_address']
            apikey = self.get_auth_key(engine_name)
            print_debug("apikey : {}".format(apikey))
            if apikey is not None:
                apicall = "environments?page_number=1&page_size=999"
                envlist_response = self.get_api_response(engine_name, apikey, apicall)
                for envname in envlist_response['responseList']:
                    print_debug("envname : {}".format(envname))
                    jobapicall = "masking-jobs?page_number=1&page_size=999&environment_id={}".format(envname['environmentId'])
                    joblist_response = self.get_api_response(engine_name, apikey, jobapicall)
                    joblist_responselist = joblist_response['responseList']
                    for joblist in joblist_responselist:
                        print_debug("joblist : {}".format(joblist))
                        fe = open(self.jobexeclistfile, "a")
                        jobexecapicall = "executions?job_id={}&page_number=1&page_size=999".format(joblist['maskingJobId'])
                        jobexeclist_response = self.get_api_response(engine_name, apikey, jobexecapicall)
                        jobexeclist_responselist = jobexeclist_response['responseList']
                        if jobexeclist_responselist != []:
                            latestexecid = max(jobexeclist_responselist, key=lambda ev: ev['executionId'])
                            print_debug("latestexecid-status = {}".format(latestexecid['status']))
                            if latestexecid['status'] == "RUNNING":
                                fe.write("{},{},{},{},{},{},{},{}\n".format(joblist['maskingJobId'], joblist['jobName'],
                                                                            joblist['maxMemory'], '0',
                                                                            envname['environmentId'],
                                                                            envname['environmentName'], 
                                                                            engine_name,
                                                                            latestexecid['status']))
                        fe.close()
        print_debug("File {} successfully generated".format(self.jobexeclistfile))

    def sync_globalobj(self):
        self.sync_syncable_objects("GLOBAL_OBJECT")
        self.sync_syncable_objects("FILE_FORMAT")
        self.sync_syncable_objects("MOUNT_INFORMATION")      

    def sync_globalfileformats(self):
        src_engine_name = self.srcmskengname
        tgt_engine_name = self.tgtmskengname
        globalfileformats = True
        i = None
        srcapikey = self.get_auth_key(src_engine_name)
        if srcapikey is not None:
            if globalfileformats:
                syncobjapicall = "syncable-objects?page_number=1&page_size=999&object_type=FILE_FORMAT"
                syncobjapicallresponse = self.get_api_response(src_engine_name, srcapikey, syncobjapicall)
                for globalfileobj in syncobjapicallresponse['responseList']:
                    i = 1
                    globalfileobjdef = []
                    globalfileobjdef.append(globalfileobj)                                        
                    srcapicall = "export"
                    srcapiresponse = self.post_api_response1(src_engine_name, srcapikey, srcapicall, globalfileobjdef, port=80)
                    
                    tgtapikey = self.get_auth_key(tgt_engine_name)
                    tgtapicall = "import?force_overwrite=true"
                    tgtapiresponse = self.post_api_response1(tgt_engine_name, tgtapikey, tgtapicall, srcapiresponse, port=80)
                    if tgtapiresponse is None:
                        print(" File Format synced failed.")
                    else:
                        print(" File Format synced successfully.")
                if i == 1:
                    print(" ")                   
        else:
            print(" Error connecting source engine {}".format(src_engine_name))

    def sync_globalmountfs(self):
        src_engine_name = self.srcmskengname
        tgt_engine_name = self.tgtmskengname
        globalmountfs = True
        i = None
        srcapikey = self.get_auth_key(src_engine_name)
        if srcapikey is not None:
            if globalmountfs:
                syncobjapicall = "syncable-objects?page_number=1&page_size=999&object_type=MOUNT_INFORMATION"
                syncobjapicallresponse = self.get_api_response(src_engine_name, srcapikey, syncobjapicall)
                for globalmountfs in syncobjapicallresponse['responseList']:
                    i = 1
                    globalmountfsdef = []
                    globalmountfsdef.append(globalmountfs)                                        
                    srcapicall = "export"
                    srcapiresponse = self.post_api_response1(src_engine_name, srcapikey, srcapicall, globalmountfsdef, port=80)
                    
                    tgtapikey = self.get_auth_key(tgt_engine_name)
                    tgtapicall = "import?force_overwrite=true"
                    tgtapiresponse = self.post_api_response1(tgt_engine_name, tgtapikey, tgtapicall, srcapiresponse, port=80)
                    if tgtapiresponse is None:
                        print(" Mount FS synced failed.")
                    else:
                        print(" Mount FS synced successfully.")
                if i == 1:
                    print(" ")                     
        else:
            print(" Error connecting source engine {}".format(src_engine_name))

    def sync_syncable_objects(self, syncable_object_type):
        src_engine_name = self.srcmskengname
        tgt_engine_name = self.tgtmskengname
        i = None
        srcapikey = self.get_auth_key(src_engine_name)
        if srcapikey is not None:
            syncobjapicall = "syncable-objects?page_number=1&page_size=999&object_type={}".format(syncable_object_type)
            syncobjapicallresponse = self.get_api_response(src_engine_name, srcapikey, syncobjapicall)
            for syncable_object_type_elem in syncobjapicallresponse['responseList']:
                i = 1
                syncable_object_type_def = []
                syncable_object_type_def.append(syncable_object_type_elem)                                        
                srcapicall = "export"
                srcapiresponse = self.post_api_response1(src_engine_name, srcapikey, srcapicall, syncable_object_type_def, port=80)
                
                tgtapikey = self.get_auth_key(tgt_engine_name)
                tgtapicall = "import?force_overwrite=true"
                tgtapiresponse = self.post_api_response1(tgt_engine_name, tgtapikey, tgtapicall, srcapiresponse, port=80)
                if tgtapiresponse is None:
                    print(" Syncable Object {} sync failed.".format(syncable_object_type))
                else:
                    print(" Syncable Object {} synced successfully.".format(syncable_object_type))
            if i == 1:
                print(" ")                  
        else:
            print(" Error connecting source engine {}".format(src_engine_name))

    def process_sync_job(self,src_engine_name,tgt_engine_name,globalobjsync,src_env_name,tgt_env_name,jobname):

        srcapikey = self.get_auth_key(src_engine_name)
        print_debug("srcapikey={}".format(srcapikey))

        tgtapikey = self.get_auth_key(tgt_engine_name)
        print_debug("tgtapikey={}".format(tgtapikey))

        if srcapikey is not None and tgtapikey is not None:
            src_job_id = self.find_job_id(jobname, src_env_name, src_engine_name)

            if globalobjsync:
                self.sync_globalobj()                 
     
            # Create dummy app to handle on the fly masking job/env
            cr_app_response = self.create_application(tgt_engine_name, self.src_dummy_conn_app)
            src_dummy_conn_app_id = cr_app_response['applicationId']

            # Create dummy env to handle on the fly masking job/env
            cr_env_response = self.create_environment(tgt_engine_name, src_dummy_conn_app_id, self.src_dummy_conn_env, "MASK")
            src_dummy_conn_env_id = cr_env_response['environmentId']

            print_debug("Source Env name = {}, Source Env purpose = {}, Source App name = {}, Source Env Id = {}, Source App Id = {}".format(self.src_dummy_conn_env, "MASK", self.src_dummy_conn_app,src_dummy_conn_env_id,src_dummy_conn_app_id))
            print(" ")
            #        

            syncobjapicall = "syncable-objects?page_number=1&page_size=999&object_type=MASKING_JOB"
            syncobjapicallresponse = self.get_api_response(src_engine_name, srcapikey, syncobjapicall)
            for jobobj in syncobjapicallresponse['responseList']:
                curr_job_id = jobobj['objectIdentifier']['id']
                if curr_job_id == src_job_id:
                    jobdef = []
                    jobdef.append(jobobj)
                    print_debug("jobobj: {}".format(jobobj))
                    src_env_id = self.find_env_id(src_env_name, src_engine_name)
                    src_env_purpose = self.find_env_purpose(src_env_id, src_engine_name)
                    src_app_id = self.find_appid_of_envid(src_env_id, src_engine_name)
                    src_app_name = self.find_app_name(src_app_id, src_engine_name)
                    print_debug("Source Env name = {}, Source Env purpose = {}, Source App name = {}, Source Env Id = {}, Source App Id = {}".format(src_env_name, src_env_purpose, src_app_name,src_env_id,src_app_id))
                    srcapicall = "export"
                    srcapiresponse = self.post_api_response1(src_engine_name, srcapikey, srcapicall, jobdef, port=80)
                   
                    tgt_env_id = self.find_env_id(tgt_env_name, tgt_engine_name)
                    tgtapicall = "import?force_overwrite=true&environment_id={}&source_environment_id={}".format(tgt_env_id,src_dummy_conn_env_id)
                    tgtapiresponse = self.post_api_response1(tgt_engine_name, tgtapikey, tgtapicall, srcapiresponse, port=80)
                    if tgtapiresponse is None:
                         print(" Job {} sync failed.".format(jobname))
                    else:            
                        print(" Job {} synced successfully. Please update password for connectors in this environment using GUI / API".format(jobname))
                    print(" ")
            #print(" ")
    
        else:
            print(" Error connecting source engine {}".format(src_engine_name))
            
    def process_sync_env(self,src_engine_name,tgt_engine_name,globalobjsync,src_env_name,tgt_env_name,sync_scope):

        srcapikey = self.get_auth_key(src_engine_name)
        print_debug("srcapikey={}".format(srcapikey))

        tgtapikey = self.get_auth_key(tgt_engine_name)
        print_debug("tgtapikey={}".format(tgtapikey))

        if srcapikey is not None and tgtapikey is not None:
            if globalobjsync:
                self.sync_globalobj()

            if sync_scope == "ENV":
                try:
                    src_env_id = self.find_env_id(src_env_name, src_engine_name)
                except:
                    sys.exit(
                        "Error: Unable to pull source env id for environment {}. Please check engine and environment name".format(
                            src_env_name))

                try:
                    tgt_env_id = self.find_env_id(tgt_env_name, tgt_engine_name)
                    if tgt_env_id is None:
                        print(" Agent will create new environment {}".format(tgt_env_name))
                        print(" ")
                except:
                    print(
                        "Error: Unable to pull target env id for environment {}. Please check engine and environment name".format(
                            tgt_env_name))


            # Create dummy app to handle on the fly masking job/env
            cr_app_response = self.create_application(tgt_engine_name, self.src_dummy_conn_app)
            src_dummy_conn_app_id = cr_app_response['applicationId']

            # Create dummy env to handle on the fly masking job/env
            cr_env_response = self.create_environment(tgt_engine_name, src_dummy_conn_app_id, self.src_dummy_conn_env, "MASK")
            src_dummy_conn_env_id = cr_env_response['environmentId']

            print_debug("Source Env name = {}, Source Env purpose = {}, Source App name = {}, Source Env Id = {}, Source App Id = {}".format(self.src_dummy_conn_env, "MASK", self.src_dummy_conn_app,src_dummy_conn_env_id,src_dummy_conn_app_id))
            print(" ")
            #        

            syncobjapicall = "syncable-objects?page_number=1&page_size=999&object_type=ENVIRONMENT"
            syncobjapicallresponse = self.get_api_response(src_engine_name, srcapikey, syncobjapicall)
            for envobj in syncobjapicallresponse['responseList']:
                curr_env_id = envobj['objectIdentifier']['id']
                if sync_scope == "ENGINE":
                    src_env_id = curr_env_id

                if curr_env_id == src_env_id:
                    envdef = []
                    envdef.append(envobj)
                    print_debug("envobj: {}".format(envobj))
                    #src_env_id = envobj['objectIdentifier']['id']
                    src_env_name = self.find_env_name(src_env_id, src_engine_name)
                    src_env_purpose = self.find_env_purpose(src_env_id, src_engine_name)
                    src_app_id = self.find_appid_of_envid(src_env_id, src_engine_name)
                    src_app_name = self.find_app_name(src_app_id, src_engine_name)
                    print_debug("Source Env name = {}, Source Env purpose = {}, Source App name = {}, Source Env Id = {}, Source App Id = {}".format(src_env_name, src_env_purpose, src_app_name,src_env_id,src_app_id))
                    srcapicall = "export"
                    #print_debug(src_engine_name, srcapikey, srcapicall, envdef, port=80)
                    srcapiresponse = self.post_api_response1(src_engine_name, srcapikey, srcapicall, envdef, port=80)
                    #print_debug("srcapiresponse={}".format(srcapiresponse))

                    cr_app_response = self.create_application(tgt_engine_name, src_app_name)
                    tgt_app_id = cr_app_response['applicationId']

                    cr_env_response = self.create_environment(tgt_engine_name, tgt_app_id, src_env_name, src_env_purpose)
                    tgt_env_id = cr_env_response['environmentId']
                    
                    print_debug("Target Env Id = {}, Target App Id = {}".format(tgt_env_id, tgt_app_id))

                    tgtapicall = "import?force_overwrite=true&environment_id={}&source_environment_id={}".format(tgt_env_id,src_dummy_conn_env_id)
                    tgtapiresponse = self.post_api_response1(tgt_engine_name, tgtapikey, tgtapicall, srcapiresponse, port=80)
                    if tgtapiresponse is None:
                         print(" Environment {} sync failed.".format(src_env_name))
                    else:            
                        print(" Environment {} synced successfully. Please update password for connectors in this environment using GUI / API".format(src_env_name))
                    print(" ")

                    if sync_scope == "ENV":
                        break
            #print(" ")
        else:
            print (" Error connecting source engine {}".format(src_engine_name))    
    
    def sync_env(self):
        src_engine_name = self.srcmskengname
        tgt_engine_name = self.tgtmskengname
        globalobjsync = self.globalobjsync
        src_env_name = self.srcenvname
        tgt_env_name = self.tgtenvname
        sync_scope = "ENV"
        self.process_sync_env(src_engine_name, tgt_engine_name, globalobjsync, src_env_name, tgt_env_name, sync_scope)

        print(" Adjust Source Connector for OTF jobs(if any)")
        print_debug(" {},{},{},{}".format(src_engine_name, tgt_engine_name, src_env_name, tgt_env_name))
        del_tmp_env = self.upd_all_otf_jobs_src_connectors(src_engine_name, tgt_engine_name, src_env_name, tgt_env_name, sync_scope)
        print_debug(" del_tmp_env = {}".format(del_tmp_env))
        print(" ")

        if del_tmp_env == 0:
            print(" Delete temporary environment {} created for OTF jobs".format(self.src_dummy_conn_env))
            dummy_conn_env_id = self.find_env_id(self.src_dummy_conn_env, tgt_engine_name)
            self.del_env_byid(tgt_engine_name, dummy_conn_env_id, None)

            print(" ")
            print(" Delete temporary application {} created for OTF jobs".format(self.src_dummy_conn_app))
            dummy_conn_app_id = self.find_app_id(self.src_dummy_conn_app, tgt_engine_name)
            self.del_app_byid(tgt_engine_name, dummy_conn_app_id, None)
            print(" ")

        conn_type_list = ["database", "file", "mainframe"]
        for conn_type in conn_type_list:
            self.test_connectors(tgt_engine_name, conn_type, sync_scope, tgt_env_name)        
    
    def sync_eng(self):
        src_engine_name = self.srcmskengname
        tgt_engine_name = self.tgtmskengname          
        globalobjsync = self.globalobjsync
        globalobjsync = True
        sync_scope = "ENGINE"
        self.process_sync_env(src_engine_name, tgt_engine_name, globalobjsync, None, None, sync_scope)

        print(" Adjust Source Connector for OTF jobs(if any)")
        src_env_name = None
        tgt_env_name = None
        print_debug(" {},{},{},{}".format(src_engine_name, tgt_engine_name, src_env_name, tgt_env_name))
        del_tmp_env = self.upd_all_otf_jobs_src_connectors(src_engine_name, tgt_engine_name, src_env_name, tgt_env_name,
                                                           sync_scope,None)
        print_debug( " del_tmp_env = {}".format(del_tmp_env))
        print(" ")

        if del_tmp_env == 0:
            print(" Delete temporary environment {} created for OTF jobs".format(self.src_dummy_conn_env))
            dummy_conn_env_id = self.find_env_id(self.src_dummy_conn_env, tgt_engine_name)
            self.del_env_byid(tgt_engine_name, dummy_conn_env_id, None)

            print(" ")
            print(" Delete temporary application {} created for OTF jobs".format(self.src_dummy_conn_app))
            dummy_conn_app_id = self.find_app_id(self.src_dummy_conn_app, tgt_engine_name)
            self.del_app_byid(tgt_engine_name, dummy_conn_app_id, None)
            print(" ")

        conn_type_list = ["database", "file", "mainframe"]
        #for conn_type in conn_type_list:
            #self.test_connectors(tgt_engine_name, conn_type, sync_scope, None)

    def sync_job(self):
        src_engine_name = self.srcmskengname
        tgt_engine_name = self.tgtmskengname
        src_env_name = self.srcenvname
        tgt_env_name = self.tgtenvname
        src_job_name = self.srcjobname
        globalobjsync = self.globalobjsync
        sync_scope = "JOB"
        self.process_sync_job(src_engine_name, tgt_engine_name, globalobjsync, src_env_name, tgt_env_name, src_job_name)

        print(" Adjust Source Connector for OTF jobs(if any)")
        print_debug(" {},{},{},{}".format(src_engine_name, tgt_engine_name, src_env_name, tgt_env_name))
        del_tmp_env = self.upd_all_otf_jobs_src_connectors(src_engine_name, tgt_engine_name, src_env_name, tgt_env_name, sync_scope, src_job_name)
        print(" ")

        if del_tmp_env == 0:
            print(" Delete temporary environment {} created for OTF jobs".format(self.src_dummy_conn_env))
            dummy_conn_env_id = self.find_env_id(self.src_dummy_conn_env, tgt_engine_name)
            self.del_env_byid(tgt_engine_name, dummy_conn_env_id, None)

            print(" ")
            print(" Delete temporary application {} created for OTF jobs".format(self.src_dummy_conn_app))
            dummy_conn_app_id = self.find_app_id(self.src_dummy_conn_app, tgt_engine_name)
            self.del_app_byid(tgt_engine_name, dummy_conn_app_id, None)
            print(" ")

        conn_type_list = ["database", "file", "mainframe"]
        for conn_type in conn_type_list:
            self.test_connectors(tgt_engine_name, conn_type, sync_scope, tgt_env_name)


    def upd_all_otf_jobs_src_connectors(self, src_engine_name, tgt_engine_name, src_env_name, tgt_env_name, sync_scope, jobname=None):
        delete_tmp_env = 0
        is_otf_job = 0
        srcapikey = self.get_auth_key(src_engine_name)
        print_debug("srcapikey={}".format(srcapikey))

        tgtapikey = self.get_auth_key(tgt_engine_name)
        print_debug("tgtapikey={}".format(tgtapikey))

        if sync_scope == "ENV" or sync_scope == "JOB":
            try:
                src_env_id = self.find_env_id(src_env_name, src_engine_name)
            except:
                sys.exit(
                    "Error: Unable to pull source env id for environment {}. Please check engine and environment name".format(
                        src_env_name))

            try:
                tgt_env_id = self.find_env_id(tgt_env_name, tgt_engine_name)
            except:
                print(
                    "Error: Unable to pull target env id for environment {}. Please check engine and environment name".format(
                        tgt_env_name))

        if srcapikey is not None and tgtapikey is not None:
            syncobjapicall = "environments?page_number=1&page_size=999"
            syncobjapicallresponse = self.get_api_response(src_engine_name, srcapikey, syncobjapicall)
            for envobj in syncobjapicallresponse['responseList']:
                curr_env_id = envobj['environmentId']
                curr_env_name = envobj['environmentName']

                if sync_scope == "ENGINE":
                    src_env_id = curr_env_id
                    src_env_name = curr_env_name
                    tgt_env_name = curr_env_name

                print_debug(
                    "upd_all_otf_jobs_src_connectors - src_env_id={},curr_env_id={}".format(src_env_id, curr_env_id))

                if curr_env_id == src_env_id:
                    print_debug("Before otf_src_job_mappings - jobname = {}".format(jobname))
                    otf_src_job_mappings = self.gen_otf_job_mappings(src_engine_name,src_env_name,sync_scope,jobname)
                    otf_tgt_job_mappings = self.gen_otf_job_mappings(tgt_engine_name,tgt_env_name,sync_scope,jobname)
                    print_debug(" otf_src_job_mappings : {}".format(otf_src_job_mappings))
                    print_debug(" otf_tgt_job_mappings : {}".format(otf_tgt_job_mappings))

                    #print(" ")
                    for i in otf_tgt_job_mappings:
                        is_otf_job = 1
                        upd_job_name = i['jobname']
                        print(' Updating Job {} on Environment {} for source connector'.format(upd_job_name, tgt_env_name))
                        src_record = self.find_conn_details(otf_src_job_mappings, upd_job_name, src_env_name)
                        tgt_record = self.find_conn_details(otf_tgt_job_mappings, upd_job_name, tgt_env_name)

                        print_debug(" src:{}".format(src_record))
                        print_debug(" tgt:{}".format(tgt_record))

                        maskingJobId = tgt_record['maskingJobId']
                        srcconnectorName = src_record['srcconnectorName']
                        srcconnectorType = src_record['srcconnectorType']
                        srcconnectorEnvName = src_record['srcconnectorEnvName']
                        srcconnectorEnvappname = src_record['srcconnectorEnvappname']
                        tgtenvironmentId = tgt_record['environmentId']

                        print_debug(" {},{},{},{},{},{},{}".format(maskingJobId, srcconnectorName, srcconnectorType, srcconnectorEnvName, tgt_engine_name, tgt_env_name,srcconnectorEnvappname))
                        return_status = self.upd_job_connector(maskingJobId, srcconnectorName, srcconnectorType, srcconnectorEnvName, tgt_engine_name, tgt_env_name,srcconnectorEnvappname)
                        if return_status == 1:
                            delete_tmp_env = 1
                    if sync_scope == "ENV":
                        if is_otf_job == 0:
                            delete_tmp_env = 1
                        elif is_otf_job == 1 and delete_tmp_env == 1:
                            delete_tmp_env = 1
                        elif is_otf_job == 1 and delete_tmp_env == 0:
                            delete_tmp_env = 0
                        break
        return delete_tmp_env

    def cr_dir(self,dirname):
        if not os.path.exists(dirname):
            try:
                os.makedirs(dirname)
            except:
                print("Unable to create directory {}. Please check permissions".format(dirname)) 

    def cr_backup_dirs(self):
        backup_dir = self.backup_dir
        x = datetime.datetime.now()
        x_dateformat = x.strftime("%m%d%Y_%H%M%S")

        bkp_main_dir = os.path.join(backup_dir, x_dateformat)
        self.cr_dir(bkp_main_dir)
        
        globalobjects_dir = os.path.join(bkp_main_dir, "globalobjects")
        self.cr_dir(globalobjects_dir) 

        roleobjects_dir = os.path.join(bkp_main_dir, "roleobjects")
        self.cr_dir(roleobjects_dir)

        userobjects_dir = os.path.join(bkp_main_dir, "userobjects")
        self.cr_dir(userobjects_dir)

        environments_dir = os.path.join(bkp_main_dir, "environments")
        self.cr_dir(environments_dir) 

        applications_dir = os.path.join(bkp_main_dir, "applications")
        self.cr_dir(applications_dir)

        mappings_dir = os.path.join(bkp_main_dir, "mappings")
        self.cr_dir(mappings_dir)

        print("Created directory structure for backups")

        return bkp_main_dir

    def bkp_syncable_objects(self, syncable_object_type, bkp_main_dir):
        src_engine_name = self.mskengname
        srcapikey = self.get_auth_key(src_engine_name)
        if srcapikey is not None:
            syncobjapicall = "syncable-objects?page_number=1&page_size=999&object_type={}".format(syncable_object_type)
            syncobjapicallresponse = self.get_api_response(src_engine_name, srcapikey, syncobjapicall)
            for syncable_object_type_elem in syncobjapicallresponse['responseList']:
                syncable_object_type_def = []
                syncable_object_type_def.append(syncable_object_type_elem)                                        
                srcapicall = "export"
                srcapiresponse = self.post_api_response1(src_engine_name, srcapikey, srcapicall, syncable_object_type_def, port=80)

                syncobj_bkp_dict = { "syncable_object_type": syncable_object_type, "srcapiresponse": srcapiresponse }
                syncobj_bkp_file = "{}/globalobjects/backup_{}.dat".format(bkp_main_dir,syncable_object_type)
                with open(syncobj_bkp_file, 'wb') as fh:
                    pickle.dump(syncobj_bkp_dict, fh)
                print("Created backup of syncable_object_type {}".format(syncable_object_type))   
        else:
            print(" Error connecting source engine {}".format(src_engine_name))

    def bkp_roles(self, bkp_main_dir):
        src_engine_name = self.mskengname
        i = None
        srcapikey = self.get_auth_key(src_engine_name)
        if srcapikey is not None:
            roleobjapicall = "roles?page_number=1&page_size=999"
            roleobjapicallresponse = self.get_api_response(src_engine_name, srcapikey, roleobjapicall)
            for role_rec in roleobjapicallresponse['responseList']:
                i = 1
                roleId = role_rec['roleId']
                roleName = role_rec['roleName']                                   
                roleNameNoSpace = roleName.replace(" ","_")
                role_bkp_dict = { "roleId": roleId, "roleName": roleName, "srcapiresponse": role_rec }
                roleobj_bkp_file = "{}/roleobjects/backup_{}.dat".format(bkp_main_dir,roleNameNoSpace)
                with open(roleobj_bkp_file, 'wb') as fh:
                    pickle.dump(role_bkp_dict, fh)
                print("Created backup of role {}".format(roleName))   
        else:
            print(" Error connecting source engine {}".format(src_engine_name))

    def bkp_users(self, bkp_main_dir):
        src_engine_name = self.mskengname
        i = None
        srcapikey = self.get_auth_key(src_engine_name)
        if srcapikey is not None:
            userobjapicall = "users?page_number=1&page_size=999"
            userobjapicallresponse = self.get_api_response(src_engine_name, srcapikey, userobjapicall)
            for user_rec in userobjapicallresponse['responseList']:
                i = 1
                userId = user_rec['userId']
                userName = user_rec['userName']                                   
                userNameNoSpace = userName.replace(" ", "_")
                user_rec['password'] = "Delphix-123"
                user_bkp_dict = { "userId": userId, "userName": userName, "srcapiresponse": user_rec }
                userobj_bkp_file = "{}/userobjects/backup_{}.dat".format(bkp_main_dir,userNameNoSpace)
                with open(userobj_bkp_file, 'wb') as fh:
                    pickle.dump(user_bkp_dict, fh)
                print("Created backup of user {}".format(userName))   
        else:
            print(" Error connecting source engine {}".format(src_engine_name))            

    def bkp_globalobj(self,bkp_main_dir):
        self.bkp_syncable_objects("GLOBAL_OBJECT",bkp_main_dir)
        self.bkp_syncable_objects("FILE_FORMAT",bkp_main_dir)
        self.bkp_syncable_objects("MOUNT_INFORMATION",bkp_main_dir)

    def bkp_otf_job_mappings(self, bkp_main_dir, srcapikey=None):
        src_engine_name = self.mskengname
        otf_job_mapping_list = []

        if srcapikey is None:
            srcapikey = self.get_auth_key(src_engine_name)
        print_debug("srcapikey={}".format(srcapikey))

        if srcapikey is not None:
            syncobjapicall = "environments?page_number=1&page_size=999"
            syncobjapicallresponse = self.get_api_response(src_engine_name, srcapikey, syncobjapicall)

            for envobj in syncobjapicallresponse['responseList']:
                src_env_id = envobj['environmentId']
                src_env_name = envobj['environmentName']

                jobobjapicall = "masking-jobs?page_number=1&page_size=999&environment_id={}".format(src_env_id)
                jobobjapicallresponse = self.get_api_response(src_engine_name, srcapikey, jobobjapicall)

                for jobobj in jobobjapicallresponse['responseList']:
                    otf_job_dict = {}
                    print_debug("{},{},{},{}".format(jobobj['maskingJobId'], jobobj['jobName'], src_env_name,
                                                     jobobj['onTheFlyMasking']))
                    if jobobj['onTheFlyMasking']:
                        otf_jobid = jobobj['maskingJobId']
                        otf_jobname = jobobj['jobName']
                        srcconnectorId = jobobj['onTheFlyMaskingSource']['connectorId']
                        srcconnectortype = jobobj['onTheFlyMaskingSource']['connectorType'].lower()

                        srcconnectorName = self.find_conn_name_by_conn_id(srcconnectorId, srcconnectortype,
                                                                          src_engine_name, srcapikey)
                        srcconnectorenvId = self.find_env_id_by_conn_id(srcconnectorId, srcconnectortype,
                                                                        src_engine_name, srcapikey)
                        srcconnectorEnvname = self.find_env_name(srcconnectorenvId, src_engine_name)

                        print_debug(
                            "params = {},{},{},{}".format(srcconnectorId, srcconnectortype, src_engine_name, srcapikey))

                        otf_job_dict['otf_jobid'] = otf_jobid
                        otf_job_dict['otf_jobname'] = otf_jobname
                        otf_job_dict['srcconnectorId'] = srcconnectorId
                        otf_job_dict['srcconnectortype'] = srcconnectortype
                        otf_job_dict['srcconnectorName'] = srcconnectorName
                        otf_job_dict['srcconnectorEnvname'] = srcconnectorEnvname
                        otf_job_dict['src_env_id'] = src_env_id
                        otf_job_dict['src_env_name'] = src_env_name

                        otf_job_mapping_list.append(otf_job_dict)

            print_debug(" ")
            print_debug("JobMapping: {}".format(otf_job_mapping_list))
            otf_job_mapping_list_file = "{}/mappings/backup_{}.dat".format(bkp_main_dir, "otf_job_mapping")
            with open(otf_job_mapping_list_file, 'wb') as fh:
                pickle.dump(otf_job_mapping_list, fh)
            print("Created backup of otf_job_mapping")

        else:
            print(" Error connecting source engine {}".format(src_engine_name))

    def offline_backup_eng(self):
        env_mapping = {}
        src_engine_name = self.mskengname       
        srcapikey = self.get_auth_key(src_engine_name)
        print_debug("srcapikey={}".format(srcapikey))
        if srcapikey is not None:
            bkp_main_dir = self.cr_backup_dirs()
            print(" ")
            self.bkp_globalobj(bkp_main_dir)
            print(" ")
            self.bkp_roles(bkp_main_dir)
            print(" ")
            self.bkp_users(bkp_main_dir)
            print(" ")
            self.bkp_otf_job_mappings(bkp_main_dir,srcapikey)
            print(" ")

            syncobjapicall = "syncable-objects?page_number=1&page_size=999&object_type=ENVIRONMENT"
            syncobjapicallresponse = self.get_api_response(src_engine_name, srcapikey, syncobjapicall)
            
            for envobj in syncobjapicallresponse['responseList']:
                
                envdef = []
                envdef.append(envobj)
                src_env_id = envobj['objectIdentifier']['id']
                src_env_name = self.find_env_name(src_env_id, src_engine_name)
                src_env_purpose = self.find_env_purpose(src_env_id, src_engine_name)
                src_app_id = self.find_appid_of_envid(src_env_id, src_engine_name)
                src_app_name = self.find_app_name(src_app_id, src_engine_name)
                print_debug("Source Env name = {}, Source Env purpose = {}, Source App name = {}, Source Env Id = {}, Source App Id = {}".format(src_env_name, src_env_purpose, src_app_name,src_env_id,src_app_id))
                
                env_mapping[src_env_id] = src_env_name

                srcapicall = "export"
                srcapiresponse = self.post_api_response1(src_engine_name, srcapikey, srcapicall, envdef, port=80)
                
                env_bkp_dict = { "src_app_id": src_app_id, "src_app_name": src_app_name, "src_env_id": src_env_id , "src_env_name": src_env_name, "src_env_purpose": src_env_purpose, "srcapiresponse": srcapiresponse }
                env_bkp_file = "{}/environments/backup_env_{}.dat".format(bkp_main_dir,src_env_id)
                with open(env_bkp_file, 'wb') as fh:
                    pickle.dump(env_bkp_dict, fh)
                print("Created backup of environment {}".format(src_env_name))

            env_mapping_file = "{}/mappings/backup_env_mapping.dat".format(bkp_main_dir)
            with open(env_mapping_file, 'wb') as fh:
                pickle.dump(env_mapping, fh)
            print("Created mapping file for environment")
            print(" ")

            print("Created backup of masking engine at {}".format(bkp_main_dir))
            print(" ")
    
        else:
            print (" Error connecting source engine {}".format(src_engine_name))

    def restore_globalobj(self, syncable_object_type, tgtapikey, tgt_engine_name, srcapiresponse, bkp_main_dir):
        tgtapicall = "import?force_overwrite=true"
        tgtapiresponse = self.post_api_response1(tgt_engine_name, tgtapikey, tgtapicall, srcapiresponse, port=80)
        if tgtapiresponse is None:
            print(" Failed to restore Syncable Object {}".format(syncable_object_type))
        else:
            print(" Restored syncable_object_type: {}".format(syncable_object_type))

    def restore_roleobj(self, roleName, tgtapikey, tgt_engine_name, srcapiresponse, bkp_main_dir):
        tgtapicall = "roles"
        tgtapiresponse = self.post_api_response1(tgt_engine_name, tgtapikey, tgtapicall, srcapiresponse, port=80)
        if tgtapiresponse is None:
            print(" Failed to restore role {}".format(roleName))
        else:
            print(" Restored role: {}".format(roleName))

    def restore_userobj(self, userName, tgtapikey, tgt_engine_name, srcapiresponse, bkp_main_dir):
        tgtapicall = "users"
        tgtapiresponse = self.post_api_response1(tgt_engine_name, tgtapikey, tgtapicall, srcapiresponse, port=80)
        if tgtapiresponse is None:
            print(" Failed to restore user {}".format(userName))
        else:
            print_debug(" tgtapiresponse: {}".format(tgtapiresponse))
            if 'errorMessage' in tgtapiresponse.keys():
                if 'User already exists' in tgtapiresponse['errorMessage']:
                    print_debug("User allready exists")
                    userid = self.find_user_id(userName, tgt_engine_name)
                    print_debug("userid = {}".format(userid))
                    updtgtapicall = "users/{}".format(userid)
                    tgtapiresponse = self.put_api_response(tgt_engine_name, tgtapikey, updtgtapicall, srcapiresponse, port=80)
                    print_debug("put tgtapiresponse = {}".format(tgtapiresponse))
                    print(" Restored user: {}".format(userName))
                else:
                    print("Unable to create user: {}".format(userName))
            else:
                print(" Restored user: {}".format(userName))

    def offline_restore_eng(self):
        tgt_engine_name = self.mskengname       
        tgtapikey = self.get_auth_key(tgt_engine_name)
        
        print_debug("tgtapikey={}".format(tgtapikey))
        if tgtapikey is not None:
            backup_dir = self.backup_dir
            print_debug("backup_dir: {}".format(backup_dir))

            globalobj_bkp_dict_file_fullpath = "{}/{}/{}".format(backup_dir, "globalobjects", "backup_GLOBAL_OBJECT.dat")
            with open(globalobj_bkp_dict_file_fullpath, 'rb') as f1:
                globalobj_bkp_dict = pickle.load(f1)
                syncable_object_type = globalobj_bkp_dict['syncable_object_type']
                srcapiresponse = globalobj_bkp_dict['srcapiresponse']
                self.restore_globalobj(syncable_object_type, tgtapikey, tgt_engine_name, srcapiresponse, backup_dir)
                print(" ")

            syncobj_bkp_dict_file_arr = os.listdir("{}/globalobjects".format(backup_dir))
            print_debug("syncobj_bkp_dict_file_arr: {}".format(syncobj_bkp_dict_file_arr))
            for syncobj_bkp_dict_file in syncobj_bkp_dict_file_arr:
                if syncobj_bkp_dict_file != "backup_GLOBAL_OBJECT.dat":
                    # Global Object is already done so skipped. Looking for mount, fileformat etc
                    print_debug("syncobj_bkp_dict_file: {}".format(syncobj_bkp_dict_file))
                    syncobj_bkp_dict_file_fullpath = "{}/{}/{}".format(backup_dir, "globalobjects", syncobj_bkp_dict_file)
                    print_debug("syncobj_bkp_dict_file_fullpath: {}".format(syncobj_bkp_dict_file_fullpath))
                    with open(syncobj_bkp_dict_file_fullpath, 'rb') as f1:
                        syncobj_bkp_dict = pickle.load(f1)
                    #print_debug(syncobj_bkp_dict) # It will be huge
                    syncable_object_type = syncobj_bkp_dict['syncable_object_type']
                    srcapiresponse = syncobj_bkp_dict['srcapiresponse']
                    self.restore_globalobj(syncable_object_type, tgtapikey, tgt_engine_name, srcapiresponse, backup_dir)
                    print(" ")

            # Create dummy app to handle on the fly masking job/env
            cr_app_response = self.create_application(tgt_engine_name, self.src_dummy_conn_app)
            src_dummy_conn_app_id = cr_app_response['applicationId']

            # Create dummy env to handle on the fly masking job/env
            cr_env_response = self.create_environment(tgt_engine_name, src_dummy_conn_app_id, self.src_dummy_conn_env, "MASK")
            src_dummy_conn_env_id = cr_env_response['environmentId']            

            print_debug("Target Env Id = {}, Target App Id = {}".format(src_dummy_conn_app_id, src_dummy_conn_env_id))

            env_bkp_dict_file_arr = os.listdir("{}/environments".format(backup_dir))
            print_debug("env_bkp_dict_file_arr: {}".format(env_bkp_dict_file_arr))
            for env_bkp_dict_file in env_bkp_dict_file_arr:
                print_debug("env_bkp_dict_file: {}".format(env_bkp_dict_file))
                env_bkp_dict_file_fullpath = "{}/{}/{}".format(backup_dir, "environments", env_bkp_dict_file)
                print_debug("env_bkp_dict_file_fullpath: {}".format(env_bkp_dict_file_fullpath))
                with open(env_bkp_dict_file_fullpath, 'rb') as f1:
                    env_bkp_dict = pickle.load(f1)
                print_debug(env_bkp_dict)

                src_app_id = env_bkp_dict['src_app_id']
                src_app_name = env_bkp_dict['src_app_name']
                src_env_id = env_bkp_dict['src_env_id']
                src_env_name = env_bkp_dict['src_env_name']
                src_env_purpose = env_bkp_dict['src_env_purpose']
                srcapiresponse = env_bkp_dict['srcapiresponse']

                if src_env_name == self.src_dummy_conn_env:
                    tgt_app_id = src_dummy_conn_app_id
                    tgt_env_id = src_dummy_conn_env_id
                else:
                    cr_app_response = self.create_application(tgt_engine_name, src_app_name)
                    tgt_app_id = cr_app_response['applicationId']

                    cr_env_response = self.create_environment(tgt_engine_name, tgt_app_id, src_env_name, src_env_purpose)
                    tgt_env_id = cr_env_response['environmentId']
                
                print_debug("Target Env Id = {}, Target App Id = {}".format(tgt_env_id, tgt_app_id))

                if src_env_name == self.src_dummy_conn_env:
                    # Handle eror : {"errorMessage":"Source environment cannot be the same as environment"}
                    tgtapicall = "import?force_overwrite=true&environment_id={}".format(tgt_env_id)
                else:
                    tgtapicall = "import?force_overwrite=true&environment_id={}&source_environment_id={}".format(tgt_env_id, src_dummy_conn_env_id)

                tgtapiresponse = self.post_api_response1(tgt_engine_name, tgtapikey, tgtapicall, srcapiresponse, port=80)
                if tgtapiresponse is None:
                    print(" Environment {} restore failed.".format(src_env_name))
                else:            
                    print(" Environment {} restored successfully. Please update password for connectors in this environment using GUI / API".format(src_env_name))

                print(" Restored environment {}".format(env_bkp_dict['src_env_name']))
                print(" ")

            #Restore OTF_JOB_MAPPING
            otf_job_mapping_file = "{}/mappings/backup_otf_job_mapping.dat".format(backup_dir)
            with open(otf_job_mapping_file, 'rb') as otf1:
                otf_job_mapping = pickle.load(otf1)
            print_debug(" Job Env Mapping :{}".format(otf_job_mapping))

            for otf_job in otf_job_mapping:
                print_debug(otf_job)
                jobname = otf_job['otf_jobname']
                src_env_name = otf_job['src_env_name']
                srcconn_name = otf_job['srcconnectorName']
                conn_type = otf_job['srcconnectortype']
                srcconnectorEnvname = otf_job['srcconnectorEnvname']
                jobid = self.find_job_id(jobname,src_env_name,tgt_engine_name)
                print_debug("Before upd_job_connector : {},{},{},{},{}".format(jobid,srcconn_name,conn_type,srcconnectorEnvname,tgt_engine_name))
                self.upd_job_connector(jobid, srcconn_name, conn_type, srcconnectorEnvname, tgt_engine_name, srcconnectorEnvname,
                                  None)
            print(" ")

            #Restore Roles
            roleobj_bkp_dict_file_arr = os.listdir("{}/roleobjects".format(backup_dir))
            print_debug("roleobj_bkp_dict_file_arr: {}".format(roleobj_bkp_dict_file_arr))
            for roleobj_bkp_dict_file in roleobj_bkp_dict_file_arr:
                if roleobj_bkp_dict_file != "backup_All_Privileges.dat":
                    # All Privileges Role is default out of the box
                    print_debug("roleobj_bkp_dict_file: {}".format(roleobj_bkp_dict_file))
                    roleobj_bkp_dict_file_fullpath = "{}/{}/{}".format(backup_dir, "roleobjects", roleobj_bkp_dict_file)
                    print_debug("roleobj_bkp_dict_file_fullpath: {}".format(roleobj_bkp_dict_file_fullpath))
                    with open(roleobj_bkp_dict_file_fullpath, 'rb') as f1:
                        roleobj_bkp_dict = pickle.load(f1)
                    #print_debug(roleobj_bkp_dict) # It will be huge
                    roleId = roleobj_bkp_dict['roleId']
                    roleName = roleobj_bkp_dict['roleName']
                    srcapiresponse = roleobj_bkp_dict['srcapiresponse']

                    self.restore_roleobj(roleName, tgtapikey, tgt_engine_name, srcapiresponse, backup_dir)
                    #print(" Restored Role {}".format(roleName))
            print(" ")                

            #Restore Users
            env_mapping_file = "{}/mappings/backup_env_mapping.dat".format(backup_dir)
            with open(env_mapping_file, 'rb') as m1:
                env_mapping = pickle.load(m1)
            print_debug(" Source Env Mapping :{}".format(env_mapping))
            tgtenvlist = []
            userobj_bkp_dict_file_arr = os.listdir("{}/userobjects".format(backup_dir))
            print_debug("userobj_bkp_dict_file_arr: {}".format(userobj_bkp_dict_file_arr))
            for userobj_bkp_dict_file in userobj_bkp_dict_file_arr:
                if userobj_bkp_dict_file != "backup_admin.dat":
                    # All Privileges user is default out of the box
                    print_debug("userobj_bkp_dict_file: {}".format(userobj_bkp_dict_file))
                    userobj_bkp_dict_file_fullpath = "{}/{}/{}".format(backup_dir, "userobjects", userobj_bkp_dict_file)
                    print_debug("userobj_bkp_dict_file_fullpath: {}".format(userobj_bkp_dict_file_fullpath))
                    with open(userobj_bkp_dict_file_fullpath, 'rb') as f1:
                        userobj_bkp_dict = pickle.load(f1)
                    #print_debug(userobj_bkp_dict) # It will be huge
                    userId = userobj_bkp_dict['userId']
                    userName = userobj_bkp_dict['userName']
                    srcapiresponse = userobj_bkp_dict['srcapiresponse']
                    print_debug(" Is Admin:{}".format(srcapiresponse['isAdmin']))
                    if not srcapiresponse['isAdmin']:
                        print_debug(" srcnonAdminProperties = {}".format(srcapiresponse['nonAdminProperties']))
                        print_debug(" srcenvlist = {}".format(srcapiresponse['nonAdminProperties']['environmentIds']))
                        srcenvlist = srcapiresponse['nonAdminProperties']['environmentIds']
                        if len(srcenvlist) != 0:
                            for envid in srcenvlist:
                                tmpenvname = env_mapping[envid]
                                tgtenvid = self.find_env_id(tmpenvname, tgt_engine_name)
                                print_debug(" tgtenvid = {}".format(tgtenvid))
                                tgtenvlist.append(tgtenvid)
                        print_debug(" tgtenvlist = {}".format(tgtenvlist))
                        print_debug(" Before : srcenvlist = {}".format(srcapiresponse['nonAdminProperties']['environmentIds']))
                        srcapiresponse['nonAdminProperties']['environmentIds'] = tgtenvlist
                        print_debug(" After  : srcenvlist = {}".format(srcapiresponse['nonAdminProperties']['environmentIds']))
                    self.restore_userobj(userName, tgtapikey, tgt_engine_name, srcapiresponse, backup_dir)
                    #print(" Restored user {}".format(userName))
            print(" ")
            del_tmp_env = 0
            if del_tmp_env == 0:
                print(" Delete temporary environment {} created for OTF jobs".format(self.src_dummy_conn_env))
                dummy_conn_env_id = self.find_env_id(self.src_dummy_conn_env, tgt_engine_name)
                self.del_env_byid(tgt_engine_name, dummy_conn_env_id, None)

                print(" ")
                print(" Delete temporary application {} created for OTF jobs".format(self.src_dummy_conn_app))
                dummy_conn_app_id = self.find_app_id(self.src_dummy_conn_app, tgt_engine_name)
                self.del_app_byid(tgt_engine_name, dummy_conn_app_id, None)
                print(" ")

            sync_scope = "ENGINE"
            conn_type_list = ["database", "file", "mainframe"]
            for conn_type in conn_type_list:
                self.test_connectors(tgt_engine_name, conn_type, sync_scope, None)

            print(" Restore Engine {} - complete".format(tgt_engine_name))
            print(" ")
        else:
            print (" Error connecting source engine {}".format(tgt_engine_name))

    def cleanup_eng(self):
        src_engine_name = self.mskengname       
        srcapikey = self.get_auth_key(src_engine_name)
        print_debug("srcapikey={}".format(srcapikey))
        i = 0
        if srcapikey is not None:
            rerun_env_id_list = []  
            syncobjapicall = "environments?page_number=1&page_size=999"
            syncobjapicallresponse = self.get_api_response(src_engine_name, srcapikey, syncobjapicall)
            
            for envobj in syncobjapicallresponse['responseList']:
                src_env_id = envobj['environmentId']
                src_env_name = envobj['environmentName']
                print_debug("srcenv = {},{}".format(src_env_id,src_env_name))

                delapicall = "environments/{}".format(src_env_id)
                delapiresponse = self.del_api_response(src_engine_name, srcapikey, delapicall)
                if delapiresponse is None:
                    # To Handle dependents especially on-the-fly-masking interdependent env
                    rerun_env_id_list.append({"src_env_id": src_env_id, "src_env_name": src_env_name})
                else:             
                    print(" Environment {} deleted successfully.".format(src_env_name))
                    #print(" ")

            if len(rerun_env_id_list) != 0:
                for rerun_env_id_rec in rerun_env_id_list:
                    src_env_id = rerun_env_id_rec['src_env_id']
                    src_env_name = rerun_env_id_rec['src_env_name']
                    delapicall = "environments/{}".format(src_env_id)
                    delapiresponse = self.del_api_response(src_engine_name, srcapikey, delapicall)
                    if delapiresponse is None:
                        print(" Unable to delete environment {}.".format(src_env_name))
                        i = 1
                    else:
                        print(" Environment {} deleted successfully.".format(src_env_name))
                    #print(" ")
    
            syncobjapicall = "applications?page_number=1&page_size=999"
            syncobjapicallresponse = self.get_api_response(src_engine_name, srcapikey, syncobjapicall)
            for appobj in syncobjapicallresponse['responseList']:
                src_app_id = appobj['applicationId']
                src_app_name = appobj['applicationName']
                print_debug("srcapp = {},{}".format(src_app_id,src_app_name))

                delapicall = "applications/{}".format(src_app_id)
                delapiresponse = self.del_api_response(src_engine_name, srcapikey, delapicall)
                if delapiresponse is None:
                    print(" Unable to delete Application {}.".format(src_app_name))
                    i = 1
                else:
                    print(" Application {} deleted successfully.".format(src_app_name))
                    #print(" ")

            print(" ")
            print(" Deleting users")
            self.del_users(src_engine_name,srcapikey)
            print(" ")
            print(" Deleting roles")
            self.del_roles(src_engine_name,srcapikey)
            print(" ")
            print(" Deleting Domains")
            self.del_domains(src_engine_name,srcapikey)

            if i == 0:
                print(" Engine {} cleanup completed.".format(src_engine_name))
            else:
                print(" Engine {} cleanup failed.".format(src_engine_name))
            print(" ")            

        else:
            print(" Error connecting source engine {}".format(src_engine_name))
               
    def gen_otf_job_mappings(self, src_engine_name,src_env_name,sync_scope=None,jobname=None):
        otf_job_mapping_list = []
        
        srcapikey = self.get_auth_key(src_engine_name)
        print_debug("srcapikey={}".format(srcapikey))
        if srcapikey is not None:

            envid = self.find_env_id(src_env_name, src_engine_name)
            syncobjapicall = "environments?page_number=1&page_size=999"
            syncobjapicallresponse = self.get_api_response(src_engine_name, srcapikey, syncobjapicall)
            
            for envobj in syncobjapicallresponse['responseList']:
                #otf_job_dict = {}
                src_env_id = envobj['environmentId']
                src_env_name = envobj['environmentName']
                src_env_purpose = envobj['purpose']
                
                if envid == src_env_id:
                    jobobjapicall = "masking-jobs?page_number=1&page_size=999&environment_id={}".format(src_env_id)
                    jobobjapicallresponse = self.get_api_response(src_engine_name, srcapikey, jobobjapicall)

                    for jobobj in jobobjapicallresponse['responseList']:
                        print_debug(" ")
                        otf_job_dict = {}
                        print_debug("{},{},{},{}".format(jobobj['maskingJobId'], jobobj['jobName'], src_env_name, jobobj['onTheFlyMasking']))
                        if jobobj['onTheFlyMasking']:
                            otf_jobid = jobobj['maskingJobId']
                            otf_jobname = jobobj['jobName']
                            if sync_scope != 'JOB':
                                jobname = otf_jobname
                            print_debug("otf_jobname={},jobname={},sync_scope={}".format(otf_jobname,jobname,sync_scope))
                            if otf_jobname ==  jobname:
                                print_debug(
                                    "Matched : otf_jobname={},jobname={},sync_scope={}".format(otf_jobname, jobname, sync_scope))
                                #otf_job_dict = {}

                                srcconnectorId = jobobj['onTheFlyMaskingSource']['connectorId']
                                srcconnectortype = jobobj['onTheFlyMaskingSource']['connectorType']
                                srcconnectorenvId = self.find_env_id_by_conn_id(srcconnectorId, srcconnectortype, src_engine_name, srcapikey)
                                srcconnectorName = self.find_conn_name_by_conn_id(srcconnectorId, srcconnectortype, src_engine_name, srcapikey)
                                srcconnectorEnvname = self.find_env_name(srcconnectorenvId, src_engine_name)
                                srcconnectorEnvappid = self.find_appid_of_envid(srcconnectorenvId, src_engine_name)
                                srcconnectorEnvappname = self.find_app_name(srcconnectorEnvappid,src_engine_name)

                                otf_job_details_dict = {}
                                otf_job_details_dict['maskingJobId'] = jobobj['maskingJobId']
                                otf_job_details_dict['environmentId'] = src_env_id
                                otf_job_details_dict['envname'] = src_env_name
                                #otf_job_details_dict['purpose'] = src_env_purpose
                                otf_job_details_dict['srcconnectorId'] = srcconnectorId
                                otf_job_details_dict['srcconnectorName'] = srcconnectorName
                                otf_job_details_dict['srcconnectorType'] = jobobj['onTheFlyMaskingSource']['connectorType'].lower()
                                otf_job_details_dict['srcconnectorEnvId'] = srcconnectorenvId
                                otf_job_details_dict['srcconnectorEnvName'] = srcconnectorEnvname
                                otf_job_details_dict['srcconnectorEnvappname'] = srcconnectorEnvappname
                                print_debug("otf_job_details_dict = {}".format(otf_job_details_dict))
                                print_debug(" ")

                                otf_jobenv_mapping_dict = {}
                                otf_jobenv_mapping_dict[src_env_name] = otf_job_details_dict
                                print_debug("otf_jobenv_mapping_dict = {}".format(otf_jobenv_mapping_dict))
                                print_debug(" ")

                                otf_job_dict[otf_jobid] = otf_jobname
                                otf_job_dict[otf_jobname] = otf_jobenv_mapping_dict
                                otf_job_dict['jobname'] = otf_jobname

                                print_debug("otf_job_dict = {}".format(otf_job_dict))
                                print_debug(" ")

                                otf_job_mapping_list.append(otf_job_dict)
                                print_debug("otf_job_mapping_list = {}".format(otf_job_mapping_list))
                                print_debug("==========================================================")
                                print_debug(" ")
                
            print_debug(" ")
            print_debug(" ")
            print_debug("JobMapping: {}".format(otf_job_mapping_list))
            return otf_job_mapping_list
        else:
            print (" Error connecting source engine {}".format(src_engine_name))

    def create_application(self,engine_name,app_name):
        apikey = self.get_auth_key(engine_name)
        apicall = "applications"
        payload = {"applicationName": "{}".format(app_name)}
        apiresponse = self.post_api_response1(engine_name, apikey, apicall, payload, port=80)
        if 'errorMessage' in apiresponse.keys():
            print(" Application {} already exists".format(app_name))
            app_id = self.find_app_id(app_name, engine_name)
            apiresponse = { "applicationId" : "{}".format(app_id)}
        else:
            print(" Application {} Created Successfully".format(app_name))
        return apiresponse

    def create_environment(self, engine_name, app_id, env_name, env_purpose):
        apikey = self.get_auth_key(engine_name)
        apicall = "environments"
        payload = { "environmentName": "{}".format(env_name), "applicationId": "{}".format(app_id), "purpose": "{}".format(env_purpose) }
        apiresponse = self.post_api_response1(engine_name, apikey, apicall, payload, port=80)
        if 'errorMessage' in apiresponse.keys():        
            print(" Environment {} already exists".format(env_name))
            env_id = self.find_env_id(env_name, engine_name)
            apiresponse = { "environmentId" : "{}".format(env_id)}     
        else:
            print(" Environment {} Created Successfully".format(env_name))
        return apiresponse     

    def find_job_id(self, jobname, paramenvname, engine_name):
        apikey = self.get_auth_key(engine_name)
        i = 0
        if apikey is not None:
            apicall = "environments?page_number=1&page_size=999"
            envlist_response = self.get_api_response(engine_name, apikey, apicall)
            for envname in envlist_response['responseList']:
                if envname['environmentName'] == paramenvname:
                    jobapicall = "masking-jobs?page_number=1&page_size=999&environment_id={}".format(envname['environmentId'])
                    joblist_response = self.get_api_response(engine_name, apikey, jobapicall)
                    joblist_responselist = joblist_response['responseList']
                    for joblist in joblist_responselist:
                        if joblist['jobName'] == jobname:
                            i = 1
                            print_debug("Job ID = {}".format(joblist['maskingJobId']))
                            return joblist['maskingJobId']
            if i == 0:
                print("Error unable to find job id for jobname {} and environment {}".format(jobname,paramenvname))
        else:
            print("Error connecting engine {}".format(engine_name))

    def find_env_id(self, paramenvname, engine_name):
        apikey = self.get_auth_key(engine_name)
        i = 0
        if apikey is not None:
            apicall = "environments?page_number=1&page_size=999"
            envlist_response = self.get_api_response(engine_name, apikey, apicall)
            for envname in envlist_response['responseList']:
                if envname['environmentName'] == paramenvname:
                    i = 1
                    #print_debug("env id = {}".format(envname['environmentId']))
                    return envname['environmentId']
            if i == 0:
                print(" Error: unable to find env id for environment {}".format(paramenvname))
                return None
        else:
            print("Error connecting engine {}".format(engine_name))

    def find_env_name(self, paramenvnid, engine_name):
        apikey = self.get_auth_key(engine_name)
        if apikey is not None:
            apicall = "environments/{}".format(paramenvnid)
            envlist_response = self.get_api_response(engine_name, apikey, apicall)
            return envlist_response['environmentName']
        else:
            print("Error connecting engine {}".format(engine_name))

    def find_env_purpose(self, paramenvnid, engine_name):
        apikey = self.get_auth_key(engine_name)
        if apikey is not None:
            apicall = "environments/{}".format(paramenvnid)
            envlist_response = self.get_api_response(engine_name, apikey, apicall)
            return envlist_response['purpose']
        else:
            print("Error connecting engine {}".format(engine_name))

    def find_appid_of_envid(self, paramenvnid, engine_name):
        apikey = self.get_auth_key(engine_name)
        i = 0
        if apikey is not None:
            apicall = "environments/{}".format(paramenvnid)
            envlist_response = self.get_api_response(engine_name, apikey, apicall)
            return envlist_response['applicationId']
        else:
            print("Error connecting engine {}".format(engine_name))

    def find_app_id(self, paramappname, engine_name):
        apikey = self.get_auth_key(engine_name)
        i = 0
        if apikey is not None:
            apicall = "applications?page_number=1&page_size=999"
            applist_response = self.get_api_response(engine_name, apikey, apicall)
            for appname in applist_response['responseList']:
                if appname['applicationName'] == paramappname:
                    i = 1
                    #print_debug("app id = {}".format(appname['applicationId']))
                    return appname['applicationId']
            if i == 0:
                print("Error unable to find app id for application {}".format(paramappname))
        else:
            print("Error connecting engine {}".format(engine_name))

    def find_app_name(self, paramappid, engine_name):
        apikey = self.get_auth_key(engine_name)
        i = 0
        if apikey is not None:
            apicall = "applications/{}".format(paramappid)
            applist_response = self.get_api_response(engine_name, apikey, apicall)
            try:
                return applist_response['applicationName']
            except:
                return None
        else:
            print("Error connecting engine {}".format(engine_name))

    def find_env_id_by_conn_id(self, paramconnid, paramconntype, engine_name, srcapikey):
        apikey = srcapikey
        i = 0
        if apikey is not None:
            if paramconntype.lower() == "database":
                apicall = "database-connectors/{}".format(paramconnid)
            elif paramconntype.lower() == "file":
                apicall = "file-connectors/{}".format(paramconnid)

            try:
                conn_response = self.get_api_response(engine_name, apikey, apicall)
                env_id = conn_response['environmentId']
                return env_id

            except Exception as e:
                print(" Error unable to find env id for connector id {}".format(paramconnid))
                return None
        else:
            print("Error connecting engine {}".format(engine_name))

    def find_conn_name_by_conn_id(self, paramconnid, paramconntype, engine_name, srcapikey):
        apikey = srcapikey
        i = 0
        if apikey is not None:
            if paramconntype.lower() == "database":
                apicall = "database-connectors/{}".format(paramconnid)
            elif paramconntype.lower() == "database":
                apicall = "file-connectors/{}".format(paramconnid)

            try:
                conn_response = self.get_api_response(engine_name, apikey, apicall)
                conn_name = conn_response['connectorName']
                return conn_name

            except Exception as e:
                print(" Error unable to find connector Name for connector id {}".format(paramconnid))
                return None
        else:
            print("Error connecting engine {}".format(engine_name))

    def find_connid_by_name(self, paramconnname, paramconntype, engine_name, srcapikey, src_env_id):
        apikey = srcapikey
        print_debug("{},{},{},{}".format(paramconnname, paramconntype, engine_name, src_env_id))
        print_debug("apikey={}".format(apikey))
        try:
            syncobjapicall = "{}-connectors?page_number=1&page_size=999&environment_id={}".format(paramconntype, src_env_id)
            print_debug("syncobjapicall: {}".format(syncobjapicall))
            syncobjapicallresponse = self.get_api_response(engine_name, apikey, syncobjapicall)
            #print("syncobjapicallresponse: {}".format(syncobjapicallresponse))
            print_debug(syncobjapicallresponse)
            for connobj in syncobjapicallresponse['responseList']:                
                print_debug(connobj)    
                conn_id = connobj["databaseConnectorId"]
                conn_name = connobj["connectorName"]
                print_debug("conn_name:{},paramconnname:{},conn_id:{}".format(conn_name,paramconnname,conn_id))
                if conn_name == paramconnname:
                    print_debug("conn_id:{},paramconnname:{}".format(conn_id,paramconnname))
                    return conn_id           
                    
        except Exception as e:
            print("   Unable to pull {} connector data".format(paramconntype))
            print_debug(e)        
        #print(" ")            

    def find_user_id(self, paramusername, engine_name):
        apikey = self.get_auth_key(engine_name)
        if apikey is not None:
            apicall = "users"
            userlist_response = self.get_api_response(engine_name, apikey, apicall)
            print_debug("userlist_response = {}".format(userlist_response))
            for user_rec in userlist_response['responseList']:
                print_debug("user_rec = {}".format(user_rec))
                if user_rec['userName'] == paramusername:
                    return user_rec['userId']
        else:
            print("Error connecting engine {}".format(engine_name))
            return 0

    def upd_job_connector(self, jobid, srcconn_name, conn_type, src_env_name, engine_name, tgt_env_name,srcconnectorEnvappname):
        return_status = 1
        apikey = self.get_auth_key(engine_name)
        print_debug("src_env_name = {}".format(src_env_name))
        print_debug("tgt_env_name = {}".format(tgt_env_name))
        src_env_id = self.find_env_id(src_env_name, engine_name)
        if src_env_id is None:
            print(" Source environment {} does not exists on masking engine {}. Please sync {} env for OTF jobs".format(src_env_name,engine_name,src_env_name))
            print(" Please sync environment {} first for syncing OTF jobs".format(src_env_name))
            print(" ")
            return_status = 1
            return return_status

            # Below can create ap and env but connector will still be missing.
            #cr_app_response = self.create_application(engine_name,srcconnectorEnvappname)
            #src_conn_app_id = cr_app_response['applicationId']
            #cr_env_response = self.create_environment(engine_name,src_conn_app_id,src_env_name)
            #src_env_id = cr_env_response['environmentId']

        print_debug("src_env_id on target engine = {}".format(src_env_id))

        newconnid = self.find_connid_by_name(srcconn_name, conn_type, engine_name, apikey, src_env_id)
        print_debug("newconnid = {}".format(newconnid))
        if apikey is not None:
            apicall = "masking-jobs/{}?page_number=1&page_size=999".format(jobid)
            print_debug("apicall: {}".format(apicall))
            mskjob_response = self.get_api_response(engine_name, apikey, apicall)
            print_debug("mskjob_response: {}".format(mskjob_response))
            mskjob_response['onTheFlyMaskingSource']['connectorId'] = newconnid
            print_debug("mskjob_response: {}".format(mskjob_response))

            res = self.put_api_response(engine_name, apikey, apicall, mskjob_response, port=80)
            print_debug("res: {}".format(res))
            print(" Job update complete")
            return_status = 0
            return return_status
        else:
            return_status = 1
            return return_status

    def del_env_byid(self, engine_name, env_id, apikey):
        if apikey is None:
            apikey = self.get_auth_key(engine_name)
        env_name = self.find_env_name(env_id,engine_name)
        delapicall = "environments/{}".format(env_id)
        delapiresponse = self.del_api_response(engine_name, apikey, delapicall)
        if delapiresponse is None:
            print(" Unable to delete environment {}".format(env_name))
        else:
            print(" Environment {} deleted successfully.".format(env_name))

    def del_app_byid(self, engine_name, app_id, apikey):
        if apikey is None:
            apikey = self.get_auth_key(engine_name)
        app_name = self.find_app_name(app_id,engine_name)
        delapicall = "applications/{}".format(app_id)
        delapiresponse = self.del_api_response(engine_name, apikey, delapicall)
        if delapiresponse is None:
            print(" Unable to delete application {}".format(app_name))
        else:
            print(" Application {} deleted successfully.".format(app_name))

    def del_users(self,src_engine_name,srcapikey):
        if srcapikey is None:
            self.get_auth_key(src_engine_name)
        syncobjapicall = "users?page_number=1&page_size=999"
        syncobjapicallresponse = self.get_api_response(src_engine_name, srcapikey, syncobjapicall)
        for userobj in syncobjapicallresponse['responseList']:
            src_user_id = userobj['userId']
            src_user_name = userobj['userName']
            print_debug("User = {},{}".format(src_user_id, src_user_name))
            if src_user_name != 'admin':
                delapicall = "users/{}".format(src_user_id)
                delapiresponse = self.del_api_response(src_engine_name, srcapikey, delapicall)
                if delapiresponse is None:
                    print(" Unable to delete User {}.".format(src_user_name))
                    i = 1
                else:
                    print(" User {} deleted successfully.".format(src_user_name))
                    # print(" ")

    def del_roles(self,src_engine_name,srcapikey):
        if srcapikey is None:
            self.get_auth_key(src_engine_name)
        syncobjapicall = "roles?page_number=1&page_size=999"
        syncobjapicallresponse = self.get_api_response(src_engine_name, srcapikey, syncobjapicall)
        for roleobj in syncobjapicallresponse['responseList']:
            src_role_id = roleobj['roleId']
            src_role_name = roleobj['roleName']
            print_debug("Role = {},{}".format(src_role_id, src_role_name))
            if src_role_name != "All Privileges":
                delapicall = "roles/{}".format(src_role_id)
                delapiresponse = self.del_api_response(src_engine_name, srcapikey, delapicall)
                if delapiresponse is None:
                    print(" Unable to delete Role {}.".format(src_role_name))
                    i = 1
                else:
                    print(" Role {} deleted successfully.".format(src_role_name))
                    # print(" ")

    def del_domains(self,src_engine_name,srcapikey):
        if srcapikey is None:
            self.get_auth_key(src_engine_name)
        syncobjapicall = "domains?page_number=1&page_size=999"
        syncobjapicallresponse = self.get_api_response(src_engine_name, srcapikey, syncobjapicall)
        for domainobj in syncobjapicallresponse['responseList']:
            src_domain_name = domainobj['domainName']
            print_debug("Domain = {}".format(src_domain_name))
            if src_domain_name not in ["LISTOFSYSTEMDOMAINS"]:
                delapicall = "domains/{}".format(src_domain_name)
                delapiresponse = self.del_api_response(src_engine_name, srcapikey, delapicall)
                if delapiresponse is None:
                    print(" Unable to delete Domain {}.".format(src_domain_name))
                    i = 1
                else:
                    print(" Domain {} deleted successfully.".format(src_domain_name))
                    # print(" ")

    def find_conn_details(self,otf_job_mappings,job_name,env_name):
        #print(" ")
        print_debug("{} {}".format(job_name,env_name))
        for jobrec in otf_job_mappings:      
            if job_name in jobrec.keys():
                env_rec = jobrec[job_name]
                if env_name in env_rec.keys():
                    print_debug("Match found")                
                    detail_rec = env_rec[env_name]
                    return detail_rec

    def test_connectors(self, engine_name, conn_type, test_scope, envname = None ):
        print(" TEST CONNECTORS ON MASKING ENGINE: {}".format(engine_name))   
        apikey = self.get_auth_key(engine_name)
        print_debug("apikey={}".format(apikey))
        if test_scope == "ENV":
            envid = self.find_env_id(envname, engine_name)
        
        if apikey is not None:
            print(" Test {} Connectors:".format(conn_type))
            try:
                syncobjapicall = "{}-connectors?page_number=1&page_size=999".format(conn_type)
                syncobjapicallresponse = self.get_api_response(engine_name, apikey, syncobjapicall)
                print_debug(syncobjapicallresponse)
                for connobj in syncobjapicallresponse['responseList']:                
                    print_debug(connobj)    
                    conn_envid = connobj["environmentId"]
                    if test_scope == "ENGINE":
                        tgt_envid = conn_envid
                    elif test_scope == "ENV":
                        tgt_envid = envid

                    if conn_envid == tgt_envid:
                        conn_id = connobj["{}ConnectorId".format(conn_type)]                
                        conn_name = connobj["connectorName"]
                        conn_envname = self.find_env_name(conn_envid, engine_name)                
                        
                        testapicall = "{}-connectors/{}/test".format(conn_type,conn_id)
                        payload = connobj
                        print_debug("payload={}".format(payload))
                        
                        try:
                            apiresponse = self.post_api_response(engine_name, apikey, testapicall, payload, port=80)
                            print_debug("apiresponse= {}".format(apiresponse))
                            if apiresponse['response'] == "Connection Succeeded":
                                print(" Env : {:35}, Connector : {:25} --> {}.".format(conn_envname,conn_name,apiresponse['response']))
                            else:
                                print(" Env : {:35}, Connector : {:25} --> {}.".format(conn_envname,conn_name,"Connection Failed"))
                        except Exception as e:
                            print(" Env : {:35}, Connector : {:25} --> {}.".format(conn_envname,conn_name,"Unable to test Connection"))
                            print_debug(e)
            except Exception as e:
                print(" Unable to pull {} connector data".format(conn_type))
                print_debug(e)        
            print(" ")            
            
        else:
            print(" Error connecting source engine {}".format(engine_name))
       
    # @track
    def list_eng_usage(self):
        if not self.mock:
            # Run this if its not mock run for demos
            self.pull_jobexeclist()
        engine_list = self.create_dictobj(self.enginelistfile)
        jobexec_list = self.create_dictobj(self.jobexeclistfile)
        enginecpu_list = self.create_dictobj(self.enginecpulistfile)

        self.add_debugspace()
        print_debug("enginecpu_list:{}".format(enginecpu_list))
        self.add_debugspace()

        engine_list = self.create_dictobj(self.enginelistfile)
        print_debug("engine_list:{}".format(engine_list))

        enginelist = []
        for engine in engine_list:
            engine_list_dict = collections.OrderedDict(ip_address=engine['ip_address'],
                                                       totalmb=int(engine['totalgb']) * 1024,
                                                       systemmb=int(engine['systemgb']) * 1024)
            enginelist.append(engine_list_dict)
        print_debug("engine_list:{}".format(engine_list))
        print_debug("enginelist:{}".format(enginelist))
        engine_list = enginelist

        engine_pool_for_job = engine_list
        print_debug("engine_pool_for_job:{}".format(engine_pool_for_job))

        bannertext = banner()

        if self.config.verbose or self.config.debug:
            print((colored(bannertext.banner_sl_box(text="Available Engine Pool:"), 'yellow')))
            print('{0:>1}{1:<35}{2:>20}{3:>20}'.format("", "Engine Name", "Total Memory(MB)", "System Memory(MB)"))
            for ind in engine_list:
                print('{0:>1}{1:<35}{2:>20}{3:>20}'.format(" ", ind['ip_address'], ind['totalmb'], ind['systemmb']))

        if self.config.verbose or self.config.debug:
            print((colored(bannertext.banner_sl_box(text="CPU Usage:"), 'yellow')))
            print('{0:>1}{1:<35}{2:>20}'.format("", "Engine Name", "Used CPU(%)"))
            for ind in enginecpu_list:
                print('{0:>1}{1:<35}{2:>20}'.format(" ", ind['ip_address'], ind['cpu']))

        print_debug('jobexec_list = {}'.format(jobexec_list))
        engineusage = self.group_job_mem_usage('ip_address', 'jobmaxmemory', jobexec_list)
        print_debug('engineusage = {}'.format(engineusage))
        if engineusage is None:
            print_debug("Creating empty list.")
            engineusage_od = []
            temporddict = {}
            for ind in engine_list:
                temporddict = collections.OrderedDict(ip_address=ind['ip_address'], totalusedmemory=0)
                engineusage_od.append(temporddict)
            print_debug(engineusage_od)
        else:
            engineusage_od = []
            temporddict = {}
            for row in engineusage:
                engineusage_od.append(collections.OrderedDict(row))

            # Add empty list for remaining engines [ not in jobexeclist ]
            print_debug('engine_list = \n{}'.format(engine_list))
            for ind in engine_list:
                i = 0
                for ind1 in engineusage:
                    if ind['ip_address'] == ind1['ip_address']:
                        i = 1
                if i == 0:
                    temporddict = collections.OrderedDict(ip_address=ind['ip_address'], totalusedmemory=0)
                    engineusage_od.append(temporddict)

        print_debug('engineusage_od = {}'.format(engineusage_od))

        if self.config.verbose or self.config.debug:
            print((colored(bannertext.banner_sl_box(text="Memory Usage:"), 'yellow')))
            print('{0:>1}{1:<35}{2:>20}'.format("", "Engine Name", "Used Memory(MB)"))
            for ind in engineusage_od:
                print('{0:>1}{1:<35}{2:>20}'.format(" ", ind['ip_address'], ind['totalusedmemory']))

        if self.config.verbose or self.config.debug:
            print((colored(bannertext.banner_sl_box(text="Engine Current Usage:"), 'yellow')))
            print('{0:>1}{1:<35}{2:>20}{3:>20}'.format("", "Engine Name", "Used Memory(MB)", "Used CPU(%)"))

        if len(enginecpu_list) != 0:
            engineusage = self.join_dict(engineusage_od, enginecpu_list, 'ip_address', 'cpu')
            self.add_debugspace()
            print_debug("engineusage:{}".format(engineusage))
            self.add_debugspace()
            if self.config.verbose or self.config.debug:
                for ind in engineusage:
                    print('{0:>1}{1:<35}{2:>20}{3:>20}'.format(" ", ind['ip_address'], ind['totalusedmemory'],
                                                               ind['cpu']))
        else:
            print("Handle this situation")

        self.add_debugspace()
        print_debug("enginecpu_list:{}".format(enginecpu_list))
        self.add_debugspace()
        print_debug('engineusage_od = \n{}\n'.format(engineusage_od))
        print_debug('enginecpu_list = \n{}\n'.format(enginecpu_list))
        print_debug('engineusage = \n{}\n'.format(engineusage))
