import collections
import csv
import json
import os
import sys
from collections import Counter
from csv import DictReader
from sys import exit

import requests
from termcolor import colored, cprint

import mskai.globals as globals
from mskai.DxLogging import print_debug
from mskai.banner import banner

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

    def gen_dxtools_csv_file(self,protocol="http"):
        f = open(globals.dxtools_file_csv, "w")
        f.write("{},{},{},{},{},{},{}\n".format('protocol','ip_address','password','port','username','default','hostname'))
        engine_list = self.create_dictobj(self.enginelistfile)
        for engine in engine_list:
            f.write("{},{},{},{},{},{},{}\n".format(protocol,engine['ip_address'],'delphix','80','admin','true',engine['ip_address']))
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
                print("No Engine found in pool".format(self.mskengname))
        except Exception as e:
            print_debug(str(e))
            print_debug("Not able to open file {}".format(self.enginelistfile))

    def del_engine(self):
        csvdir = self.outputdir
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

        api_url_base = 'http://{}:{}/masking/api/'.format(ip_address, port)
        headers = {'Content-Type': 'application/json'}
        api_url = '{0}login'.format(api_url_base)
        credentials = {"username": self.username, "password": self.password}
        # print_debug('{},{},{},{},{},{}'.format(ip_address,port,api_url_base,headers,api_url,credentials))
        try:
            response = requests.post(api_url, headers=headers, json=credentials)
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
        api_url_base = 'http://{}:{}/masking/api/'.format(ip_address, port)
        headers = {'Content-Type': 'application/json', 'Authorization': '{0}'.format(api_token)}
        api_url = '{0}{1}'.format(api_url_base, apicall)
        response = requests.get(api_url, headers=headers)
        if response.status_code == 200:
            data = json.loads(response.content.decode('utf-8'))
            return data
        else:
            print_debug(response.content.decode('utf-8'))
            return None

    def del_api_response(self, ip_address, api_token, apicall, port=80):
        api_url_base = 'http://{}:{}/masking/api/'.format(ip_address, port)
        headers = {'Content-Type': 'application/json', 'Authorization': '{0}'.format(api_token)}
        api_url = '{0}{1}'.format(api_url_base, apicall)
        response = requests.delete(api_url, headers=headers)
        if response.status_code == 200:
            data = response.content.decode('utf-8')
            return data
        else:
            print_debug(response.content.decode('utf-8'))
            return None

    def post_api_response(self, ip_address, api_token, apicall, body, port=80):
        api_url_base = 'http://{}:{}/masking/api/'.format(ip_address, port)
        headers = {'Content-Type': 'application/json', 'Authorization': '{0}'.format(api_token)}
        api_url = '{0}{1}'.format(api_url_base, apicall)
        response = requests.post(api_url, headers=headers, json=body)
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
        api_url_base = 'http://{}:{}/masking/api/'.format(ip_address, port)
        headers = {'Content-Type': 'application/json', 'Accept': 'application/json', 'Authorization': '{0}'.format(api_token)}
        api_url = '{0}{1}'.format(api_url_base, apicall)
        response = requests.post(api_url, headers=headers, json=body)
        #print(response)
        #data = json.loads(response.content.decode('utf-8'))
        if response.status_code == 200:
            data = json.loads(response.content.decode('utf-8'))
            return data
        elif response.status_code == 409:
            data = json.loads(response.content.decode('utf-8'))
            return data            
        else:
            print_debug(response.content.decode('utf-8'))
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
        print_debug("joblistunq:\n{}".format(joblistunq))
        jobreqlist = self.get_jobreqlist(joblistunq, self.jobname, self.envname)
        print_debug("jobreqlist:\n{}".format(jobreqlist))
        
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
            if self.config.verbose or self.config.debug:
                for ind in engineusage:
                    print('{0:>1}{1:<35}{2:>20}{3:>20}'.format(" ", ind['ip_address'], ind['totalusedmemory'],
                                                               ind['cpu']))
        else:
            print("Handle this situation")

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
                if apikey is not None:
                    apicall = "environments?page_number=1"
                    envlist_response = self.get_api_response(engine_name, apikey, apicall)

                    f = open(self.joblistfile, "a")

                    for envname in envlist_response['responseList']:
                        jobapicall = "masking-jobs?page_number=1&environment_id={}".format(envname['environmentId'])
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
                apicall = "environments?page_number=1"
                envlist_response = self.get_api_response(engine_name, apikey, apicall)
                f = open(self.joblistfile, "a")
                for envname in envlist_response['responseList']:
                    jobapicall = "masking-jobs?page_number=1&environment_id={}".format(envname['environmentId'])
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
                apicall = "environments?page_number=1"
                envlist_response = self.get_api_response(engine_name, apikey, apicall)
                for envname in envlist_response['responseList']:
                    print_debug("envname : {}".format(envname))
                    jobapicall = "masking-jobs?page_number=1&environment_id={}".format(envname['environmentId'])
                    joblist_response = self.get_api_response(engine_name, apikey, jobapicall)
                    joblist_responselist = joblist_response['responseList']
                    for joblist in joblist_responselist:
                        print_debug("joblist : {}".format(joblist))
                        fe = open(self.jobexeclistfile, "a")
                        jobexecapicall = "executions?job_id={}&page_number=1".format(joblist['maskingJobId'])
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
                syncobjapicall = "syncable-objects?page_number=1&object_type=FILE_FORMAT"
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
                syncobjapicall = "syncable-objects?page_number=1&object_type=MOUNT_INFORMATION"
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
            syncobjapicall = "syncable-objects?page_number=1&object_type={}".format(syncable_object_type)
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

    def call_sync_env(self):
        src_engine_name = self.srcmskengname
        tgt_engine_name = self.tgtmskengname
        src_env_name = self.srcenvname
        tgt_env_name = self.tgtenvname
        globalobjsync = self.globalobjsync

        if globalobjsync:
            self.sync_globalobj()

        src_env_id = self.find_env_id(src_env_name, src_engine_name)
        src_env_purpose = self.find_env_purpose(src_env_name, src_engine_name)
        src_app_id = self.find_appid_of_envid(src_env_id, src_engine_name)
        src_app_name = self.find_app_name(src_app_id, src_engine_name)

        tgtapikey = self.get_auth_key(tgt_engine_name)
        cr_app_response = self.create_application(tgt_engine_name, src_app_name)
        tgt_app_id = cr_app_response['applicationId']

        cr_env_response = self.create_environment(tgt_engine_name, tgt_app_id, tgt_env_name, src_env_purpose)
        tgt_env_id = cr_env_response['environmentId']

        print_debug(" Source Env Id = {}".format(src_env_id))
        print_debug(" Target App Id = {}".format(tgt_app_id))
        print_debug(" Target Env Id = {}".format(tgt_env_id))
        self.sync_env(src_engine_name,tgt_engine_name,src_env_name,tgt_env_name)

    def sync_env(self,src_engine_name,tgt_engine_name,src_env_name,tgt_env_name):

        src_env_id = self.find_env_id(src_env_name, src_engine_name)
        tgt_env_id = self.find_env_id(tgt_env_name, tgt_engine_name)


        srcapikey = self.get_auth_key(src_engine_name)
        if srcapikey is not None:
            syncobjapicall = "syncable-objects?page_number=1&object_type=ENVIRONMENT"
            syncobjapicallresponse = self.get_api_response(src_engine_name, srcapikey, syncobjapicall)
            for envobj in syncobjapicallresponse['responseList']:
                if envobj['objectIdentifier']['id'] == src_env_id:
                    envdef = []
                    envdef.append(envobj)                                        
                    srcapicall = "export"
                    srcapiresponse = self.post_api_response1(src_engine_name, srcapikey, srcapicall, envdef, port=80)
                    
                    tgtapikey = self.get_auth_key(tgt_engine_name)
                    tgtapicall = "import?force_overwrite=true&environment_id={}".format(tgt_env_id)
                    tgtapiresponse = self.post_api_response1(tgt_engine_name, tgtapikey, tgtapicall, srcapiresponse, port=80)                
                    print(" Environment synced successfully. Please update password for connectors in this environment using GUI / API")
    
        else:
            print (" Error connecting source engine {}".format(src_engine_name))

    def sync_eng(self):
        src_engine_name = self.srcmskengname
        tgt_engine_name = self.tgtmskengname          
        globalobjsync = self.globalobjsync
        globalobjsync = True
        srcapikey = self.get_auth_key(src_engine_name)
        print_debug("srcapikey={}".format(srcapikey))
        if srcapikey is not None:
            #AJAY UNCOMMENT THIS
            if globalobjsync:
                self.sync_globalobj()                    
            #print(" Test Connectors on {} :".format(tgt_engine_name))
            #self.test_dbconnectors(tgt_engine_name)
            # REMOVE ABOVE 2 LINES USED FOR TESTING            
            syncobjapicall = "syncable-objects?page_number=1&object_type=ENVIRONMENT"
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
                srcapicall = "export"
                #print_debug(src_engine_name, srcapikey, srcapicall, envdef, port=80)
                srcapiresponse = self.post_api_response1(src_engine_name, srcapikey, srcapicall, envdef, port=80)
                #print_debug("srcapiresponse={}".format(srcapiresponse))

                tgtapikey = self.get_auth_key(tgt_engine_name)
                cr_app_response = self.create_application(tgt_engine_name, src_app_name)
                tgt_app_id = cr_app_response['applicationId']

                cr_env_response = self.create_environment(tgt_engine_name, tgt_app_id, src_env_name, src_env_purpose)
                tgt_env_id = cr_env_response['environmentId']
                
                print_debug("Target Env Id = {}, Target App Id = {}".format(tgt_env_id, tgt_app_id))
                                
                tgtapicall = "import?force_overwrite=true&environment_id={}".format(tgt_env_id)
                tgtapiresponse = self.post_api_response1(tgt_engine_name, tgtapikey, tgtapicall, srcapiresponse, port=80)                
                print(" Environment {} synced successfully. Please update password for connectors in this environment using GUI / API".format(src_env_name))
                print(" ")
            print(" Test Connectors on {} :".format(tgt_engine_name))
            self.test_dbconnectors(tgt_engine_name)
    
        else:
            print (" Error connecting source engine {}".format(src_engine_name))

    def cleanup_eng(self):
        src_engine_name = self.mskengname       
        srcapikey = self.get_auth_key(src_engine_name)
        print_debug("srcapikey={}".format(srcapikey))
        if srcapikey is not None:
                           
            syncobjapicall = "environments?page_number=1"
            syncobjapicallresponse = self.get_api_response(src_engine_name, srcapikey, syncobjapicall)

            for envobj in syncobjapicallresponse['responseList']:
                src_env_id = envobj['environmentId']
                src_env_name = envobj['environmentName']
                print_debug("srcenv = {},{}".format(src_env_id,src_env_name))

                delapicall = "environments/{}".format(src_env_id)
                delapiresponse = self.del_api_response(src_engine_name, srcapikey, delapicall)              
                print(" Environment {} deleted successfully.".format(src_env_name))
                print(" ")
    
            syncobjapicall = "applications?page_number=1"
            syncobjapicallresponse = self.get_api_response(src_engine_name, srcapikey, syncobjapicall)
            for appobj in syncobjapicallresponse['responseList']:
                src_app_id = appobj['applicationId']
                src_app_name = appobj['applicationName']
                print_debug("srcapp = {},{}".format(src_app_id,src_app_name))

                delapicall = "applications/{}".format(src_app_id)
                delapiresponse = self.del_api_response(src_engine_name, srcapikey, delapicall)              
                print(" Application {} deleted successfully.".format(src_app_name))
                print(" ")

            print(" Engine {} cleanup completed.".format(src_engine_name))
            print(" ")            

        else:
            print(" Error connecting source engine {}".format(src_engine_name))
            
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

    def test_dbconnectors(self, src_engine_name):
        if src_engine_name is None:
            src_engine_name = self.mskengname       
        srcapikey = self.get_auth_key(src_engine_name)
        print_debug("srcapikey={}".format(srcapikey))
        if srcapikey is not None:
            print(" Test Database Connectors:")
            syncobjapicall = "database-connectors?page_number=1"
            syncobjapicallresponse = self.get_api_response(src_engine_name, srcapikey, syncobjapicall)
            print_debug(syncobjapicallresponse)
            for connobj in syncobjapicallresponse['responseList']:
                print_debug(connobj)                
                src_conn_id = connobj["databaseConnectorId"]
                src_conn_envid = connobj["environmentId"]
                src_conn_name = connobj["connectorName"]
                src_conn_envname = self.find_env_name(src_conn_envid, src_engine_name)                
                
                testapicall = "database-connectors/{}/test".format(src_conn_id)
                payload = connobj
                print_debug("payload={}".format(payload))
                if src_conn_name not in ["MF_CN1"]:
                    apiresponse = self.post_api_response(src_engine_name, srcapikey, testapicall, payload, port=80)
                    print_debug("apiresponse= {}".format(apiresponse))
                    if apiresponse['response'] == "Connection Succeeded":
                        print(" Env : {:20}, Connector : {:15} --> {}.".format(src_conn_envname,src_conn_name,apiresponse['response']))
                    else:
                        print(" Env : {:20}, Connector : {:15} --> {}.".format(src_conn_envname,src_conn_name,"Connection Failed"))
            print(" ")

            print(" Test File Connectors:")
            syncobjapicall = "file-connectors?page_number=1"
            syncobjapicallresponse = self.get_api_response(src_engine_name, srcapikey, syncobjapicall)
            print_debug(syncobjapicallresponse)
            for connobj in syncobjapicallresponse['responseList']:
                #print_debug("====================================================================================================")
                print_debug(connobj)                
                src_conn_id = connobj["fileConnectorId"]
                src_conn_envid = connobj["environmentId"]
                src_conn_name = connobj["connectorName"]
                src_conn_envname = self.find_env_name(src_conn_envid, src_engine_name)                
                
                testapicall = "file-connectors/{}/test".format(src_conn_id)
                payload = connobj
                print_debug("payload={}".format(payload))
                if src_conn_name not in ["MF_CN1"]:
                    apiresponse = self.post_api_response(src_engine_name, srcapikey, testapicall, payload, port=80)
                    print_debug("apiresponse= {}".format(apiresponse))
                    if apiresponse['response'] == "Connection Succeeded":
                        print(" Env : {:20}, Connector : {:15} --> {}.".format(src_conn_envname,src_conn_name,apiresponse['response']))
                    else:
                        print(" Env : {:20}, Connector : {:15} --> {}.".format(src_conn_envname,src_conn_name,"Connection Failed"))

            print(" Test Mainframe Connectors:")
            syncobjapicall = "mainframe-dataset-connectors?page_number=1"
            syncobjapicallresponse = self.get_api_response(src_engine_name, srcapikey, syncobjapicall)
            print_debug(syncobjapicallresponse)
            for connobj in syncobjapicallresponse['responseList']:
                print_debug(connobj)                
                src_conn_id = connobj["mainframe-datasetConnectorId"]
                src_conn_envid = connobj["environmentId"]
                src_conn_name = connobj["connectorName"]
                src_conn_envname = self.find_env_name(src_conn_envid, src_engine_name)                
                
                testapicall = "mainframe-dataset-connectors/{}/test".format(src_conn_id)
                payload = connobj
                print_debug("payload={}".format(payload))
                if src_conn_name not in ["MF_CN1"]:
                    apiresponse = self.post_api_response(src_engine_name, srcapikey, testapicall, payload, port=80)
                    print_debug("apiresponse= {}".format(apiresponse))
                    if apiresponse['response'] == "Connection Succeeded":
                        print(" Env : {:20}, Connector : {:15} --> {}.".format(src_conn_envname,src_conn_name,apiresponse['response']))
                    else:
                        print(" Env : {:20}, Connector : {:15} --> {}.".format(src_conn_envname,src_conn_name,"Connection Failed"))

            print(" ")
            
        else:
            print(" Error connecting source engine {}".format(src_engine_name))

    def sync_job(self):
        src_engine_name = self.srcmskengname
        tgt_engine_name = self.tgtmskengname
        src_env_name = self.srcenvname
        tgt_env_name = self.tgtenvname
        src_job_name = self.srcjobname
        src_env_id = self.find_env_id(src_env_name, src_engine_name)
        tgt_env_id = self.find_env_id(tgt_env_name, tgt_engine_name)
        src_job_id = self.find_job_id(src_job_name, src_env_name, src_engine_name)
        globalobjsync = self.globalobjsync
        print_debug("Src Env Id = {}, Tgt Env Id = {},Src Job Id = {}".format(src_env_id,tgt_env_id,src_job_id))
        
        srcapikey = self.get_auth_key(src_engine_name)        
        if srcapikey is not None:
            if globalobjsync:
                syncobjapicall = "syncable-objects?page_number=1&object_type=GLOBAL_OBJECT"
                syncobjapicallresponse = self.get_api_response(src_engine_name, srcapikey, syncobjapicall)
                for globalobj in syncobjapicallresponse['responseList']:
                    if globalobj['objectIdentifier']['id'] == "global":
                        globalobjdef = []
                        globalobjdef.append(globalobj)                                        
                        srcapicall = "export"
                        srcapiresponse = self.post_api_response1(src_engine_name, srcapikey, srcapicall, globalobjdef, port=80)
                        
                        tgtapikey = self.get_auth_key(tgt_engine_name)
                        tgtapicall = "import?force_overwrite=true"
                        tgtapiresponse = self.post_api_response1(tgt_engine_name, tgtapikey, tgtapicall, srcapiresponse, port=80)
                        if tgtapiresponse is None:
                            print(" Global Objects synced failed.")
                        else:
                            print(" Global Objects synced successfully.")
                            
            syncobjapicall = "syncable-objects?page_number=1&object_type=MASKING_JOB"
            syncobjapicallresponse = self.get_api_response(src_engine_name, srcapikey, syncobjapicall)
            for jobobj in syncobjapicallresponse['responseList']:
                if jobobj['objectIdentifier']['id'] == src_job_id:
                    
                    envdef = []
                    envdef.append(jobobj)                                        
                    srcapicall = "export"
                    srcapiresponse = self.post_api_response1(src_engine_name, srcapikey, srcapicall, envdef, port=80)
                    
                    tgtapikey = self.get_auth_key(tgt_engine_name)
                    tgtapicall = "import?force_overwrite=true&environment_id={}".format(tgt_env_id)
                    #print(tgtapikey)
                    #print(tgtapicall)
                    tgtapiresponse = self.post_api_response1(tgt_engine_name, tgtapikey, tgtapicall, srcapiresponse, port=80)                
                    #print(tgtapiresponse)
                    print(" Job synced successfully. Please update password for connectors of this job using GUI")

        else:
            print ("Error connecting source engine {}".format(src_engine_name))

    def find_job_id(self, jobname, paramenvname, engine_name):
        apikey = self.get_auth_key(engine_name)
        i = 0
        if apikey is not None:
            apicall = "environments?page_number=1"
            envlist_response = self.get_api_response(engine_name, apikey, apicall)
            for envname in envlist_response['responseList']:
                if envname['environmentName'] == paramenvname:
                    jobapicall = "masking-jobs?page_number=1&environment_id={}".format(envname['environmentId'])
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
            apicall = "environments?page_number=1"
            envlist_response = self.get_api_response(engine_name, apikey, apicall)
            for envname in envlist_response['responseList']:
                if envname['environmentName'] == paramenvname:
                    i = 1
                    #print_debug("env id = {}".format(envname['environmentId']))
                    return envname['environmentId']
            if i == 0:
                print("Error unable to find env id for environment {}".format(paramenvname))
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
            apicall = "applications?page_number=1"
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
            return applist_response['applicationName']
        else:
            print("Error connecting engine {}".format(engine_name))


    # @track
    def list_green_eng(self):
        if self.config.debug:
            print_debug("Parameter List:")
            print_debug("  username = {}".format(self.username))
        
        if not self.mock:
            # Run this if its not mock run for demos
            self.pull_jobexeclist()
        engine_list = self.create_dictobj(self.enginelistfile)
        job_list = self.create_dictobj(self.joblistfile)
        jobexec_list = self.create_dictobj(self.jobexeclistfile)
        enginecpu_list = self.create_dictobj(self.enginecpulistfile)

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

        bannertext = banner()
        print(" ")

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
            if self.config.verbose or self.config.debug:
                for ind in engineusage:
                    print('{0:>1}{1:<35}{2:>20}{3:>20}'.format(" ", ind['ip_address'], ind['totalusedmemory'],
                                                               ind['cpu']))
        else:
            print("Handle this situation")

        print_debug('engineusage_od = \n{}\n'.format(engineusage_od))
        print_debug('enginecpu_list = \n{}\n'.format(enginecpu_list))
        print_debug('engineusage = \n{}\n'.format(engineusage))

