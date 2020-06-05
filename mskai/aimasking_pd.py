import collections
import csv
import json
import os
import sys
from collections import Counter
from csv import DictReader
from sys import exit

import pandas as pd
import requests
from termcolor import colored, cprint

import mskai.globals as globals
from mskai.DxLogging import print_debug
from mskai.banner import banner


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

        self.df_enginelist = pd.DataFrame()
        self.df_joblist = pd.DataFrame()
        self.df_jobexeclist = pd.DataFrame()
        self.df_joblistunq = pd.DataFrame()
        self.df_enginecpulist = pd.DataFrame()

        if "jobid" in kwargs.keys():
            self.jobid = kwargs['jobid']
        if "envname" in kwargs.keys():
            self.envname = kwargs['envname']
        if "run" in kwargs.keys():
            self.run = kwargs['run']
        if "password" in kwargs.keys():
            self.password = kwargs['password']
        if "mskengname" in kwargs.keys():
            self.mskengname = kwargs['mskengname']
        if "totalgb" in kwargs.keys():
            self.totalgb = kwargs['totalgb']
        if "systemgb" in kwargs.keys():
            self.systemgb = kwargs['systemgb']

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

    def filter_dict(self, mydictname, myjobid, myenvname):
        filtereddata = filter(lambda row: (str(myjobid) == row['jobid'] and myenvname == row['environmentname']),
                              mydictname)
        return list(filtereddata)

    def filter_dict_unqjoblist_unused(self, mydictname, myjobid, myenvname):
        filtereddata = mydictname
        newfiltereddata = []
        for row in filtereddata:
            del row['ip_address']
            newfiltereddata.append(row)
        filtereddatafinal1 = filter(lambda row: (str(myjobid) == row['jobid'] and myenvname == row['environmentname']),
                                    newfiltereddata)
        filtereddatafinal2 = list({v['jobid']: v for v in filtereddatafinal1}.values())
        filtereddataR = filtereddatafinal2
        return list(filtereddataR)

    def unqlist(self, mydict, ignore_field):
        return [dict(data) for data in
                sorted(set(tuple((key, value) for key, value in row.items() if key != ignore_field) for row in mydict))]

    def get_jobreqlist(self, mydictname, myjobid, myenvname):
        filtereddatafinal1 = filter(lambda row: (str(myjobid) == row['jobid'] and myenvname == row['environmentname']),
                                    mydictname)
        # filtereddatafinal2 = list({v['jobid']:v for v in filtereddatafinal1}.values())
        filtereddataR = filtereddatafinal1
        return list(filtereddataR)

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
        emptycpu = collections.OrderedDict([(emptyfield, '0')])
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
                res = {**item1, **emptycpu}
                mergedictlist.append(res)
        return mergedictlist

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

    def read_data_from_file(self, filename):
        rc = []
        with open(filename) as f:
            records = csv.DictReader(f)
            for row in records:
                rc.append(row)
        return rc

    def add_engine(self):
        csvdir = self.outputdir
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
        credentials = {"username": "admin", "password": "Admin-12"}
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

    def post_api_response(self, ip_address, api_token, apicall, body, port=80):
        api_url_base = 'http://{}:{}/masking/api/'.format(ip_address, port)
        headers = {'Content-Type': 'application/json', 'Authorization': '{0}'.format(api_token)}
        api_url = '{0}{1}'.format(api_url_base, apicall)
        response = requests.post(api_url, headers=headers, json=body)
        if response.status_code == 200:
            data = json.loads(response.content.decode('utf-8'))
            return data
        else:
            print_debug(response.content.decode('utf-8'))
            return None

    def exec_job(self, ip_address, api_token, jobid):
        jobpayload = {"jobId": jobid}
        data = self.post_api_response(ip_address, api_token, 'executions', jobpayload)
        return data

    def run_job(self):
        if self.config.debug:
            print_debug("Parameter List:")
            print_debug("  jobid   = {}".format(self.jobid))
            print_debug("  envname = {}".format(self.envname))
            print_debug("  run     = {}".format(self.run))
        # print_debug("  password= {}".format(self.password))
        csvdir = self.outputdir
        self.read_configs()

    def pull_joblist(self):
        csvdir = self.outputdir
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
        csvdir = self.outputdir

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
            engine_name = engine['ip_address']
            apikey = self.get_auth_key(engine_name)
            if apikey is not None:
                apicall = "environments?page_number=1"
                envlist_response = self.get_api_response(engine_name, apikey, apicall)
                for envname in envlist_response['responseList']:
                    jobapicall = "masking-jobs?page_number=1&environment_id={}".format(envname['environmentId'])
                    joblist_response = self.get_api_response(engine_name, apikey, jobapicall)
                    joblist_responselist = joblist_response['responseList']
                    for joblist in joblist_responselist:
                        fe = open(self.jobexeclistfile, "a")
                        jobexecapicall = "executions?job_id={}&page_number=1".format(joblist['maskingJobId'])
                        jobexeclist_response = self.get_api_response(engine_name, apikey, jobexecapicall)
                        jobexeclist_responselist = jobexeclist_response['responseList']
                        if jobexeclist_responselist != []:
                            latestexecid = max(jobexeclist_responselist, key=lambda ev: ev['executionId'])
                            if latestexecid['status'] == "RUNNING":
                                fe.write("{},{},{},{},{},{},{},{}\n".format(joblist['maskingJobId'], joblist['jobName'],
                                                                            joblist['maxMemory'], '0',
                                                                            envname['environmentId'],
                                                                            envname['environmentName'], engine_name,
                                                                            latestexecid['status']))
                        fe.close()
        print_debug("File {} successfully generated".format(self.jobexeclistfile))

    # @track
    def read_configs(self):
        # on windows
        # os.system('color')
        ####self.pull_jobexeclist()
        engine_list = self.create_dictobj(self.enginelistfile)
        job_list = self.create_dictobj(self.joblistfile)
        jobexec_list = self.create_dictobj(self.jobexeclistfile)
        enginecpu_list = self.create_dictobj(self.enginecpulistfile)

        self.df_enginelist = pd.read_csv(self.enginelistfile)
        self.df_enginelist['totalgb'] = self.df_enginelist['totalgb'] * 1024
        self.df_enginelist['systemgb'] = self.df_enginelist['systemgb'] * 1024
        self.df_enginelist.rename(columns={'totalgb': 'totalmb', 'systemgb': 'systemmb'}, inplace=True)

        enginelist = []
        for engine in engine_list:
            engine_list_dict = collections.OrderedDict(ip_address=engine['ip_address'],
                                                       totalmb=int(engine['totalgb']) * 1024,
                                                       systemmb=int(engine['systemgb']) * 1024)
            enginelist.append(engine_list_dict)
        print_debug("engine_list:\n{}".format(engine_list))
        print_debug("enginelist:\n{}".format(enginelist))
        engine_list = enginelist

        if os.path.exists(self.enginecpulistfile):
            self.df_enginecpulist = pd.read_csv(self.enginecpulistfile)
            if self.df_enginecpulist.empty:
                self.df_enginecpulist['cpu'] = (100 - self.df_enginecpulist['cpu'])

        self.df_joblist = pd.read_csv(self.joblistfile)
        self.df_jobexeclist = pd.read_csv(self.jobexeclistfile)

        self.df_joblistunq = self.df_joblist.drop_duplicates(
            subset=['jobid', 'jobname', 'jobmaxmemory', 'reservememory', 'environmentid', 'environmentname'],
            keep='first')
        job_requirement = self.df_joblistunq.query("environmentname == @self.envname and jobid == @self.jobid")
        jobmaxmemory = job_requirement['jobmaxmemory'].values[0]
        reservememory = job_requirement['reservememory'].values[0]

        bannertext = banner()
        print(" ")
        print((colored(bannertext.banner_sl_box(text="Requirements:"), 'yellow')))
        print(' Jobid     = {}'.format(self.jobid))
        print(' Env       = {}'.format(self.envname))
        print(' MaxMB     = {} MB'.format(jobmaxmemory))
        print(' ReserveMB = {} MB'.format(reservememory))
        print(' Total     = {} MB'.format(jobmaxmemory + reservememory))

        if self.config.verbose or self.config.debug:
            print((colored(bannertext.banner_sl_box(text="Available Engine Pool:"), 'yellow')))
            print('{0:>1}{1:<35}{2:>20}{3:>20}'.format("", "Engine Name", "Total Memory(MB)", "System Memory(MB)"))
            for ind in self.df_enginelist.index:
                print('{0:>1}{1:<35}{2:>20}{3:>20}'.format(" ", self.df_enginelist['ip_address'][ind],
                                                           self.df_enginelist['totalmb'][ind],
                                                           self.df_enginelist['systemmb'][ind]))

        print((colored(bannertext.banner_sl_box(text="CPU Usage:"), 'yellow')))
        print('{0:>1}{1:<35}{2:>20}'.format("", "Engine Name", "Used CPU(%)"))
        for ind in enginecpu_list:
            print('{0:>1}{1:<35}{2:>20}'.format(" ", ind['ip_address'], ind['cpu']))

        engineusage = self.df_jobexeclist.query("jobstatus == 'RUNNING'").groupby('ip_address')[
            'jobmaxmemory'].sum().reset_index(name="totalusedmemory")
        if engineusage.empty:
            engineusage = pd.DataFrame()
            engineusage = self.df_enginelist[['ip_address']].copy()
            engineusage['totalusedmemory'] = 0

        print((colored(bannertext.banner_sl_box(text="Memory Usage:"), 'yellow')))
        print('{0:>1}{1:<35}{2:>20}'.format("", "Engine Name", "Used Memory(MB)"))
        for ind in engineusage.index:
            print(
                '{0:>1}{1:<35}{2:>20}'.format(" ", engineusage['ip_address'][ind], engineusage['totalusedmemory'][ind]))
        # for ind in engineusage_od:
        #	print ('{0:>1}{1:<35}{2:>20}'.format(" ",ind['ip_address'],ind['totalusedmemory'] ))

        if self.config.verbose or self.config.debug:
            print((colored(bannertext.banner_sl_box(text="Engine Current Usage:"), 'yellow')))
            print('{0:>1}{1:<35}{2:>20}{3:>20}'.format("", "Engine Name", "Used Memory(MB)", "Used CPU(%)"))

        if self.df_enginecpulist.empty:
            engineusage['cpu'] = 0
        else:
            engineusage = pd.merge(engineusage, self.df_enginecpulist, on="ip_address", how="left").fillna(0)

        if self.config.verbose or self.config.debug:
            for ind in engineusage.index:
                print('{0:>1}{1:<35}{2:>20}{3:>20}'.format(" ", engineusage['ip_address'][ind],
                                                           engineusage['totalusedmemory'][ind],
                                                           engineusage['cpu'][ind]))

        if self.config.verbose or self.config.debug:
            print((colored(bannertext.banner_sl_box(text="Shortlisted Engines for running Job:"), 'yellow')))
            print('{0:>1}{1:<35}{2:>20}{3:>20}'.format("", "Engine Name", "Job ID", "Env Name"))

        engine_pool_for_job = self.df_joblist.query("environmentname == @self.envname and jobid == @self.jobid")
        if self.config.verbose or self.config.debug:
            for ind in engine_pool_for_job.index:
                print('{0:>1}{1:<35}{2:>20}{3:>20}'.format(" ", engine_pool_for_job['ip_address'][ind],
                                                           engine_pool_for_job['jobid'][ind],
                                                           engine_pool_for_job['environmentname'][ind]))

        # print((colored(bannertext.banner_sl_box(text="Result:"),'yellow')))
        jpd1 = pd.merge(engine_pool_for_job, self.df_enginelist, on="ip_address", how="left")
        jpd2 = pd.merge(jpd1, engineusage, on="ip_address", how="left").fillna(0)
        jpd2['availablemb'] = jpd2['totalmb'] - jpd2['systemmb'] - jpd2[
            'totalusedmemory'] - jobmaxmemory - reservememory

        qualified_engines = jpd2.query("availablemb > 0")
        unqualified_engines = jpd2.query("availablemb < 1")
        if qualified_engines.empty:
            redcandidate = unqualified_engines.groupby('ip_address')['availablemb'].max().reset_index(
                name="maxavailablememory")
            redcandidate['maxavailablememory'] = redcandidate['maxavailablememory'] + jobmaxmemory + reservememory
            if self.df_enginecpulist.empty:
                redcandidatewithcpu = redcandidate
                redcandidate['cpu'] = 0
            else:
                redcandidatewithcpu = pd.merge(redcandidate, self.df_enginecpulist, on="ip_address", how="left").fillna(
                    0)

            if self.config.verbose or self.config.debug:
                print((colored(bannertext.banner_sl_box(text="Red Engines:"), 'yellow')))
                print(colored(redcandidatewithcpu, 'red'))

            print("  All engines are busy. Running job# {} of environment {} may cause issues.".format(self.jobid,
                                                                                                       self.envname))
            print("  Existing jobs may complete after sometime and create additional capacity to execute new job.")
            print("  Please retry later.")
        else:
            if not unqualified_engines.empty:
                redcandidate = unqualified_engines.groupby('ip_address')['availablemb'].max().reset_index(
                    name="maxavailablememory")
                redcandidate['maxavailablememory'] = redcandidate['maxavailablememory'] + jobmaxmemory + reservememory
                if self.df_enginecpulist.empty:
                    redcandidatewithcpu = redcandidate
                    redcandidate['cpu'] = 0
                else:
                    redcandidatewithcpu = pd.merge(redcandidate, self.df_enginecpulist, on="ip_address",
                                                   how="left").fillna(0)

                if self.config.verbose or self.config.debug:
                    print((colored(bannertext.banner_sl_box(text="Red Engines:"), 'yellow')))
                    print(colored(redcandidatewithcpu, 'red'))
            bestcandidate = qualified_engines.groupby('ip_address')['availablemb'].max().reset_index(
                name="maxavailablememory")
            if self.df_enginecpulist.empty:
                bestcandidatedetails = bestcandidate
                bestcandidatedetails['cpu'] = 0
            else:
                bestcandidatedetails = pd.merge(bestcandidate, self.df_enginecpulist, on="ip_address",
                                                how="left").fillna(0)

            if self.config.verbose or self.config.debug:
                print((colored(bannertext.banner_sl_box(text="Green Engines:"), 'yellow')))
                print(colored(bestcandidatedetails, 'green'))
            print((colored(bannertext.banner_sl_box(text="Best Candidate:"), 'yellow')))
            print(" ")
            win_engine = bestcandidatedetails.iloc[bestcandidatedetails['maxavailablememory'].idxmax()]
            engine_name = win_engine['ip_address']
            engine_mem = win_engine['maxavailablememory']
            engine_cpu = win_engine['cpu']
            print(colored(
                " Engine : {} , Available Memory : {} MB ,  Available CPU : {}% ".format(engine_name, engine_mem,
                                                                                         engine_cpu), color='green',
                attrs=['reverse', 'blink', 'bold']))

            if self.run:
                apikey = self.get_auth_key(engine_name)
                # print(apikey)
                job_exec_response = self.exec_job(engine_name, apikey, self.jobid)
                if job_exec_response is not None:
                    if job_exec_response['status'] == 'RUNNING':
                        executionId = job_exec_response['executionId']
                        # print(colored(" Execution of Masking job# {} with execution ID {} on Engine {} is in progress".format(self.jobid,executionId,engine_name),'green'))
                        print_green_on_white = lambda x: cprint(x, 'blue', 'on_white')
                        print_green_on_white(
                            " Execution of Masking job# {} with execution ID {} on Engine {} is in progress".format(
                                self.jobid, executionId, engine_name))
                    else:
                        # print(colored(" Execution of Masking job# {} on Engine {} failed".format(self.jobid,engine_name),'red'))
                        print_red_on_white = lambda x: cprint(x, 'red', 'on_white')
                        print_red_on_white(
                            " Execution of Masking job# {} on Engine {} failed".format(self.jobid, engine_name))
                else:
                    print_red_on_white = lambda x: cprint(x, 'red', 'on_white')
                    print_red_on_white(
                        " Execution of Masking job# {} on Engine {} failed".format(self.jobid, engine_name))
            print(" ")
