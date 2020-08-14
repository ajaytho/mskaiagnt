# Author  : Ajay Thotangare
# Created : 05/11/2020
# Purpose : Masking AI agent to run masking job intellegently.
############################################################################
# Copyright and license:
#
#       Licensed under the Apache License, Version 2.0 (the "License"); you may
#       not use this file except in compliance with the License.
#
#       You may obtain a copy of the License at
#     
#               http://www.apache.org/licenses/LICENSE-2.0
#
#       Unless required by applicable law or agreed to in writing, software
#       distributed under the License is distributed on an "AS IS" basis,
#       WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
#       See the License for the specific language governing permissions and
#       limitations under the License.
#     
#       Copyright (c) 2020 by Delphix.  All rights reserved.
#
# Description:
#
#   Call this agent to run masking job manually or via scheduler.
#
# Command-line parameters:
#
#   -e      Environment Name of Masking job
#   -j      Masking Job Id
# ================================================================================
VERSION = "1.0.3"

import collections
import os

import click

import mskai.globals as globals
from mskai.DxLogging import print_debug
from mskai.Virtualization import virtualization
from mskai.aimasking import aimasking
# from mskai.aimasking_pd import aimasking
from mskai.banner import banner

try:
    if not os.path.exists('output'):
        os.makedirs('output')        
except Exception as e:
    print("Unable to create ./output directory in current folder")
    print(str(e))
    raise

class Config(object):
    def __init__(self):
        self.verbose = False
        self.debug = False


pass_config = click.make_pass_decorator(Config, ensure=True)


class OrderedGroup(click.Group):
    def __init__(self, name=None, commands=None, **attrs):
        super(OrderedGroup, self).__init__(name, commands, **attrs)
        #: the registered subcommands by their exported names.
        self.commands = commands or collections.OrderedDict()

    def list_commands(self, ctx):
        return self.commands


def print_banner():
    bannertext = banner()
    mybannero = bannertext.banner_sl_box_open(text="Artificial Intellegence.")
    mybannera = bannertext.banner_sl_box_addline(text="AI Agent for Delphix Masking Server")
    mybannerc = bannertext.banner_sl_box_close()
    #print(mybannero)
    print(mybannera)
    print(mybannerc)

# Common Options
# @click.group()
@click.group(cls=OrderedGroup)

@click.option('--verbose', '-v', is_flag=True)
@click.option('--debug', '-d', is_flag=True)
@pass_config
def cli(config, verbose, debug):
    if verbose:
        config.verbose = verbose
    if debug:
        config.debug = debug
    click.echo(VERSION)

# gen-dxtoolsconf
@cli.command()
@pass_config
def version(config):
    """ Script Version"""
    if config.verbose:
        click.echo('Verbose mode enabled')

    print_banner()
    click.echo('Script Version : {}'.format(VERSION))

# add_engine
@cli.command()
@click.option('--mskengname', '-m', default='', prompt='Enter Masking Engine name',
              help='Masking Engine name')
@click.option('--totalgb', '-t', default='', prompt='Enter total memory in GB for masking engine',
              help='Total memory in GB for masking engine')
@click.option('--systemgb', '-s', default='', prompt='Enter system memory in GB for masking engine',
              help='System memory in GB for masking engine')
# @click.option('--mskaiagntuser','-u', default='', prompt='Enter Masking AI Agent Username',
#           help='Masking AI Agent Username for masking engine')
# @click.option('--enabled','-e', default='Y', prompt='Enable Masking Engine for pooling',
#            type=click.Choice(['Y', 'N'], case_sensitive=True),
#            help='Add Engine to Pool')
@pass_config
# def add_engine(config, mskengname, totalgb, systemgb, mskaiagntuser, enabled):
def add_engine(config, mskengname, totalgb, systemgb):
    """ This module will add engine to pool"""

    print_banner()
    if config.debug:
        globals.initialize()
        globals.debug = config.debug

    if config.verbose:
        print_debug('Verbose mode enabled')

    globals.arguments['--debug'] = config.debug
    globals.arguments['--config'] = './dxtools.conf'

    mskai = aimasking(config, mskengname=mskengname, totalgb=totalgb, systemgb=systemgb)
    mskai.add_engine()


# list_engine
@cli.command()
@pass_config
def list_engine(config):
    """ This module will remove engine from pool"""
    if config.verbose:
        click.echo('Verbose mode enabled')

    mskai = aimasking(config, noparam='noparam')
    mskai.list_engine()


# del_engine
@cli.command()
@click.option('--mskengname', '-m', default='', prompt='Enter Masking Engine name',
              help='Masking Engine name')
@pass_config
def del_engine(config, mskengname):
    """ This module will remove engine from pool"""
    if config.verbose:
        click.echo('Verbose mode enabled')
        click.echo('mskengname = {0}'.format(mskengname))
    mskai = aimasking(config, mskengname=mskengname)
    mskai.del_engine()


# pulljoblist
@cli.command()
@click.option('--mskengname', '-m', default='all', prompt='Enter Masking Engine name',
              help='Masking Engine name')
@click.option('--username', '-u', prompt='Enter Masking username',
                       help='Masking mskaiagnt username to connect masking engines')
@click.password_option('--password', '-p',
                       help='Masking mskaiagnt password to connect masking engines')
@click.option('--protocol', default='http', prompt='Enter protocol http|https to access Masking Engines',
              help='http protocol')                       
@pass_config
def pull_joblist(config, mskengname, username, password, protocol):
    """ This module will pull joblist from engine"""
    if config.verbose or config.debug:
        click.echo('Verbose mode enabled')
        click.echo('mskengname = {0}'.format(mskengname))
        click.echo('username   = {0}'.format(username))
        click.echo('protocol   = {0}'.format(protocol))

    if config.debug:
        globals.initialize()
        globals.debug = config.debug

    print_banner()
    mskai = aimasking(config, mskengname=mskengname, username=username, password=password, protocol=protocol)
    mskai.pull_joblist()

# gen-dxtoolsconf
@cli.command()
@click.option('--protocol', default='http', prompt='Enter protocol http|https to access Masking Engines',
              help='http protocol')
@pass_config
def gen_dxtools_conf(config, protocol):
    """ This module will generate dxtools conf file for engine"""
    if config.verbose:
        click.echo('Verbose mode enabled')

    print_banner()
    mskai = aimasking(config, protocol=protocol)
    mskai.gen_dxtools_conf()

# syncjob
@cli.command()
@click.option('--srcmskengname', default='', prompt='Enter Source Masking Engine name',
              help='Source Masking Engine name')
@click.option('--srcenvname', default='', prompt='Enter Source Masking Engine env name',
              help='Source Masking Engine Environment name')
@click.option('--srcjobname', default='', prompt='Enter Source Masking Engine job name',
              help='Source Masking Engine Job name')              
@click.option('--tgtmskengname', default='', prompt='Enter Target Masking Engine name',
              help='Target Masking Engine name')
@click.option('--tgtenvname', default='', prompt='Enter Target Masking Engine env name',
              help='Target Masking Engine Environment name')
@click.option('--globalobjsync','-g', default=False, is_flag=True, prompt='Sync global Objects',
              help='Sync global Objects')              
@click.option('--username', '-u', prompt='Enter Masking username',
                       help='Masking mskaiagnt username to connect masking engines')
@click.password_option('--password', '-p',
                       help='Masking mskaiagnt password to connect masking engines')
@click.option('--protocol', default='http', prompt='Enter protocol http|https to access Masking Engines',
              help='http protocol')

@pass_config
def sync_job(config, srcmskengname, srcenvname, srcjobname, tgtmskengname, tgtenvname, globalobjsync, username, password, protocol):
    """ This module will sync particular job between 2 engines"""

    print_banner()
    if config.debug:
        globals.initialize()
        globals.debug = config.debug

    if config.verbose:
        print_debug('Verbose mode enabled')
        print_debug('srcmskengname = {0}'.format(srcmskengname))
        print_debug('srcenvname    = {0}'.format(srcenvname))
        print_debug('srcjobname    = {0}'.format(srcjobname))
        print_debug('tgtmskengname = {0}'.format(tgtmskengname))
        print_debug('globalobjsync = {0}'.format(globalobjsync))          
        print_debug('username      = {0}'.format(username))
        print_debug('protocol      = {0}'.format(protocol))

    try:
        mskai = aimasking(config, srcmskengname=srcmskengname, srcenvname=srcenvname, srcjobname=srcjobname, tgtmskengname=tgtmskengname, tgtenvname=tgtenvname, globalobjsync=globalobjsync, username=username, password=password, protocol=protocol)
        mskai.sync_job()
    except Exception as e:
        print("Error in MSK module")
        print(str(e))
        return


# syncenv
@cli.command()
@click.option('--srcmskengname', default='', prompt='Enter Source Masking Engine name',
              help='Source Masking Engine name')
@click.option('--srcenvname', default='', prompt='Enter Source Masking Engine env name',
              help='Source Masking Engine Environment name')
@click.option('--tgtmskengname', default='', prompt='Enter Target Masking Engine name',
              help='Target Masking Engine name')
@click.option('--tgtenvname', default='', prompt='Enter Target Masking Engine env name',
              help='Target Masking Engine Environment name')
@click.option('--globalobjsync','-g', default=False, is_flag=True, prompt='Sync global Objects',
              help='Sync global Objects')                       
@click.option('--username', '-u', prompt='Enter Masking username',
                       help='Masking mskaiagnt username to connect masking engines')
@click.password_option('--password', '-p',
                       help='Masking mskaiagnt password to connect masking engines')
@click.option('--protocol', default='http', prompt='Enter protocol http|https to access Masking Engines',
              help='http protocol')                       
@pass_config
def sync_env(config, srcmskengname, srcenvname, tgtmskengname, tgtenvname, globalobjsync, username, password, protocol):
    """ This module will sync particular env between 2 engines"""

    print_banner()
    if config.debug:
        globals.initialize()
        globals.debug = config.debug

    if config.verbose:
        print_debug('Verbose mode enabled')
        print_debug('srcmskengname = {0}'.format(srcmskengname))
        print_debug('srcenvname    = {0}'.format(srcenvname))
        print_debug('tgtmskengname = {0}'.format(tgtmskengname))
        print_debug('tgtenvname    = {0}'.format(tgtenvname))        
        print_debug('globalobjsync = {0}'.format(globalobjsync))        
        print_debug('username      = {0}'.format(username))
        print_debug('protocol      = {0}'.format(protocol))

    try:
        mskai = aimasking(config, srcmskengname=srcmskengname, srcenvname=srcenvname, tgtmskengname=tgtmskengname, tgtenvname=tgtenvname, globalobjsync=globalobjsync, username=username, password=password, protocol=protocol)
        mskai.call_sync_env()
    except Exception as e:
        print("Error in MSK module")
        print(str(e))
        return

# synceng
@cli.command()
@click.option('--srcmskengname', default='', prompt='Enter Source Masking Engine name',
              help='Source Masking Engine name')
@click.option('--tgtmskengname', default='', prompt='Enter Target Masking Engine name',
              help='Target Masking Engine name')
@click.option('--globalobjsync','-g', default=True, is_flag=True, prompt='Sync global Objects',
              help='Sync global Objects')                       
@click.option('--username', '-u', prompt='Enter Masking username',
                       help='Masking mskaiagnt username to connect masking engines')
@click.password_option('--password', '-p',
                       help='Masking mskaiagnt password to connect masking engines')
@click.option('--protocol', default='http', prompt='Enter protocol http|https to access Masking Engines',
              help='http protocol')                       
@pass_config
def sync_eng(config, srcmskengname, tgtmskengname, globalobjsync, username, password, protocol):
    """ This module will sync particular env between 2 engines"""

    print_banner()
    if config.debug:
        globals.initialize()
        globals.debug = config.debug

    if config.verbose:
        print_debug('Verbose mode enabled')
        print_debug('srcmskengname = {0}'.format(srcmskengname))
        print_debug('tgtmskengname = {0}'.format(tgtmskengname))    
        print_debug('globalobjsync = {0}'.format(globalobjsync))        
        print_debug('username      = {0}'.format(username))
        print_debug('protocol      = {0}'.format(protocol))
    globalobjsync = True
    try:
        mskai = aimasking(config, srcmskengname=srcmskengname, tgtmskengname=tgtmskengname, globalobjsync=globalobjsync, username=username, password=password, protocol=protocol)
        mskai.sync_eng()
    except Exception as e:
        print("Error in MSK module")
        print(str(e))
        return

# sync_globalobj
@cli.command()
@click.option('--srcmskengname', default='', prompt='Enter Source Masking Engine name',
              help='Source Masking Engine name')
@click.option('--tgtmskengname', default='', prompt='Enter Target Masking Engine name',
              help='Target Masking Engine name')
@click.option('--globalobjsync','-g', default=False, is_flag=True, prompt='Sync global Objects',
              help='Sync global Objects')                       
@click.option('--username', '-u', prompt='Enter Masking username',
                       help='Masking mskaiagnt username to connect masking engines')
@click.password_option('--password', '-p',
                       help='Masking mskaiagnt password to connect masking engines')
@click.option('--protocol', default='http', prompt='Enter protocol http|https to access Masking Engines',
              help='http protocol')                       
@pass_config
def sync_globalobj(config, srcmskengname, srcenvname, tgtmskengname, tgtenvname, globalobjsync, username, password, protocol):
    """ This module will sync global objects between 2 engines"""

    print_banner()
    if config.debug:
        globals.initialize()
        globals.debug = config.debug

    if config.verbose:
        print_debug('Verbose mode enabled')
        print_debug('srcmskengname = {0}'.format(srcmskengname))
        print_debug('tgtmskengname = {0}'.format(tgtmskengname))    
        print_debug('globalobjsync = {0}'.format(globalobjsync))        
        print_debug('username      = {0}'.format(username))
        print_debug('protocol      = {0}'.format(protocol))        

    try:
        mskai = aimasking(config, srcmskengname=srcmskengname, tgtmskengname=tgtmskengname, globalobjsync=globalobjsync, username=username, password=password, protocol=protocol)
        mskai.sync_globalobj()
    except Exception as e:
        print("Error in MSK module")
        print(str(e))
        return

# cleanup-eng
@cli.command()
@click.option('--mskengname', default='', prompt='Enter Source Masking Engine name',
              help='Source Masking Engine name')                    
@click.option('--username', '-u', prompt='Enter Masking username',
                       help='Masking mskaiagnt username to connect masking engines')
@click.password_option('--password', '-p',
                       help='Masking mskaiagnt password to connect masking engines')
@click.option('--protocol', default='http', prompt='Enter protocol http|https to access Masking Engines',
              help='http protocol')                       
@pass_config
def cleanup_eng(config, mskengname, username, password, protocol):
    """ This module will cleanup engine"""

    print_banner()
    if config.debug:
        globals.initialize()
        globals.debug = config.debug

    if config.verbose:
        print_debug('Verbose mode enabled')
        print_debug('mskengname = {0}'.format(mskengname))      
        print_debug('username      = {0}'.format(username))
        print_debug('protocol      = {0}'.format(protocol))

    try:
        mskai = aimasking(config, mskengname=mskengname, username=username, password=password, protocol=protocol)
        mskai.cleanup_eng()
    except Exception as e:
        print("Error in MSK module")
        print(str(e))
        return

# runjob
@cli.command()
@click.option('--jobname', '-j', default='', prompt='Enter Masking Job Name',
              help='Masking Job name from Masking Engine')         
@click.option('--envname', '-e', default='mskenv', prompt='Enter Environment Name of Masking Job',
              help='Environment Name of Masking Job')
@click.option('--run', '-r', default=False, is_flag=True,
              help='Execute Job. In Absence display only decision')
@click.option('--mock', '-m', default=False, is_flag=True,
              help='Mock run - just for demos')
@click.option('--username', '-u', prompt='Enter Masking username',
                       help='Masking mskaiagnt username to connect masking engines')              
@click.password_option('--password', '-p', default='mskenv',
                       help='Masking mskaiagnt password to connect masking engines')
@click.option('--protocol', default='http', prompt='Enter protocol http|https to access Masking Engines',
              help='http protocol')                       
@pass_config
def run_job(config, jobname, envname, run, mock, username, password, protocol):
    """ This module will execute masking job on best candidate engine"""

    print_banner()
    if config.debug:
        globals.initialize()
        globals.debug = config.debug

    if config.verbose:
        print_debug('Verbose mode enabled')
        print_debug('jobname  = {0}'.format(jobname))
        print_debug('envname  = {0}'.format(envname))
        print_debug('run      = {0}'.format(run))
        print_debug('mock     = {0}'.format(mock))
        print_debug('username = {0}'.format(username))
        print_debug('protocol      = {0}'.format(protocol))

    globals.arguments['--debug'] = config.debug
    globals.arguments['--config'] = './dxtools.conf'
    globals.arguments['--all'] = True
    globals.arguments['--engine'] = None
    globals.arguments['--logdir'] = './dx_skel.log'
    globals.arguments['--parallel'] = None
    globals.arguments['--poll'] = '10'
    globals.arguments['--version'] = False
    globals.arguments['--single_thread'] = True

    try:
        mskai = aimasking(config, jobname=jobname, envname=envname, run=run, mock=mock, username=username, password=password, protocol=protocol)
        mskai.pull_jobexeclist()
        chk_status = mskai.chk_job_running()
        #print("chk_status={}".format(chk_status))
        if chk_status != 0:
            print(" Job {} on Env {} is already running on engine {}. Please retry later".format(jobname, envname, chk_status))
            return
    except Exception as e:
        print("Error in MSK module")
        print(str(e))
        return

    try:
        print_debug("Capture CPU usage data...")
        scriptdir = os.path.dirname(os.path.abspath(__file__))
        outputdir = os.path.join(scriptdir, 'output')
        aive = virtualization(config, config_file_path='./dxtools.conf', scriptdir=scriptdir, outputdir=outputdir, protocol=protocol)
        aive.gen_cpu_file()
        print_debug("Capture CPU usage data : done")
    except:
        print("Error in VE module")
        return

    try:
        mskai = aimasking(config, jobname=jobname, envname=envname, run=run, mock=mock, username=username, password=password, protocol=protocol)
        mskai.run_job()
    except Exception as e:
        print("Error in MSK module")
        print(str(e))
        return

# list_green_eng
@cli.command()     
@click.option('--username', '-u', prompt='Enter Masking username',
                       help='Masking mskaiagnt username to connect masking engines')              
@click.password_option('--password', '-p', default='mskenv',
                       help='Masking mskaiagnt password to connect masking engines')
@click.option('--protocol', default='http', prompt='Enter protocol http|https to access Masking Engines',
              help='http protocol')                       
@pass_config
def list_green_eng(config, username, password, protocol):
    """ This module will find green engines"""

    print_banner()
    if config.debug:
        globals.initialize()
        globals.debug = config.debug

    if config.verbose:
        print_debug('Verbose mode enabled')
        print_debug('username = {0}'.format(username))
        print_debug('protocol      = {0}'.format(protocol))

    globals.arguments['--debug'] = config.debug
    globals.arguments['--config'] = './dxtools.conf'
    globals.arguments['--all'] = True
    globals.arguments['--engine'] = None
    globals.arguments['--logdir'] = './dx_skel.log'
    globals.arguments['--parallel'] = None
    globals.arguments['--poll'] = '10'
    globals.arguments['--version'] = False
    globals.arguments['--single_thread'] = True

    try:
        print_debug("Capture CPU usage data...")
        scriptdir = os.path.dirname(os.path.abspath(__file__))
        outputdir = os.path.join(scriptdir, 'output')
        aive = virtualization(config, config_file_path='./dxtools.conf', scriptdir=scriptdir, outputdir=outputdir, protocol=protocol)
        aive.gen_cpu_file()
        print_debug("Capture CPU usage data : done")
    except:
        print("Error in VE module")
        return

    try:
        mskai = aimasking(config, username=username, password=password, protocol=protocol)
        mskai.list_green_eng()
    except Exception as e:
        print("Error in MSK module")
        print(str(e))
        return

if __name__ == "__main__":
    cli()
