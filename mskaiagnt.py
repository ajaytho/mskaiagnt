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
#	Call this agent to run masking job manually or via scheduler.
#
# Command-line parameters:
#
#	-e		Environment Name of Masking job
#	-j		Masking Job Id
# ================================================================================

import collections
import os

import click

import mskai.globals as globals
from mskai.DxLogging import print_debug
from mskai.Virtualization import virtualization
from mskai.aimasking import aimasking
# from mskai.aimasking_pd import aimasking
from mskai.banner import banner


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


# add_engine
@cli.command()
@click.option('--mskengname', '-m', default='', prompt='Enter Masking Engine name',
              help='Masking Engine name')
@click.option('--totalgb', '-t', default='', prompt='Enter total memory in GB for masking engine',
              help='Total memory in GB for masking engine')
@click.option('--systemgb', '-s', default='', prompt='Enter system memory in GB for masking engine',
              help='System memory in GB for masking engine')
# @click.option('--mskaiagntuser','-u', default='', prompt='Enter Masking AI Agent Username',
#			help='Masking AI Agent Username for masking engine')
# @click.option('--enabled','-e', default='Y', prompt='Enable Masking Engine for pooling',
#			 type=click.Choice(['Y', 'N'], case_sensitive=True),
#			 help='Add Engine to Pool')
@pass_config
# def add_engine(config, mskengname, totalgb, systemgb, mskaiagntuser, enabled):
def add_engine(config, mskengname, totalgb, systemgb):
    """ This module will add engine to pool"""
    if config.verbose:
        click.echo('Verbose mode enabled')
        click.echo('mskengname = {0}'.format(mskengname))
        click.echo('totalgb = {0}'.format(totalgb))
        click.echo('systemgb = {0}'.format(systemgb))
    # click.echo('mskaiagntuser = {0}'.format(mskaiagntuser))
    # click.echo('enabled = {0}'.format(enabled))
    # mskai = aimasking(config, mskengname=mskengname, mskaiagntuser=mskaiagntuser, totalgb=totalgb, systemgb=systemgb, enabled=enabled)
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
@click.password_option('--password', '-p',
                       help='Masking mskaiagnt password to connect masking engines')
@pass_config
def pull_joblist(config, mskengname, password):
    """ This module will pull joblist from engine"""
    if config.verbose:
        click.echo('Verbose mode enabled')
        click.echo('mskengname = {0}'.format(mskengname))

    bannertext = banner()
    mybannero = bannertext.banner_sl_box_open(text="Artificial Intellegence.")
    mybannera = bannertext.banner_sl_box_addline(text="AI Agent for Delphix Masking Server")
    mybannerc = bannertext.banner_sl_box_close()
    print(mybannero)
    print(mybannera)
    print(mybannerc)

    mskai = aimasking(config, mskengname=mskengname, password=password)
    mskai.pull_joblist()


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
@click.password_option('--password', '-p',
              help='Masking mskaiagnt password to connect masking engines')
@pass_config
def sync_job(config, srcmskengname, srcenvname, srcjobname, tgtmskengname, tgtenvname, password):
    """ This module will sync particular job between 2 engines"""

    bannertext = banner()
    mybannero = bannertext.banner_sl_box_open(text="Artificial Intellegence.")
    mybannera = bannertext.banner_sl_box_addline(text="AI Agent for Delphix Masking Server")
    mybannerc = bannertext.banner_sl_box_close()
    print(mybannero)
    print(mybannera)
    print(mybannerc)

    if config.debug:
        globals.initialize()
        globals.debug = config.debug
        print_debug('Debug mode enabled')
        print_debug('Parameter jobid = {0}'.format(jobid))
        print_debug('envname = {0}'.format(envname))

    try:
        mskai = aimasking(config, srcmskengname=srcmskengname, srcenvname=srcenvname, srcjobname=srcjobname, tgtmskengname=tgtmskengname, tgtenvname=tgtenvname, password=password)
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
@click.password_option('--password', '-p',
                       help='Masking mskaiagnt password to connect masking engines')
@pass_config
def sync_env(config, srcmskengname, srcenvname, tgtmskengname, tgtenvname, password):
    """ This module will sync particular env between 2 engines"""

    bannertext = banner()
    mybannero = bannertext.banner_sl_box_open(text="Artificial Intellegence.")
    mybannera = bannertext.banner_sl_box_addline(text="AI Agent for Delphix Masking Server")
    mybannerc = bannertext.banner_sl_box_close()
    print(mybannero)
    print(mybannera)
    print(mybannerc)

    if config.debug:
        globals.initialize()
        globals.debug = config.debug
        print_debug('Debug mode enabled')
        print_debug('Parameter jobid = {0}'.format(jobid))
        print_debug('envname = {0}'.format(envname))

    try:
        mskai = aimasking(config, srcmskengname=srcmskengname, srcenvname=srcenvname, tgtmskengname=tgtmskengname, tgtenvname=tgtenvname, password=password)
        mskai.sync_env()
    except Exception as e:
        print("Error in MSK module")
        print(str(e))
        return


# syncglobalobj
@cli.command()
@click.option('--srcmskengname', default='', prompt='Enter Source Masking Engine name',
              help='Source Masking Engine name')
@click.option('--tgtmskengname', default='', prompt='Enter Target Masking Engine name',
              help='Target Masking Engine name')
@click.password_option('--password', '-p',
                       help='Masking mskaiagnt password to connect masking engines')
@pass_config
def sync_globalobj(config, srcmskengname, tgtmskengname, password):
    """ This module will sync global objects between 2 engines"""

    bannertext = banner()
    mybannero = bannertext.banner_sl_box_open(text="Artificial Intellegence.")
    mybannera = bannertext.banner_sl_box_addline(text="AI Agent for Delphix Masking Server")
    mybannerc = bannertext.banner_sl_box_close()
    print(mybannero)
    print(mybannera)
    print(mybannerc)

    if config.debug:
        globals.initialize()
        globals.debug = config.debug
        print_debug('Debug mode enabled')
        print_debug('Parameter jobid = {0}'.format(jobid))
        print_debug('envname = {0}'.format(envname))
    return

# runjob
@cli.command()
@click.option('--jobname', '-n', default='', prompt='Enter Masking Job Name',
              help='Masking Job name from Masking Engine')
@click.option('--jobid', '-j', default='1', prompt='Enter Masking Job ID',
              help='Masking Job ID from Masking Engine')              
@click.option('--envname', '-e', default='mskenv', prompt='Enter Environment Name of Masking Job',
              help='Environment Name of Masking Job')
@click.option('--run', '-r', default=False, is_flag=True,
              help='Execute Job. In Absence display only decision')
@click.password_option('--password', '-p', default='mskenv',
                       help='Masking mskaiagnt password to connect masking engines')
@pass_config
def run_job(config, jobid, jobname, envname, run, password):
    """ This module will execute masking job on best candidate engine"""

    bannertext = banner()
    mybannero = bannertext.banner_sl_box_open(text="Artificial Intellegence.")
    mybannera = bannertext.banner_sl_box_addline(text="AI Agent for Delphix Masking Server")
    mybannerc = bannertext.banner_sl_box_close()
    print(mybannero)
    print(mybannera)
    print(mybannerc)

    if config.debug:
        globals.initialize()
        globals.debug = config.debug
        print_debug('Debug mode enabled')
        print_debug('Parameter jobid = {0}'.format(jobid))
        print_debug('envname = {0}'.format(envname))

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
        aive = virtualization(config, config_file_path='./dxtools.conf', scriptdir=scriptdir, outputdir=outputdir)
        aive.gen_cpu_file()
        print_debug("Capture CPU usage data : done")
    except:
        print("Error in VE module")
        return

    try:
        mskai = aimasking(config, jobid=jobid, jobname=jobname, envname=envname, run=run, password=password)
        mskai.run_job()
    except Exception as e:
        print("Error in MSK module")
        print(str(e))
        return


if __name__ == "__main__":
    cli()
