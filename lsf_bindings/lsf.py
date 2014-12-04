#!/usr/bin/python

from pythonlsf import lsf
from pythonlsf.lsf import submit as jobRequest
from pythonlsf.lsf import submitReply as jobReply
import json
import time
import copy

jobRequestKeys = ['JobName', 'LogInShell', 'QueueName', 'ProjectName', 'OutputFile',\
                  'ErrorFile','MailUser', 'resReq', 'Command', 'RunLimit']

job_exit_info =  {
      0: "TERM_UNKNOWN: LSF cannot determine a termination reason",
      1: "TERM_PREEMPT: Job killed after preemption",
      2: "TERM_WINDOW: Job killed after queue run window closed",
      3: "TERM_LOAD: Job killed after load exceeds threshold",
      4: "TERM_OTHER: Member of a chunk job in WAIT state killed and requeued after being switched to another queue.",
      5: "TERM_RUNLIMIT: Job killed after reaching LSF run time limit",
      6: "TERM_DEADLINE: Job killed after deadline expires",
      7: "TERM_PROCESSLIMIT: Job killed after reaching LSF process limit",
      8: "TERM_FORCE_OWNER: Job killed by owner without time for cleanup",
      9: "TERM_FORCE_ADMIN: Job killed by root or LSF administrator without time for cleanup",
      10:"TERM_REQUEUE_OWNER: Job killed and requeued by owner",
      11:"TERM_REQUEUE_ADMIN: Job killed and requeued by root or LSF administrator",
      12:"TERM_CPULIMIT: Job killed after reaching LSF CPU usage limit",
      13:"TERM_CHKPNT: Job killed after checkpointing",
      14:"TERM_OWNER: Job killed by owner",
      15:"TERM_ADMIN: Job killed by root or LSF administrator",
      16:"TERM_MEMLIMIT: Job killed after reaching LSF memory usage limit",
      17:"TERM_EXTERNAL_SIGNAL: Job killed by a signal external to LSF",
      18:"TERM_RMS: Job exited from an RMS system error",
      19:"TERM_ZOMBIE: Job exited while LSF is not available",
      20:"TERM_SWAP: Job killed after reaching LSF swap usage limit",
      21:"TERM_THREADLIMIT: Job killed after reaching LSF thread limit",
      22:"TERM_SLURM: N/A",
      23:"TERM_BUCKET_KILL: Job killed with bkill -b",
      24:"TERM_CTRL_PID: N/A",
      25:"TERM_CWD_NOTEXIST: Current working directory is not accessible or does not exist on the execution host",
      26:"TERM_REMOVE_HUNG_JOB: Job removed from LSF",
      27:"TERM_ORPHAN_SYSTEM: The orphan job was automatically terminated by LSF",
                    }

_copied_attributes_ = {
    #used (renaming is opational_
    'avgMem':'mem_avg',
    'cpuTime':'cputime',
    'endTime':'end_time',
    'exitInfo':'exit_info',
    'exitStatus':'exit_status',
    'jobId':'id',  
    'jobPriority':'job_priority',
    'maxMem':'mem_max',
    'runTime':'run_time',
    'startTime':'start_time',
    'status':'status',
    'submitTime':'submit_time',
    'user':'user',
    
    #used only in this file
    'numExHosts':'numExHosts',
    'runRusage':'runRusage',
}
    
_not_copied_attributes_ = {
    #not used
    'acJobWaitTime':'acJobWaitTime',
    'acinfo':'acinfo',
    'aclMask':'aclMask',
    'additionalInfo':'additionalInfo',
    'adminAps':'adminAps',
    'adminFactorVal':'adminFactorVal',
    'appResReq':'appResReq',
    'aps':'aps',
    'brunJobTime':'brunJobTime',
    'brunJobUser':'brunJobUser',
    'chargedSAAP':'chargedSAAP',
    'clusterId':'clusterId',
    'combinedCpuFrequency':'combinedCpuFrequency',
    'combinedResReq':'combinedResReq',
    'counter':'counter',
    'cpuFactor':'cpuFactor',
    'cwd':'cwd',
    'detailReason':'detail_reason',
    'dstCluster':'dstCluster',
    'dstJobId':'dstJobId',
    'duration':'duration',
    'effectiveResReq':'effectiveResReq',
    'energyPolicy':'energyPolicy',
    'energyPolicyTag':'energyPolicyTag',
    'exHosts':'exHosts',
    'exceptMask':'exceptMask',
    'execCwd':'execCwd',
    'execHome':'execHome',
    'execRusage':'execRusage',
    'execUid':'execUid',
    'execUsername':'execUsername',
    'externalMsg':'externalMsg',
    'fromHost':'fromHost',
    'fwdTime':'fwdTime',
    'hostAffinity':'hostAffinity',
    'hostRusage':'hostRusage',
    'idleFactor':'idleFactor',
    'isInProvisioning':'isInProvisioning',
    'items':'items',
    'jName':'jName',
    'jRusageUpdateTime':'jRusageUpdateTime',
    'jStartExHosts':'jStartExHosts',
    'jStartNumExHosts':'jStartNumExHosts',
    'jType':'jType',
    'jobPid':'jobPid',
    'lastEvent':'lastEvent',
    'lastResizeTime':'lastResizeTime',
    'licenseNames':'licenseNames',
    'loadSched':'loadSched',
    'loadStop':'loadStop',
    'localClusterName':'localClusterName',
    'nIdx':'nIdx',
    'networkAlloc':'networkAlloc',
    'nextEvent':'nextEvent',
    'numExternalMsg':'numExternalMsg',
    'numLicense':'numLicense',
    'numReasons':'num_reasons',
    'num_network':'num_network',
    'numhRusages':'numhRusages',
    'numhostAffinity':'numhostAffinity',
    'outdir':'outdir',
    'parentGroup':'parentGroup',
    'port':'port',
    'predictedCpuFrequency':'predictedCpuFrequency',
    'predictedStartTime':'predictedStartTime',
    'qResReq':'qResReq',
    'reasons':'reasons',
    'reasonTb':'reasonTb',
    'reserveCnt':'reserveCnt',
    'reserveTime':'reserveTime',
    'resizeMax':'resizeMax',
    'resizeMin':'resizeMin',
    'resizeReqTime':'resizeReqTime',
    'rsvInActive':'rsvInActive',
    'serial_job_energy':'serial_job_energy',
    'srcCluster':'srcCluster',
    'srcJobId':'srcJobId',
    'subHomeDir':'subHomeDir', 
    'subcwd':'subcwd',
    'subreasons':'subreasons',
    'totalProvisionTime':'totalProvisionTime',
    'umask':'umask',
    'warningAction':'warningAction',
    'warningTimePeriod':'warningTimePeriod',
}

class JobInfo ():
    
    def _copy_submit_(self,lsf_job):
        self.__dict__['name']=lsf_job.submit.jobName
        self.__dict__['project_name']=lsf_job.submit.projectName
        self.__dict__['queue']=lsf_job.submit.queue
        self.__dict__['command']=lsf_job.submit.command
        self.__dict__['resreq']=lsf_job.submit.resReq
        self.__dict__['out_file']=lsf_job.submit.outFile
        self.__dict__['err_file']=lsf_job.submit.errFile
        self.__dict__['user_priority']=lsf_job.submit.userPriority
        self.__dict__['mail_user']=lsf_job.submit.mailUser
        self.__dict__['run_limits']= {
                'cpu_time'     : lsf_job.submit.rLimits[0], #in ms
                'file_size'    : lsf_job.submit.rLimits[1],
                'data_size'    : lsf_job.submit.rLimits[2],
                'open_files'   : lsf_job.submit.rLimits[6],
                'swap_mem'     : lsf_job.submit.rLimits[8],
                'wall_time'    : lsf_job.submit.rLimits[9], #in seconds
                'processes'    : lsf_job.submit.rLimits[10],
                'threads '     : lsf_job.submit.rLimits[11],
                                     }
        #rLimits's definition in lsbatch.h
        #define DEFAULT_RLIMIT     -1
        #define LSF_RLIMIT_CPU      0            /* cpu time in milliseconds */
        #define LSF_RLIMIT_FSIZE    1            /* maximum file size */
        #define LSF_RLIMIT_DATA     2            /* data size */
        #define LSF_RLIMIT_STACK    3            /* stack size */
        #define LSF_RLIMIT_CORE     4            /* core file size */
        #define LSF_RLIMIT_RSS      5            /* resident set size */
        #define LSF_RLIMIT_NOFILE   6            /* open files */
        #define LSF_RLIMIT_OPEN_MAX 7            /* (from HP-UX) */
        #define LSF_RLIMIT_VMEM     8            /* maximum swap mem */
        #define LSF_RLIMIT_SWAP     LSF_RLIMIT_VMEM
        #define LSF_RLIMIT_RUN      9            /* max wall-clock time limit */
        #define LSF_RLIMIT_PROCESS  10           /* process number limit */
        #define LSF_RLIMIT_THREAD   11           /* thread number limit (introduced in LSF6.0) */
        #define LSF_RLIM_NLIMITS    12           /* number of resource limits */


    _specially_copied_attributes_ = {'submit':_copy_submit_}
    
    #Job related information
    def __init__(self, lsf_job):
        for lsf_job_attribute, self_attribute in _copied_attributes_.iteritems():
            self.__dict__[self_attribute]=lsf_job.__getattribute__(lsf_job_attribute)
        for lsf_job_attribute, self_callback in self._specially_copied_attributes_.iteritems():
            self_callback(self,lsf_job)

def init():
    lsf.lsb_init('Filco')

def get_job_info(job_id=0L, job_name=None, user_name=None, queue_name=None, host_name=None, options=0 ):
    '''a wrapper for c lsf api: 
       int lsb_openjobinfo(
       LS_LONG_INT jobId, 
       char *jobName, 
       char *userName, 
       char *queueName, 
       char *hostName, 
       int options) '''
    if user_name == None:
        user_name = lsf.ALL_USERS
    if options == 0:
        options = lsf.ALL_JOB
        # ALL_JOB Select jobs matching any status, including unfinished jobs and recently finished jobs.
        # LSF batch remembers finished jobs within the CLEAN_PERIOD, as defined in the lsb.params file.
        # in /hpc/lsf/conf/lsbatch/minerva/configdir/lsb.params:
        # CLEAN_PERIOD=1800
        
        # CUR_JOB Return jobs that have not finished yet
        # DONE_JOB Return jobs that have finished recently.
        # PEND_JOB Return jobs that are in the pending status.
        # SUSP_JOB Return jobs that are in the suspended status.
        # LAST_JOB Return jobs that are submitted most recently.
        # JGRP_ARRAY_INFO Return job array information.

    count = lsf.lsb_openjobinfo(job_id, job_name, user_name, queue_name, host_name, options)
    jobs = read_all_jobs(count)
    lsf.lsb_closejobinfo()
    return jobs

def get_all_job_info():
    return get_job_info()

def get_job_info_id(job_ids):
    if isinstance(job_ids, list):
        jobs = []
        for job_id in job_ids:
            jobs = jobs + get_job_info(long(job_id),None,None,None,None,0)
        return jobs
    else:
        return get_job_info(long(job_ids),None,None,None,None,0)

def get_job_info_user(userName):
    return get_job_info(0, None, userName, None, None,0)

def read_all_jobs(count):
    jobs = []
    more = lsf.new_intp()
    while count > 0:
        jobp = lsf.lsb_readjobinfo(more)
        #create a copy of data
        job = JobInfo(jobp)
        jobs.append(job)
        #print job
        count = lsf.intp_value(more)
    lsf.delete_intp(more)
    return jobs

def submit_job(jobReqDict):
    # Reserved keys for jobRequestDict
    #
    # JobName, LogInShell, QueueName, ProjectName, OutputFile, ErrorFile, MailUser,
    # resReq, Command
    pass

    #Example Script
    #BSUB -J PTXXX_16M2.NGS.2_7_2b.WES
    #BSUB -W 72:00
    #BSUB -n 4
    #BSUB -q premium
    #BSUB -P acc_PBG
    #BSUB -o /sc/orga/projects/PBG/RUNRAFI/PTXXX_16M2/Processed/NGS.2_7_2b.WES/log.out
    #BSUB -e /sc/orga/projects/PBG/RUNRAFI/PTXXX_16M2/Processed/NGS.2_7_2b.WES/log.err
    #BSUB -u hardik.shah@mssm.edu
    #BSUB -R span[hosts=1]
    #BSUB -L /bin/bash



DEFAULT_FEATURES = ['total_jobs','pending_jobs','running_jobs','hosts/processers','threads','mem','mem_avg','pending_time_avg','pending_time']
_all = 'all_users'

def lsf_stats():
    jobs = get_all_job_info()
    user_stats = {}
    feature_stats = {}
    feature_stats['total_jobs']       = {'title':'Total Jobs','unit':'','users':{}}
    feature_stats['pending_jobs']     = {'title':'Pending Jobs','unit':'','users':{}}
    feature_stats['running_jobs']     = {'title':'Running Jobs','unit':'','users':{}}
    feature_stats['hosts/processers'] = {'title':'Processors','unit':'','users':{}}
    feature_stats['threads']          = {'title':'Threads','unit':'','users':{}}
    feature_stats['mem']              = {'title':'Instantaneous Memory Usage','unit':'GB','users':{}}
    feature_stats['mem_avg']          = {'title':'Average Memory Usage','unit':'GB','users':{}}
    feature_stats['pending_time_avg'] = {'title':'Average Waiting Time of Pending Jobs','unit':'min','users':{}}
    feature_stats['pending_time']     = {'title':'Total Waiting Time of Pending Jobs','unit':'s','users':{}}

    def create_user(user_stats, feature_stats, name):
        user_stats[name] = {}
        user_stats[name]['total_jobs'] = 0
        feature_stats['total_jobs']['users'][name] = 0
        user_stats[name]['pending_jobs'] = 0
        feature_stats['pending_jobs']['users'][name] = 0
        user_stats[name]['pending_time_avg'] = 0
        feature_stats['pending_time_avg']['users'][name] = 0
        user_stats[name]['running_jobs'] = 0
        feature_stats['running_jobs']['users'][name] = 0
        user_stats[name]['hosts/processers'] = 0
        feature_stats['hosts/processers']['users'][name] = 0
        user_stats[name]['threads'] = 0
        feature_stats['threads']['users'][name] = 0
        user_stats[name]['mem'] = 0
        feature_stats['mem']['users'][name] = 0
        user_stats[name]['mem_avg'] = 0
        feature_stats['mem_avg']['users'][name] = 0
        user_stats[name]['pending_time'] = 0
        feature_stats['pending_time']['users'][name] = 0
        
    create_user(user_stats, feature_stats, _all)
   
    def increment(feature_name, user, N=1):
        user_stats[_all][feature_name] = user_stats[_all][feature_name] + N
        feature_stats[feature_name]['users'][_all] = feature_stats[feature_name]['users'][_all] + N
        user_stats[user][feature_name] = user_stats[user][feature_name] + N
        feature_stats[feature_name]['users'][user] = feature_stats[feature_name]['users'][user] + N

    def pending_time_avg_min(name):
        new_avg = user_stats[_all]['pending_time'] / user_stats[_all]['pending_jobs'] / 60
        user_stats[_all]['pending_time_avg'] = int(new_avg)
        feature_stats['pending_time_avg']['users'][_all] = int(new_avg)
        
        new_avg = user_stats[name]['pending_time'] / user_stats[name]['pending_jobs'] / 60
        user_stats[name]['pending_time_avg'] = int(new_avg)
        feature_stats['pending_time_avg']['users'][name] = int(new_avg)

    for job in jobs:
        if job.user not in user_stats:
            create_user(user_stats,feature_stats,job.user)
        increment('total_jobs',job.user)
        if job.status == 1:
            #job is pending
            #if job is pending more that two weeks, ignore it
            if (time.time() - job.submit_time) < 1209600:
                increment('pending_jobs',job.user)
                increment('pending_time',job.user, int((time.time() - job.submit_time)))
                pending_time_avg_min(job.user)
        elif job.status ==4:
            #running jobs
            increment('running_jobs',job.user)
            increment('hosts/processers',job.user,job.numExHosts)
            increment('threads',job.user,job.runRusage.nthreads)
            increment('mem',job.user,int(job.runRusage.mem/1024))
            increment('mem_avg', job.user, int(job.mem_avg/1024))
    
    if not user_stats:
        return None
    return user_stats, feature_stats 


def print_csv_user_stats(user_stats, features, all=True, no_header = False):
    if not no_header:
        header = {
            'total_jobs':'total_jobs',
            'pending_jobs':'pending_jobs',
            'running_jobs':'running_jobs',
            'hosts/processers':'hosts/processers',
            'threads':'threads',
            'mem':'mem(GB)',
            'mem_avg':'mem_avg(GB)',
            'pending_time_avg':'pending_time_avg(min)',
            }
        print ','.join( ['user'] + [ header[x] for x in features ] )
    if all:
        for user, stats in user_stats.iteritems():
            print ','.join(   [ user ] + 
                              [ str(stats[x]) for x in features] )
    else:
        print ','.join(   [ _all ] + 
                          [ str(user_stats[_all][x]) for x in features] )
        
def print_json_user_stats(user_stats, features, all=True):
    import copy
    unselected_features = copy.deepcopy(DEFAULT_FEATURES)
    for feature in features:
        if feature in unselected_features: unselected_features.remove(feature)

    for user in user_stats.keys():
        for unselected_feature in unselected_features:
            user_stats[user].pop(unselected_feature,None)
    if all:
        print json.dumps(user_stats)
    else:
        print json.dumps({_all:user_stats[_all]})

def print_csv_feature_stats(feature_stats, features, all=True, no_header = False):
    if all:
        users = feature_stats['total_jobs']['users'].keys()
        users.remove('all_users')
        users.insert(0, 'all_users')
    else:
        users = ['all_users']
    if not no_header:
        print ','.join( ['feature'] + users)
    unselected_features = copy.deepcopy(DEFAULT_FEATURES)
    for feature in features:
        if feature in unselected_features: unselected_features.remove(feature)
    for unselected_feature in unselected_features:
        feature_stats.pop(unselected_feature,None)
    
    for feature in features:
        print ','.join( [feature] + [ str(feature_stats[feature]['users'][user]) for user in users] )

def print_json_feature_stats(feature_stats, features, all=True):
    unselected_features = copy.deepcopy(DEFAULT_FEATURES)
    for feature in features:
        if feature in unselected_features: unselected_features.remove(feature)
    for unselected_feature in unselected_features:
        feature_stats.pop(unselected_feature,None)
    if all:
        print json.dumps(feature_stats)
    else:
        for feature,data in feature_stats.iteritems():
            data['users'] = {_all:data['users'][_all]}
        print json.dumps(feature_stats)
     

def main():
    import argparse
    init()
    description = "get_lsf_stats: printout current loading information of LSF"
    ap = argparse.ArgumentParser(description=description)
    available_features = copy.deepcopy(DEFAULT_FEATURES)
    available_features.remove('pending_time')
    ap.add_argument('--feature', nargs='+', type=str, choices=available_features, default=available_features)
    ap.add_argument('--output_format', choices = ['json','csv'], default='json')
    ap.add_argument('--all', action = 'store_true')
    ap.add_argument('--no_header', action='store_true')
    ap.add_argument('--user-wise', action='store_true')
    args = ap.parse_args()  
    user_stats, feature_stats = lsf_stats()
    if args.user_wise:
        if args.output_format == 'json':
            print_json_user_stats(user_stats,args.feature,all=args.all)
        if args.output_format == 'csv':
            print_csv_user_stats(user_stats,args.feature,all=args.all,no_header=args.no_header)
    else:
        if args.output_format == 'json':
            print_json_feature_stats(feature_stats, args.feature,all=args.all)
        if args.output_format == 'csv':
            print_csv_feature_stats(feature_stats, args.feature, all=args.all, no_header=args.no_header)
    return 0

if __name__ == "__main__":
    main()
