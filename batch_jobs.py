import subprocess
import argparse
import os

parser = argparse.ArgumentParser()


parser.add_argument("--directory", type = str, help="directory to write output, I will add exp to it", default='/reg/data/ana03/scratch/smarches/tvdc/data')
parser.add_argument("--test", type = int, help="0 prints the command, 1 will execute", default=1)

parser.add_argument("--exp", type = str, help="experiment", default='xcsx1001121')
parser.add_argument("--runs", type = str, help="runs, e.g. 'range(155, 163)'", default='range(155,163)')
parser.add_argument("--abserrors", type = str, help="absolute errors, e.g. '[0, 1, 5, 10]'", default='[0, 1, 5, 10, 25, 50, 100, 150, 200]')
parser.add_argument("--pretransforms", type = str, help="pre transforms: e.g. '[0,1,2]': 0 Id, 1: log, 2: sqrt", default='[0]')

#parser.add_argument("--compressorE", type = int, help="compression abs error", default=0)


args = parser.parse_args()


for_real = args.test


exp = args.exp #'xcsx1001121'
dirname = args.directory+'/'+exp+''
runs = eval(args.runs) #range(155,163)
pretransforms = eval(args.pretransforms)# [0, 1, 2]
#error_list = [1, 5, 8, 10, 15, 20]
error_list= eval(args.abserrors) #[0, 1, 5, 10, 25, 50, 100, 150, 200]


#%%
que2 = 'psanaq'
#que2 = 'psfehq'



def batchSubmit(cmd, queue=que2, cores=1, log='%j.log', jobName=None, batchType='slurm', params=None):
    """
    Simplify batch jobs submission for lsf & slurm
    :param cmd: command to be executed as batch job
    :param queue: name of batch queue
    :param cores: number of cores
    :param log: log file
    :param batchType: lsf or slurm
    :return: commandline string
    """
    if batchType == "lsf":
        _cmd = "bsub -q " + queue + \
               " -n " + str(cores) + \
               " -o " + log
        if jobName:
            _cmd += " -J " + jobName
        _cmd += cmd
    elif batchType == "slurm":
        _cmd = "sbatch --partition=" + queue + \
               " --output=" + log
        if params is not None:
            for key, value in params.items():
                _cmd += " "+key+"="+str(value)
        else:
            _cmd += " --ntasks=" + str(cores)
        if jobName:
            _cmd += " --job-name=" + jobName
        _cmd += " --wrap=\"" + cmd + "\""
    return _cmd

#exp = 'cxic0415'


cmds=[]


emul = 10


for pre_transform_method in pretransforms:
#pre_transform_method = 1
 for AError in error_list:
  for ii in runs:

    # data is saved here:
    directory = f'{dirname}_{pre_transform_method}_{AError}'
    print('saving to folder',directory)
    if not os.path.isdir(directory):
            os.makedirs(directory)

    #output log is here:
    logf= f'{directory}/r{ii}.log'
    #logf= f'r{pre_transform_method}_{AError}_{ii}.log'

    #command is this:
    cmd = f'python producers/smd_producer.py --experiment {exp} --run {ii} --compressorE {AError} --directory  {directory}  --pretransform {pre_transform_method}'

    # job name (for squeue, limited in size):
    #jobN = f"{exp[-3:]}_{ii:03}"
    jobN = f"{pre_transform_method}{AError*emul:03}{(ii-100):02}"

    print("Submitting batch job: ", cmd, ', name', jobN)
    #process = subprocess.Popen(cmdb, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    cmdb = batchSubmit(cmd, log = logf, jobName = jobN)
    print("batch job command: ", cmdb)
    print("batch job name: ", jobN)

    if for_real:
       process = subprocess.Popen(cmdb, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
       out, err = process.communicate()
       cmds.append(cmdb)
       cmds.append(cmd)
     #print(cmdb, ii, cmd)

#print(cmds)



#cmd = batchSubmit(cmd, self.parent.hf.spiParam_queue, self.parent.hf.spiParam_cpus, runDir + "/%J.log",
#                                  "hit" + str(run), self.parent.batch)



#print("Submitting batch job: ", cmd, cmdb)
#process = subprocess.Popen(cmdb, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
#out, err = process.communicate()

#print('out',out,'err:',err)


#print('out',out)

