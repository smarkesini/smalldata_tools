from libpressio import PressioCompressor
from timeit import default_timer as timer
from mpi4py import MPI


Verbose = 1 # 1 will print stats
eps = 1 # regularize the log-transform
rank = 0 # mpi rank

# 0: identity, 1: square_root, 2: log-transform
try:
    rank = MPI.COMM_WORLD.Get_rank()
except:
    rank = 0

# do a pre-transform ( 0: Identity, 1: log-transform, 2: sqrt transform) while keeping the sign
def pre_transform(data, direction = 1, pre_transform_method = 0):
    if direction == 1:
       if pre_transform_method == 0:
          return data
       elif pre_transform_method == 1:
          msk = np.sign(data)
          #print('np.sqrt(data*msk)*msk', msk)
          return np.sqrt(data*msk)*msk
       elif pre_transform_method == 2:
          msk = np.sign(data)
          return np.log(data*msk+eps)*msk
    elif direction == -1:
       if pre_transform_method == 0:
          return data
       elif pre_transform_method == 1:
          msk = np.sign(data)
          msk *= (data*data)
          return msk
       elif pre_transform_method == 2:
          msk = np.sign(data)
          return (np.exp(data*msk)+eps)*msk


lpconfig = {}



lpconfig = {
            "compressor_id": "sz3",
            "compressor_config": {
            'sz3:abs_error_bound': 1, # to do, determine this with psocake
            'sz3:metric': "size"
            }}



def compress_and_decompress(data, error_tol, pre_transform_method = 0, Verbose = Verbose):
    start=timer()

    # apply pretransform to error bound
    lpconfig['compressor_config']['sz3:abs_error_bound']=pre_transform(error_tol, pre_transform_method=pre_transform_method)

    compressor = PressioCompressor.from_config(lpconfig)
    # apply pretransform
    dcdata = pre_transform(data, pre_transform_method=pre_transform_method)
    # apply compression
    cdata  = compressor.encode(dcdata)

    # compression time
    ctime = timer()-start

    start = timer()
    # decompress
    dcdata = compressor.decode(cdata, dcdata)
    # inverse pretransform
    dcdata  = pre_transform(dcdata,  direction = -1,  pre_transform_method=pre_transform_method)


    dtime = timer()-start
    data_type_ratio = 0.5 # raw data is 16 bits, input is 32
    if rank == 0 and Verbose == 1:
       print(f'pretransform:{pre_transform_method}, Compression E {error_tol}, ratio: {data.nbytes/cdata.nbytes*data_type_ratio}, ctime {ctime}, dtime {dtime}, data type {data.dtype} ')

    return dcdata

'''

if compressor == 'qoz':
       pressio_opts = {"pressio:abs": absError, "qoz":{'qoz:stride': 8} }
elif compressor == 'sz3':
       pressio_opts = {"pressio:abs": absError }
else:
    print(f'use sz3 or qoz as compressor, not {compressor}. Exiting')
    quit()



binsize = 2
lp_json={
	    "compressor_id": "pressio",
	    "early_config": {
		"pressio": {
		    "pressio:compressor": "roibin",
		    "roibin": {
		        "roibin:metric": "composite",
		        "roibin:background": "mask_binning",
		        "roibin:roi": "fpzip",
		        "background": {
		            "binning:compressor": "pressio",
		            "mask_binning:compressor": "pressio",
		            "pressio": {"pressio:compressor": compressor},
		        },
		        "composite": {"composite:plugins": ["size", "time", "input_stats", "error_stat"]},
		    },
		}
	    },
	    "compressor_config": {
		"pressio": {
		    "roibin": {
		        "roibin:roi_size": [roiWindowSize, roiWindowSize, 0],
		        "roibin:centers": None, # "roibin:roi_strategy": "coordinates",
		        "roibin:nthreads": 4,
		        "roi": {"fpzip:prec": 0},
		        "background": {
		            "mask_binning:mask": None,
		            "mask_binning:shape": [binSize, binSize, 1],
		            "mask_binning:nthreads": 4,
		            "pressio": pressio_opts,
		        },
		    }
		}
	    },
	    "name": "pressio",
	}
'''

