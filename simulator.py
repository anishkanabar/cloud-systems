import csv

class Trace():
    def __init__(self):
        self.all_jobs = []
        self.jobs_to_start = []
        self.completed_jobs = []
    def read_trace(self, data_file):
        '''Reads in the trace and outputs a list of jobs'''
        with open(data_file, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                j = Job()
                j.prediction_time = int(row['prediction_time'])
                j.actual_duration = int(row['actual_duration'])
                j.id = row['job_id']
                self.all_jobs.append(j)
                self.jobs_to_start.append(j)

class GPU():
    def __init__(self):
        self.id = ''
        self.state = 'Available'
        self.job = None

class Cluster:
    def __init__(self):
        self.n_gpu = None
        self.gpus = []
        self.gpus_available = []
    def get_gpus(self):
        '''Returns a list of available gpus'''
        for g in range(len(self.gpus)):
            if g.state == 'available':
                self.gpus_available.append(g)
        return self.gpus_available
    def __len__(self):
        return len(self.gpus_available)

class Job:
    def __init__(self):
        self.arrival_time = None
        self.id = None
        self.arrival_time = None
        self.start_time = None
        self.end_time = 1000
        self.actual_duration = 0
        self.prediction_time = None
        self.state = ' '
    def get_predicted_duration():
        '''Gets the job duration from the trace file'''
        pass
    
def write_to_csv(text):
    '''Opens and writes output to a csv file'''
    with open('output.csv', 'w') as f:
        f.write(str(text))

def scheduler(job, cluster,trace):
    '''Assigns a job to an available GPU'''
    if job is not None:
        job.arrival_time = counter
        job.state = 'Waiting'
        queue.append(job)
    for gpu in cluster.gpus:
        if gpu.state == 'Available':
            while len(queue) > 0:
                queue.sort(key=lambda x: x.prediction_time)
                job_to_run = queue[0]
                gpu.job = job_to_run
                gpu.state = 'Allocated'
                job_to_run.start_time = counter
                job_to_run.state = 'Running'
                job_to_run.end_time = job_to_run.arrival_time + (job_to_run.start_time - job_to_run.arrival_time) + job_to_run.actual_duration
                #print('gpu job ',gpu.job.id)
                #print('scheduler job ',gpu.job.id)
                #input()
                queue.pop(0)
                write_to_csv(job_to_run.start_time)
                break
    #print("end time ", gpu.job.end_time)

def print_cluster_job_info(cluster, trace):
    for g in cluster.gpus:
        gpu_state = g.state
        gpu_id = g.id
        try:
            job_id = g.job.id
        except:
            job_id = None
        output = gpu_state + ' ' + str(gpu_id) + ' ' + str(job_id)
        print('Cluster GPU state',output)
    for job in trace.all_jobs:
        job_id = job.id
        prediction_time = job.prediction_time
        output = job_id + ' ' + str(prediction_time) +  ' ' + job.state
        print('Cluster Job State',output)
    number_to_complete = len(trace.all_jobs) - len(trace.completed_jobs) 
    print('Jobs to complete:',number_to_complete)

if __name__ == "__main__":
    queue = []
    counter = 0
    data_file = 'sample_data.csv'
    trace = Trace()
    trace.read_trace(data_file)
    n_gpus = 5 # change to exact number
    gpus = []
    
    for n in range(n_gpus):
        g = GPU()
        g.id = n
        gpus.append(g)
    cluster = Cluster()
    cluster.n_gpus = n_gpus
    cluster.gpus = gpus

    #job_iter = iter(trace.jobs_to_complete)
    while len(trace.completed_jobs) < len(trace.all_jobs):
        if len(trace.jobs_to_start) > 0:
            incoming_job = trace.jobs_to_start[0]
            scheduler(incoming_job,cluster,trace)
            print('Job',incoming_job.id,'arrives')
            trace.jobs_to_start.pop(0)
        else:
            incoming_job = None
            scheduler(incoming_job,cluster,trace)
        #print('before update')
        #print_cluster_job_info(cluster, trace)
        #input()
        for gpu in cluster.gpus:
            # print('gpu job after scheduling',gpu.job)
            try:
                if gpu.job.end_time <= counter:
                    gpu.state = 'Available'
                    gpu.job.state = 'Completed'
                    trace.completed_jobs.append(gpu.job)
                    gpu.job = None
                    print(gpu.job.id, 'has completed')
                    
            except:
                #print('no gpu job is completed')
                print(' ')
        #print('after update')
        #print_cluster_job_info(cluster, trace)
        counter += 1
        print_cluster_job_info(cluster, trace)
        input()
