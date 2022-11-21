from datetime import datetime
import csv

class Trace():
    def __init__(self):
        self.jobs_list = []
    def read_trace(self, data_file):
        '''Reads in the trace and outputs a list of jobs'''
        pass 
        return self.jobs_list
    def __len__(self):
        return len(self.jobs_list)

class GPU():
    def __init__(self):
        self.id = None
        self.state = 'available'

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
        self.start_time =None
        self.end_time = None
        self.duration = None
    def get_predicted_duration():
        '''Gets the job duration from the trace file'''
        pass
    
def write_to_csv(text):
    '''Opens and writes output to a csv file'''
    with open('path/to/csv_file', 'w') as f:
        writer = csv.writer(f)
        writer.writerow(text)

def get_time():
    '''Gets the current time'''
    now = datetime.now()
    current_time = now.strftime("%H:%M:%S")
    return current_time
    
def scheduler(job, queue, cluster):
    '''Assigns a job to an available GPU'''
    if len(cluster) == 0:
        queue.append(job)
    else:
        queue.sort(key=lambda x: x.duration)
        gpu = cluster.gpus_available[0]
        gpu.state = 'allocated'
        c.gpus_available.remove(gpu)
        queue.remove(job)
        job.start_time = get_time()
        write_to_csv(job.start_time)
        job.end_time = job.arrival_time+job.start_time+job.job_duration
        counter += 1
        ### add something to end job and make gpu available again

if __name__ == "__main__":
    queue = []
    counter = 0
    data_file = ''
    t = Trace()
    t.read_trace(data_file)
    n_gpus = 6500 # change to exact number
    gpus = []
    
    for n in range(len(n_gpus)):
        g = GPU()
        gpus.append(g)
    c = Cluster()
    c.n_gpus = n_gpus
    c.gpus = gpus

    for u in range(len(t)):
        j = Job()
        scheduler(j, t, queue, c)

