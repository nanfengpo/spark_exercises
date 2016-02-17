import plotly.plotly as py
import re
import subprocess
import time
import sys
import os
from plotly.graph_objs import *

#query_numbers = ["query12" , "query21", "query50", "query85"]
query = "query21"
hosts = ["vm2","vm3","vm4"]
configuration = ["0.02", "0.05", "0.1", "0.2", "0.4", "0.9"]
interface = "eth0"

def get_network_bytes(host):
    output = subprocess.Popen(['ssh','ubuntu@'+host,'ifconfig eth0'], stdout=subprocess.PIPE).communicate()[0]
    rx_bytes = long(re.findall('RX bytes:([0-9]*) ', output)[0])
    tx_bytes = long(re.findall('TX bytes:([0-9]*) ', output)[0])
    return (rx_bytes, tx_bytes)

def get_storage_bytes(host):
    write_bytes = long(subprocess.check_output(['ssh','ubuntu@'+host, "awk '/vda1/ {print $10 * 512}' /proc/diskstats"]))
    read_bytes = long(subprocess.check_output(['ssh','ubuntu@'+host, "awk '/vda1/ {print $6 * 512}' /proc/diskstats"]))
    return (read_bytes, write_bytes)

def clear_cache(host):
    subprocess.call(['ssh','ubuntu@'+host, 'sudo sh -c "sync; echo 3 > /proc/sys/vm/drop_caches"']) 

def start_thrift_server(confvalue):
    os.system("start-thriftserver.sh --master spark://10.0.1.86:7077 --driver-memory 1g --conf spark.eventLog.enabled=true --conf spark.eventLog.dir=/home/ubuntu/storage/logs --conf spark.executor.memory=21000m --conf spark.executor.cores=4 --conf spark.task.cpus=1  --conf spark.sql.shuffle.partitions=50 --conf spark.storage.memoryFraction="+confvalue+" &")
def stop_thrift_server():
    subprocess.call("stop-thriftserver.sh");

def start_job(namespace, query, conf):
	subprocess.check_output("/home/ubuntu/software/spark-1.5.0-bin-hadoop2.6/bin/beeline -u jdbc:hive2://10.0.1.86:10000/tpcds_text_db_1_50 -n ubuntu -p ignored -f /home/ubuntu/workload/hive-tpcds-tpch-workload/sample-queries-tpcds/"+query+".sql 2> /home/ubuntu/workload/hive-tpcds-tpch-workload/output/"+namespace+"_tpcds_"+query+"_"+conf+"_spark.out", shell=True)

def use_database(namespace):
    subprocess.check_output("/home/ubuntu/software/spark-1.5.0-bin-hadoop2.6/bin/beeline -u jdbc:hive2://10.0.1.86:10000/tpcds_text_db_1_50 -n ubuntu -p ignored -f /home/ubuntu/workload/hive-tpcds-tpch-workload/sample-queries-tpcds/query00.sql 2> /home/ubuntu/workload/hive-tpcds-tpch-workload/output/"+namespace+"_tpcds_query00_spark.out", shell=True)

def clear_local_dir(host):
    subprocess.call(['ssh','ubuntu@'+host, 'rm -rf /home/ubuntu/storage/data/spark/rdds_shuffle/*'])

def build_bar_trace(xaxis, yaxis, name):
    return Bar(x=xaxis, y=yaxis, name=name)

def plot_grouped_graph(trace1, file_name, plot_name, xaxis_name, yaxis_name):
    data = Data([trace1])
    layout = Layout(barmode='group', title=plot_name, xaxis=XAxis(title=xaxis_name), yaxis=YAxis(title=yaxis_name))
    fig = Figure(data=data, layout=layout)
    plot_url = py.plot(fig, filename=file_name, auto_open=False)

def executeJobs(namespace):
    nw_read = []
    nw_write = []
    st_write = []
    st_read = []
    query_exec_time = []
    for conf in configuration:
        
        stop_thrift_server()
        
        map(clear_cache,hosts)
        map(clear_local_dir,hosts)
        
        start_thrift_server(conf)
        time.sleep(15)
        
        print namespace + " "+query
        
        use_database(namespace)
        
        nw_before = list(map(get_network_bytes,hosts))
        st_before = list(map(get_storage_bytes,hosts))
        time_before = time.time()
        start_job(namespace,query,conf)
        #subprocess.check_output("/home/ubuntu/software/spark-1.5.0-bin-hadoop2.6/bin/beeline -u jdbc:hive2://10.0.1.86:10000/tpcds_text_db_1_50 -n ubuntu -p ignored -f /home/ubuntu/workload/hive-tpcds-tpch-workload/sample-queries-tpcds/query21.sql 2> /home/ubuntu/workload/hive-tpcds-tpch-workload/output/"+namespace+"_tpcds_"+query+"_"+conf+"_spark.out", shell=True)
        #subprocess.check_output([framework, query, namespace ])
        time_after = time.time()
        nw_after = list(map(get_network_bytes,hosts))
        st_after = list(map(get_storage_bytes,hosts))
        
        nw_read_hosti=0
        nw_write_hosti=0
        st_read_hosti=0
        st_write_hosti=0
        for i in range(len(hosts)):
            nw_read_hosti+=nw_after[i][0]-nw_before[i][0]
            nw_write_hosti+=nw_after[i][1]-nw_before[i][1]
            st_read_hosti+=st_after[i][0]-st_before[i][0]
            st_write_hosti+=st_after[i][1]-st_before[i][1]
        
        nw_read.append(bytesToMb(nw_read_hosti))
        nw_write.append(bytesToMb(nw_write_hosti))
        st_read.append(bytesToMb(st_read_hosti))
        st_write.append(bytesToMb(st_write_hosti))
        query_exec_time.append(time_after - time_before)
    
    print namespace + " " + query    
    print query_exec_time
    print nw_read
    print nw_write
    print st_read
    print st_write
    print "\n"
    return (nw_read,nw_write,st_read,st_write,query_exec_time) 
 
def bytesToMb(b): return (b/(1024.0*1024.0))
def main():
    namespace = sys.argv[1]
    
    print "Executing Spark jobs"
    #spark = executeJobs("/home/ubuntu/workload/hive-tpcds-tpch-workload/run_query_spark.sql", namespace)
    spark = executeJobs(namespace)
    #plot_grouped_graph(build_bar_trace(configuration, spark[4], "Spark"),  
    #"Project2_"+namespace+"_question_1Ba","Query Completion Time","spark.sql.shuffle.partitions","seconds")
    
if __name__ == '__main__':
    main()
