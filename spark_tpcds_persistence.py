import sys
import subprocess
import re
import time
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, HiveContext

hosts = ['vm2', 'vm3', 'vm4']

def get_conf(name):
    conf = (SparkConf().set('spark.app.name', name)
                       .set('spark.master', 'spark://10.0.1.86:7077')
                       .set('spark.driver.memory', '1g')
                       .set('spark.eventLog.enabled', 'true')
                       .set('spark.eventLog.dir', '/home/ubuntu/storage/logs')
                       .set('spark.executor.memory', '21g')
                       .set('spark.executor.cores', '4')
                       .set('spark.task.cpus', '1'))
    return conf

def get_network_bytes(host):
    """Get amount of data in bytes read/write over network in host."""

    output = subprocess.Popen(['ssh', 'ubuntu@' + host, 'ifconfig eth0'],
                              stdout=subprocess.PIPE).communicate()[0]
    rx_bytes = long(re.findall('RX bytes:([0-9]*) ', output)[0])
    tx_bytes = long(re.findall('TX bytes:([0-9]*) ', output)[0])

    return (rx_bytes, tx_bytes)

def get_storage_bytes(host):
    """Get amount of data read/written to disk on host."""

    write_bytes = long(subprocess.check_output(['ssh', 'ubuntu@' + host,
                          "awk '/vda1/ {print $10 * 512}' /proc/diskstats"]))
    read_bytes = long(subprocess.check_output(['ssh', 'ubuntu@' + host,
                         "awk '/vda1/ {print $6 * 512}' /proc/diskstats"]))

    return (read_bytes, write_bytes)

def bytes_to_mb(b):
    """Convert b in bytes to Mega Bytes (MB) and return."""

    return (b / (1024.0 * 1024.0))

def clear_cache(host):
    """Clear the memory cache on host."""

    subprocess.call(['ssh', 'ubuntu@' + host
                    ,'sudo sh -c "sync; echo 3 > /proc/sys/vm/drop_caches"'])

def clear_local_dir(host):
    """Clear the $SPARK_LOCAL_DIR on host."""

    subprocess.call(['ssh', 'ubuntu@' + host,
                     'rm -rf /home/ubuntu/storage/data/spark/rdds_shuffle/*'])

def parse_cmd_line_arg():
    """Get the run no. from the command-line."""

    try:
        return int(sys.argv[1])

    except KeyError:
        return 1

def main():
    """Run the query and print the statistics."""

    run = parse_cmd_line_arg()

    # Clear cache
    map(clear_cache, hosts)
    map(clear_local_dir, hosts)

    name = 'CS-838-Assignment2-Question2'
    sc = SparkContext(conf=get_conf(name))
    hc = HiveContext(sc)

    hc.sql('use tpcds_text_db_1_50')

    query12 = """
        select  i_item_desc
              ,i_category
              ,i_class
              ,i_current_price
              ,i_item_id
              ,sum(ws_ext_sales_price) as itemrevenue
              ,sum(ws_ext_sales_price)*100/sum(sum(ws_ext_sales_price)) over
                  (partition by i_class) as revenueratio
        from
                web_sales
                ,item
                ,date_dim
        where
                web_sales.ws_item_sk = item.i_item_sk
                and item.i_category in ('Jewelry', 'Sports', 'Books')
                and web_sales.ws_sold_date_sk = date_dim.d_date_sk
                and date_dim.d_date between '2001-01-12' and '2001-02-11'
        group by
                i_item_id
                ,i_item_desc
                ,i_category
                ,i_class
                ,i_current_price
        order by
                i_category
                ,i_class
                ,i_item_id
                ,i_item_desc
                ,revenueratio
        limit 100
        """

    query54 = """
        with my_customers as (
         select  c_customer_sk
                , c_current_addr_sk
         from
                ( select cs_sold_date_sk sold_date_sk,
                         cs_bill_customer_sk customer_sk,
                         cs_item_sk item_sk
                  from   catalog_sales
                  union all
                  select ws_sold_date_sk sold_date_sk,
                         ws_bill_customer_sk customer_sk,
                         ws_item_sk item_sk
                  from   web_sales
                 ) cs_or_ws_sales,
                 item,
                 date_dim,
                 customer
         where   sold_date_sk = d_date_sk
                 and item_sk = i_item_sk
                 and i_category = 'Jewelry'
                 and i_class = 'football'
                 and c_customer_sk = cs_or_ws_sales.customer_sk
                 and d_moy = 3
                 and d_year = 2000
                 group by  c_customer_sk
                , c_current_addr_sk
         )
         , my_revenue as (
         select c_customer_sk,
                sum(ss_ext_sales_price) as revenue
         from   my_customers,
                store_sales,
                customer_address,
                store,
                date_dim
         where  c_current_addr_sk = ca_address_sk
                and ca_county = s_county
                and ca_state = s_state
                and ss_sold_date_sk = d_date_sk
                and c_customer_sk = ss_customer_sk
                and d_month_seq between (1203)
                                   and  (1205)
         group by c_customer_sk
         )
         , segments as
         (select cast((revenue/50) as int) as segment
          from   my_revenue
         )
          select  segment, count(*) as num_customers, segment*50 as segment_base
         from segments
         group by segment
         order by segment, num_customers
         limit 100
         """

    # cache runs
    if run == 2 or run == 3:
        # tables in query12 used for collecting stats
        hc.cacheTable('web_sales')
        hc.cacheTable('item')
        hc.cacheTable('date_dim')

        # to circumvent lazy computation and force cache, we run a query
        # that involves the above cached tables
        if run == 2:
            # we will avoid running query12 now since we want to run
            # it below and collect stats
            # Instead, we run query54 which involves all the above 3
            # cached tables
            df = hc.sql(query54)

        # to force the caching of the outputRDD
        elif run == 3:
            # running the same query used to collect stats: query12
            # since we want to cache the output
            df = hc.sql(query12)
            df.cache()

        df.show()
        time.sleep(120)

    # record stats befor starting
    nw_before = map(get_network_bytes, hosts)
    st_before = map(get_storage_bytes, hosts)
    time_before = time.time()

    # actually run the query for collecting stastics
    hc.sql(query12).show()

    # record stat after completion
    time_after = time.time()
    nw_after = map(get_network_bytes, hosts)
    st_after = map(get_storage_bytes, hosts) 

    # calculate the difference in stats
    nw_read_hosti = 0
    nw_write_hosti = 0
    st_read_hosti = 0
    st_write_hosti = 0
    for i in range(len(hosts)):
        nw_read_hosti += nw_after[i][0] - nw_before[i][0]
        nw_write_hosti += nw_after[i][1] - nw_before[i][1]
        st_read_hosti += st_after[i][0] - st_before[i][0]
        st_write_hosti += st_after[i][1] - st_before[i][1]

    # output the stats
    print time_after - time_before
    print bytes_to_mb(nw_read_hosti)
    print bytes_to_mb(nw_write_hosti)
    print bytes_to_mb(st_read_hosti)
    print bytes_to_mb(st_write_hosti)

    sc.stop()

if __name__ == '__main__':
    main()
