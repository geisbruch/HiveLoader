## Hadoop Hive Loader
This simple project enable us import data into hive automaticly
Now we are using it to import flume data into hive

## How use it

It's a very simple process that read the a configuration and look for a directory using a regex,
when found a file that match use other regex to get the partition information and load these data to hive

Example config:
[
    {
            "cron":"",  //Quartz cron expresion if not set runs once at start
            "filesFolder":"/tmp/nginx_access_log", //Folder to monitor
            "filesRegex":".*.snappy$", //Regex of valid files to import
            "hdfsUri":"localhost:8020", //namenode dir
            "hiveTable":"nginx_access_log", //hive tablename
            "hiveUrl":"localhost:10000", //hive thrift connection
            "partitionsFieldRegex":[
                    {  "name":"ds",  //partition name
                       "regex":".*(\\d{4})-(\\d{2})-(\\d{2})_(\\d{2})_(\\d{2})_(\\d{2})\\.(\\w+)\\..*", //Regex extract from file (It run over file to import and extract the data from this
                       "partition":"$1-$2-$3 $4:$5:$6" },
                    {  "name":"traffic",
                       "regex":".*(\\d{4})-(\\d{2})-(\\d{2})_(\\d{2})_(\\d{2})_(\\d{2})\\.(\\w+)\\..*",
                       "partition":"$7" 
                    }
            ]
    }
    
]


##To Do's
Well really are many to do's but the first will be do tests

##Contributions
Feel free to fork this repo.


