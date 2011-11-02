package com.ml.hadoop;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URLDecoder;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.jdbc.HiveConnection;
import org.apache.hadoop.hive.jdbc.HiveStatement;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.processors.SetProcessor;
import org.apache.hadoop.hive.ql.security.HadoopDefaultAuthenticator;
import org.apache.hadoop.hive.ql.security.HiveAuthenticationProvider;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.service.HiveClient;
import org.apache.hadoop.hive.service.ThriftHive;
import org.apache.hadoop.hive.thrift.HadoopThriftAuthBridge;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.quartz.CronScheduleBuilder;
import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;
import org.quartz.impl.triggers.SimpleTriggerImpl;

public class HiveMigrator implements Job {
	private static Collection<HiveMigratorConf> configs = new HashSet<HiveMigratorConf>();
	private static String configFile;
	static Boolean isRunning = false;
	static Logger log = Logger.getLogger(HiveMigrator.class);
	private static String log4jFile = null;
	
	//It's created at scheduler execution time
	private HiveMigratorConf conf;
	
	public HiveMigrator(){
	}
	
	private void reloadConfig() throws FileNotFoundException, JSONException {
		JSONTokener t = new JSONTokener(new FileReader(configFile));
		JSONArray arr =(JSONArray) t.nextValue();
		configs.clear();
		for(int k=0; k< arr.length(); k++){
			JSONObject j = arr.getJSONObject(k);
			
			HiveMigratorConf conf = new HiveMigratorConf();
			conf.setCronExp(j.has("cron")?j.getString("cron"):null);
			conf.setFilesFolder(j.getString("filesFolder"));
			conf.setFilesRegex(j.getString("filesRegex"));
			conf.setHdfsUri(j.getString("hdfsUri"));
			conf.setHiveTable(j.getString("hiveTable"));
			conf.setHiveUrl(j.getString("hiveUrl"));
			
			if(j.has("userName")){
				conf.setUserName(j.getString("userName"));
				conf.setGroup(j.getString("group"));
			}
			
			JSONArray partitionsFieldRegex = j.getJSONArray("partitionsFieldRegex");
			//Use treemap for mantains the order
			Collection<PartitionGeneratorDto>  partitionGenerator= new ArrayList<PartitionGeneratorDto>();
			
			
			for(int i=0; i< partitionsFieldRegex.length() ; i++){
				JSONObject part = partitionsFieldRegex.getJSONObject(i);
				partitionGenerator.add(new PartitionGeneratorDto(part.getString("name"), Pattern.compile(part.getString("regex")), part.getString("partition")));
			}
			
			conf.setPartitionsGenerators(partitionGenerator);
			configs.add(conf);
		}
	}

	public static void main(String[] args) throws SchedulerException, ParseException, FileNotFoundException, JSONException {
		configFile = args[0];
		if(args.length>1)
			log4jFile = args[1];
		new HiveMigrator().run();
	}

	private void run() throws SchedulerException, ParseException, FileNotFoundException, JSONException {
		reloadConfig();
		
		log.info("Job scheduler starting");
		if(log4jFile!=null)
			PropertyConfigurator.configure(log4jFile);
		SchedulerFactory sf = new StdSchedulerFactory();
		int i = 0;
		for(HiveMigratorConf conf : configs){
			Scheduler sched = sf.getScheduler();
			JobDataMap data = new JobDataMap();
			data.put("config", conf);
			
			JobDetail job =  JobBuilder.newJob(HiveMigrator.class)
		    .withIdentity("Hive Migrator [Instance: "+(++i)+"]")
		    .usingJobData(data)
		    .build();
			
			
			Trigger trigger;
			if(conf.getCronExp() != null && !"".equals(conf.getCronExp())){
				log.info("Using cron expression ["+conf.cronExp+"]");
				trigger = TriggerBuilder.newTrigger()
			    .withIdentity("Dejavu to Hadoop Trigger [Instance: "+(++i)+"]")
			    .withSchedule(CronScheduleBuilder.cronSchedule(conf.cronExp))
			    .build();
			}else{
				trigger = new SimpleTriggerImpl("Trigger [Instance: "+(++i)+"]", null, new Date(), null, 0, 0L);
	
			}
			sched.scheduleJob(job, trigger);
			sched.start();
			log.info("Job scheduler started");
		}
		
	}

	@Override
	public void execute(JobExecutionContext context)
			throws JobExecutionException {
		try {
			this.conf = (HiveMigratorConf)context.getJobDetail().getJobDataMap().get("config");
		} catch (Exception e){
			throw new JobExecutionException("Error reloading conf",e);
		}
		FileSystem hdfs = null;
		
		PathFilter pathFinder = new PathFilter() {
			@Override
			public boolean accept(Path p) {
				return conf.getFilesRegex().matcher(p.getName()).matches();
			}
		};
		
		try{
			if(isRunning){
				log.info("An other instance is running");
				return;
			}
			Configuration config = new Configuration();
			config.set("fs.default.name", this.conf.getHdfsUri());
			config.set("dfs.blocksize", "134217728");
			config.set("dfs.block.size", "134217728");
			hdfs = FileSystem.newInstance(config);
			for(FileStatus f : hdfs.listStatus(new Path(conf.getFilesFolder()),pathFinder)){
				log.info("Loading in hive ["+f.getPath().toUri().toASCIIString()+"]");
				insertInHive(f.getPath());
			}
			isRunning = true;
		}catch(Exception e){
			log.error("Error runnign job",e);
		}finally{
			isRunning=false;
			if(hdfs != null)
				try {
					hdfs.close();
				} catch (IOException e) {
					log.error("Error closing filesystem",e);
				}
		}
		
	}

	private void insertInHive(Path path) throws Exception {
		Class.forName("org.apache.hadoop.hive.jdbc.HiveDriver");
		String connectionURL = "jdbc:hive://"+this.conf.getHiveUrl()+"/default";
 		HiveConnection con = (HiveConnection) DriverManager.getConnection(connectionURL);
		HiveStatement stmt = (HiveStatement) con.createStatement();
		try {
				String part = getPartition(path);
				String query = "LOAD DATA INPATH '" +path.toUri().toASCIIString()
								+ "' INTO TABLE " + this.conf.getHiveTable();
				if(!"".equals(part))
						query+=" PARTITION("+part+")";
				log.info("Loading data in hive [" + query + "]");
				stmt.execute(query);
		} catch (Exception e) {
			throw e;
		}finally{
			con.close();
		}
		
	}

	private String getPartition(Path path) {
		StringBuilder sb = new StringBuilder();
		for(PartitionGeneratorDto entry : conf.getPartitionsGenerators()){
			//Se desencodea dos veces porque si enviamos caracteres encodeados son vueltos a encodear por el api de hadoop
			String url = URLDecoder.decode(path.toUri().toASCIIString());
			url = URLDecoder.decode(url);
			log.info("URL: to upload ["+url+"]");
			Matcher m = entry.getRegex().matcher(url);
			if(m.find()){
				sb.append(",");
				sb.append(entry.getName());
				sb.append("='");
				sb.append(m.replaceAll(entry.getReplace()));
				sb.append("'");
			}
		}
		return sb.substring(1).toString();
	}
	
}
