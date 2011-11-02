package com.ml.hadoop;

import java.util.Collection;
import java.util.Map;
import java.util.regex.Pattern;

public class HiveMigratorConf {

	String hiveUrl;
	String hiveTable;
	Pattern filesRegex;
	String filesFolder;
	String hdfsUri;
	Collection<PartitionGeneratorDto> partitions;
	String cronExp;
	Map<String, String> replaces;
	String userName;
	String group;
	
	
	public String getHiveUrl() {
		return hiveUrl;
	}
	public void setHiveUrl(String hiveUrl) {
		this.hiveUrl = hiveUrl;
	}
	public String getHiveTable() {
		return hiveTable;
	}
	public void setHiveTable(String hiveTable) {
		this.hiveTable = hiveTable;
	}
	public Pattern getFilesRegex() {
		return filesRegex;
	}
	public void setFilesRegex(String filesRegex) {
		this.filesRegex = Pattern.compile(filesRegex);
	}
	public String getFilesFolder() {
		return filesFolder;
	}
	public void setFilesFolder(String filesFolder) {
		this.filesFolder = filesFolder;
	}
	public Collection<PartitionGeneratorDto> getPartitionsGenerators() {
		return partitions;
	}
	public void setPartitionsGenerators(Collection<PartitionGeneratorDto> partitions) {
		this.partitions = partitions;
	}
	public String getCronExp() {
		return cronExp;
	}
	public void setCronExp(String cronExp) {
		this.cronExp = cronExp;
	}
	public String getHdfsUri() {
		return hdfsUri;
	}
	public void setHdfsUri(String hdfsUri) {
		this.hdfsUri = hdfsUri;
	}
	public void setReplaceOnInsertPartition(Map<String, String> replaces) {
		this.replaces = replaces;
	}
	
	public Map<String, String> getReplaceOnInsertPartition(){
		return this.replaces;
	}
	public String getUserName() {
		return userName;
	}
	public String getGroup() {
		return group;
	}
	public void setUserName(String userName) {
		this.userName = userName;
	}
	public void setGroup(String group) {
		this.group = group;
	}
	
	
	
	
}
