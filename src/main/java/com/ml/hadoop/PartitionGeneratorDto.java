package com.ml.hadoop;

import java.util.regex.Pattern;

public class PartitionGeneratorDto {

	String name;
	Pattern regex;
	String replace;
	
	
	
	public PartitionGeneratorDto(String name, Pattern regex, String replace) {
		super();
		this.name = name;
		this.regex = regex;
		this.replace = replace;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public Pattern getRegex() {
		return regex;
	}
	public void setRegex(Pattern regex) {
		this.regex = regex;
	}
	public String getReplace() {
		return replace;
	}
	public void setReplace(String replace) {
		this.replace = replace;
	}
	
	
}
