package edu.rit.CSCI652.demo;

import java.util.List;

public class Topic {
	private int id;
	private String keywords;
	private String name;

	public Topic(int id, String keywords, String name) {
		this.id = id;
		this.keywords = keywords;
		this.name = name;
	}

	@Override
	public String toString() {
		return keywords + "," + name;
	}
}
