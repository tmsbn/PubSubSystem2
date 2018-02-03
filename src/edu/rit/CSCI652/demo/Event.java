package edu.rit.CSCI652.demo;

import java.util.List;

public class Event {

    public Event(int id, int topicId, String title, String content, int publishDateTime) {
        this.id = id;
        this.topicId = topicId;
        this.title = title;
        this.content = content;
        this.publishDateTime = publishDateTime;
    }

    private int id;
    private int topicId;
    private String title;
    private String content;
    private int publishDateTime;

    @Override
    public String toString() {
        return topicId + "," + title + "," + content;
    }
}
