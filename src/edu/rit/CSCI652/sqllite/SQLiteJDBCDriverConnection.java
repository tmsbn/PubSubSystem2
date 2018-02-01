package edu.rit.CSCI652.sqllite;
 
import edu.rit.CSCI652.demo.Event;
import edu.rit.CSCI652.demo.Topic;

import java.io.File;
import java.sql.*;
import java.util.ArrayList;

/**
 *
 * @author sqlitetutorial.net
 */
public class SQLiteJDBCDriverConnection {

    static String databasePath;

    private static final SQLiteJDBCDriverConnection INSTANCE = new SQLiteJDBCDriverConnection();


    public static SQLiteJDBCDriverConnection getInstance() {

        createDatabase();
        createTables();
        return INSTANCE;
    }

    public SQLiteJDBCDriverConnection(){

        String databaseDir = new File("jdbc:sqlite:"  + System.getProperty("user.dir"), "database").toString();
        databasePath = new File(databaseDir, "pubsub.db").toString();

    }

    public static void createDatabase() {

        try (Connection conn = DriverManager.getConnection(databasePath)) {

            if (conn != null) {
                DatabaseMetaData meta = conn.getMetaData();
                System.out.println("The driver name is " + meta.getDriverName());
                System.out.println("A new database has been created.");
            }

        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    public void insertEvent(String id, String topic, String title, String content){

        String insertTopicSql = "INSERT INTO event(id, topic, title, content)\n" +
                "VALUES('"+ id + "', '" + topic + "', '" + title + "', '" + content + "');";

        System.out.println(insertTopicSql);

        try (Connection conn = DriverManager.getConnection(databasePath);
             Statement stmt = conn.createStatement()) {

            stmt.execute(insertTopicSql);

        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    public ArrayList<Event> getAllEvents(){

        String sql = "SELECT *  FROM event";
        ArrayList<Event> eventList= new ArrayList<>();

        try (Connection conn = DriverManager.getConnection(databasePath);
             Statement stmt  = conn.createStatement();
             ResultSet rs    = stmt.executeQuery(sql)){

            // loop through the result set
            while (rs.next()) {

                int id = rs.getInt("id");
                int topicId = rs.getInt("topicid");
                String title = rs.getString("title");
                String content = rs.getString("content");
                eventList.add(new Event(id, topicId, title, content));
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }

        return eventList;
    }

    public void insertTopic(String name, String keywords){

        String insertTopicSql = "INSERT INTO topic(name, keywords)\n" +
                "VALUES('"+ name + "', '" + keywords + "');";


        try (Connection conn = DriverManager.getConnection(databasePath);
             Statement stmt = conn.createStatement()) {

            stmt.execute(insertTopicSql);

        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    public ArrayList<Topic> getTopics(){

        String sql = "SELECT *  FROM topic";
        ArrayList<Topic> topicList= new ArrayList<>();

        try (Connection conn = DriverManager.getConnection(databasePath);
             Statement stmt  = conn.createStatement();
             ResultSet rs    = stmt.executeQuery(sql)){

            // loop through the result set
            while (rs.next()) {

                    int id = rs.getInt("id");
                    String words = rs.getString("keywords");
                    String name = rs.getString("name");
                    Topic topic = new Topic(id, name, words);
                    topicList.add(topic);
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }

        return topicList;
    }



    public static void createTables(){

        String topicSql = "CREATE TABLE IF NOT EXISTS topic (\n"
                + "	id integer PRIMARY KEY,\n"
                + "	name text NOT NULL,\n"
                + "	keywords text NOT NULL"
                + ");";

        String eventSql = "CREATE TABLE IF NOT EXISTS event (\n"
                + "	id integer PRIMARY KEY,\n"
                + " topic_id integer, \n"
                + "	title text NOT NULL,\n"
                + "	content text NOT NULL"
                + ");";

        String subscriberSql = "CREATE TABLE IF NOT EXISTS subscriber (\n"
                + "	id integer PRIMARY KEY,\n"
                + "	name text NOT NULL"
                + ");";

        String subscriberTopicSql = "CREATE TABLE IF NOT EXISTS subscriber_topic (\n"
                + "	subscriber_id integer PRIMARY KEY,\n"
                + "	topic_id integer NOT NULL"
                + ");";

        try (Connection conn = DriverManager.getConnection(databasePath);
             Statement stmt = conn.createStatement()) {

            stmt.execute(topicSql);
            stmt.execute(eventSql);
            stmt.execute(subscriberSql);
            stmt.execute(subscriberTopicSql);



        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }

    }

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {

            SQLiteJDBCDriverConnection conn = SQLiteJDBCDriverConnection.getInstance();
            conn.insertTopic("a", "c");
            conn.insertTopic("b", "d");
            conn.insertTopic("g", "i");
            ArrayList<Topic> topics = conn.getTopics();
            for(Topic topic:topics){
                System.out.println(topic);
            }


    }
}