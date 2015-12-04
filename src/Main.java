import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.*;

import org.bson.Document;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

public class Main {

    public static void main(String[] args) {

        MongoClient mongoClient = new MongoClient();
        MongoDatabase MDB = (MongoDatabase) mongoClient.getDatabase("partup");

        String DB_PATH = "data/graph.db";
        GraphDatabaseService graphDb = new GraphDatabaseFactory().newEmbeddedDatabase( new File( DB_PATH ));
        registerShutdownHook( graphDb );

        //User
        List<Document> users = MDB.getCollection("Users").find().into(new ArrayList<Document>());
        for (Document user : users) {
            String _id = (String) user.get("_id");
            Document profile = (Document) user.get("profile");
            String name_raw = (String) profile.get("name");
            String name = name_raw.replace("'", "");
            Document settings = (Document) profile.get("settings");
            String language = (String) settings.get("locale");
            Document location = (Document) profile.get("location");
            if (location != null) {
                String place_id = (String) location.get("place_id");
                if (place_id != null) {
                    String city_raw = (String) location.get("city");
                    String city = city_raw.replace("'", "");
                    String country = (String) location.get("country");
                    String user_query = "MERGE (ci:City {_id: '" + place_id + "'})" +
                            "ON CREATE SET ci.name= '" + city + "'" +
                            "MERGE (co:Country {name: '" + country + "'})" +
                            "MERGE (u:User {_id:'" + _id + "'})" +
                            "SET u.name='" + name + "'," +
                            "u.language='" + language + "'," +
                            "u.tags=[]" +
                            "CREATE UNIQUE (u)-[:LIVES_IN]->(ci)," +
                            "(ci)-[:LOCATED_IN]->(co)";
                    graphDb.execute(user_query);
                } else {
                    String user_query = "MERGE (u:User {_id:'" + _id + "'})" +
                            "SET u.name='" + name + "'," +
                            "u.language='" + language + "'," +
                            "u.tags=[]";
                    graphDb.execute(user_query);
                }
            } else{
                String user_query = "MERGE (u:User {_id:'" + _id + "'})" +
                        "SET u.name='" + name + "'," +
                        "u.language='" + language + "'," +
                        "u.tags=[]";
                graphDb.execute(user_query);
            }
            List tags = (List) profile.get("tags");
            if (tags != null) {
                for (int i = 0; i < tags.size(); i++) {
                    String query = "MERGE (u:User {_id: '" + _id + "'})" +
                            "SET u.tags=u.tags + ['" + tags.get(i) + "']";
                    graphDb.execute(query);
                }
            }
        }
        System.out.println(users.size() + " users imported into Neo4j.");

        //Networks
        List<Document> networks = MDB.getCollection("Networks").find().into(new ArrayList<Document>());
        for (Document network : networks) {
            String _id = (String) network.get("_id");
            String name = (String) network.get("name");
            int privacy_type = (int) network.get("privacy_type");
            String admin_id = (String) network.get("admin_id");
            String language = (String) network.get("language");
            Document location = (Document) network.get("location");
            if (location != null) {
                String place_id = (String) location.get("place_id");
                if (place_id != null) {
                    String city_raw = (String) location.get("city");
                    String city = city_raw.replace("'", "\\'");
                    String country = (String) location.get("country");
                    String query = "MERGE (ci:City {_id: '" + place_id + "'})" +
                            "ON CREATE SET ci.name= '" + city + "'" +
                            "MERGE (co:Country {name: '" + country + "'})" +
                            "MERGE (u:User {_id: '" + admin_id + "'})" +
                            "MERGE (n:Network {_id:'" + _id + "'})" +
                            "SET n.name='" + name + "'," +
                            "n.privacy_type=" + privacy_type + "," +
                            "n.language='" + language + "'" +
                            "CREATE UNIQUE (u)-[:MEMBER_OF {admin:true}]->(n)," +
                            "(n)-[:LOCATED_IN]->(ci)," +
                            "(ci)-[:LOCATED_IN]->(co)";
                    graphDb.execute(query);
                } else {
                    String query = "MERGE (u:User {_id: '" + admin_id + "'})" +
                            "MERGE (n:Network {_id:'" + _id + "'})" +
                            "SET n.name='" + name + "'," +
                            "n.privacy_type=" + privacy_type + "," +
                            "n.language='" + language + "'" +
                            "CREATE UNIQUE (u)-[:MEMBER_OF {admin:true}]->(n)";
                    graphDb.execute(query);
                }
            } else {
                String query = "MERGE (u:User {_id: '" + admin_id + "'})" +
                        "MERGE (n:Network {_id:'" + _id + "'})" +
                        "SET n.name='" + name + "'," +
                        "n.privacy_type=" + privacy_type + "," +
                        "n.language='" + language + "'" +
                        "CREATE UNIQUE (u)-[:MEMBER_OF {admin:true}]->(n)";
                graphDb.execute(query);
            }
            List uppers = (List) network.get("uppers");
            if (uppers != null) {
                for (int i = 0; i < uppers.size(); i++) {
                    if (uppers.get(i) != admin_id) {
                        String query = "MERGE (u:User {_id: '" + uppers.get(i) + "'})" +
                                "MERGE (n:Network {_id:'" + _id + "'})" +
                                "CREATE UNIQUE (u)-[:MEMBER_OF]->(n)";
                        graphDb.execute(query);
                    }
                }
            }
        }
        System.out.println(networks.size() + " networks imported into Neo4j.");

        //Teams
        List<Document> partups = MDB.getCollection("Teams").find().into(new ArrayList<Document>());
        for (Document partup : partups) {
            String _id = (String) partup.get("_id");
            String name_raw = (String) partup.get("name");
            String name = name_raw.replace("'", "");
            String creator_id = (String) partup.get("creator_id");
            String purpose = (String) partup.get("purpose");
            String language = (String) partup.get("language");
            int privacy_type = (int) partup.get("privacy_type");
            String type_partup = (String) partup.get("type");
            String phase = (String) partup.get("phase");
            Integer activity_count = (Integer) partup.get("activity_count");
            Date end_date_raw = (Date) partup.get("end_date");
            SimpleDateFormat date_format = new SimpleDateFormat("yyyyMMdd");
            String end_date = date_format.format(end_date_raw);
            String network_id = (String) partup.get("network_id");
            if (network_id != null) {
                Document location = (Document) partup.get("location");
                if (location != null) {
                    String place_id = (String) location.get("place_id");
                    if (place_id != null) {
                        String city_raw = (String) location.get("city");
                        String city = city_raw.replace("'", "");
                        String country = (String) location.get("country");
                        String user_query = "MERGE (ci:City {_id: '" + place_id + "'})" +
                                "ON CREATE SET ci.name= '" + city + "'" +
                                "MERGE (co:Country {name: '" + country + "'})" +
                                "MERGE (n:Network {_id: '" + network_id + "'})" +
                                "MERGE (u:User {_id: '" + creator_id + "'})" +
                                "MERGE (t:Team {_id:'" + _id + "'})" +
                                "SET t.name='" + name + "'," +
                                "t.end_date=" + end_date + "," +
                                "t.tags=[]," +
                                "t.purpose='"+ purpose +"'," +
                                "t.language='" + language + "'," +
                                "t.privacy_type=" + privacy_type + "," +
                                "t.activity_count=" + activity_count + "," +
                                "t.type='"+ type_partup + "'," +
                                "t.phase='" + phase + "'" +
                                "CREATE UNIQUE (u)-[:PARTNER_IN {creator:true, comments:0, contributions:0, pageViews:0, ratings:[], weight:1.5}]->(t)," +
                                "(t)-[:PART_OF]->(n)," +
                                "(t)-[:LOCATED_IN]->(ci)," +
                                "(ci)-[:LOCATED_IN]->(co)";
                        graphDb.execute(user_query);
                    } else {
                        String user_query = "MERGE (u:User {_id: '" + creator_id + "'})" +
                                "MERGE (t:Team {_id:'" + _id + "'})" +
                                "SET t.name='" + name + "'," +
                                "t.end_date=" + end_date + "," +
                                "t.tags=[]," +
                                "t.purpose='"+ purpose +"'," +
                                "t.language='" + language + "'," +
                                "t.privacy_type=" + privacy_type + "," +
                                "t.type='"+ type_partup + "'," +
                                "t.phase='" + phase + "'" +
                                "CREATE UNIQUE (u)-[:PARTNER_IN {creator:true, comments:0, contributions:0, pageViews:0, ratings:[], weight:1.5}]->(t)";
                        graphDb.execute(user_query);
                    }
                } else{
                    String user_query = "MERGE (u:User {_id: '" + creator_id + "'})" +
                            "MERGE (t:Team {_id:'" + _id + "'})" +
                            "SET t.name='" + name + "'," +
                            "t.end_date=" + end_date + "," +
                            "t.tags=[]," +
                            "t.purpose='"+ purpose +"'," +
                            "t.language='" + language + "'," +
                            "t.privacy_type=" + privacy_type + "," +
                            "t.type='"+ type_partup + "'," +
                            "t.phase='" + phase + "'" +
                            "CREATE UNIQUE (u)-[:PARTNER_IN {creator:true, comments:0, contributions:0, pageViews:0, ratings:[], weight:1.5}]->(t)";
                    graphDb.execute(user_query);
                }
            } else {
                Document location = (Document) partup.get("location");
                if (location != null) {
                    String place_id = (String) location.get("place_id");
                    if (place_id != null) {
                        String city_raw = (String) location.get("city");
                        String city = city_raw.replace("'", "");
                        String country = (String) location.get("country");
                        String user_query = "MERGE (ci:City {_id: '" + place_id + "'})" +
                                "ON CREATE SET ci.name= '" + city + "'" +
                                "MERGE (co:Country {name: '" + country + "'})" +
                                "MERGE (u:User {_id: '" + creator_id + "'})" +
                                "MERGE (t:Team {_id:'" + _id + "'})" +
                                "SET t.name='" + name + "'," +
                                "t.end_date=" + end_date + "," +
                                "t.tags=[]," +
                                "t.purpose='"+ purpose +"'," +
                                "t.language='" + language + "'," +
                                "t.privacy_type=" + privacy_type + "," +
                                "t.type='"+ type_partup + "'," +
                                "t.phase='" + phase + "'" +
                                "CREATE UNIQUE (u)-[:PARTNER_IN {creator:true, comments:0, contributions:0, pageViews:0, ratings:[], weight:1.5}]->(t)," +
                                "(t)-[:LOCATED_IN]->(ci)," +
                                "(ci)-[:LOCATED_IN]->(co)";
                        graphDb.execute(user_query);
                    } else {
                        String user_query = "MERGE (u:User {_id: '" + creator_id + "'})" +
                                "MERGE (t:Team {_id:'" + _id + "'})" +
                                "SET t.name='" + name + "'," +
                                "t.end_date=" + end_date + "," +
                                "t.tags=[]," +
                                "t.purpose='"+ purpose +"'," +
                                "t.language='" + language + "'," +
                                "t.privacy_type=" + privacy_type + "," +
                                "t.type='"+ type_partup + "'," +
                                "t.phase='" + phase + "'" +
                                "CREATE UNIQUE (u)-[:PARTNER_IN {creator:true, comments:0, contributions:0, pageViews:0, ratings:[], weight:1.5}]->(t)";
                        graphDb.execute(user_query);
                    }
                } else{
                    String user_query = "MERGE (u:User {_id: '" + creator_id + "'})" +
                            "MERGE (t:Team {_id:'" + _id + "'})" +
                            "SET t.name='" + name + "'," +
                            "t.end_date=" + end_date + "," +
                            "t.tags=[]," +
                            "t.purpose='"+ purpose +"'," +
                            "t.language='" + language + "'," +
                            "t.privacy_type=" + privacy_type + "," +
                            "t.type='"+ type_partup + "'," +
                            "t.phase='" + phase + "'" +
                            "CREATE UNIQUE (u)-[:PARTNER_IN {creator:true, comments:0, contributions:0, pageViews:0, ratings:[], weight:1.5}]->(t)";
                    graphDb.execute(user_query);
                }
            }
            List partners = (List) partup.get("uppers");
            for (int i = 0; i < partners.size(); i++) {
                if (!partners.get(i).equals(creator_id)){
                    String query = "MERGE (u:User {_id: '" + partners.get(i) + "'})" +
                            "MERGE (t:Team {_id:'" + _id + "'})" +
                            "CREATE UNIQUE (u)-[:PARTNER_IN {comments:0, contributions:0, pageViews:0, ratings:[], weight:1}]->(t)";
                    graphDb.execute(query);
                }
            }
            List supporters = (List) partup.get("supporters");
            if (supporters != null) {
                for (int i = 0; i < supporters.size(); i++) {
                    String query = "MERGE (u:User {_id: '" + supporters.get(i) + "'})" +
                            "MERGE (t:Team {_id:'" + _id + "'})" +
                            "CREATE UNIQUE (u)-[:SUPPORTER_IN {comments:0, pageViews:0, weight:0.5}]->(t)";
                    graphDb.execute(query);
                }
            }
            List tags = (List) partup.get("tags");
            if (tags != null) {
                for (int i = 0; i < tags.size(); i++) {
                    String query = "MERGE (t:Team {_id: '" + _id + "'})" +
                            "SET t.tags=t.tags + ['" + tags.get(i) + "']";
                    graphDb.execute(query);
                }
            }
            Date deleted_at_raw = (Date) partup.get("deleted_at");
            if (deleted_at_raw!=null){
                String deleted_at = date_format.format(deleted_at_raw);
                String query = "MERGE (t:Team {_id: '" + _id + "'})" +
                        "SET t.deleted_at=" + deleted_at + "," +
                        "t.active=false";
                graphDb.execute(query);
            }
        }
        System.out.println(partups.size() + " teams imported into Neo4j.");

        //Comments
        List<Document> comments = MDB.getCollection("Updates").find().into(new ArrayList<Document>());
        int count_comments = 0;
        for (Document comment : comments) {
            String type = (String) comment.get("type");
            String _id = (String) comment.get("_id");
            if (type.equals("partups_message_added") || type.equals("partups_activities_comments_added") || type.equals("partups_contributions_comments_added")){
                String upper_id = (String) comment.get("upper_id");
                String partup_id = (String) comment.get("partup_id");
                String query = "MATCH (u:User {_id: '" + upper_id + "'})-[r]->(t:Team {_id: '" + partup_id + "'})" +
                        "SET r.comments=r.comments+1";
                graphDb.execute(query);
                count_comments = count_comments + 1;
            }
            int comments_count = (int) comment.get("comments_count");
            if (comments_count > 0){
                List repliesList = (List) comment.get("comments");
                for (int i = 0; i < repliesList.size(); i++) {
                    Document reply = (Document) repliesList.get(i);
                    String reply_type = (String) reply.get("type");
                    Boolean reply_system = (Boolean) reply.get("system");
                    if (reply_type==null && reply_system==null){
                        Document reply_creator = (Document) reply.get("creator");
                        String reply_upper_id = (String) reply_creator.get("_id");
                        String reply_partup_id = (String) comment.get("partup_id");
                        String query_reply = "MATCH (u:User {_id: '" + reply_upper_id + "'})-[r]->(t:Team {_id: '" + reply_partup_id + "'})" +
                                "SET r.comments=r.comments+1";
                        graphDb.execute(query_reply);
                        count_comments = count_comments + 1;
                    }
                }
            }
        }
        System.out.println(count_comments + " comments imported into Neo4j.");

        //Contributions
        List<Document> contributions = MDB.getCollection("Contributions").find().into(new ArrayList<Document>());
        int count_contributions = 0;
        for (Document contribution : contributions) {
            Boolean verified = (Boolean) contribution.get("verified");
            if (verified) {
                String upper_id = (String) contribution.get("upper_id");
                String partup_id = (String) contribution.get("partup_id");
                String query = "MATCH (u:User {_id: '" + upper_id + "'})-[r:PARTNER_IN]->(t:Team {_id: '" + partup_id + "'})" +
                        "SET r.contributions=r.contributions+1";
                graphDb.execute(query);
                count_contributions = count_contributions + 1;
            }
        }
        System.out.println(count_contributions + " contributions imported into Neo4j.");

        //Ratings
        List<Document> ratings = MDB.getCollection("Ratings").find().into(new ArrayList<Document>());
        for (Document rating : ratings) {
            String rated_upper_id = (String) rating.get("rated_upper_id");
            String partup_id = (String) rating.get("partup_id");
            int rating_value = (int) rating.get("rating");
            String query = "MATCH (u:User {_id: '" + rated_upper_id + "'})-[r:PARTNER_IN]->(t:Team {_id: '" + partup_id + "'})" +
                  "SET r.ratings=r.ratings+["+ rating_value + "]";
            graphDb.execute(query);
        }
        System.out.println(ratings.size() + " ratings imported into Neo4j.");

        System.out.println("Happy Hunting!");
    }

    private static void registerShutdownHook( final GraphDatabaseService graphDb )
    {
        // Registers a shutdown hook for the Neo4j instance so that it
        // shuts down nicely when the VM exits (even if you "Ctrl-C" the
        // running application).
        Runtime.getRuntime().addShutdownHook( new Thread()
        {
            @Override
            public void run()
            {
                graphDb.shutdown();
            }
        } );
    }
}