import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;

import java.io.File;
import java.util.*;

import org.bson.Document;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

public class Main {

    public static void main(String[] args) {

        MongoClient mongoClient = new MongoClient();
        MongoDatabase MDB = (MongoDatabase) mongoClient.getDatabase("partup");

        String DB_PATH = "neo/data/graph.db";
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
                            "CREATE UNIQUE (u)-[:LOCATED_IN]->(ci)," +
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
        System.out.println(users.size() + " users imported in Neo4j.");

        //Networks
        List<Document> networks = MDB.getCollection("Networks").find().into(new ArrayList<Document>());
        for (Document network : networks) {
            String _id = (String) network.get("_id");
            String name = (String) network.get("name");
            String admin_id = (String) network.get("admin_id");
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
                            "SET n.name='" + name + "'" +
                            "CREATE UNIQUE (u)-[:ADMIN_OF]->(n)," +
                            "(n)-[:LOCATED_IN]->(ci)," +
                            "(ci)-[:LOCATED_IN]->(co)";
                    graphDb.execute(query);
                } else {
                    String query = "MERGE (u:User {_id: '" + admin_id + "'})" +
                            "MERGE (n:Network {_id:'" + _id + "'})" +
                            "SET n.name='" + name + "'" +
                            "CREATE UNIQUE (u)-[:ADMIN_OF]->(n)";
                    graphDb.execute(query);
                }
            } else {
                String query = "MERGE (u:User {_id: '" + admin_id + "'})" +
                        "MERGE (n:Network {_id:'" + _id + "'})" +
                        "SET n.name='" + name + "'" +
                        "CREATE UNIQUE (u)-[:ADMIN_OF]->(n)";
                graphDb.execute(query);
            }
            List uppers = (List) network.get("uppers");
            if (uppers != null) {
                for (int i = 0; i < uppers.size(); i++) {
                    String query = "MERGE (u:User {_id: '" + uppers.get(i) + "'})" +
                            "MERGE (n:Network {_id:'" + _id + "'})" +
                            "CREATE UNIQUE (u)-[:MEMBER_OF]->(n)";
                    graphDb.execute(query);
                }
            }
        }
        System.out.println(networks.size() + " networks imported in Neo4j.");

        //partups
        List<Document> partups = MDB.getCollection("Partups").find().into(new ArrayList<Document>());
        for (Document partup : partups) {
            String _id = (String) partup.get("_id");
            String name_raw = (String) partup.get("name");
            String name = name_raw.replace("'", "");
            String creator_id = (String) partup.get("creator_id");
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
                            "t.tags=[]" +
                            "CREATE UNIQUE (u)-[:CREATOR_OF]->(t)," +
                            "(t)-[:LOCATED_IN]->(ci)," +
                            "(ci)-[:LOCATED_IN]->(co)";
                    graphDb.execute(user_query);
                } else {
                    String user_query = "MERGE (u:User {_id: '" + creator_id + "'})" +
                            "MERGE (t:Team {_id:'" + _id + "'})" +
                            "SET t.name='" + name + "'," +
                            "t.tags=[]" +
                            "CREATE UNIQUE (u)-[:CREATOR_OF]->(t)";
                    graphDb.execute(user_query);
                }
            } else{
                String user_query = "MERGE (u:User {_id: '" + creator_id + "'})" +
                        "MERGE (t:Team {_id:'" + _id + "'})" +
                        "SET t.name='" + name + "'," +
                        "t.tags=[]" +
                        "CREATE UNIQUE (u)-[:CREATOR_OF]->(t)";
                graphDb.execute(user_query);
            }
            List partners = (List) partup.get("uppers");
            if (partners != null) {
                for (int i = 0; i < partners.size(); i++) {
                    String query = "MERGE (u:User {_id: '" + partners.get(i) + "'})" +
                            "MERGE (t:Team {_id:'" + _id + "'})" +
                            "CREATE UNIQUE (u)-[:PARTNER_IN]->(t)";
                    graphDb.execute(query);
                }
            }
            List supporters = (List) partup.get("supporters");
            if (supporters != null) {
                for (int i = 0; i < supporters.size(); i++) {
                    String query = "MERGE (u:User {_id: '" + supporters.get(i) + "'})" +
                            "MERGE (t:Team {_id:'" + _id + "'})" +
                            "CREATE UNIQUE (u)-[:SUPPORTER_IN]->(t)";
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
        }
        System.out.println(partups.size() + " teams imported in Neo4j.");
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
