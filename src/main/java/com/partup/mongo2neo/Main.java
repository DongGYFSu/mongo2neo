package com.partup.mongo2neo;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.filter.HTTPBasicAuthFilter;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.List;
import javax.ws.rs.core.MediaType;

import org.apache.commons.lang3.StringUtils;

import org.bson.Document;

import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

/**
 * MongoDB to Neo4j importer for Part-up data.
 * The data can be imported via a REST-connection or local filepath.
 * Each document is imported via one or multiple queries. This limits the scalability of this script.
 *
 * @author Maurits van der Goes
 */

public class Main {

    private static final String SERVER_ROOT_URI = "http://localhost:7474/db/data/";
    private static final String username = "neo4j";
    private static final String password = "partup";
    private static final String txUri = SERVER_ROOT_URI + "transaction/commit";

    private static final SimpleDateFormat date_format = new SimpleDateFormat("yyyyMMdd");

    private static List<Document> usersDocuments;
    private static List<Document> networksDocuments;
    private static List<Document> teamsDocuments;
    private static List<Document> commentsDocuments;
    private static List<Document> contributionsDocuments;
    private static List<Document> ratingsDocuments;

    private static WebResource resource;


    public static void main(String[] args) {

        MongoConnect();
        NeoConnect();

        CreateConstraints();

        ImportUsers();
        ImportNetworks();
        ImportTeams();
        ImportComments();
        ImportContributions();
        ImportRatings();

        SetScores();
        SetSimilarities();

        System.out.println("Happy Hunting!");

    }

    public static void MongoConnect() {
        MongoClient mongoClient = new MongoClient();
        MongoDatabase MDB = mongoClient.getDatabase("partup");
        usersDocuments = MDB.getCollection("users").find().into(new ArrayList<Document>());
        networksDocuments = MDB.getCollection("networks").find().into(new ArrayList());
        teamsDocuments = MDB.getCollection("partups").find().into(new ArrayList());
        commentsDocuments = MDB.getCollection("updates").find().into(new ArrayList<Document>());
        contributionsDocuments = MDB.getCollection("contributions").find().into(new ArrayList<Document>());
        ratingsDocuments = MDB.getCollection("ratings").find().into(new ArrayList<Document>());
    }

    public static void NeoConnect() {
        Client c = Client.create();
        c.addFilter(new HTTPBasicAuthFilter(username, password));
        resource = c.resource( txUri );
    }

    private static void sendQuery(String query) {
        String payload = "{\"statements\" : [ {\"statement\" : \"" + query + "\"} ]}";
        ClientResponse response = resource
                .accept( MediaType.APPLICATION_JSON )
                .type( MediaType.APPLICATION_JSON )
                .entity( payload )
                .post( ClientResponse.class );

        //Prints the response from Neo4j.
//        System.out.println(String.format(
//                "POST [%s] to [%s], status code [%d], returned data: "
//                        + System.getProperty("line.separator") + "%s",
//                payload, txUri, response.getStatus(),
//                response.getEntity(String.class)));

        response.close();

        /** Local database connection via the filepath.
         *  Registers a shutdown hook for the Neo4j instance so that it shuts down
         *  nicely when the VM exits (even if you "Ctrl-C" the running application).
         */
//        String DB_PATH = "data/graph.db";
//        GraphDatabaseService graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(new File( DB_PATH ));
//        graphDb.execute(query);
//        Runtime.getRuntime().addShutdownHook( new Thread()
//        {
//            @Override
//            public void run()
//            {
//                graphDb.shutdown();
//            }
//        } );
    }

    private static String ProcessTags(List tags) {
        List<String> tagsList = new ArrayList();
        for (int i = 0; i < tags.size(); i++) {
            tagsList.add(tags.get(i).toString());
        }
        String tagsString = "'" + StringUtils.join(tagsList, "','") + "'";
        return tagsString;
    }

    private static void CreateConstraints() {

        /** Creates constraints on a property for a node type.
         *  A constraint restricts the creation of a new node with the same value for the property.
         *  Automatically also creates a index for this property. This improves the performance of the database.
         */

        sendQuery("CREATE CONSTRAINT ON (u:User) ASSERT u._id IS UNIQUE");
        sendQuery("CREATE CONSTRAINT ON (n:Network) ASSERT n._id IS UNIQUE");
        sendQuery("CREATE CONSTRAINT ON (t:Team) ASSERT t._id IS UNIQUE");
        sendQuery("CREATE CONSTRAINT ON (C:City) ASSERT c._id IS UNIQUE");
        sendQuery("CREATE CONSTRAINT ON (c:Country) ASSERT c.name IS UNIQUE");
        sendQuery("CREATE CONSTRAINT ON (s:Strength) ASSERT s.code IS UNIQUE");

        System.out.println("Constraints created for User, Network, Team, City, Country and Strength nodes in Neo4j");
    }

    private static void ImportUsers() {

        String userQuery;

        for (Document user : usersDocuments) {
            String _id = user.getString("_id");
            String mergeUser = String.format("MERGE (u:User {_id:'%s'})", _id);
            Document profile = (Document) user.get("profile");
            String name = profile.getString("normalized_name");
            Document settings = (Document) profile.get("settings");
            String language = settings.getString("locale");
            Document location = (Document) profile.get("location");
            //A user is not required to have a location. Neo4j does not filters out null.
            if (location != null) {
                String place_id = location.getString("place_id");
                //A location can be removed. This leaves an empty string.
                if (place_id != null) {
                    String city_raw = location.getString("city");
                    String city = city_raw.replace("'", "");
                    String country = location.getString("country");
                    userQuery = "MERGE (ci:City {_id: '" + place_id + "'})  " +
                            "ON CREATE SET ci.name= '" + city + "' " +
                            "MERGE (co:Country {name: '" + country + "'}) " +
                            mergeUser + " " +
                            "SET u.name='" + name + "', " +
                            "u.language='" + language + "', " +
                            "u.tags=[], " +
                            "u.active=true " +
                            "CREATE UNIQUE (u)-[:LIVES_IN]->(ci), " +
                            "(ci)-[:LOCATED_IN]->(co)";
                } else {
                    userQuery = mergeUser + " " +
                            "SET u.name='" + name + "', " +
                            "u.language='" + language + "', " +
                            "u.tags=[], " +
                            "u.active=true";
                }
            } else {
                userQuery = mergeUser + " " +
                        "SET u.name='" + name + "', " +
                        "u.language='" + language + "', " +
                        "u.tags=[], " +
                        "u.active=true";
            }
            sendQuery(userQuery);

            List tags = (List) profile.get("tags");
            if (tags != null) {
                String queryT = mergeUser + " SET u.tags=[" + ProcessTags(tags) + "]";
                sendQuery(queryT);
            }
            Date deactivatedAt_raw = user.getDate("deactivatedAt");
            if (deactivatedAt_raw != null) {
                String deactivatedAt = date_format.format(deactivatedAt_raw);
                String query = "MERGE (u:User {_id: '" + _id + "'}) " +
                        "SET u.deactivatedAt=" + deactivatedAt + ", " +
                        "u.active=false";
                sendQuery(query);
            }
            Document meurs = (Document) profile.get("meurs");
            if (meurs != null) {
                Boolean fetched_results = (Boolean) meurs.get("fetched_results");
                if (fetched_results != null) {
                    List results = (List) meurs.get("results");
                    Document results_0 = (Document) results.get(0);
                    int code_0 = results_0.getInteger("code");
                    String name_0 = results_0.getString("name");
                    int score_0 = results_0.getInteger("score");
                    Document results_1 = (Document) results.get(1);
                    int code_1 = results_1.getInteger("code");
                    String name_1 = results_1.getString("name");
                    int score_1 = results_1.getInteger("score");
                    String query = "MERGE (u:User {_id: '" + _id + "'}) " +
                            "MERGE (s0:Strength {code: '" + code_0 + "'}) " +
                            "ON CREATE SET s0.name= '" + name_0 + "' " +
                            "MERGE (s1:Strength {code: '" + code_1 + "'}) " +
                            "ON CREATE SET s1.name= '" + name_1 + "' " +
                            "CREATE UNIQUE (u)-[r0:HOLDS]->(s0) " +
                            "SET r0.score=" + score_0 + " " +
                            "CREATE UNIQUE (u)-[r1:HOLDS]->(s1) " +
                            "SET r1.score=" + score_1;
                    sendQuery(query);
                }
            }
        }
        System.out.println(usersDocuments.size() + " users imported into Neo4j.");

    }
    private static void ImportNetworks() {

        String networkQuery;

        for (Document network : networksDocuments) {
            String _id = network.getString("_id");
            String mergeNetwork = String.format("MERGE (n:Network {_id:'%s'})", _id);
            String name_raw = network.getString("name");
            //Networks do not have normalized names, but accept special characters.
            String name = name_raw.replace("'", "");
            int privacy_type = network.getInteger("privacy_type");
            String language = network.getString("language");
            String slug = network.getString("slug");
            String created_at = date_format.format(network.getDate("created_at"));
            Document location = (Document) network.get("location");
            if (location != null) {
                String place_id = location.getString("place_id");
                if (place_id != null) {
                    String city_raw = location.getString("city");
                    String city = city_raw.replace("'", "");
                    String country = location.getString("country");
                    networkQuery = mergeNetwork + " " +
                            "MERGE (ci:City {name: '" + place_id + "'}) " +
                            "ON CREATE SET ci.name= '" + city + "' " +
                            "MERGE (co:Country {name: '" + country + "'}) " +
                            "SET n.name='" + name + "', " +
                            "n.tags=[], " +
                            "n.privacy_type=" + privacy_type + ", " +
                            "n.language='" + language + "', " +
                            "n.slug='" + slug + "', " +
                            "n.created_at='" + created_at + "' " +
                            "CREATE UNIQUE (n)-[:LOCATED_IN]->(ci), " +
                            "(ci)-[:LOCATED_IN]->(co)";
                } else {
                    networkQuery = mergeNetwork + " " +
                            "SET n.name='" + name + "', " +
                            "n.tags=[], " +
                            "n.privacy_type=" + privacy_type + ", " +
                            "n.language='" + language + "', " +
                            "n.slug='" + slug + "', " +
                            "n.created_at='" + created_at + "'";
                }
            } else {
                networkQuery = mergeNetwork + " " +
                        "SET n.name='" + name + "', " +
                        "n.tags=[], " +
                        "n.privacy_type=" + privacy_type + ", " +
                        "n.language='" + language + "', " +
                        "n.slug='" + slug + "', " +
                        "n.created_at='" + created_at + "'";
            }
            sendQuery(networkQuery);

            List uppers = (List) network.get("uppers");
            if (uppers != null) {
                List<String> mergeUser = new ArrayList();
                List<String> createUnique = new ArrayList();
                for (int i = 0; i < uppers.size(); i++) {
                    mergeUser.add(String.format("MERGE (u%s:User {_id: '%s'})", i, uppers.get(i)));
                    createUnique.add(String.format("CREATE UNIQUE (u%s)-[:MEMBER_OF]->(n)", i));
                }

                String queryUppers = StringUtils.join(mergeUser, " ") + " " + mergeNetwork + " " + StringUtils.join(createUnique, " ");
                sendQuery(queryUppers);
            }

            //Sets the admin status for the listed users as property on the user-network-edge.
            List<String> adminsList = (List<String>) network.get("admins");
            if (adminsList != null) {
                List<String> mergeAdmins = new ArrayList();
                List<String> setAdmins = new ArrayList();
                for (int i = 0; i < adminsList.size(); i++) {
                    mergeAdmins.add(String.format("MERGE (:User {_id: '%s'})-[e%s:MEMBER_OF]->(n)", adminsList.get(i), i));
                    setAdmins.add(String.format("SET e%s.admin=true", i));
                }

                String queryAdmins = mergeNetwork + " " + StringUtils.join(mergeAdmins, " ") + " " + StringUtils.join(setAdmins, " ");
                sendQuery(queryAdmins);
            }

            //Sets the colleague status for the listed users as property on the user-network-edge.
            List<String> colleaguesList = (List<String>) network.get("colleagues");
            if (colleaguesList != null) {
                List<String> mergeColleagues = new ArrayList();
                List<String> setColleagues = new ArrayList();
                for (int i = 0; i < colleaguesList.size(); i++) {
                    mergeColleagues.add(String.format("MERGE (:User {_id: '%s'})-[e%s:MEMBER_OF]->(n)", colleaguesList.get(i), i));
                    setColleagues.add(String.format("SET e%s.colleague=true", i));
                }

                String queryColleagues = mergeNetwork + " " + StringUtils.join(mergeColleagues, " ") + " " + StringUtils.join(setColleagues, " ");
                sendQuery(queryColleagues);
            }

            List tags = (List) network.get("tags");
            if (tags != null) {
                String queryT = mergeNetwork + " SET n.tags=[" + ProcessTags(tags) + "]";
                sendQuery(queryT);
            }
        }
        System.out.println(networksDocuments.size() + " networks imported into Neo4j.");

    }
    private static void ImportTeams() {

        String teamQuery;

        for (Document partup : teamsDocuments) {
            String _id = partup.getString("_id");
            String mergeTeam = String.format("MERGE (t:Team {_id:'%s'})", _id);
            String name_raw = partup.getString("name");
            //Teams do not have normalized names, but accept special characters.
            String name = name_raw.replace("'", "");
            String creator_id = partup.getString("creator_id");
            String language = partup.getString("language");
            int privacy_type = partup.getInteger("privacy_type");
            String type_partup = partup.getString("type");
            String phase = partup.getString("phase");
            Integer activity_count = (Integer) partup.get("activity_count");
            String slug = partup.getString("slug");
            String end_date = date_format.format(partup.getDate("end_date"));
            String network_id = partup.getString("network_id");
            String created_at = date_format.format(partup.getDate("created_at"));
            if (network_id != null) {
                Document location = (Document) partup.get("location");
                if (location != null) {
                    String place_id = location.getString("place_id");
                    if (place_id != null) {
                        String city_raw = location.getString("city");
                        String city = city_raw.replace("'", "");
                        String country = location.getString("country");
                        teamQuery = "MERGE (ci:City {_id: '" + place_id + "'}) " +
                                "ON CREATE SET ci.name= '" + city + "' " +
                                "MERGE (co:Country {name: '" + country + "'}) " +
                                "MERGE (n:Network {_id: '" + network_id + "'}) " +
                                "MERGE (u:User {_id: '" + creator_id + "'}) " +
                                mergeTeam + " " +
                                "SET t.name='" + name + "', " +
                                "t.end_date='" + end_date + "', " +
                                "t.tags=[], " +
                                "t.language='" + language + "', " +
                                "t.privacy_type=" + privacy_type + ", " +
                                "t.type='" + type_partup + "', " +
                                "t.phase='" + phase + "', " +
                                "t.activity_count=" + activity_count + ", " +
                                "t.slug=" + slug +", " +
                                "t.active=true, " +
                                "t.created_at='" + created_at + "' " +
                                "CREATE UNIQUE (u)-[:ACTIVE_IN {creator:true, comments:0, contributions:0, pageViews:0, participation:0.0, ratings:[], role:2.0}]->(t), " +
                                "(t)-[:PART_OF]->(n), " +
                                "(t)-[:LOCATED_IN]->(ci), " +
                                "(ci)-[:LOCATED_IN]->(co)";
                    } else {
                        teamQuery = "MERGE (n:Network {_id: '" + network_id + "'}) " +
                                "MERGE (u:User {_id: '" + creator_id + "'}) " +
                                mergeTeam + " " +
                                "SET t.name='" + name + "', " +
                                "t.end_date='" + end_date + "', " +
                                "t.tags=[], " +
                                "t.language='" + language + "', " +
                                "t.privacy_type=" + privacy_type + ", " +
                                "t.type='" + type_partup + "', " +
                                "t.phase='" + phase + "', " +
                                "t.activity_count=" + activity_count + ", " +
                                "t.slug=" + slug + ", " +
                                "t.active=true, " +
                                "t.created_at='" + created_at + "' " +
                                "CREATE UNIQUE (u)-[:ACTIVE_IN {creator:true, comments:0, contributions:0, pageViews:0, participation:0.0, ratings:[], role:2.0}]->(t)," +
                                "(t)-[:PART_OF]->(n)";
                    }
                } else {
                    teamQuery = "MERGE (n:Network {_id: '" + network_id + "'}) " +
                            "MERGE (u:User {_id: '" + creator_id + "'}) " +
                            mergeTeam + " " +
                            "SET t.name='" + name + "', " +
                            "t.end_date='" + end_date + "', " +
                            "t.tags=[], " +
                            "t.language='" + language + "', " +
                            "t.privacy_type=" + privacy_type + ", " +
                            "t.type='" + type_partup + "', " +
                            "t.phase='" + phase + "', " +
                            "t.activity_count=" + activity_count + ", " +
                            "t.slug=" + slug + ", " +
                            "t.active=true, " +
                            "t.created_at='" + created_at + "' " +
                            "CREATE UNIQUE (u)-[:ACTIVE_IN {creator:true, comments:0, contributions:0, pageViews:0, participation:0.0, ratings:[], role:2.0}]->(t), " +
                            "(t)-[:PART_OF]->(n)";
                }
            } else {
                Document location = (Document) partup.get("location");
                if (location != null) {
                    String place_id = location.getString("place_id");
                    if (place_id != null) {
                        String city_raw = location.getString("city");
                        String city = city_raw.replace("'", "");
                        String country = location.getString("country");
                        teamQuery = "MERGE (ci:City {_id: '" + place_id + "'}) " +
                                "ON CREATE SET ci.name= '" + city + "' " +
                                "MERGE (co:Country {name: '" + country + "'}) " +
                                "MERGE (u:User {_id: '" + creator_id + "'}) " +
                                mergeTeam + " " +
                                "SET t.name='" + name + "', " +
                                "t.end_date='" + end_date + "', " +
                                "t.tags=[], " +
                                "t.language='" + language + "', " +
                                "t.privacy_type=" + privacy_type + ", " +
                                "t.type='" + type_partup + "', " +
                                "t.phase='" + phase + "', " +
                                "t.activity_count=" + activity_count + ", " +
                                "t.slug=" + slug + ", " +
                                "t.active=true, " +
                                "t.created_at='" + created_at + "' " +
                                "CREATE UNIQUE (u)-[:ACTIVE_IN {creator:true, comments:0, contributions:0, pageViews:0, participation:0.0, ratings:[], role:2.0}]->(t), " +
                                "(t)-[:LOCATED_IN]->(ci), " +
                                "(ci)-[:LOCATED_IN]->(co)";
                    } else {
                        teamQuery = "MERGE (u:User {_id: '" + creator_id + "'}) " +
                                mergeTeam + " " +
                                "SET t.name='" + name + "', " +
                                "t.end_date='" + end_date + "', " +
                                "t.tags=[], " +
                                "t.language='" + language + "', " +
                                "t.privacy_type=" + privacy_type + ", " +
                                "t.type='" + type_partup + "', " +
                                "t.phase='" + phase + "', " +
                                "t.activity_count=" + activity_count + ", " +
                                "t.slug=" + slug + ", " +
                                "t.active=true, " +
                                "t.created_at='" + created_at + "' " +
                                "CREATE UNIQUE (u)-[:ACTIVE_IN {creator:true, comments:0, contributions:0, pageViews:0, participation:0.0, ratings:[], role:2.0}]->(t)";
                    }
                } else {
                    teamQuery = "MERGE (u:User {_id: '" + creator_id + "'}) " +
                            mergeTeam + " " +
                            "SET t.name='" + name + "', " +
                            "t.end_date='" + end_date + "', " +
                            "t.tags=[], " +
                            "t.language='" + language + "', " +
                            "t.privacy_type=" + privacy_type + ", " +
                            "t.type='" + type_partup + "', " +
                            "t.phase='" + phase + "', " +
                            "t.activity_count=" + activity_count + ", " +
                            "t.slug=" + slug + ", " +
                            "t.active=true, " +
                            "t.created_at='" + created_at + "' " +
                            "CREATE UNIQUE (u)-[:ACTIVE_IN {creator:true, comments:0, contributions:0, pageViews:0, participation:0.0, ratings:[], role:2.0}]->(t)";
                }
            }
            sendQuery(teamQuery);

            List partners = (List) partup.get("uppers");
            List<String> mergePartner = new ArrayList();
            List<String> createUniqueP = new ArrayList();
            for (int i = 0; i < partners.size(); i++) {
                if (!partners.get(i).equals(creator_id)) {
                    mergePartner.add(String.format("MERGE (u%s:User {_id: '%s'})", i, partners.get(i)));
                    createUniqueP.add(String.format("CREATE UNIQUE (u%s)-[:ACTIVE_IN {comments:0, contributions:0, pageViews:0, participation:0.0, ratings:[], role:1.5}]->(t)", i));
                }
            }
            String queryP = StringUtils.join(mergePartner, " ") + " " + mergeTeam + " " + StringUtils.join(createUniqueP, " ");
            sendQuery(queryP);

            List supporters = (List) partup.get("supporters");
            if (supporters != null) {
                List<String> mergeSupporter = new ArrayList();
                List<String> createUniqueS = new ArrayList();
                for (int i = 0; i < supporters.size(); i++) {
                    if (!supporters.get(i).equals(creator_id)) {
                        mergeSupporter.add(String.format("MERGE (u%s:User {_id: '%s'})", i, supporters.get(i)));
                        createUniqueS.add(String.format("CREATE UNIQUE (u%s)-[:ACTIVE_IN {comments:0, contributions:0, pageViews:0, participation:0.0, role:1.0}]->(t)", i));
                    }
                }
                String queryS = StringUtils.join(mergeSupporter, " ") + " " + mergeTeam + " " + StringUtils.join(createUniqueS, " ");
                sendQuery(queryS);
            }
            List tags = (List) partup.get("tags");
            if (tags != null) {
                String queryT = mergeTeam + " SET t.tags=[" + ProcessTags(tags) + "]";
                sendQuery(queryT);
            }
            
            Date deleted_at_raw = partup.getDate("deleted_at");
            if (deleted_at_raw != null) {
                String deleted_at = date_format.format(deleted_at_raw);
                String query = "MERGE (t:Team {_id: '" + _id + "'}) " +
                        "SET t.deleted_at='" + deleted_at + "', " +
                        "t.deleted=true, " +
                        "t.active=false";
                sendQuery(query);
            }
            
            Date archived_at_raw = partup.getDate("archived_at");
            if (archived_at_raw != null) {
                String archived_at = date_format.format(archived_at_raw);
                String query = "MERGE (t:Team {_id: '" + _id + "'}) " +
                        "SET t.archived_at='" + archived_at + "', " +
                        "t.archived=true, " +
                        "t.active=false";
                sendQuery(query);
            }
        }
        System.out.println(teamsDocuments.size() + " teams imported into Neo4j.");

    }
    private static void ImportComments() {

        /** Sets the total number of comments a user has in a team to the user-team-edge as an integer.
         *  This total number is not directly provided by MongoDB.
         *  It iterates through the comments collection and adds one for each match.
         *  The comments collection is the updates collection. It contains multiple types of updates.
         *  Only the updates with these types are considered to be comments:
         *  - "partups_message_added"
         *  - "partups_activities_comments_added"
         *  - "partups_contributions_comments_added"
         *  - Replies that are not system messages.
         */

        int count_comments = 0;
        for (Document comment : commentsDocuments) {
            String type = comment.getString("type");
            String _id = comment.getString("_id");
            if (type.equals("partups_message_added") || type.equals("partups_activities_comments_added") || type.equals("partups_contributions_comments_added")) {
                String upper_id = comment.getString("upper_id");
                String partup_id = comment.getString("partup_id");
                String query = "MATCH (u:User {_id: '" + upper_id + "'})-[r:ACTIVE_IN]->(t:Team {_id: '" + partup_id + "'})" +
                        "SET r.comments=r.comments+1";
                sendQuery(query);
                count_comments = count_comments + 1;
            }
            int comments_count = comment.getInteger("comments_count");
            if (comments_count > 0) {
                List repliesList = (List) comment.get("comments");
                for (int i = 0; i < repliesList.size(); i++) {
                    Document reply = (Document) repliesList.get(i);
                    String reply_type = reply.getString("type");
                    Boolean reply_system = reply.getBoolean("system");
                    if (reply_type != null && reply_system != null) {
                        Document reply_creator = (Document) reply.get("creator");
                        String reply_upper_id = reply_creator.getString("_id");
                        String reply_partup_id = comment.getString("partup_id");
                        String query_reply = "MATCH (u:User {_id: '" + reply_upper_id + "'})-[r:ACTIVE_IN]->(t:Team {_id: '" + reply_partup_id + "'}) " +
                                "SET r.comments=r.comments+1";
                        sendQuery(query_reply);
                        count_comments = count_comments + 1;
                    }
                }
            }
        }
        System.out.println(count_comments + " comments imported into Neo4j.");

    }
    private static void ImportContributions() {

        /** Sets the total number of contributions a user has in a team to the user-team-edge as an integer.
         *  This total number is not directly provided by MongoDB.
         *  It iterates through the contribution collection and adds one for each match.
         */

        int count_contributions = 0;
        for (Document contribution : contributionsDocuments) {
            Boolean verified = contribution.getBoolean("verified");
            if (verified) {
                String upper_id = contribution.getString("upper_id");
                String partup_id = contribution.getString("partup_id");
                String query = "MATCH (u:User {_id: '" + upper_id + "'})-[r:ACTIVE_IN]->(t:Team {_id: '" + partup_id + "'}) " +
                        "SET r.contributions=r.contributions+1";
                sendQuery(query);
                count_contributions = count_contributions + 1;
            }
        }
        System.out.println(count_contributions + " contributions imported into Neo4j.");

    }
    private static void ImportRatings() {

        //Sets the ratings a user received from other users to the user-team-edge as a collection.

        for (Document rating : ratingsDocuments) {
            String rated_upper_id = rating.getString("rated_upper_id");
            String partup_id = rating.getString("partup_id");
            int rating_value = rating.getInteger("rating");
            String query = "MATCH (u:User {_id: '" + rated_upper_id + "'})-[r:ACTIVE_IN]->(t:Team {_id: '" + partup_id + "'}) " +
                  "SET r.ratings=r.ratings+["+ rating_value + "]";
            sendQuery(query);
        }
        System.out.println(ratingsDocuments.size() + " ratings imported into Neo4j.");

    }
    private static void SetScores() {

        /** Sets a participation score of a user in a team to the user-team-edge as a float.
         *  This participation score is the average of two or more values:
         *  - Implicit rating:
         *    > The role a user has in a team: creator (2.0), partner (1.5) or supporter (1.0).
         *    > Number of contributions compared to the maximum number of contributions by a user. Weight: 2
         *    > Number of comments compared to the maximum number of comments by a user. Weight: 1
         *  - Received ratings transformed to a scale of 0 to 1.
         */

        for (Document user : usersDocuments) {
            String _id = user.getString("_id");
            String query = "MATCH (u:User)-[r:ACTIVE_IN]->(t:Team) " +
                    "WHERE u._id='" + _id + "' " +
                    "WITH MAX(r.contributions) AS maxContributions, MAX(r.comments) AS maxComments, u " +
                    "MATCH (u)-[r:ACTIVE_IN]->(t:Team) " +
                    "WITH r.role+(r.contributions/(toFloat(maxContributions)+0.00001)*2.0)+(r.comments/(toFloat(maxComments)+0.00001)*1.0) AS part, r " +
                    "SET r.participation=((REDUCE(avg=0, i IN r.ratings | avg + (i/20)))+part)/(LENGTH(r.ratings)+1)";
            sendQuery(query);
        }
        System.out.println("Participation scores set in Neo4j.");

    }
    private static void SetSimilarities() {

        /** Sets a similarity score of two teams to a new edge between those teams as a float.
         *  This score is the Pearson correlation-based similarity.
         *  It is based on the participation score of users in teams.
         */

        String query = "MATCH (t1:Team), (t2:Team) " +
                "WHERE t1<>t2 " +
                "MATCH (t1)<-[r:ACTIVE_IN]-(u:User) " +
                "WITH toFloat(AVG(r.participation)) AS t1Mean, t1, t2 " +
                "MATCH (t2)<-[r:ACTIVE_IN]-(u:User) " +
                "WITH toFloat(AVG(r.participation)) AS t2Mean, t1Mean, t1, t2 " +
                "MATCH (t1)<-[r1:ACTIVE_IN]-(u:User)-[r2:ACTIVE_IN]->(t2) " +
                "WITH SUM((r1.participation-t1Mean)*(r2.participation-t2Mean)) AS numerator," +
                "SQRT(SUM((r1.participation-t1Mean)^2) * SUM((r2.participation-t2Mean)^2)) AS denominator, t1, t2, COUNT(r1) AS r1Count " +
                "WHERE denominator<>0 AND r1Count>2 " +
                "MERGE (t1)-[q:SIMILARITY]-(t2) " +
                "SET q.coefficient=(numerator/denominator)";
        sendQuery(query);
        System.out.println("Similarity scores set in Neo4j.");

    }
}