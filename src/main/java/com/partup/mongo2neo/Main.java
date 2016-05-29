package com.partup.mongo2neo;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.filter.HTTPBasicAuthFilter;

import java.awt.*;
import java.io.File;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.ws.rs.core.MediaType;

import org.apache.commons.lang3.StringUtils;

import org.bson.Document;

import org.neo4j.cypher.internal.compiler.v2_2.perty.Doc;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

/**
 * MongoDB to Neo4j importer for Part-up data.
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

        ImportUsers();
        ImportNetworks();
        ImportTeams();
        ImportComments();
        ImportContributions();
        ImportRatings();

        SetScores();
        SetSimilarities();
        CreateIndexes();

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

        System.out.println(String.format(
                "POST [%s] to [%s], status code [%d], returned data: "
                        + System.getProperty("line.separator") + "%s",
                payload, txUri, response.getStatus(),
                response.getEntity(String.class)));

        response.close();

        //Local
//        String DB_PATH = "data/graph.db";
//        GraphDatabaseService graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(new File( DB_PATH ));
//        graphDb.execute(query);
    }

    private static String ProcessTags(List tags) {
        List<String> tagsList = new ArrayList();
        for (int i = 0; i < tags.size(); i++) {
            tagsList.add(tags.get(i).toString());
        }
        String tagsString = "'" + StringUtils.join(tagsList, "','") + "'";
        return tagsString;
    }

    private static void ImportUsers() {

        String userQuery;

        for (Document user : usersDocuments) {
            String _id = user.getString("_id");
            String mergeUser = String.format("MERGE (u:User {_id:'%s'})", _id);
            Document profile = (Document) user.get("profile");
            Document settings = (Document) profile.get("settings");
            String language = settings.getString("locale");
            Document location = (Document) profile.get("location");
            if (location != null) {
                String place_id = location.getString("place_id");
                if (place_id != null) {
                    String city_raw = location.getString("city");
                    String city = city_raw.replace("'", "");
                    String country = location.getString("country");
                    userQuery = "MERGE (ci:City {_id: '" + place_id + "'})  " +
                            "ON CREATE SET ci.name= '" + city + "' " +
                            "MERGE (co:Country {name: '" + country + "'}) " +
                            mergeUser + " " +
                            "SET u.language='" + language + "', " +
                            "u.tags=[], " +
                            "u.active=true " +
                            "CREATE UNIQUE (u)-[:LIVES_IN]->(ci), " +
                            "(ci)-[:LOCATED_IN]->(co)";
                } else {
                    userQuery = mergeUser + " " +
                            "SET u.language='" + language + "', " +
                            "u.tags=[], " +
                            "u.active=true";
                }
            } else {
                userQuery = mergeUser + " " +
                        "SET u.language='" + language + "', " +
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
            int privacy_type = network.getInteger("privacy_type");
            List<String> adminList = (List<String>) network.get("admins");
            String admin_id = adminList.get(0);
            String language = network.getString("language");
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
                            "MERGE (u:User {_id: '" + admin_id + "'}) " +
                            "SET n.tags=[], " +
                            "n.privacy_type=" + privacy_type + ", " +
                            "n.language='" + language + "' " +
                            "CREATE UNIQUE (u)-[:MEMBER_OF {admin:true}]->(n), " +
                            "(n)-[:LOCATED_IN]->(ci), " +
                            "(ci)-[:LOCATED_IN]->(co)";
                } else {
                    networkQuery = mergeNetwork + " " +
                            "MERGE (u:User {_id: '" + admin_id + "'}) " +
                            "SET n.tags=[], " +
                            "n.privacy_type=" + privacy_type + ", " +
                            "n.language='" + language + "' " +
                            "CREATE UNIQUE (u)-[:MEMBER_OF {admin:true}]->(n)";
                }
            } else {
                networkQuery = "MERGE (u:User {_id: '" + admin_id + "'}) " +
                        mergeNetwork + " " +
                        "SET n.tags=[], " +
                        "n.privacy_type=" + privacy_type + ", " +
                        "n.language='" + language + "' " +
                        "CREATE UNIQUE (u)-[:MEMBER_OF {admin:true}]->(n)";
            }
            sendQuery(networkQuery);

            List uppers = (List) network.get("uppers");
            if (uppers != null) {
                List<String> mergeUser = new ArrayList();
                List<String> createUnique = new ArrayList();
                for (int i = 0; i < uppers.size(); i++) {
                    if (!uppers.get(i).equals(admin_id)) {
                        mergeUser.add(String.format("MERGE (u%s:User {_id: '%s'})", i, uppers.get(i)));
                        createUnique.add(String.format("CREATE UNIQUE (u%s)-[:MEMBER_OF]->(n)", i));
                    }
                }

                String query = StringUtils.join(mergeUser, " ") + " " + mergeNetwork + " " + StringUtils.join(createUnique, " ");
                sendQuery(query);
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
            String creator_id = partup.getString("creator_id");
            String language = partup.getString("language");
            int privacy_type = partup.getInteger("privacy_type");
            String type_partup = partup.getString("type");
            String phase = partup.getString("phase");
            Integer activity_count = (Integer) partup.get("activity_count");
            Date end_date_raw = partup.getDate("end_date");
            String end_date = date_format.format(end_date_raw);
            String network_id = partup.getString("network_id");
            Date created_at_raw = partup.getDate("created_at");
            String created_at = date_format.format(created_at_raw);
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
                                "SET t.end_date=" + end_date + ", " +
                                "t.tags=[], " +
                                "t.language='" + language + "', " +
                                "t.privacy_type=" + privacy_type + ", " +
                                "t.type='" + type_partup + "', " +
                                "t.phase='" + phase + "', " +
                                "t.activity_count=" + activity_count + ", " +
                                "t.partners=1, " +
                                "t.active=true, " +
                                "t.created_at=" + created_at + " " +
                                "CREATE UNIQUE (u)-[:ACTIVE_IN {creator:true, comments:0, contributions:0, pageViews:0, participation:0.0, ratings:[], role:2.0}]->(t), " +
                                "(t)-[:PART_OF]->(n), " +
                                "(t)-[:LOCATED_IN]->(ci), " +
                                "(ci)-[:LOCATED_IN]->(co)";
                    } else {
                        teamQuery = "MERGE (u:User {_id: '" + creator_id + "'}) " +
                                mergeTeam + " " +
                                "SET t.end_date=" + end_date + ", " +
                                "t.tags=[], " +
                                "t.language='" + language + "', " +
                                "t.privacy_type=" + privacy_type + ", " +
                                "t.type='" + type_partup + "', " +
                                "t.phase='" + phase + "', " +
                                "t.activity_count=" + activity_count + ", " +
                                "t.partners=1, " +
                                "t.active=true, " +
                                "t.created_at=" + created_at + " " +
                                "CREATE UNIQUE (u)-[:ACTIVE_IN {creator:true, comments:0, contributions:0, pageViews:0, participation:0.0, ratings:[], role:2.0}]->(t)";
                    }
                } else {
                    teamQuery = "MERGE (u:User {_id: '" + creator_id + "'}) " +
                            mergeTeam + " " +
                            "SET t.end_date=" + end_date + ", " +
                            "t.tags=[], " +
                            "t.language='" + language + "', " +
                            "t.privacy_type=" + privacy_type + ", " +
                            "t.type='" + type_partup + "', " +
                            "t.phase='" + phase + "', " +
                            "t.activity_count=" + activity_count + ", " +
                            "t.partners=1, " +
                            "t.active=true, " +
                            "t.created_at=" + created_at + " " +
                            "CREATE UNIQUE (u)-[:ACTIVE_IN {creator:true, comments:0, contributions:0, pageViews:0, participation:0.0, ratings:[], role:2.0}]->(t)";
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
                                "SET t.end_date=" + end_date + ", " +
                                "t.tags=[], " +
                                "t.language='" + language + "', " +
                                "t.privacy_type=" + privacy_type + ", " +
                                "t.type='" + type_partup + "', " +
                                "t.phase='" + phase + "', " +
                                "t.activity_count=" + activity_count + ", " +
                                "t.partners=1, " +
                                "t.active=true, " +
                                "t.created_at=" + created_at + " " +
                                "CREATE UNIQUE (u)-[:ACTIVE_IN {creator:true, comments:0, contributions:0, pageViews:0, participation:0.0, ratings:[], role:2.0}]->(t), " +
                                "(t)-[:LOCATED_IN]->(ci), " +
                                "(ci)-[:LOCATED_IN]->(co)";
                    } else {
                        teamQuery = "MERGE (u:User {_id: '" + creator_id + "'}) " +
                                mergeTeam + " " +
                                "SET t.end_date=" + end_date + ", " +
                                "t.tags=[], " +
                                "t.language='" + language + "', " +
                                "t.privacy_type=" + privacy_type + ", " +
                                "t.type='" + type_partup + "', " +
                                "t.phase='" + phase + "', " +
                                "t.activity_count=" + activity_count + ", " +
                                "t.partners=1, " +
                                "t.active=true, " +
                                "t.created_at=" + created_at + " " +
                                "CREATE UNIQUE (u)-[:ACTIVE_IN {creator:true, comments:0, contributions:0, pageViews:0, participation:0.0, ratings:[], role:2.0}]->(t)";
                    }
                } else {
                    teamQuery = "MERGE (u:User {_id: '" + creator_id + "'}) " +
                            mergeTeam + " " +
                            "SET t.end_date=" + end_date + ", " +
                            "t.tags=[], " +
                            "t.language='" + language + "', " +
                            "t.privacy_type=" + privacy_type + ", " +
                            "t.type='" + type_partup + "', " +
                            "t.phase='" + phase + "', " +
                            "t.activity_count=" + activity_count + ", " +
                            "t.partners=1, " +
                            "t.active=true, " +
                            "t.created_at=" + created_at + " " +
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
                        "SET t.deleted_at=" + deleted_at + ", " +
                        "t.deleted=true, " +
                        "t.active=false";
                sendQuery(query);
            }
            
            Date archived_at_raw = partup.getDate("archived_at");
            if (archived_at_raw != null) {
                String archived_at = date_format.format(archived_at_raw);
                String query = "MERGE (t:Team {_id: '" + _id + "'}) " +
                        "SET t.archived_at=" + archived_at + ", " +
                        "t.archived=true, " +
                        "t.active=false";
                sendQuery(query);
            }
        }
        System.out.println(teamsDocuments.size() + " teams imported into Neo4j.");

    }
    private static void ImportComments() {

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

        //Ratings

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

    }
    private static void SetSimilarities() {

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

    }
    private static void CreateIndexes() {

        sendQuery("CREATE INDEX ON :User(_id)");
        sendQuery("CREATE INDEX ON :Network(_id)");
        sendQuery("CREATE INDEX ON :Team(_id)");
        sendQuery("CREATE INDEX ON :City(_id)");
        sendQuery("CREATE INDEX ON :Country(name)");
        sendQuery("CREATE INDEX ON :Strength(code)");
        System.out.println("Indexes for User, Network, Team, City, Country and Strength nodes created in Neo4j");

        System.out.println("Happy Hunting!");

        return;
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