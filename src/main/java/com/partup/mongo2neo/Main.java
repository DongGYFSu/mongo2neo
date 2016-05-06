package com.partup.mongo2neo;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;

import org.bson.Document;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

/**
 * MongoDB to Neo4j importer for Part-up data.
 * @author Maurits van der Goes
 */

public class Main {

    public static void main(String[] args) {

        MongoClient mongoClient = new MongoClient();
        MongoDatabase MDB = mongoClient.getDatabase("partup");

        String DB_PATH = "data/graph.db";
        GraphDatabaseService graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(new File( DB_PATH ));

        registerShutdownHook( graphDb );

        SimpleDateFormat date_format = new SimpleDateFormat("yyyyMMdd");

        //User
        List<Document> users = MDB.getCollection("users").find().into(new ArrayList<Document>());
        for (Document user : users) {
            String _id = user.getString("_id");
            Document profile = (Document) user.get("profile");
            String name_raw = profile.getString("name");
            String name = name_raw.replace("'", "");
            List email_list = (List) user.get("emails");
            Document email_doc = (Document) email_list.get(0);
            String email = email_doc.getString("address");
            Document settings = (Document) profile.get("settings");
            String language = settings.getString("locale");
            String website = (String) profile.get("website");
            double participation_score = Double.valueOf(user.get("participation_score").toString()).intValue();
            Document location = (Document) profile.get("location");
            if (location!=null) {
                String place_id = location.getString("place_id");
                if (place_id!=null) {
                    String city_raw = location.getString("city");
                    String city = city_raw.replace("'", "");
                    String country = location.getString("country");
                    String user_query = "MERGE (ci:City {_id: '" + place_id + "'})\n" +
                            "ON CREATE SET ci.name= '" + city + "'\n" +
                            "MERGE (co:Country {name: '" + country + "'})\n" +
                            "MERGE (u:User {_id:'" + _id + "'})\n" +
                            "SET u.name='" + name + "',\n" +
                            "u.email='" + email + "',\n" +
                            "u.language='" + language + "',\n" +
                            "u.tags=[],\n" +
                            "u.website='" + website + "',\n" +
                            "u.participation_score=" + participation_score + "," +
                            "u.active=true\n" +
                            "CREATE UNIQUE (u)-[:LIVES_IN]->(ci),\n" +
                            "(ci)-[:LOCATED_IN]->(co)";
                    graphDb.execute(user_query);
                } else {
                    String user_query = "MERGE (u:User {_id:'" + _id + "'})\n" +
                            "SET u.name='" + name + "',\n" +
                            "u.email='" + email + "',\n" +
                            "u.language='" + language + "',\n" +
                            "u.tags=[],\n" +
                            "u.website='" + website + "',\n" +
                            "u.participation_score=" + participation_score + "," +
                            "u.active=true";
                    graphDb.execute(user_query);
                }
            } else{
                String user_query = "MERGE (u:User {_id:'" + _id + "'})\n" +
                        "SET u.name='" + name + "',\n" +
                        "u.email='" + email + "',\n" +
                        "u.language='" + language + "',\n" +
                        "u.tags=[],\n" +
                        "u.website='" + website + "',\n" +
                        "u.participation_score=" + participation_score + "," +
                        "u.active=true";
                graphDb.execute(user_query);
            }
            List tags = (List) profile.get("tags");
            if (tags!=null) {
                for (int i = 0; i < tags.size(); i++) {
                    String query = "MERGE (u:User {_id: '" + _id + "'})\n" +
                            "SET u.tags=u.tags + ['" + tags.get(i) + "']";
                    graphDb.execute(query);
                }
            }
            Date deactivatedAt_raw = user.getDate("deactivatedAt");
            if (deactivatedAt_raw!=null){
                String deactivatedAt = date_format.format(deactivatedAt_raw);
                String query = "MERGE (u:User {_id: '" + _id + "'})\n" +
                        "SET u.deactivatedAt=" + deactivatedAt + ",\n" +
                        "u.active=false";
                graphDb.execute(query);
            }
            Document meurs = (Document) profile.get("meurs");
            if (meurs!=null){
                Boolean fetched_results = (Boolean) meurs.get("fetched_results");
                if (fetched_results!=null){
                    List results = (List) meurs.get("results");
                    Document results_0 = (Document) results.get(0);
                    int code_0 = results_0.getInteger("code");
                    String name_0 = results_0.getString("name");
                    int score_0 = results_0.getInteger("score");
                    Document results_1 = (Document) results.get(1);
                    int code_1 = results_1.getInteger("code");
                    String name_1 = results_1.getString("name");
                    int score_1 = results_1.getInteger("score");
                    String query = "MERGE (u:User {_id: '" + _id + "'})\n" +
                            "MERGE (s0:Strength {code: '" + code_0 + "'})\n" +
                            "ON CREATE SET s0.name= '" + name_0 + "'\n" +
                            "MERGE (s1:Strength {code: '" + code_1 + "'})\n" +
                            "ON CREATE SET s1.name= '" + name_1 + "'\n" +
                            "CREATE UNIQUE (u)-[r0:HOLDS]->(s0)\n" +
                            "SET r0.score=" + score_0 + "\n" +
                            "CREATE UNIQUE (u)-[r1:HOLDS]->(s1)\n" +
                            "SET r1.score=" + score_1;
                    graphDb.execute(query);
                }
            }
        }
        System.out.println(users.size() + " users imported into Neo4j.");

        //Networks
        List<Document> networks = MDB.getCollection("networks").find().into(new ArrayList<Document>());
        for (Document network : networks) {
            String _id = network.getString("_id");
            String name_raw = network.getString("name");
            String name = name_raw.replace("'", "");
            int privacy_type = network.getInteger("privacy_type");
            String slug = network.getString("slug");
            String admin_id = network.getString("admin_id");
            String language = network.getString("language");
            Document location = (Document) network.get("location");
            if (location!=null) {
                String place_id = location.getString("place_id");
                if (place_id!=null) {
                    String city_raw = location.getString("city");
                    String city = city_raw.replace("'", "\\'");
                    String country = location.getString("country");
                    String query = "MERGE (ci:City {_id: '" + place_id + "'})\n" +
                            "ON CREATE SET ci.name= '" + city + "'\n" +
                            "MERGE (co:Country {name: '" + country + "'})\n" +
                            "MERGE (u:User {_id: '" + admin_id + "'})\n" +
                            "MERGE (n:Network {_id:'" + _id + "'})\n" +
                            "SET n.name='" + name + "',\n" +
                            "n.tags=[],\n" +
                            "n.privacy_type=" + privacy_type + ",\n" +
                            "n.slug='" + slug + "',\n" +
                            "n.language='" + language + "'\n" +
                            "CREATE UNIQUE (u)-[:MEMBER_OF {admin:true}]->(n),\n" +
                            "(n)-[:LOCATED_IN]->(ci),\n" +
                            "(ci)-[:LOCATED_IN]->(co)";
                    graphDb.execute(query);
                } else {
                    String query = "MERGE (u:User {_id: '" + admin_id + "'})\n" +
                            "MERGE (n:Network {_id:'" + _id + "'})\n" +
                            "SET n.name='" + name + "',\n" +
                            "n.tags=[],\n" +
                            "n.privacy_type=" + privacy_type + ",\n" +
                            "n.slug='" + slug + "',\n" +
                            "n.language='" + language + "'\n" +
                            "CREATE UNIQUE (u)-[:MEMBER_OF {admin:true}]->(n)";
                    graphDb.execute(query);
                }
            } else {
                String query = "MERGE (u:User {_id: '" + admin_id + "'})\n" +
                        "MERGE (n:Network {_id:'" + _id + "'})\n" +
                        "SET n.name='" + name + "',\n" +
                        "n.tags=[],\n" +
                        "n.privacy_type=" + privacy_type + ",\n" +
                        "n.slug='" + slug + "',\n" +
                        "n.language='" + language + "'\n" +
                        "CREATE UNIQUE (u)-[:MEMBER_OF {admin:true}]->(n)";
                graphDb.execute(query);
            }
            List uppers = (List) network.get("uppers");
            if (uppers!=null) {
                for (int i = 0; i < uppers.size(); i++) {
                    if (uppers.get(i) != admin_id) {
                        String query = "MERGE (u:User {_id: '" + uppers.get(i) + "'})\n" +
                                "MERGE (n:Network {_id:'" + _id + "'})\n" +
                                "CREATE UNIQUE (u)-[:MEMBER_OF]->(n)";
                        graphDb.execute(query);
                    }
                }
            }
            List tags = (List) network.get("tags");
            if (tags!=null) {
                for (int i = 0; i < tags.size(); i++) {
                    String query = "MERGE (n:Network {_id: '" + _id + "'})\n" +
                            "SET n.tags=n.tags + ['" + tags.get(i) + "']";
                    graphDb.execute(query);
                }
            }
        }
        System.out.println(networks.size() + " networks imported into Neo4j.");

        //Teams
        List<Document> partups = MDB.getCollection("partups").find().into(new ArrayList<Document>());
        for (Document partup : partups) {
            String _id = partup.getString("_id");
            String name_raw = partup.getString("name");
            String name = name_raw.replace("'", "");
            String creator_id = partup.getString("creator_id");
            String language = partup.getString("language");
            int privacy_type = partup.getInteger("privacy_type");
            String type_partup = partup.getString("type");
            String phase = partup.getString("phase");
            Integer activity_count = (Integer) partup.get("activity_count");
            Date end_date_raw = partup.getDate("end_date");
            String end_date = date_format.format(end_date_raw);
            String network_id = partup.getString("network_id");
            String slug = partup.getString("slug");
            Date current_date_raw = new Date();
            Date created_at_raw = partup.getDate("created_at");
            String created_at = date_format.format(created_at_raw);
            long diff = current_date_raw.getTime() - created_at_raw.getTime();
            long days_active = TimeUnit.DAYS.convert(diff, TimeUnit.MILLISECONDS);
            if (network_id!=null) {
                Document location = (Document) partup.get("location");
                if (location!=null) {
                    String place_id = location.getString("place_id");
                    if (place_id!=null) {
                        String city_raw = location.getString("city");
                        String city = city_raw.replace("'", "");
                        String country = location.getString("country");
                        String user_query = "MERGE (ci:City {_id: '" + place_id + "'})\n" +
                                "ON CREATE SET ci.name= '" + city + "'\n" +
                                "MERGE (co:Country {name: '" + country + "'})\n" +
                                "MERGE (n:Network {_id: '" + network_id + "'})\n" +
                                "MERGE (u:User {_id: '" + creator_id + "'})\n" +
                                "MERGE (t:Team {_id:'" + _id + "'})\n" +
                                "SET t.name='" + name + "',\n" +
                                "t.end_date=" + end_date + ",\n" +
                                "t.tags=[],\n" +
                                "t.language='" + language + "',\n" +
                                "t.privacy_type=" + privacy_type + ",\n" +
                                "t.type='"+ type_partup + "',\n" +
                                "t.phase='" + phase + "',\n" +
                                "t.activity_count=" + activity_count + ",\n" +
                                "t.partners=1,\n" +
                                "t.active=true,\n" +
                                "t.created_at=" + created_at + ",\n" +
                                "t.days_active=" + days_active + ",\n" +
                                "t.link='https://part-up.com/partups/" + slug + "'\n" +
                                "CREATE UNIQUE (u)-[:ACTIVE_IN {creator:true, comments:0, contributions:0, pageViews:0, participation:0.0, ratings:[], role:2.0}]->(t),\n" +
                                "(t)-[:PART_OF]->(n),\n" +
                                "(t)-[:LOCATED_IN]->(ci),\n" +
                                "(ci)-[:LOCATED_IN]->(co)";
                        graphDb.execute(user_query);
                    } else {
                        String user_query = "MERGE (u:User {_id: '" + creator_id + "'})\n" +
                                "MERGE (t:Team {_id:'" + _id + "'})\n" +
                                "SET t.name='" + name + "',\n" +
                                "t.end_date=" + end_date + ",\n" +
                                "t.tags=[],\n" +
                                "t.language='" + language + "',\n" +
                                "t.privacy_type=" + privacy_type + ",\n" +
                                "t.type='"+ type_partup + "',\n" +
                                "t.phase='" + phase + "',\n" +
                                "t.activity_count=" + activity_count + ",\n" +
                                "t.partners=1,\n" +
                                "t.active=true,\n" +
                                "t.created_at=" + created_at + ",\n" +
                                "t.days_active=" + days_active + ",\n" +
                                "t.link='https://part-up.com/partups/" + slug + "'\n" +
                                "CREATE UNIQUE (u)-[:ACTIVE_IN {creator:true, comments:0, contributions:0, pageViews:0, participation:0.0, ratings:[], role:2.0}]->(t)";
                        graphDb.execute(user_query);
                    }
                } else{
                    String user_query = "MERGE (u:User {_id: '" + creator_id + "'})\n" +
                            "MERGE (t:Team {_id:'" + _id + "'})\n" +
                            "SET t.name='" + name + "',\n" +
                            "t.end_date=" + end_date + ",\n" +
                            "t.tags=[],\n" +
                            "t.language='" + language + "',\n" +
                            "t.privacy_type=" + privacy_type + ",\n" +
                            "t.type='"+ type_partup + "',\n" +
                            "t.phase='" + phase + "',\n" +
                            "t.activity_count=" + activity_count + ",\n" +
                            "t.partners=1,\n" +
                            "t.active=true,\n" +
                            "t.created_at=" + created_at + ",\n" +
                            "t.days_active=" + days_active + ",\n" +
                            "t.link='https://part-up.com/partups/" + slug + "'\n" +
                            "CREATE UNIQUE (u)-[:ACTIVE_IN {creator:true, comments:0, contributions:0, pageViews:0, participation:0.0, ratings:[], role:2.0}]->(t)";
                    graphDb.execute(user_query);
                }
            } else {
                Document location = (Document) partup.get("location");
                if (location!=null) {
                    String place_id = location.getString("place_id");
                    if (place_id!=null) {
                        String city_raw = location.getString("city");
                        String city = city_raw.replace("'", "");
                        String country = location.getString("country");
                        String user_query = "MERGE (ci:City {_id: '" + place_id + "'})\n" +
                                "ON CREATE SET ci.name= '" + city + "'\n" +
                                "MERGE (co:Country {name: '" + country + "'})\n" +
                                "MERGE (u:User {_id: '" + creator_id + "'})\n" +
                                "MERGE (t:Team {_id:'" + _id + "'})\n" +
                                "SET t.name='" + name + "',\n" +
                                "t.end_date=" + end_date + ",\n" +
                                "t.tags=[],\n" +
                                "t.language='" + language + "',\n" +
                                "t.privacy_type=" + privacy_type + ",\n" +
                                "t.type='"+ type_partup + "',\n" +
                                "t.phase='" + phase + "',\n" +
                                "t.activity_count=" + activity_count + ",\n" +
                                "t.partners=1,\n" +
                                "t.active=true,\n" +
                                "t.created_at=" + created_at + ",\n" +
                                "t.days_active=" + days_active + ",\n" +
                                "t.link='https://part-up.com/partups/" + slug + "'\n" +
                                "CREATE UNIQUE (u)-[:ACTIVE_IN {creator:true, comments:0, contributions:0, pageViews:0, participation:0.0, ratings:[], role:2.0}]->(t),\n" +
                                "(t)-[:LOCATED_IN]->(ci),\n" +
                                "(ci)-[:LOCATED_IN]->(co)";
                        graphDb.execute(user_query);
                    } else {
                        String user_query = "MERGE (u:User {_id: '" + creator_id + "'})\n" +
                                "MERGE (t:Team {_id:'" + _id + "'})\n" +
                                "SET t.name='" + name + "',\n" +
                                "t.end_date=" + end_date + ",\n" +
                                "t.tags=[],\n" +
                                "t.language='" + language + "',\n" +
                                "t.privacy_type=" + privacy_type + ",\n" +
                                "t.type='"+ type_partup + "',\n" +
                                "t.phase='" + phase + "',\n" +
                                "t.activity_count=" + activity_count + ",\n" +
                                "t.partners=1,\n" +
                                "t.active=true,\n" +
                                "t.created_at=" + created_at + ",\n" +
                                "t.days_active=" + days_active + ",\n" +
                                "t.link='https://part-up.com/partups/" + slug + "'\n" +
                                "CREATE UNIQUE (u)-[:ACTIVE_IN {creator:true, comments:0, contributions:0, pageViews:0, participation:0.0, ratings:[], role:2.0}]->(t)";
                        graphDb.execute(user_query);
                    }
                } else{
                    String user_query = "MERGE (u:User {_id: '" + creator_id + "'})\n" +
                            "MERGE (t:Team {_id:'" + _id + "'})\n" +
                            "SET t.name='" + name + "',\n" +
                            "t.end_date=" + end_date + ",\n" +
                            "t.tags=[],\n" +
                            "t.language='" + language + "',\n" +
                            "t.privacy_type=" + privacy_type + ",\n" +
                            "t.type='"+ type_partup + "',\n" +
                            "t.phase='" + phase + "',\n" +
                            "t.activity_count=" + activity_count + ",\n" +
                            "t.partners=1,\n" +
                            "t.active=true,\n" +
                            "t.created_at=" + created_at + ",\n" +
                            "t.days_active=" + days_active + ",\n" +
                            "t.link='https://part-up.com/partups/" + slug + "'\n" +
                            "CREATE UNIQUE (u)-[:ACTIVE_IN {creator:true, comments:0, contributions:0, pageViews:0, participation:0.0, ratings:[], role:2.0}]->(t)";
                    graphDb.execute(user_query);
                }
            }
            List partners = (List) partup.get("uppers");
            for (int i = 0; i < partners.size(); i++) {
                if (!partners.get(i).equals(creator_id)){
                    String query = "MERGE (u:User {_id: '" + partners.get(i) + "'})\n" +
                            "MERGE (t:Team {_id:'" + _id + "'})\n" +
                            "SET t.partners=t.partners+1\n" +
                            "CREATE UNIQUE (u)-[:ACTIVE_IN {comments:0, contributions:0, pageViews:0, participation:0.0, ratings:[], role:1.5}]->(t)";
                    graphDb.execute(query);
                }
            }
            List supporters = (List) partup.get("supporters");
            if (supporters!=null) {
                for (int i = 0; i < supporters.size(); i++) {
                    String query = "MERGE (u:User {_id: '" + supporters.get(i) + "'})\n" +
                            "MERGE (t:Team {_id:'" + _id + "'})\n" +
                            "CREATE UNIQUE (u)-[:ACTIVE_IN {comments:0, contributions:0, pageViews:0, participation:0.0, ratings:[], role:1.0}]->(t)";
                    graphDb.execute(query);
                }
            }
            List tags = (List) partup.get("tags");
            if (tags!=null) {
                for (int i = 0; i < tags.size(); i++) {
                    String query = "MERGE (t:Team {_id: '" + _id + "'})\n" +
                            "SET t.tags=t.tags + ['" + tags.get(i) + "']";
                    graphDb.execute(query);
                }
            }
            Date deleted_at_raw = partup.getDate("deleted_at");
            if (deleted_at_raw!=null){
                String deleted_at = date_format.format(deleted_at_raw);
                String query = "MERGE (t:Team {_id: '" + _id + "'})\n" +
                        "SET t.deleted_at=" + deleted_at + ",\n" +
                        "t.deleted=true,\n" +
                        "t.active=false";
                graphDb.execute(query);
            }
            Date archived_at_raw = partup.getDate("archived_at");
            if (archived_at_raw!=null){
                String archived_at = date_format.format(archived_at_raw);
                String query = "MERGE (t:Team {_id: '" + _id + "'})\n" +
                        "SET t.archived_at=" + archived_at + ",\n" +
                        "t.archived=true,\n" +
                        "t.active=false";
                graphDb.execute(query);
            }
        }
        System.out.println(partups.size() + " teams imported into Neo4j.");

        //Comments
        List<Document> comments = MDB.getCollection("updates").find().into(new ArrayList<Document>());
        int count_comments = 0;
        for (Document comment : comments) {
            String type = comment.getString("type");
            String _id = comment.getString("_id");
            if (type.equals("partups_message_added") || type.equals("partups_activities_comments_added") || type.equals("partups_contributions_comments_added")){
                String upper_id = comment.getString("upper_id");
                String partup_id = comment.getString("partup_id");
                String query = "MATCH (u:User {_id: '" + upper_id + "'})-[r:ACTIVE_IN]->(t:Team {_id: '" + partup_id + "'})" +
                        "SET r.comments=r.comments+1";
                graphDb.execute(query);
                count_comments = count_comments + 1;
            }
            int comments_count = comment.getInteger("comments_count");
            if (comments_count > 0){
                List repliesList = (List) comment.get("comments");
                for (int i = 0; i < repliesList.size(); i++) {
                    Document reply = (Document) repliesList.get(i);
                    String reply_type = reply.getString("type");
                    Boolean reply_system = reply.getBoolean("system");
                    if (reply_type!=null && reply_system!=null){
                        Document reply_creator = (Document) reply.get("creator");
                        String reply_upper_id = reply_creator.getString("_id");
                        String reply_partup_id = comment.getString("partup_id");
                        String query_reply = "MATCH (u:User {_id: '" + reply_upper_id + "'})-[r:ACTIVE_IN]->(t:Team {_id: '" + reply_partup_id + "'})\n" +
                                "SET r.comments=r.comments+1";
                        graphDb.execute(query_reply);
                        count_comments = count_comments + 1;
                    }
                }
            }
        }
        System.out.println(count_comments + " comments imported into Neo4j.");

        //Contributions
        List<Document> contributions = MDB.getCollection("contributions").find().into(new ArrayList<Document>());
        int count_contributions = 0;
        for (Document contribution : contributions) {
            Boolean verified = contribution.getBoolean("verified");
            if (verified) {
                String upper_id = contribution.getString("upper_id");
                String partup_id = contribution.getString("partup_id");
                String query = "MATCH (u:User {_id: '" + upper_id + "'})-[r:ACTIVE_IN]->(t:Team {_id: '" + partup_id + "'})\n" +
                        "SET r.contributions=r.contributions+1";
                graphDb.execute(query);
                count_contributions = count_contributions + 1;
            }
        }
        System.out.println(count_contributions + " contributions imported into Neo4j.");

        //Ratings
        List<Document> ratings = MDB.getCollection("ratings").find().into(new ArrayList<Document>());
        for (Document rating : ratings) {
            String rated_upper_id = rating.getString("rated_upper_id");
            String partup_id = rating.getString("partup_id");
            int rating_value = rating.getInteger("rating");
            String query = "MATCH (u:User {_id: '" + rated_upper_id + "'})-[r:ACTIVE_IN]->(t:Team {_id: '" + partup_id + "'})\n" +
                  "SET r.ratings=r.ratings+["+ rating_value + "]";
            graphDb.execute(query);
        }
        System.out.println(ratings.size() + " ratings imported into Neo4j.");

        //Score
//        for (Document user : users) {
//            String _id = user.getString("_id");
//            String query = "MATCH (u:User)-[r:ACTIVE_IN]->(t:Team)\n" +
//                    "WHERE u._id='" + _id + "'\n" +
//                    "WITH MAX(r.contributions) AS maxContributions, MAX(r.comments) AS maxComments, u\n" +
//                    "MATCH (u)-[r:ACTIVE_IN]->(t:Team)\n" +
//                    "WITH r.role+(r.contributions/(toFloat(maxContributions)+0.00001)*2.0)+(r.comments/(toFloat(maxComments)+0.00001)*1.0) AS part, r\n" +
//                    "SET r.participation=((REDUCE(avg=0, i IN r.ratings | avg + (i/20)))+part)/(LENGTH(r.ratings)+1)";
//            graphDb.execute(query);
//        }

        //Similarity
//        String query = "MATCH (t1:Team), (t2:Team)\n" +
//                "WHERE t1<>t2\n" +
//                "MATCH (t1)<-[r:ACTIVE_IN]-(u:User)\n" +
//                "WITH toFloat(AVG(r.participation)) AS t1Mean, t1, t2\n" +
//                "MATCH (t2)<-[r:ACTIVE_IN]-(u:User)\n" +
//                "WITH toFloat(AVG(r.participation)) AS t2Mean, t1Mean, t1, t2\n" +
//                "MATCH (t1)<-[r1:ACTIVE_IN]-(u:User)-[r2:ACTIVE_IN]->(t2)\n" +
//                "WITH SUM((r1.participation-t1Mean)*(r2.participation-t2Mean)) AS numerator," +
//                "SQRT(SUM((r1.participation-t1Mean)^2) * SUM((r2.participation-t2Mean)^2)) AS denominator, t1, t2, COUNT(r1) AS r1Count\n" +
//                "WHERE denominator<>0 AND r1Count>2\n" +
//                "MERGE (t1)-[q:SIMILARITY]-(t2)\n" +
//                "SET q.coefficient=(numerator/denominator)";
//        graphDb.execute(query);
//
        graphDb.execute("CREATE INDEX ON :User(_id)");
        graphDb.execute("CREATE INDEX ON :Network(_id)");
        graphDb.execute("CREATE INDEX ON :Team(_id)");
        graphDb.execute("CREATE INDEX ON :City(_id)");
        graphDb.execute("CREATE INDEX ON :Country(name)");
        graphDb.execute("CREATE INDEX ON :Strength(code)");
        System.out.println("Indexes for User, Network, Team, City, Country and Strength nodes created in Neo4j");

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