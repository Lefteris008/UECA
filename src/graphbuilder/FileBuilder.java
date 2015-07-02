package graphbuilder;

import db.lucene.LuceneRetriever;
import db.mongo.MongoHelper;
import entities.MyTweet;
import java.awt.Color;
import entities.Cluster;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.util.List;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.UnknownHostException;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TimeZone;
import java.util.regex.*;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.index.lucene.QueryContext;
import statistics.GeneralStatistics;
//import entities.LuceneEdgeEntity;

/**
 * 
 * @author  Paraskevas Eleftherios
 * @version 2015.7.2_1836
 */
public class FileBuilder {
    
    public class userMap {
        public int nodeCounter;
        public String userId;
    }

    public class edgesArray {
        public String source;
        public String destination;
    }

    public class Color {
        public int r;
        public int g;
        public int b;

        public Color(int r, int g, int b){
            this.r = r;
            this.b = b;
            this.g = g;
        }
    }
    
    /**
     * Replaces underscores with white space.
     * @param str The string to be replaced with white space
     * @return The cleaned string
     */
    public static String replaceUnderscoresWithWhiteSpace(String str) {
        return str.replace("_", " ");
    }
    
    /**
     * This method builds the stop words hash map.
     * @return A hash map which contains the stop words
     * @throws FileNotFoundException
     * @throws IOException 
     */
    public static HashMap<String, Integer> buildStopWordsMap() throws FileNotFoundException, IOException {
        
        String stopWordsFile = FilePaths.DEFAULT_ROOT_PATH+FilePaths.DATA_PARENT_FOLDER+FilePaths.BACKEND_LOCATION+FilePaths.STOP_WORDS_FILE+FilePaths.METAFILES_EXT;
        FileInputStream fstream = new FileInputStream(stopWordsFile);
        DataInputStream in = new DataInputStream(fstream);
        BufferedReader br = new BufferedReader(new InputStreamReader(in));
        String str;
        HashMap<String, Integer> stopWords = new HashMap<String, Integer>();
        while((str = br.readLine()) != null) {
            stopWords.put(str, 0);
        }
        return stopWords;
    }
    
    /**
     * Determines whether a given is string is URL.
     * @param str The string to be checked
     * @return True if the given string is URL, false otherwise
     */
    public static boolean isURL(String str) {
        if(str.length() > 5) {
            char first = str.charAt(0);
            char second = str.charAt(1);
            char third = str.charAt(2);
            char fourth = str.charAt(3);
            char fifth = str.charAt(4);
            if(first == 'h' && second == 't' && third == 't' && fourth == 'p' && fifth == ':' ) {
                return true;
            }
            else {
                return false;
            }
        }
        return false;
    }
    
    /**
     * This method extracts the terms from a give tweet
     * @param hashtags A list that after the execution of this method will contain the hashtags of the tweet
     * @param urls A list that after the execution of this method will contain the urls of the tweet
     * @param tweet The tweet to be analyzed
     * @param stopWordsMap A hash map that after the execution of this method will contain the stop words of the tweet
     * @param terms A map that after the execution of this method will contain the terms of the tweet
     */
    public static void extractTermsFromTweet(List<String> hashtags, List<String> urls, String tweet, HashMap<String, Integer> stopWordsMap, Map<String, Integer> terms) {
        
        String word;
        String cleanedWord = null;
        int index; //Inde of ' '
        while(tweet != null) {
            index = tweet.indexOf(" "); //Calculate index of ' '
            if(index != -1) {
                word = tweet.substring(0, index); //Get the word
                cleanedWord = word.replaceAll("[^a-zA-Z0-9@]+"," ");
            }
            else {
                word = tweet;
                cleanedWord = word.replaceAll("[^a-zA-Z0-9@]+"," ");
            }
            if(!word.equals("") && word.charAt(0) != '#' && !isURL(word) && !stopWordsMap.containsKey(cleanedWord.toLowerCase())) { //If the word is not a hashtag, a URL or a stop word, add it
                if(terms.containsKey(cleanedWord.toLowerCase())) {
                    terms.put(cleanedWord.toLowerCase(), terms.get(cleanedWord.toLowerCase()) + 1);
                }
                else {
                    terms.put(cleanedWord.toLowerCase(), 1);
                }
            }
            if(index != -1) {
                tweet = tweet.substring(index+1, tweet.length()); //Remove the word for the tweet
            }
            else {
                tweet = null;
            }

        }
    } 
    
    /**
     * This method collects the hashtags/tweets/terms for a given theme from the corresponding DBs
     * @param choice 1 for tweets, 2 for hashtags, 3 for terms
     * @param theme The theme for which we want to extract the hashtags/tweets/terms
     * @param tf The timeframe in which we want to extract the needed information
     * @param graphDB A handler for the Neo4j DB
     * @param lucR A handler for the LuceneDB
     * @param type The type of the tweet. 'null' stands for mentions, while retweets and replies are given with their specific words
     * @param hasRetweets States if the picked theme has or has not retweets
     * @return A map that contains the tweets/hashtags/terms as keys and the number of their occurrences as values
     * @throws IOException 
     */
    public static Map<String, Integer> collectTweetsHashtagsTerms(int choice, String theme, String tf, GraphDatabaseService graphDB, LuceneRetriever lucR, String type, boolean hasRetweets) throws IOException {
        
        //String neoPath = FilePaths.DEFAULT_ROOT_PATH+FilePaths.DATA_PARENT_FOLDER+FilePaths.DBs_FOLDER+ theme +FilePaths.NEO_FOLDER;
        //GraphDatabaseService graphDB = new GraphDatabaseFactory().newEmbeddedDatabase(neoPath); //Opening Neo4j DB
        IndexManager index1 = graphDB.index();
        Index<Node> index = index1.forNodes("clusternodes");
        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        IndexHits<Node> hits = index.query(QueryContext.numericRange("reference_time_frame", Long.valueOf(tf), Long.valueOf(tf), true, true)); 

        //get clusters
        List<Cluster> clusters = new ArrayList<Cluster>();
        for(Node cl : hits){
            
            try {
                clusters.add(new Cluster((Long)cl.getProperty("cluster_id"), (Integer)cl.getProperty("local_cluster_id"),Long.valueOf(tf), dateFormat.parse((String)cl.getProperty("from_date")), dateFormat.parse((String)cl.getProperty("to_date")),(String)cl.getProperty("rel_type")));
             } catch (ParseException e) {
                e.printStackTrace();
            }
        }
        Node cnode;
        Iterable<Relationship> rels = null;
        Map<Long, Long> clustAss = new HashMap<Long, Long>();

        for(Cluster cl : clusters) {
            //Get cluster node
            cnode = index.get("cluster_id", cl.getId()).getSingle();
            try {
                //For each cluster, get all users
                rels = cnode.getRelationships(RelTypes.BELONGS_IN);
            }
            catch (NullPointerException e) {
                e.printStackTrace();
                graphDB.shutdown();
                return null;
            }
            for(Relationship r : rels) {
                //Store all user nodes to 'users' list
                clustAss.put(Long.parseLong(r.getOtherNode(cnode).getProperty("user_id").toString()),cl.getId());
            }
        }
        //graphDB.shutdown();
        List<List<MyTweet>> superList = new ArrayList<List<MyTweet>>();
        MongoHelper mongo = new MongoHelper(theme, hasRetweets);
        //String lucenePath = FilePaths.DEFAULT_ROOT_PATH+FilePaths.DATA_PARENT_FOLDER+FilePaths.DBs_FOLDER+ theme +FilePaths.LUCENE_FOLDER;
        //LuceneRetriever lucR;
       
        Set<Long> keys = clustAss.keySet();
        //για κάθε ζεύγος χρηστών μέσα σε κάθε cluster πάρε τις μεταξύ τους σχέσεις (από LuceneRetriever)

        //lucR = new LuceneRetriever(theme, false, lucenePath, false);
        List<entities.LuceneEdgeEntity> list1;
        List<entities.LuceneEdgeEntity> list;
        for(Long z : keys) {
            list1 = lucR.getUserRelationships(Long.parseLong(tf), z);
            for(int j=0;j<list1.size();j++) { 
                list = lucR.getRelationshipsBetweenUsers(list1.get(j).getTf(), list1.get(j).getDest(), list1.get(j).getSrc());
                for(int i=0;i<list.size();i++) {
                    String relId = "";
                    entities.LuceneEdgeEntity e;
                    e = list.get(i);
                    if(type == null) {
                        if(e.getRel().equals(db.neo.properties.RelTypes.MENTION)){
                            relId = e.getSrc() + "_" + e.getDest() + "_MENTION";
                        }else if(e.getRel().equals(db.neo.properties.RelTypes.REPLY)){
                            relId = e.getSrc() + "_" + e.getDest() + "_REPLY";
                        }
                        else if(e.getRel().equals(db.neo.properties.RelTypes.RETWEET)){
                            relId = e.getSrc() + "_" + e.getDest() + "_RETWEET";
                        }
                        else{
                            System.err.println("Unsupported relationship type: " + e.getRel().toString());
                            System.exit(99);
                        }
                    }
                    else if(type.equals("MENTION")) {
                        if(e.getRel().equals(db.neo.properties.RelTypes.MENTION)){
                            relId = e.getSrc() + "_" + e.getDest() + "_MENTION";
                        }
                        else {
                            continue;
                        }
                    }
                    else if(type.equals("REPLY")) {
                        if(e.getRel().equals(db.neo.properties.RelTypes.REPLY)){
                            relId = e.getSrc() + "_" + e.getDest() + "_REPLY";
                        }
                        else {
                            continue;
                        }
                    }
                    else { //RETWEET
                        if(e.getRel().equals(db.neo.properties.RelTypes.RETWEET)){
                            relId = e.getSrc() + "_" + e.getDest() + "_RETWEET";
                        }
                        else {
                            continue;
                        }
                    }
                    superList.add(mongo.getAllTweetsInRelationship(relId));
                }
            }
        }
        Map<String, Integer> resultsMap = new HashMap<String, Integer>();
        String key;
        int numOfTweets = 0;
        if(choice == 1) { //TWEETS
            for(int z=0;z<superList.size();z++) {
                for(int w=0;w<superList.get(z).size();w++) {
                    numOfTweets += superList.get(z).size();
                }
            }
            resultsMap.put("tweets", numOfTweets);
        }
        else if(choice == 2) { //HASHTAGS
            for(int i=0;i<superList.size();i++) {
                for(int j=0;j<superList.get(i).size();j++) {
                    for(int z=0;z<superList.get(i).get(j).tags.size();z++) {
                        key = superList.get(i).get(j).tags.get(z);
                        if(resultsMap.containsKey(key)) {
                            resultsMap.put(key, resultsMap.get(key) + 1);
                        }
                        else {
                            resultsMap.put(key, 1);
                        }
                    }
                }
            }
        }
        else {
            String tweet;
            HashMap<String, Integer> stopWords = new HashMap<String, Integer>();
            stopWords = buildStopWordsMap();
            for(int i=0;i<superList.size();i++) {
                for(int j=0;j<superList.get(i).size();j++) {
                    tweet = superList.get(i).get(j).text;
                    extractTermsFromTweet(superList.get(i).get(j).tags, superList.get(i).get(j).uris, tweet, stopWords, resultsMap);
                }
            } 
        }
        return resultsMap;
    }   
     
    /**
     * This method creates, builds and stores the communities GEXF file that will be used for visualizing the communities social graphs.
     * @param users A list with the communities users
     * @param cluster The community
     * @param theme The picked theme
     * @param tf The timeframe in which we want to examine the community
     * @return True if the file was successfully created, false otherwise
     * @throws IOException 
     */
    public boolean buildCommGEXF(List<Node> users, Cluster cluster, String theme, String tf) throws IOException {
   //     GraphBuilder gb = new GraphBuilder();
        String gexfPath = FilePaths.DEFAULT_ROOT_PATH+FilePaths.DATA_PARENT_FOLDER+FilePaths.METAFILES_FOLDER+ theme +FilePaths.GEXF_FOLDER;
        String lucenePath = FilePaths.DEFAULT_ROOT_PATH+FilePaths.DATA_PARENT_FOLDER+FilePaths.DBs_FOLDER+ theme +FilePaths.LUCENE_FOLDER;
        Map<Long,Integer> usersMap = new HashMap<Long,Integer>();
        String community = null;
        if(cluster != null) {
            community = Long.toString(cluster.getId());
            File f = null;
            FileWriter fstream = null;
            BufferedWriter out = null;
            int nodeCounter = 0;
            int edgeCounter = 0;
//            int source = -1;
//            int destination = -1;
            String userId;
            String userName;
//            StringBuilder userIdPair = new StringBuilder();
//            StringBuilder userIdPairTemp = new StringBuilder();
//            StringBuilder userType = new StringBuilder();

//            int j;
//            int k;
//            boolean foundJ = false;
//            boolean foundK = false;
            try {
                f = new File(gexfPath+"/"+community+".gexf");
                if(!f.exists()) {
                    f.createNewFile();
                    fstream = new FileWriter(gexfPath+"/"+community+".gexf");
                    out = new BufferedWriter(fstream);
                    out.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
                    out.newLine();
                    out.write("<gexf xmlns=\"http://www.gexf.net/1.2draft\" xmlns:viz=\"http://www.gexf.net/1.1draft/viz\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:schemaLocation=\"http://www.gexf.net/1.2draft http://www.gexf.net/1.2draft/gexf.xsd\" version=\"1.2\">"
                    +"");
                    out.newLine();
                    out.write("\t<graph type=\"static\" defaultedgetype=\"undirected\">=");
                    out.newLine();
                    out.write("\t\t<nodes>");
                    out.newLine();
                    
                    Set<Long> userSet = new HashSet<Long>();
                    for(Node user : users) {
                        userId = user.getProperty("user_id").toString();
                        userName = user.getProperty("username").toString();
                        userSet.add(Long.parseLong(userId));
                        // userIdPairTemp = new StringBuilder();
                        //   userType = new StringBuilder();
                        // gb.luceneHandler(lucenePath, userId, userIdPairTemp, userType);
                        if(!userName.equals("UNDEFINED")) {
                            out.write("\t\t\t<node id=\""+nodeCounter+"\" label=\""+userName+" ("+userId+")\">");
                        }
                        else {
                            out.write("\t\t\t<node id=\""+nodeCounter+"\" label=\""+userId+"\">");
                        }
                        // userMap usersMap = new userMap();
                        // usersMap.nodeCounter = nodeCounter;
                        // usersMap.userId = userId;
                        usersMap.put(Long.parseLong(userId),nodeCounter);
                        //ob.add(usersMap);
                        nodeCounter++;
                        out.newLine();
//                        if("MENTION".equals(userType.toString())) {
//                            out.write("\t\t\t\t<color r=\"207\" g=\"41\" b=\"46\"/>");
//                            out.newLine();
//                        }
//                        else if("RETWEET".equals(userType.toString())) {
//                            out.write("\t\t\t\t<color r=\"51\" g=\"83\" b=\"236\"/>");
//                            out.newLine();
//                        }
//                        else {
//                            out.write("\t\t\t\t<color r=\"208\" g=\"204\" b=\"55\"/>");
//                            out.newLine();
//                        }
                        out.write("\t\t\t\t<color r=\"112\" g=\"112\" b=\"112\"/>");
                        out.newLine();
                        out.write("\t\t\t</node>");
                        out.newLine();
                    }
                    out.write("\t\t</nodes>");
                    out.newLine();
                    out.write("\t\t<edges>");
                    out.newLine();

                    LuceneHandler luc = new LuceneHandler(lucenePath);
                    List<LuceneEdgeEntity> userMRels = luc.getEdgesInTimeframe(Long.parseLong(tf), RelTypes.MENTION);
                    
                    for(LuceneEdgeEntity en : userMRels){
                        if(userSet.contains(en.getSrc()) && userSet.contains(en.getDest())){
                            out.write("\t\t\t<edge id=\""+edgeCounter+"\" source=\""+usersMap.get(en.getSrc()) +"\" target=\""+usersMap.get(en.getDest())+"\" weight=\""+ en.getWeight()*2.0 +"\"" + ">");
                            edgeCounter++;
                            out.newLine();
                            out.write("\t\t\t\t<color r=\"207\" g=\"41\" b=\"46\"/>");
                            out.newLine();
                            out.write("\t\t\t</edge>");
                            out.newLine();
                        }
                                
                    }
                     List<LuceneEdgeEntity> userRRels = luc.getEdgesInTimeframe(Long.parseLong(tf), RelTypes.REPLY);
                     for(LuceneEdgeEntity en : userRRels){
                        if(userSet.contains(en.getSrc()) && userSet.contains(en.getDest())){
                            out.write("\t\t\t<edge id=\""+edgeCounter+"\" source=\""+usersMap.get(en.getSrc()) +"\" target=\""+usersMap.get(en.getDest())+"\" weight=\""+ en.getWeight()*2.0 +"\"" + ">");
                            edgeCounter++;
                            out.newLine();
                            out.write("\t\t\t\t<color r=\"208\" g=\"204\" b=\"55\"/>");
                            out.newLine();
                            out.write("\t\t\t</edge>");
                            out.newLine();
                        }
                                
                    }
                     List<LuceneEdgeEntity> userRetRels = luc.getEdgesInTimeframe(Long.parseLong(tf), RelTypes.RETWEET);
                     for(LuceneEdgeEntity en : userRetRels){
                        if(userSet.contains(en.getSrc()) && userSet.contains(en.getDest())){
                            out.write("\t\t\t<edge id=\""+edgeCounter+"\" source=\""+usersMap.get(en.getSrc()) +"\" target=\""+usersMap.get(en.getDest())+"\" weight=\""+ en.getWeight()*2.0 +"\"" + ">");
                            edgeCounter++;
                            out.newLine();
                            out.write("\t\t\t\t<color r=\"51\" g=\"83\" b=\"236\"/>");
                            out.newLine();
                            out.write("\t\t\t</edge>");
                            out.newLine();
                        }
                                
                    }
                 
                    out.write("\t\t</edges>");
                    out.newLine();
                    out.write("\t</graph>");
                    out.newLine();
                    out.write("</gexf>");
                    out.close();

                }
                
            }
            catch (IOException e) {
                return false;
            }
        }
        
        return true;
    }
    
    /**
     * Returns the hexadecimal number for a given color
     * @param colorStr The given color
     * @return The hexadecimal number of the color
     */
    public Color hex2Rgb(String colorStr) {
    return new Color(
            Integer.valueOf( colorStr.substring( 0, 2 ), 16 ),
            Integer.valueOf( colorStr.substring( 2, 4 ), 16 ),
            Integer.valueOf( colorStr.substring( 4, 6 ), 16 ) );
    }
    
    /**
     * This method creates a GEXF for all the communities that exist in a given timeframe.
     * @param clustAss A map containing the clusters in the given timeframe
     * @param clustAssUserNames A map containing the users in the given timeframe
     * @param hubs A set containing the hubs
     * @param outliers A set containing the outliers
     * @param theme The picked theme
     * @param entirePath An auxiliary variable for the file path
     * @param tf The given timeframe
     * @param hasMentions Boolean variable of mentions
     * @param hasRetweets Boolean variable of retweets
     * @param hasReplies Boolean variable of replies
     * @return True if the file was successfully created, false otherwise
     * @throws IOException 
     */
    public boolean buildTFGEXF(Map<Long, Long> clustAss, Map<Long, String> clustAssUserNames, Set<Long> hubs, Set<Long> outliers, String theme, String entirePath, String tf, boolean hasMentions, boolean hasRetweets, boolean hasReplies) throws IOException {
  
        Map<Long,Integer> usersMap = new HashMap<Long,Integer>();
        String lucenePath = FilePaths.DEFAULT_ROOT_PATH+FilePaths.DATA_PARENT_FOLDER+FilePaths.DBs_FOLDER+ theme +FilePaths.LUCENE_FOLDER;
        
        File f = null;
        FileWriter fstream = null;
        BufferedWriter out = null;
        int nodeCounter = 0;
        int edgeCounter = 0;
        int source = -1;
        int destination = -1;
        String userName = null;
      //  String userId;
//        StringBuilder userIdPair = new StringBuilder();
//        StringBuilder userIdPairTemp = new StringBuilder();
//        StringBuilder userType = new StringBuilder();

//        int i;
//        int j;
//        int k;
//        boolean foundJ = false;
//        boolean foundK = false;
        
            try {
                f = new File(entirePath+tf+FilePaths.SEPARATOR); //Folder
                if(!f.exists()) {
                    f.mkdirs();
                    f.setWritable(true);
                    f.setReadable(true);
                }
                f = new File(entirePath+tf+FilePaths.SEPARATOR+tf+".gexf"); //e.g. 4.gexf, is the entire graph of theme in timeframe 4
                if(!f.exists()) {
                    f.createNewFile();
                    f.setWritable(true);
                    f.setReadable(true);
                    fstream = new FileWriter(entirePath+tf+FilePaths.SEPARATOR+tf+".gexf");
                    out = new BufferedWriter(fstream);
                    out.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
                    out.newLine();
                    out.write("<gexf xmlns=\"http://www.gexf.net/1.2draft\" xmlns:viz=\"http://www.gexf.net/1.1draft/viz\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:schemaLocation=\"http://www.gexf.net/1.2draft http://www.gexf.net/1.2draft/gexf.xsd\" version=\"1.2\">"
                    +"");
                    out.newLine();
                    out.write("\t<graph type=\"static\" defaultedgetype=\"directed\">=");
                    out.newLine();
                    out.write("\t\t<nodes>");
                    out.newLine();
                    int r = -1;
                    int g = -1;
                    int b = -1;
//                    r = 0 + (int)(Math.random() * ((256 - 0) + 1)); //Generate a random number between 0-256
//                    g = 0 + (int)(Math.random() * ((256 - 0) + 1)); //Generate a random number between 0-256
//                    b = 0 + (int)(Math.random() * ((256 - 0) + 1)); //Generate a random number between 0-256
                   String[] colourValues = new String[] { 
                        "FF0000", "00FF00", "0000FF", "FFFF00", "FF00FF", "00FFFF",  
                        "800000", "008000", "000080", "808000", "800080", "008080", "808080", 
                        "C00000", "00C000", "0000C0", "C0C000", "C000C0", "00C0C0", "C0C0C0", 
                        "400000", "004000", "000040", "404000", "400040", "004040", "404040", 
                        "200000", "002000", "000020", "202000", "200020", "002020", "202020", 
                        "600000", "006000", "000060", "606000", "600060", "006060", "606060", 
                        "A00000", "00A000", "0000A0", "A0A000", "A000A0", "00A0A0", "A0A0A0", 
                        "E00000", "00E000", "0000E0", "E0E000", "E000E0", "00E0E0", "E0E0E0", 
                    };
                   
                    Set<Long> users = new HashSet<Long>();
                    LuceneHandler luc = new LuceneHandler(lucenePath);
                    List<LuceneEdgeEntity> userMRels = luc.getEdgesInTimeframe(Long.parseLong(tf), RelTypes.MENTION);
                    for(LuceneEdgeEntity en : userMRels){
                        users.add(en.getSrc());
                        users.add(en.getDest());
                    }
                    List<LuceneEdgeEntity> userRRels = luc.getEdgesInTimeframe(Long.parseLong(tf), RelTypes.REPLY);
                    for(LuceneEdgeEntity en : userRRels){
                        users.add(en.getSrc());
                        users.add(en.getDest());
                    }
                    List<LuceneEdgeEntity> userRetRels = luc.getEdgesInTimeframe(Long.parseLong(tf), RelTypes.RETWEET);
                    for(LuceneEdgeEntity en : userRetRels){
                        users.add(en.getSrc());
                        users.add(en.getDest());
                    }
                    
                    Color currColor = null;
                    double currSize = 1.0;
                    Map<Long,Color> clustColors = new HashMap<Long,Color>();
                    Color hubColor = new Color(0,0,0);
                    Color outlierColor = new Color(0,0,0);
                    Set<String> usedColors = new HashSet<String>();
                    Random rand = new Random();
                    for(Long userId : users) {
                        Long currClustId = clustAss.get(userId);
                        if(currClustId != null){
                            if(clustColors.containsKey(currClustId)) {
                                currColor = clustColors.get(currClustId);
                                currSize = 1.0;
                            }
                            else{
//                                r = 0 + (int)(Math.random() * ((256 - 0) + 1)); //Generate a random number between 0-256
//                                g = 0 + (int)(Math.random() * ((256 - 0) + 1)); //Generate a random number between 0-256
//                                b = 0 + (int)(Math.random() * ((256 - 0) + 1)); //Generate a random number between 0-256
//                               
//                                while(r == 0 && b == 0 && g == 0){
//                                    r = 0 + (int)(Math.random() * ((256 - 0) + 1)); //Generate a random number between 0-256
//                                    g = 0 + (int)(Math.random() * ((256 - 0) + 1)); //Generate a random number between 0-256
//                                    b = 0 + (int)(Math.random() * ((256 - 0) + 1)); //Generate a random number between 0-256
//                                }
                               
                               int index = rand.nextInt(colourValues.length);
                                while(usedColors.contains(colourValues[index])){
                                    index = rand.nextInt(colourValues.length);
                                }
                                usedColors.add(colourValues[index]);
                                currColor = hex2Rgb(colourValues[index]);
                              //  currColor = new Color(r,g,b);
                                clustColors.put(currClustId, currColor);
                                currSize = 1.0;
                            }
                        }else if(hubs != null && hubs.contains(userId)){
                            currColor = new Color(0,0,0);
                            currSize = 3;
                        }else if(outliers != null && outliers.contains(userId)){
                            currColor = new Color(0,0,0);
                            currSize = 0.5;
                        }
                        userName = clustAssUserNames.get(userId);
                        if(userName == null) {
                            userName = "UNDEFINED";
                        }
                        if(!userName.equals("UNDEFINED")) {
                            out.write("\t\t\t<node id=\""+nodeCounter+"\" label=\""+userName+" ("+userId+")\">");
                        }
                        else {
                            out.write("\t\t\t<node id=\""+nodeCounter+"\" label=\""+userId+"\">");
                        }
                        //out.write("\t\t\t<node id=\""+nodeCounter+"\" label=\""+userId+"\">");
                        usersMap.put(userId,nodeCounter);
                        nodeCounter++;
                        out.newLine();
                        out.write("\t\t\t\t<color b=\""+ currColor.b+"\" g=\""+currColor.g+"\" r=\""+currColor.r+"\"/>");
                        out.newLine();
                        out.write("\t\t\t\t<size value=\"" + currSize + "\"/>");
                        out.newLine();
                        out.write("\t\t\t</node>");
                        out.newLine();
                    }
                  
                    out.write("\t\t</nodes>");
                    out.newLine();
                    out.write("\t\t<edges>");
                    out.newLine();

                    for(LuceneEdgeEntity en : userMRels){
                        out.write("\t\t\t<edge id=\""+edgeCounter+"\" source=\""+usersMap.get(en.getSrc()) +"\" target=\""+usersMap.get(en.getDest())+ "\" weight=\""+ en.getWeight()*2.0 +"\"" + ">");
                        edgeCounter++;
                        out.newLine();
                        out.write("\t\t\t\t<color r=\"0\" g=\"0\" b=\"0\"/>");
                        out.newLine();
                        out.write("\t\t\t</edge>");
                        out.newLine();   
                    }
                    
                     for(LuceneEdgeEntity en : userRRels){
                            out.write("\t\t\t<edge id=\""+edgeCounter+"\" source=\""+usersMap.get(en.getSrc()) +"\" target=\""+usersMap.get(en.getDest())+ "\" weight=\""+ en.getWeight()*2.0 +"\"" + ">");
                            edgeCounter++;
                            out.newLine();
                            out.write("\t\t\t\t<color r=\"0\" g=\"0\" b=\"0\"/>");
                            out.newLine();
                            out.write("\t\t\t</edge>");
                            out.newLine();
                                
                    }
                     for(LuceneEdgeEntity en : userRetRels){
                            out.write("\t\t\t<edge id=\""+edgeCounter+"\" source=\""+usersMap.get(en.getSrc()) +"\" target=\""+usersMap.get(en.getDest())+ "\" weight=\""+ en.getWeight()*2.0 +"\"" + ">");
                            edgeCounter++;
                            out.newLine();
                            out.write("\t\t\t\t<color r=\"0\" g=\"0\" b=\"0\"/>");
                            out.newLine();
                            out.write("\t\t\t</edge>");
                            out.newLine();
                        }
                                
                    }
                    out.write("\t\t</edges>");
                    out.newLine();
                    out.write("\t</graph>");
                    out.newLine();
                    out.write("</gexf>");
                    out.close();
             }

            catch (IOException e) {
                e.printStackTrace();
                return false;
            }
        return true;
    }
    
    /**
     * This method handles the split of a community into others. It basically searches the entire file of the community evolution
     * and finds the evolution of a single community after a split (and supplementary, the evolution of the communities that the
     * primary community had been split to). The evolution is stored in the first parameter.
     * @param comEvolListCurrent The evolution of the current community
     * @param comEvolListAll The evolution of all the communities
     * @param splitID The ID of the community that has been split
     * @return True if the process has been successful, false otherwise
     */
    public boolean handleSplit(List<String> comEvolListCurrent, ArrayList comEvolListAll, String splitID) {
        List<String> temp = new ArrayList<String>();
        int pointer = 0;
        int i;
        int j;
        List<String> temp1 = new ArrayList<String>();
        i = 0;
        boolean found = false;
        while(i<comEvolListCurrent.size() && !found) {
            if(splitID.equals(comEvolListCurrent.get(i))) { //If we didn't reached splitID
                pointer = i; //Get position
                found = true;
                i++;
            }
            else {
                temp.add(comEvolListCurrent.get(i));
                i++;
            }
        }
        for(i=0;i<comEvolListAll.size();i++) {
            temp1 = (List<String>) comEvolListAll.get(i);
            if(!splitID.equals(temp1.get(0))) {
                continue; //Iterate until we find splitID cluster evolution
            }
            for(j=0;j<temp1.size();j++) {
                temp.add(temp1.get(j)); //Add splitID cluster evolution
            }
            break;
        }
        for(i=pointer+1;i<comEvolListCurrent.size();i++) {
            temp.add(comEvolListCurrent.get(i));
        }
        comEvolListCurrent.removeAll(comEvolListCurrent);
        comEvolListCurrent.addAll(temp);
        return true;
    }
    
    /**
     * This method handles the merge of two or more communities into one. Specifically, it tracks the evolution of the communities that have been merged
     * and the evolution of the communities that has been generated and stores that in the 'ob' variable.
     * @param ob A list containing the evolution of the merged communities
     * @param comEvolListAll The evolution of all the communities
     * @param mergeID The ID of the communities that had been merged into
     * @return True if the process has been successful, false otherwise
     */
    public boolean handleMerge(List<Object> ob, ArrayList comEvolListAll, String mergeID) {
        int i;
        int j;
        List<String> temp = new ArrayList<String>();
        for(i=0;i<comEvolListAll.size();i++) {
            temp = (List<String>) comEvolListAll.get(i); //Get first
            for(j=0;j<temp.size();j++) {
                if(temp.get(j).equals("M") && temp.get(j+1).equals(mergeID)) { //When we find a merge code and this merge refers to the mergeID
                    edgesArray eA = new edgesArray();
                    eA.source = temp.get(j-1); //Before the 'M' code is the source
                    eA.destination = mergeID; //mergeID is the destination
                    if(!containsThis(eA, ob)) { //If the edge isn't stored
                        ob.add(eA); //Store it
                    }
                }
            }
        }
        return true;
    }
    
    /**
     * Checks whether an edge is contained into the list 'ob'
     * @param eA The edge to be checked
     * @param ob The list containing the edges
     * @return True if the process has been successful, false otherwise
     */
    public boolean containsThis(edgesArray eA, List<Object> ob) {
        String s;
        String d;
        s = eA.source;
        d = eA.destination;
        edgesArray temp = new edgesArray();
        int i;
        for(i=0;i<ob.size();i++) {
            temp = (edgesArray) ob.get(i);
            if( (s.equals(temp.source)) && (d.equals(temp.destination)) ) {
                return true;
            }
        }
        return false;
    }
    
    /**
     * Find and returns the level of a cluster
     * @param source The source to begin from
     * @param groupList A list with the clusters to be checked
     * @param size The size of the list (TOBEREMOVED)
     * @return The level of the cluster
     */
    public String getLevelNumber(String source, String[][] groupList, int size) {
        int i = 0;
        for(i=0;i<size;i++) {
            if(groupList[i][0].equals(source)) { //When we find the cluster
                return groupList[i][1]; //Return its level
            }
        }
        return null;
    }
    
    /**
     * Calculates and stores the level of a tree.
     * @param levelList A list with the tree levels
     * @param clusterTemp An auxiliary list with clusters 
     * @param ob A list with objects
     * @return True if the process succeeds, false otherwise
     */
    public boolean calculateTreeLevel(String[][] levelList, List<String> clusterTemp, List<Object> ob) {
        int j;
        boolean found = false;
        int temp;
        edgesArray eA = new edgesArray();
        levelList[0][0] = clusterTemp.get(0);
        levelList[0][1] = "1"; //First cluster is at level 1
        for(int i=1;i<clusterTemp.size();i++) {
            levelList[i][0] = clusterTemp.get(i);
            j = 0;
            //Find its father
            while(j < ob.size() && !found) {
                eA = (edgesArray) ob.get(j);
                //If current cluster is a child, an edge would have already been stored
                if(clusterTemp.get(i).equals(eA.destination)) { //When found
                    temp = Integer.valueOf(getLevelNumber(eA.source, levelList, clusterTemp.size())); //Get its source level number
                    temp++; //Increase it by one
                    levelList[i][1] = Integer.toString(temp); //Store the new level number
                    found = true;
                }
                j++;
            }
            found = false;
        }
        return true;
    }
    
    public void calculateGroup(String[][] levelList, String[][] groupList, int size) {
        int i;
        //Sort array
        Arrays.sort(levelList, new Comparator<String[]>() {
            @Override
            public int compare(final String[] entry1, final String[] entry2) {
                final String level1 = entry1[1];
                final String level2 = entry2[1];
                return level1.compareTo(level2);
            }
        });
        int maxLevel = Integer.parseInt(levelList[size-1][1]); //Store max level
        int minLevel = Integer.parseInt(levelList[0][1]); //Store min level
        int tempLevel;
        int levelToGroup;
        for(i=0;i<size;i++) {
            groupList[i][0] = levelList[i][0]; //Store cluster ID
            tempLevel = Integer.parseInt(levelList[i][1]);
            levelToGroup = maxLevel - (tempLevel - minLevel);
            groupList[i][1] = Integer.toString(levelToGroup); //Store group ID
        }
    }
    
    /**
     * Determines whether a string is a number or not.
     * @param s The string to be checked
     * @return True if the string is a number, false otherwise
     * @throws NumberFormatException 
     */
    public boolean isNumber(String s) throws NumberFormatException {
        //Try to parse the string
        try {
            Integer.parseInt(s);
        }
        //String is not a number, return false
        catch (NumberFormatException e) {
            return false;
        }
        //String is a number, return true
        return true;
    }
    
    /**
     * This method gets a number and returns its corresponding color in red-to-green palette.
     * @param power The number for which the method will generate the color
     * @return A hexadecimal string (the color)
     */
    public String generateRedToGreenColorPalete(double power) {
        double H = power * 0.4; // Hue (note 0.4 = Green)
        double S = 0.9; // Saturation
        double B = 0.9; // Brightness
        
        int r = java.awt.Color.getHSBColor((float)H, (float)S, (float)B).getRed();
        int g = java.awt.Color.getHSBColor((float)H, (float)S, (float)B).getGreen();
        int b = java.awt.Color.getHSBColor((float)H, (float)S, (float)B).getBlue();
        
        java.awt.Color.getHSBColor((float)H, (float)S, (float)B);
        
        String hex = String.format("#%02x%02x%02x", r, g, b);
        
        return hex;
    } 
    
    /**
     * This method outputs the longest chains (longest evolution of communities).
     * @param out A file handler
     * @param greatestChains An array that contains the longest chains
     * @throws IOException 
     */
    public void outputLongestChains(PrintWriter out, ArrayList<ArrayList<Integer>> greatestChains) throws IOException {
        
        int[][] gC = new int[greatestChains.size()][2];
        int i;
        
        for(i=0;i<gC.length;i++) {
            gC[i][0] = greatestChains.get(i).get(0);
            gC[i][1] = greatestChains.get(i).get(1);
        }
        
        Arrays.sort(gC, new Comparator<int[]>() {
           @Override
           public int compare(int[] o1, int[] o2) {
               return ((Integer) o2[0]).compareTo(o1[0]);
           }
        }); 
        //System.out.println(""+gC[0][1]);
        //try {
        //.out.println(""+gC[1][1]);
        //} catch(ArrayIndexOutOfBoundsException e) {
            //e.printStackTrace();
        //}
        
        out.println(gC[0][1]);
        if(gC.length > 1) {
            out.println(gC[1][1]);
        }
    }
    
    /**
     * Builds the community evolution JSON that will be used by the external application.
     * @param comEvolList
     * @param comEvolTabs
     * @param theme
     * @param comEvolListAll
     * @param firstTF
     * @param timeframeOfCom
     * @return
     * @throws IOException 
     */
    public boolean buildEvolJSON(ArrayList comEvolList, List<Integer> comEvolTabs, String theme, ArrayList comEvolListAll, String firstTF, Map<Integer,Integer> timeframeOfCom) throws IOException {
        List<String> temp = new ArrayList<String>();
        String clusterName = null;
        boolean firstCluster = true;
        File f = null;
        FileWriter fstream = null;
        int j = 0;
        int size = 25; //Default value
        BufferedWriter out = null;
        BufferedWriter out1 = null;
        int group = 0;
        int height = 0;
        String source = null;
        String destination = null;
        boolean isSource = false;
        boolean isDestination = false;
        boolean noEdges = false;
        int placeOfSource = -1;
        int placeOfDestination = -1;
        boolean foundSource = false;
        boolean foundDestination = false;
        List<String> childIds = null;
        int nodes;
        int maxLevel = 0;
        int currentTF;
        int outputTF = 0;
        boolean newTF = false;
        String resultsPath = FilePaths.DEFAULT_ROOT_PATH+FilePaths.DATA_PARENT_FOLDER+FilePaths.METAFILES_FOLDER+ theme +FilePaths.EVOL_RESULTS_FILE+FilePaths.METAFILES_EXT;
        String jsonPath = FilePaths.DEFAULT_ROOT_PATH+FilePaths.DATA_PARENT_FOLDER+FilePaths.METAFILES_FOLDER+ theme +FilePaths.JSON_FOLDER;
        String chainsPath = FilePaths.DEFAULT_ROOT_PATH+FilePaths.DATA_PARENT_FOLDER+FilePaths.METAFILES_FOLDER+ theme +FilePaths.LONGEST_CHAINS_FILE+FilePaths.METAFILES_EXT;
        File txt = new File(resultsPath); 
        if(txt.exists()) {
            txt.delete();
        }
        if(!txt.exists()) {
            try {
                txt.createNewFile();
                FileWriter fstreamTXT = new FileWriter(resultsPath);
                out1 = new BufferedWriter(fstreamTXT);
            }
            catch (IOException e) {
                return false;
            }
        }
        //First delete all old files
        File fileJson = new File(jsonPath);        
        String[] myFiles;      
        if(fileJson.isDirectory()){  
            myFiles = fileJson.list();  
            for (int i=0; i<myFiles.length; i++) {  
                File myFile = new File(fileJson, myFiles[i]);   
                myFile.delete();  
            }  
        }
        int z;
        
        PrintWriter chains = new PrintWriter(chainsPath, "UTF-8");
        
        ArrayList<ArrayList<Integer>> longestChains = new ArrayList<ArrayList<Integer>>();
        ArrayList<Integer> tempChain;
        for(int i=0;i<comEvolList.size();i++) {
            
            List<Object> ob = new ArrayList<Object>();
            temp = (List<String>) comEvolList.get(i);
            boolean hasSplit = false;
            int tempNodes = 0;
            noEdges = false;
            //Find max group size
            for(int k=0;k<temp.size();k++) {
                if("C".equals(temp.get(k)) || "E".equals(temp.get(k)) || "P".equals(temp.get(k)) || "D".equals(temp.get(k)) || "M".equals(temp.get(k)) || "G".equals(temp.get(k)) || "*".equals(temp.get(k)) ) {
                    group++;
                }
                else if("S".equals(temp.get(k))) {
                    hasSplit = true;
                    
                    group++;
                    String luckyTemp = temp.get(k-1);
                    z = k+1;
                    int childs = 0;
                    childIds = new ArrayList<String>();
                    //Store all child-edges
                    while(isNumber(temp.get(z+1))) {
                        edgesArray edgeA = new edgesArray();
                        edgeA.source = luckyTemp;
                        edgeA.destination = temp.get(z);
                        //Check if edge is already stored
                        if(!this.containsThis(edgeA, ob)) {                          
                            childIds.add(temp.get(z));
                            ob.add(edgeA);
                            childs++;
                        }
                        z++;
                    }
                    //Store last child
                    edgesArray edgeA = new edgesArray();
                    edgeA.source = luckyTemp;
                    edgeA.destination = temp.get(z);
                    //Check if edge is already stored
                    if(!this.containsThis(edgeA, ob)) {
                        childIds.add(temp.get(z));
                        ob.add(edgeA);
                        childs++;
                    }
                    z = k+1;
                    for(int l=0;l<childs;l++) {
                        this.handleSplit(temp, comEvolListAll, childIds.get(l));
                        z++;
                    }
                    if(!childIds.isEmpty()) { //Handle first split infinite loop
                        int d = 0;
                        boolean MSC = false; //More Split Codes
                        while(d<temp.size() && !MSC) {
                            String temp1 = temp.get(d);
                            if(isNumber(temp1)) { //If current element is number
                                String temp2 = temp.get(d+1); //Get next element
                                if(isNumber(temp2)) { //If next element is number, then we have more splits
                                    MSC = true; //We have at least one more split code
                                }
                                else { //Next element is a code
                                    d++; //Iterate
                                    continue;
                                }
                            }
                            d++;
                        }
                        if(MSC) { //If we have more split codes
                            k = 0; //Reinitiate iteration and search again all list
                            childIds.clear();
                        }
                        else { //No more split codes
                            temp.remove(temp.size()-1); //Last index is always "D" which is not used
                            k = temp.size(); //End iteration
                        }
                    }
                }
                //Handle merge
                else if(temp.get(k).equals("M")) {
                    String mergeID = temp.get(k+1); //Next is the community in which the previous ID is merged
                    edgesArray eA = new edgesArray();
                    eA.source = temp.get(k-1); //Element before M is source
                    eA.destination = mergeID;
                    ob.add(eA); //Add edge
                    handleMerge(ob, comEvolListAll, mergeID); //Find and add/update all other merge edges
                }
            }
            z = 0;
            //Trim extra 'D's
            while(z<temp.size()) {
                if(temp.get(z).equals("D") && z+1!=temp.size()) {
                    while(temp.get(z+1).equals("D") && z+2!=temp.size()) { //If we have 'D' next to a 'D'
                        temp.remove(z+1); //Remove extra 'D'
                    }
                }
                z++;
            }
            //Create a list with cluster IDs only
            List<String> clusterTemp = new ArrayList<String>(); 
            for(z=0;z<temp.size();z++) {
                if(isNumber(temp.get(z))) {
                    clusterTemp.add(temp.get(z));
                }
            }
            height = group + 1;
            if(firstCluster) {
                //create file
                clusterName = temp.get(0);
                currentTF = Integer.valueOf(firstTF) + comEvolTabs.get(i); //(e.g. 4 + 0 -tabs- = 4)
                if(currentTF > outputTF) {
                    if(outputTF!=0 && !newTF) {
                        newTF = true;
                    }
                    outputTF = currentTF;
                    out1.write("tf "+outputTF+" ("+GraphBuilder.getTimeFrameDate(theme, String.valueOf(outputTF))+")");
                    out1.newLine();
                }
                out1.write(clusterName);
                out1.newLine();
                f = new File(jsonPath+clusterName+".json");
                if(!f.exists()) {
                    try {
                        f.createNewFile();
                        fstream = new FileWriter(jsonPath+clusterName+".json");
                        out = new BufferedWriter(fstream);
                        out.write("{");
                        out.newLine();
                        out.write("\t\"nodes\": [");
                        out.newLine();
                    }
                    catch (IOException e) {
                        return false;
                    }
                }
                firstCluster = false;
            }
            //Write the nodes and update edgesArray
            j=0;
            isSource = true;
            boolean EOC = false; //End of community
            nodes = 0;
            boolean firstNode;
            if(!hasSplit) {
                String color;
                double power = 1.0;
                firstNode = true;
                int numberOfNodes = 0;
                
                for(j=0;j<temp.size();j++) {
                    if(isNumber(temp.get(j))) {
                        numberOfNodes++;
                    }
                }
                j = 0;
                double colorStep = power / (double) numberOfNodes; 
                while(j < temp.size() && !EOC) {
                    //If temp.get(j) is a node
                    if(isNumber(temp.get(j))) {
                        color = new String();
                        if(firstNode) { //First node is always bright green
                            color = generateRedToGreenColorPalete(1.0); //Generate green color
                            power -= colorStep;
                            firstNode = false;
                        }
                        else if(temp.get(j+1).equals("D") || temp.get(j+1).equals("*")){ //If current node is the last in the evolution queue
                            power -= colorStep; //Reinitiate power value
                            color = generateRedToGreenColorPalete(power); //Generate bright red color
                            
                        }
                        else { //Any other node
                            power-=colorStep; //Increase power by 0.1
                            color = generateRedToGreenColorPalete(power);
                            
                        }
                        
                        out.write("\t\t{\"name\":\"Community "+temp.get(j)+" in timeframe "+timeframeOfCom.get(Integer.parseInt(temp.get(j))) +" ("+GraphBuilder.getTimeFrameDate(theme, String.valueOf(timeframeOfCom.get(Integer.parseInt(temp.get(j)))))+")"+"\",\"group\":"+group+",\"size\":"+size+",\"color\":\""+color+"\",\"community\":\""+temp.get(j)+"\"}");
                        nodes++;
                        //Update edgesArray
                        if(isSource && j==0) {
                            source = temp.get(j).toString();
                            isSource = false;
                            isDestination = true;
                            noEdges = true;
                        }
                        else if(isSource && j!=0) {
                            source = destination;
                            destination = temp.get(j);
                            edgesArray edges = new edgesArray();
                            edges.source = source;
                            edges.destination = destination;
                            //Check if edge is already stored
                            if(!this.containsThis(edges, ob)) {
                                ob.add(edges);
                            }
                            //isSource = false;
                            //isDestination = true;
                        }
                        else if(isDestination) {
                            destination = temp.get(j).toString();
                            isSource = true;
                            isDestination = false;
                            edgesArray edges = new edgesArray();
                            edges.source = source;
                            edges.destination = destination;
                            //Check if edge is already stored
                            if(!this.containsThis(edges, ob)) {
                                ob.add(edges);
                            }
                            if(noEdges) {
                                noEdges = false;
                            }
                        }
                        j++;
                        continue; 
                    }
                    //Split code
                    else if("S".equals(temp.get(j))) {
                        out.write(",");
                        out.newLine();
                        j++;
                        continue;
                    }
                    //Contraction code
                    else if("C".equals(temp.get(j)) || "G".equals(temp.get(j))) {
                        size-=3; //Reduce cluster size by 3
                        out.write(",");
                        out.newLine();
                        group--;
                        j++; //Get next
                        continue;
                    }
                    //Expand code
                    else if("E".equals(temp.get(j))) {
                        size+=3; //Increase cluster size by 3
                        out.write(",");
                        out.newLine();
                        group--;
                        j++; //Get next
                        continue;
                    }
                    //Persistence code, size doesn't change
                    else if("P".equals(temp.get(j))) {
                        out.write(",");
                        out.newLine();
                        group--;
                        j++; //Get next
                        continue;
                    }
                    else if("M".equals(temp.get(j))) {
                        out.write(",");
                        out.newLine();
                        group--;
                        j++;
                        continue;
                    }
                    else if("D".equals(temp.get(j)) || "*".equals(temp.get(j))) { //Death or END-OF-DATA code
                        if(j+1 != temp.size()) {
                            out.write(",");
                            out.newLine();
                            j++;
                            continue;
                        }
                        else {
                            out.newLine();
                            out.write("\t],");
                            out.newLine();
                            if(!noEdges  && nodes%2==1) {
                                destination = temp.get(j-1).toString();
                                edgesArray edges = new edgesArray();
                                edges.destination = destination;
                                edges.source = source;
                                if(!this.containsThis(edges, ob)) {
                                    ob.add(edges);
                                }
                            }
                            j++;
                            EOC = true; // We reached the end of current community
                        continue;
                        }
                    }
                }
            }
            //Create edges for split case
            else {
                while(j < temp.size()) {
                    //If temp.get(j) is a node
                    if(isNumber(temp.get(j))) {
                        //Update edgesArray
                        if(isSource && j==0) {
                            source = temp.get(j).toString();
                            isSource = false;
                            isDestination = true;
                        }
                        else if(isSource && j!=0) {
                            source = destination;
                            destination = temp.get(j).toString();
                            edgesArray edges = new edgesArray();
                            edges.source = source;
                            edges.destination = destination;
                            //Check if edge is already stored
                            if(!this.containsThis(edges, ob)) {
                                ob.add(edges);
                            }
                            //isSource = false;
                            //isDestination = true;
                        }
                        else if(isDestination) {
                            destination = temp.get(j).toString();
                            isSource = true;
                            isDestination = false;
                            if("-1".equals(source)) {
                                j++;
                                continue;
                            }
                            else {
                                edgesArray edges = new edgesArray();
                                edges.source = source;
                                edges.destination = destination;
                                //Check if edge is already stored
                                if(!this.containsThis(edges, ob)) {
                                    ob.add(edges);
                                }
                            }
                        }
                        j++;
                        continue; 
                    }
                    else if("D".equals(temp.get(j))) {
                        source = "-1";
                        isSource = false;
                        isDestination = true;
                        j++;
                        continue;
                    }
                    j++;
                }
                String[][] levelList = null;
                String[][] groupList = null;
                if(hasSplit) { //If the community had at least on split
                    levelList = new String[clusterTemp.size()][2];
                    //First calculate the level of every node
                    calculateTreeLevel(levelList, clusterTemp, ob); //Calculate level number
                    groupList = new String[clusterTemp.size()][2];
                    //Then calculate its group number
                    calculateGroup(levelList, groupList, clusterTemp.size()); //Calculate group number
                    //Find max group size
                    maxLevel = Integer.parseInt(levelList[clusterTemp.size()-1][1]); //Store max level
                }
                //Now write nodes to file
                j = 0;
                String color;
                double splitPower = 0;
                double power = 1.0;
                firstNode = true;
                int numberOfNodes = 0;
                
                for(j=0;j<temp.size();j++) {
                    if(isNumber(temp.get(j))) {
                        numberOfNodes++;
                    }
                    if(temp.get(j).equals("D")) {
                        j = temp.size() + 1 ;
                    }
                }
                j = 0;
                double colorStep = power / (double) numberOfNodes;
                while(j < temp.size() && !EOC) {
                    //If temp.get(j) is a node
                    if(isNumber(temp.get(j))) {
                        color = new String();
                        if(firstNode) { //First node is always bright green
                            color = generateRedToGreenColorPalete(1.0); //Generate green color
                            firstNode = false;
                            power-=colorStep;
                            if(temp.get(j+1).equals("S")) {
                                splitPower = power;
                            }
                        }
                        else if(temp.get(j+1).equals("D") || temp.get(j+1).equals("*")){ //If current node is the last in the evolution queue
                            color = generateRedToGreenColorPalete(0.0); //Generate bright red color
                            power = splitPower; //Reinitiate power value to the value of the last split node
                        }
                        else { //Any other node
                            power -= colorStep; //Reinitiate power value
                            color = generateRedToGreenColorPalete(power);
                            //power-=0.2; //Decrease power by 0.1
                            if(temp.get(j+1).equals("S")) {
                                splitPower = power;
                            }
                        }
                        out.write("\t\t{\"name\":\"Community "+temp.get(j)+" in timeframe "+timeframeOfCom.get(Integer.parseInt(temp.get(j)))+" ("+GraphBuilder.getTimeFrameDate(theme, String.valueOf(timeframeOfCom.get(Integer.parseInt(temp.get(j)))))+")"+"\",\"group\":"+getLevelNumber(temp.get(j), groupList, temp.size())+",\"size\":"+size+",\"color\":\""+color+"\",\"community\":\""+temp.get(j)+"\"}");
                        nodes++;
                        j++;
                    }
                    //Split code
                    else if("S".equals(temp.get(j))) {
                        out.write(",");
                        out.newLine();
                        j++;
                        continue;
                    }
                    //Contraction code
                    else if("C".equals(temp.get(j)) || "G".equals(temp.get(j)) ) {
                        size-=3; //Reduce cluster size
                        out.write(",");
                        out.newLine();
                        j++; //Get next
                        continue;
                    }
                    //Expand code
                    else if("E".equals(temp.get(j))) {
                        size+=3; //Increase cluster size
                        out.write(",");
                        out.newLine();
                        j++; //Get next
                        continue;
                    }
                    //Persistence code, size doesn't change
                    else if("P".equals(temp.get(j))) {
                        out.write(",");
                        out.newLine();
                        j++; //Get next
                        continue;
                    }
                    else if("M".equals(temp.get(j))) {
                        out.write(",");
                        out.newLine();
                        j++;
                        continue;
                    }
                    else if("D".equals(temp.get(j)) || "*".equals(temp.get(j))) { //Death or END-OF-DATA code
                        if(j+1 != temp.size()) {
                            out.write(",");
                            out.newLine();
                            size = 25;
                            j++;
                            continue;
                        }
                        else {
                            out.newLine();
                            out.write("\t],");
                            out.newLine();
                            j++;
                            EOC = true; // We reached the end of current community
                            continue;
                        }
                    }
                }
            }
            //If we don't have any edges, then the noEdges flag will be true
            if(noEdges) {
                out.write("\t\"links\": [");
                out.newLine();
            }
            else { //We do have edges, so write them
                out.write("\t\"links\": [");
                out.newLine();
                for(int k=0;k<ob.size();k++) {
                    edgesArray eA = new edgesArray();
                    eA = (edgesArray) ob.get(k);
                    source = eA.source;
                    destination = eA.destination;
                    for(j=0;j<temp.size();j++) {
                        if(temp.get(j).equals(source)) {
                            placeOfSource = j - j/2;
                            foundSource = true;
                        }
                        else if(temp.get(j).equals(destination)) {
                            placeOfDestination = j - j/2;
                            foundDestination = true;
                        }
                        
                        if(foundSource && foundDestination) {
                            out.write("\t\t{\"source\":"+placeOfSource+", \"target\":"+placeOfDestination+",\"value\":3,\"size\":1,\"name\":\"\"}");
                            foundSource = false;
                            foundDestination = false;
                        }
                    }
                    if(k+1!=ob.size()) {
                        out.write(",");
                    }
                    out.newLine();
                }
            }
            out.write("\t],");
            out.newLine();
            out.write("\t\"info\": [");
            out.newLine();
            if(!hasSplit) {
                out.write("\t\t{\"height\":"+height+"}");
            }
            else {
                height = maxLevel + 1;
                out.write("\t\t{\"height\":"+height+"}");
            }
            out.newLine();
            out.write("\t]");
            out.newLine();
            out.write("}");
            out.close();
            tempChain = new ArrayList<Integer>();
            if(newTF) {
                outputLongestChains(chains, longestChains); //output first and second chain
                longestChains = new ArrayList<ArrayList<Integer>>(); //reinitialize array list
                tempChain.add(height);
                tempChain.add(Integer.parseInt(clusterName));
                longestChains.add(tempChain); //add new height
                newTF = false;
            }
            else {
                tempChain.add(height);
                tempChain.add(Integer.parseInt(clusterName));
                longestChains.add(tempChain); //add new height
            }
            source = null;
            destination = null;
            EOC = false;
            isSource = true;
            isDestination = false;
            out.close();
            j = 0;
            group = 0;
            size = 25;
            firstCluster = true;         
        }
        outputLongestChains(chains, longestChains);
        chains.close();
        out1.close();
        return true;
    }
    
    public double normalizeResult(double result, double lowerBound, double upperBound) {
        
        double finalResult;
        int tempResult;
        
        //Normalize using classic normalization function.
        //Old range is provided by the parameters, while the
        //new range is between [0.01,0.99]
        finalResult = ((result - lowerBound) / (upperBound - lowerBound))*(0.99 - 0.01) + 0.01;
        
        //Round the value to one decimal place
        tempResult = (int)(finalResult * 100.0);
        finalResult = ((double)tempResult)/100.0;
        
        return finalResult;
    }
    
    public void getMaxMinValues(int choice, double[] bound, long tfs[], statistics.GeneralStatistics gStat, db.lucene.LuceneRetriever lc, String theme) throws FileNotFoundException, IOException, UnknownHostException, ParseException {
        
        double[] resultsForSort = new double[tfs.length];
        int i;
        
        //Number of tweets
        if(choice == 1) {
            /*
             * 
             * UNUSED CASE
             * 
             */
            //Date[] dates = new Date[2];
            //for(i=0;i<tfs.length;i++) {
                //dates = this.calculateDateStream((int)tfs[i], theme);
                //resultsForSort[i] = gStat.getNumberOfTweetsInTimeFrame(dates[0], dates[1]);
                //resultsForSort[i] = collectTweetsHashtagsURLs(1, theme, String.valueOf(tfs[i]));
            //}
            //Arrays.sort(resultsForSort);
            //bound[0] = resultsForSort[0];
            //bound[1] = resultsForSort[tfs.length-1];
        }
        else if(choice == 2) {
            for(i=0;i<tfs.length;i++) {
                resultsForSort[i] = gStat.numOfUsersPerTimeFrame(tfs[i]);
            }
            Arrays.sort(resultsForSort);
            bound[0] = resultsForSort[0];
            bound[1] = resultsForSort[tfs.length-1];
        }
        else if(choice == 3) {
            for(i=0;i<tfs.length;i++) {
                resultsForSort[i] = gStat.stdSizeOfCommunitiesPerTimeFrame(tfs[i]);
            }
            Arrays.sort(resultsForSort);
            bound[0] = resultsForSort[0];
            bound[1] = resultsForSort[tfs.length-1];
        }
        else if(choice == 4) {
            for(i=0;i<tfs.length;i++) {
                resultsForSort[i] = gStat.meanSizeOfCommunitiesPerTimeFrame(tfs[i]);
            }
            Arrays.sort(resultsForSort);
            bound[0] = resultsForSort[0];
            bound[1] = resultsForSort[tfs.length-1];
        }
        else if(choice == 5) {
            for(i=0;i<tfs.length;i++) {
                resultsForSort[i] = gStat.communitiesPerTimeFrameTotal(tfs[i]);
            }
            Arrays.sort(resultsForSort);
            bound[0] = resultsForSort[0];
            bound[1] = resultsForSort[tfs.length-1];
        }
        else if(choice == 6) {
            long tf[] = new long[1];
            for(i=0;i<tfs.length;i++) {
                tf[0] = tfs[i];
                resultsForSort[i] = lc.numPercentageOfUsersPerTimeFrame(tf);
            }
            Arrays.sort(resultsForSort);
            bound[0] = resultsForSort[0];
            bound[1] = resultsForSort[tfs.length-1];
        }
    }
    
    public Date[] calculateDateStream(int timeframe, String theme) throws IOException {
        
        //Open TIMESTEP_INDEX file
        String filename = FilePaths.DEFAULT_ROOT_PATH+FilePaths.DATA_PARENT_FOLDER+FilePaths.DBs_FOLDER+ theme +FilePaths.TIMESTEP_INDEX_FILE;
        String strLine;
        boolean firstLine = true;
        boolean found = false;
        String timeframe_n;
        
        FileInputStream fstream;
        DataInputStream in;
        BufferedReader br;
        
        String fromStream = null;
        String untilStream = null;
        int tabIndex = 0;
        
        try {
            fstream = new FileInputStream(filename);
            in = new DataInputStream(fstream);
            br = new BufferedReader(new InputStreamReader(in));
        }
        catch(IOException e) {
            return null;
        }
        
        //Iterate until you find the desired timeframe
        while((strLine = br.readLine()) != null && !found) {
            if(firstLine) {
                firstLine = false;
                continue;
            }
            tabIndex = strLine.indexOf("\t");
            timeframe_n = String.valueOf(strLine.substring(0, tabIndex));
            if(timeframe != Integer.parseInt(timeframe_n)) {
                continue;
            }
            else {
                fromStream = strLine.substring(2, 19);
                untilStream = strLine.substring(20, strLine.length());
                found = true;
            }
        }
        
        //Transform string date stream to 'Date' type
        SimpleDateFormat utcFormat = new SimpleDateFormat("yyyy_MM_dd_HHmmss");
        utcFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        try {
            Date from = utcFormat.parse(fromStream);
            Date until = utcFormat.parse(untilStream);                
            Date[] dates = {
                            from,
                            until
                            };
            return dates;
        }
        catch(Exception e) {
            return null;
        }
    }
    
    public static void copyFile(File source, File destination) {
        InputStream inStream = null;
        OutputStream outStream = null;
        try {
 
            inStream = new FileInputStream(source);
            outStream = new FileOutputStream(destination);
 
            byte[] buffer = new byte[1024];
 
            int length;
            while ((length = inStream.read(buffer)) > 0){
                outStream.write(buffer, 0, length);
            }
 
            if (inStream != null) {
                inStream.close();
            }
            if (outStream != null) {
                outStream.close();
            }
        }catch(IOException e){
            e.printStackTrace();
        }
    }
    //Build JSON file for EventWheel visualization
    public boolean buildEventWheelJSON(String tfEarly, String tfLate, String themeA, String themeB, String nameA, String nameB, boolean hasLeft, String nameOfEventWheel, boolean hasMentionsA, boolean hasRetweetsA, boolean hasRepliesA, boolean hasMentionsB, boolean hasRetweetsB, boolean hasRepliesB) throws FileNotFoundException, IOException, ParseException {
        
        FileInputStream fstream;
        DataInputStream in;
        BufferedReader br;
        String strLine;
        
        if(!nameA.equals("null")) {
            nameA = replaceUnderscoresWithWhiteSpace(nameA);
        }
        
        if(!nameB.equals("null")) {
            nameB = replaceUnderscoresWithWhiteSpace(nameB);
        }
        
        
        //----------------------------------------------------------------
        //****************************************************************
        //Default number of axes is 6. If you want to add more quantities,
        //change the following number and add your quantity-metrics code
        //in the highlited areas above.
        //****************************************************************
        int numOfAxes = 6;
        //----------------------------------------------------------------

        String lowerTF = null;
        String upperTF = null;
        try {
            String timeframesFile = FilePaths.DEFAULT_ROOT_PATH+FilePaths.DATA_PARENT_FOLDER+FilePaths.DBs_FOLDER+ themeB + FilePaths.TIMEFRAMES_FILE+FilePaths.METAFILES_EXT;
            fstream = new FileInputStream(timeframesFile);
            in = new DataInputStream(fstream);
            br = new BufferedReader(new InputStreamReader(in));
            if((strLine = br.readLine()) != null) {
                lowerTF = strLine; //First line is the lowerTF
                upperTF = br.readLine(); //Nex line is the upperTF
            }
        }
        catch(Exception e) {
            e.printStackTrace();
            return false;
        }
        
        int lowerTFValue = Integer.parseInt(lowerTF);
        int tfEarlyValue = 0;
        int tfLateValue = 0;

        tfEarlyValue = lowerTFValue + Integer.parseInt(tfEarly);
        tfLateValue = lowerTFValue + Integer.parseInt(tfLate);

        //Number of circles
        int numOfCircles = Integer.parseInt(tfLate) - Integer.parseInt(tfEarly) + 1; //timeframe index starts from 0
        String eventWheelPath = FilePaths.DEFAULT_ROOT_PATH+FilePaths.DATA_PARENT_FOLDER+FilePaths.METAFILES_FOLDER+themeB+FilePaths.EVENTWHEEL_FOLDER+nameOfEventWheel+".json";
        File f = new File(eventWheelPath);
        if(!f.exists()) {
            f.createNewFile();
        }
        
        FileWriter fwriter = new FileWriter(eventWheelPath); //Location of JSON file
        BufferedWriter out = new BufferedWriter(fwriter);
        try {
            //Start writing to JSON file
            out.write("{");
            out.newLine();
            out.write("\t\"info\": [");
            out.newLine();
            out.write("\t\t{ \"circles\" : "+numOfCircles+", \"axes\" : "+numOfAxes+" },");
            out.newLine();
            out.write("\t\t{ \"left\" : \""+nameA+"\", \"right\" : \""+nameB+"\" },");
            out.newLine();

            //----------------------------------------------------------------
            //****************************************************************
            //Delete, add or modify the labels of the axes here
            //To add your quantity-metrics code, see above the
            //highlighted areas
            //****************************************************************
            String [] axesLabels = {"num of tweets",  
                                    "num of users", 
                                    "std of mean size of communities", 
                                    "mean size of communities", 
                                    "num of communities",
                                    "% users in communities"};
            //----------------------------------------------------------------


            for(int i=0;i<numOfAxes-1;i++) {
                out.write("\t\t{ \"name\" : \""+axesLabels[i]+"\" },");
                out.newLine();
            }
            out.write("\t\t{ \"name\" : \""+axesLabels[numOfAxes-1]+"\" }");
            out.newLine();
            out.write("\t],");
            out.newLine();

            int iter = 1;
            if(hasLeft) {
                iter = 2;
            }
            
            //Generate tfs[] secondary array
            long[] tfs = new long[numOfCircles];
            for(int i=0;i<numOfCircles;i++) {
                tfs[i] = (long)(tfEarlyValue+i);
            }
            
            double[][] legendValues;
            if(hasLeft) {
                legendValues = new double[2*numOfAxes][2];
            }
            else {
                legendValues = new double[numOfAxes][2];
            }
            
            DecimalFormat df = new DecimalFormat("#,###,##0.00");
            DecimalFormatSymbols otherSymbols = new DecimalFormatSymbols(Locale.ENGLISH);
            otherSymbols.setDecimalSeparator('.');
            otherSymbols.setGroupingSeparator(',');
            df.setDecimalFormatSymbols(otherSymbols);
            df.setMinimumFractionDigits(0);
            df.setMaximumFractionDigits(2);
            
            GeneralStatistics gStat;
            String dir;
            db.lucene.LuceneRetriever lucR;
            
            boolean hasRetweets = hasRetweetsB;
            //Initialize right object
            gStat = new GeneralStatistics(themeB, hasRetweetsB);
            boolean firstIteration = true;
            double[] bound = new double[2];
            bound[0] = 0;
            bound[1] = 0;
            String theme = themeB;
            for(int j=0;j<iter;j++) {

                if(firstIteration) { //Handle case with only right side
                    //Right axis
                    out.write("\t\"right\" : [");
                    out.newLine();
                }

                //Variables used in iterations
                Integer result = 0;
                double resultDouble = 0;
                double finalResult = 0;
                String color = null;
                int i;

                //================
                //Number of Tweets
                
                ArrayList<Integer> tmp_result = new ArrayList<Integer>();
                int[] sort_result = new int[numOfCircles]; //For sort
                
                gStat.closeConnections(); //Close connections, collectTweetsHashtagsURLs opens its own
                String lucenePath = FilePaths.DEFAULT_ROOT_PATH+FilePaths.DATA_PARENT_FOLDER+FilePaths.DBs_FOLDER+ theme +FilePaths.LUCENE_FOLDER;
                String neoPath = FilePaths.DEFAULT_ROOT_PATH+FilePaths.DATA_PARENT_FOLDER+FilePaths.DBs_FOLDER+ theme +FilePaths.NEO_FOLDER;
                lucR = new LuceneRetriever(theme, hasRetweets, lucenePath, false);
                GraphDatabaseService graphDB = new GraphDatabaseFactory().newEmbeddedDatabase(neoPath);
                Map<String, Integer> map;
                for(i=0;i<numOfCircles;i++) {
                    
                    //Calculate quantity
                    map = this.collectTweetsHashtagsTerms(1, theme, String.valueOf(tfEarlyValue+i), graphDB, lucR, null, hasRetweets);
                    result = map.get("tweets");
                    tmp_result.add(result);
                    sort_result[i] = result;
                    map.clear();
                }
                lucR.closeConnection();
                graphDB.shutdown();
                gStat = new GeneralStatistics(theme, hasRetweets); //Reopen connections
                Arrays.sort(sort_result);
                bound[0] = sort_result[0]; //Min value
                bound[1] = sort_result[sort_result.length-1]; //Max value
                
                //Store legend values
                legendValues[0+j*6][0] = bound[0];
                legendValues[0+j*6][1] = bound[1];
                
                for(i=0;i<numOfCircles;i++) {
                    
                    //Normalize result to 0.01 - 0.99 scale
                    finalResult = this.normalizeResult((double)tmp_result.get(i), bound[0], bound[1]);

                    //Generate red-to-green color
                    color = this.generateRedToGreenColorPalete(1.00-finalResult);

                    //Save in JSON
                    out.write("\t\t{ \"number\" : "+finalResult+", \"color\" : \""+color+"\", \"label\" : \""+tmp_result.get(i)+"\" },");
                    out.newLine();
                }
                
                
                //===============
                //Number of Users

                //Find quantity max/min values
                this.getMaxMinValues(2, bound, tfs, gStat, null, theme);
                
                //Store legend values
                legendValues[1+j*6][0] = bound[0];
                legendValues[1+j*6][1] = bound[1];
                
                for(i=0;i<numOfCircles;i++) {

                    //Calculate quantity
                    result = gStat.numOfUsersPerTimeFrame(tfEarlyValue+i);
                    //result = 5133;

                    //Normalize result to 0.01 - 0.99 scale
                    finalResult = this.normalizeResult((double)result, bound[0], bound[1]);
                    //finalResult = this.normalizeResult(result, 115, 6544);

                    //Generate red-to-green color
                    color = this.generateRedToGreenColorPalete(1.00-finalResult);

                    //Save in JSON
                    out.write("\t\t{ \"number\" : "+finalResult+", \"color\" : \""+color+"\", \"label\" : \""+df.format(result)+"\" },");
                    out.newLine();
                }

                //====================================
                //Standard of Mean Size of Communities

                //Find quantity max/min values
                this.getMaxMinValues(3, bound, tfs, gStat, null, theme);
                
                //Store legend values
                legendValues[2+j*6][0] = bound[0];
                legendValues[2+j*6][1] = bound[1];
                
                for(i=0;i<numOfCircles;i++) {

                    //Calculate quantity
                    resultDouble = gStat.stdSizeOfCommunitiesPerTimeFrame(tfEarlyValue+i);
                    //resultDouble = 34.2;

                    //Normalize result to 0.01 - 0.99 scale
                    finalResult = this.normalizeResult(resultDouble, bound[0], bound[1]);
                    //finalResult = this.normalizeResult(resultDouble, 11.14, 77.61);

                    //Generate red-to-green color
                    color = this.generateRedToGreenColorPalete(1.00-finalResult);

                    //Save in JSON
                    out.write("\t\t{ \"number\" : "+finalResult+", \"color\" : \""+color+"\", \"label\" : \""+df.format(resultDouble)+"\" },");
                    out.newLine();
                }

                //========================
                //Mean Size of Communities

                //Find quantity max/min values
                this.getMaxMinValues(4, bound, tfs, gStat, null, theme);
                
                //Store legend values
                legendValues[3+j*6][0] = bound[0];
                legendValues[3+j*6][1] = bound[1];
                
                for(i=0;i<numOfCircles;i++) {

                    //Calculate quantity
                    resultDouble = gStat.meanSizeOfCommunitiesPerTimeFrame(tfEarlyValue+i);
                    //resultDouble = 44.2;

                    //Normalize result to 0.01 - 0.99 scale
                    finalResult = this.normalizeResult(resultDouble, bound[0], bound[1]);
                    //finalResult = this.normalizeResult(resultDouble, 44.2, 182.4);

                    //Generate red-to-green color
                    color = this.generateRedToGreenColorPalete(1.00-finalResult);

                    //Save in JSON
                    out.write("\t\t{ \"number\" : "+finalResult+", \"color\" : \""+color+"\", \"label\" : \""+df.format(resultDouble)+"\" },");
                    out.newLine();
                }

                //==================
                //Num of Communities

                //Find quantity max/min values
                this.getMaxMinValues(5, bound, tfs, gStat, null, theme);
                
                //Store legend values
                legendValues[4+j*6][0] = bound[0];
                legendValues[4+j*6][1] = bound[1];
                
                for(i=0;i<numOfCircles;i++) {

                    //Calculate quantity
                    result = gStat.communitiesPerTimeFrameTotal(tfEarlyValue+i);
                    //result = 1214;

                    //Normalize result to 0.01 - 0.99 scale
                    finalResult = this.normalizeResult((double)result, bound[0], bound[1]);
                    //finalResult = this.normalizeResult(result, 1, 1214);

                    //Generate red-to-green color
                    color = this.generateRedToGreenColorPalete(1.00-finalResult);

                    //Save in JSON
                    out.write("\t\t{ \"number\" : "+finalResult+", \"color\" : \""+color+"\", \"label\" : \""+df.format(result)+"\" },");
                    out.newLine();
                }
                gStat.closeConnections();
                dir = FilePaths.DEFAULT_ROOT_PATH+FilePaths.DATA_PARENT_FOLDER+FilePaths.DBs_FOLDER+ theme +FilePaths.LUCENE_FOLDER;
                lucR = new db.lucene.LuceneRetriever(theme, false, dir, false);
                
                //======================
                //Percentage of Users in Communities

                //Find max/min values of quantity
                this.getMaxMinValues(6, bound, tfs, gStat, lucR, theme);
                
                //Store legend values
                legendValues[5+j*6][0] = bound[0];
                legendValues[5+j*6][1] = bound[1];
                
                long[] tfEarlyTable = new long[1];
                for(i=0;i<numOfCircles;i++) {

                    //Calculate quantity
                    tfEarlyTable[0] = tfEarlyValue+i;
                    resultDouble = lucR.numPercentageOfUsersPerTimeFrame(tfEarlyTable);
                    //resultDouble = 33.33;

                    //Normalize result to 0.01 - 0.99 scale
                    finalResult = this.normalizeResult(resultDouble, bound[0], bound[1]);
                    //finalResult = this.normalizeResult(resultDouble, 0.15, 66.42);

                    //Generate red-to-green color
                    color = this.generateRedToGreenColorPalete(1.00-finalResult);

                    //Save in JSON
                    if(i == numOfCircles-1) { //then next i would be numOfCircles-1
                        out.write("\t\t{ \"number\" : "+finalResult+", \"color\" : \""+color+"\", \"label\" : \""+df.format(resultDouble)+"\" }");
                        out.newLine();
                    }
                    else {
                        out.write("\t\t{ \"number\" : "+finalResult+", \"color\" : \""+color+"\", \"label\" : \""+df.format(resultDouble)+"\" },");
                        out.newLine();
                    }
                }
                lucR.closeConnection();

                //----------------------------------------------------------------
                //****************************************************************
                //Place here your quantity-metrics code
                //CODE
                //CODE
                //WARNING: Don't forget to chnage getMaxMinValues()
                //according to the other quantities and add this if-else check at your LAST
                //quantity (in order to ommit final comma).
                //****************************************************************
                //----------------------------------------------------------------

                if(hasLeft && firstIteration) { //If there's a left side (and we are still on the first iteration),
                                                //to continue with the left-side data a comma must be added to ']'                    
                    out.write("\t],");
                    out.newLine();
                    //Left axis
                    out.write("\t\"left\" : [");
                    out.newLine();
                    hasRetweets = hasRetweetsA;
                    //Reinitialize object with left-side theme
                    gStat = new GeneralStatistics(themeA, hasRetweetsA);

                    firstIteration  = false; //We are on second iteration
                    theme = themeA;
                }
            }
            out.write("\t],");
            out.newLine();
            out.write("\t\"legend\" : [");
            out.newLine();
            
            //Output the legend values
            for(int j=0;j<legendValues.length;j++) {
                if(j == legendValues.length-1) {
                    out.write("\t\t{ \"high\" : \""+df.format(legendValues[j][1])+"\", \"low\" : \""+df.format(legendValues[j][0])+"\" }");
                    out.newLine();
                }
                else {
                    out.write("\t\t{ \"high\" : \""+df.format(legendValues[j][1])+"\", \"low\" : \""+df.format(legendValues[j][0])+"\" },");
                    out.newLine();
                }
            }
            
            //Close structure
            out.write("\t]");
            out.newLine();
            out.write("}");

            //Close file
            out.close();
            if(hasLeft) { //If has left side, copy json to the left side dataset directory
                eventWheelPath = FilePaths.DEFAULT_ROOT_PATH+FilePaths.DATA_PARENT_FOLDER+FilePaths.METAFILES_FOLDER+ themeA +FilePaths.EVENTWHEEL_FOLDER+nameOfEventWheel+".json";
                File f1 = new File(eventWheelPath);
                if(!f1.exists()) {
                    f1.createNewFile();
                }
                copyFile(f, f1);
            }
        }
        catch (IOException  e) {
            return false;
        }catch(NumberFormatException e) {
            return false;
        }
        return true;
    }
    
    public boolean buildTagCloudJSON(String theme, int timeframe, boolean hasMentions, boolean hasRetweets, boolean hasReplies) throws IOException {
        
        String entirePath = FilePaths.DEFAULT_ROOT_PATH+FilePaths.DATA_PARENT_FOLDER+FilePaths.METAFILES_FOLDER+ theme +FilePaths.ENTIRE_FOLDER;
        File file1 = new File(entirePath+String.valueOf(timeframe)+FilePaths.SEPARATOR+FilePaths.HASHTAGS_JSON);
        File file2 = new File(entirePath+String.valueOf(timeframe)+FilePaths.SEPARATOR+FilePaths.TERMS_JSON);
        if(file1.exists() && file2.exists()) {
            return true;
        }
        
        String neoPath = FilePaths.DEFAULT_ROOT_PATH+FilePaths.DATA_PARENT_FOLDER+FilePaths.DBs_FOLDER+ theme +FilePaths.NEO_FOLDER;
        String lucenePath = FilePaths.DEFAULT_ROOT_PATH+FilePaths.DATA_PARENT_FOLDER+FilePaths.DBs_FOLDER+ theme +FilePaths.LUCENE_FOLDER;
        
        GraphDatabaseService graphDB = new GraphDatabaseFactory().newEmbeddedDatabase(neoPath);
        LuceneRetriever lucR = new LuceneRetriever(theme, hasRetweets, lucenePath, false);
        
        Map<String, Integer> mapHashtags;
        Map<String, Integer> mapTerms;
        
        mapHashtags = collectTweetsHashtagsTerms(2, theme, String.valueOf(timeframe), graphDB, lucR, null, hasRetweets);
        mapTerms = collectTweetsHashtagsTerms(3, theme, String.valueOf(timeframe), graphDB, lucR, null, hasRetweets);
        
        mapHashtags = sortByValue(mapHashtags);
        mapTerms = sortByValue(mapTerms);
        
        graphDB.shutdown();
        lucR.closeConnection();
        
        String path = FilePaths.DEFAULT_ROOT_PATH+FilePaths.DATA_PARENT_FOLDER+FilePaths.METAFILES_FOLDER+ theme +FilePaths.ENTIRE_FOLDER+""+String.valueOf(timeframe)+"/";
        File f = new File(path);
        if(!f.exists()) {
            f.mkdirs();
        }
        
        FileWriter fwrite;
        BufferedWriter out;
        int i;
        for(int j=0;j<2;j++) {
            try {
                if(j == 0) {
                    fwrite = new FileWriter(path+FilePaths.HASHTAGS_JSON);
                }
                else {
                    fwrite = new FileWriter(path+FilePaths.TERMS_JSON);
                }
                out = new BufferedWriter(fwrite);

                //Initialize structure
                out.write("{");
                out.newLine();
                out.write("\t\"pairs\" : [");
                out.newLine();
                
                
                if(j == 0) { //Hashtags
                    i = 0;
                    for(Map.Entry<String, Integer> map : mapHashtags.entrySet()) {
                        if( i <= 50 ) { //Filter out hashtags with showing lower than 50
                            if(i+1 == mapHashtags.keySet().size()) {
                                out.write("\t\t{ \"keywords\" : \"#"+map.getKey()+"\", \"showings\" : \""+(map.getValue()*1.5)+"\" }");
                                out.newLine();
                            }
                            else {
                                out.write("\t\t{ \"keywords\" : \"#"+map.getKey()+"\", \"showings\" : \""+(map.getValue()*1.5)+"\" },");
                                out.newLine();
                            }
                        }
                        i++;
                    }
                }
                else {
                    i = 0;
                    for(Map.Entry<String, Integer> map : mapTerms.entrySet()) {
                        if( i <= 50 ) { //Filter out terms with showing lower than 50
                            if(i+1 == mapTerms.keySet().size()) {
                                out.write("\t\t{ \"keywords\" : \""+map.getKey().trim()+"\", \"showings\" : \""+(map.getValue()*1.5)+"\" }");
                                out.newLine();
                            }
                            else {
                                out.write("\t\t{ \"keywords\" : \""+map.getKey().trim()+"\", \"showings\" : \""+(map.getValue()*1.5)+"\" },");
                                out.newLine();
                            }
                        }
                        i++;
                    } 
                }

                //Close structure
                out.write("\t]");
                out.newLine();
                out.write("}");
                out.close();

            }
            catch(IOException e) {
                return false;
            }
            
        }
        return true;
    }
    
    public static Map<Integer, Integer> sortByKey(Map<Integer, Integer> map) {
        List<Map.Entry<Integer, Integer>> list = new LinkedList<Map.Entry<Integer, Integer>>(map.entrySet());

        Collections.sort(list, new Comparator<Map.Entry<Integer, Integer>>() {
            @Override
            public int compare(Map.Entry<Integer, Integer> m1, Map.Entry<Integer, Integer> m2) {
                return (m1.getKey()).compareTo(m2.getKey());
            }
        });

        Map<Integer, Integer> result = new LinkedHashMap<Integer, Integer>();
        for (Map.Entry<Integer, Integer> entry : list) {
            result.put(entry.getKey(), entry.getValue());
        }
        return result;
    }
    
    public static Map<String, Integer> sortByValue(Map<String, Integer> map) {
        List<Map.Entry<String, Integer>> list = new LinkedList<Map.Entry<String, Integer>>(map.entrySet());

        Collections.sort(list, new Comparator<Map.Entry<String, Integer>>() {
            @Override
            public int compare(Map.Entry<String, Integer> m1, Map.Entry<String, Integer> m2) {
                return (m2.getValue()).compareTo(m1.getValue());
            }
        });

        Map<String, Integer> result = new LinkedHashMap<String, Integer>();
        for (Map.Entry<String, Integer> entry : list) {
            result.put(entry.getKey(), entry.getValue());
        }
        return result;
    }
    
    public static int calculatePie(db.neo.properties.RelTypes relation, LuceneRetriever lucR, List<String> timeframesRange) throws IOException {
        
        int i;
        int count = 0;
        for(i=Integer.parseInt(timeframesRange.get(0));i<Integer.parseInt(timeframesRange.get(timeframesRange.size()-1));i++) {
            count += lucR.getEdgesInTimeframe((long) i, relation).size();
        }
        return count;
    }
    
    public static boolean buildStatistics(int choice, String theme, boolean hasMentions, boolean hasRetweets, boolean hasReplies) throws IOException {
        
        if(choice == 1) {
            int numOfTweets;
            List<String> tfs = new ArrayList<String>();
            tfs = GraphBuilder.getTimeFrameRange(theme);
            int i;
            int records = 0;
            String chartTitle = "Tweets per timeframe";
            String xTitle = "Timeframes";
            String yTitle = "Tweets";
            Map<String, Integer> map;
            String neoPath = FilePaths.DEFAULT_ROOT_PATH+FilePaths.DATA_PARENT_FOLDER+FilePaths.DBs_FOLDER+ theme +FilePaths.NEO_FOLDER;
            String lucenePath = FilePaths.DEFAULT_ROOT_PATH+FilePaths.DATA_PARENT_FOLDER+FilePaths.DBs_FOLDER+ theme + FilePaths.LUCENE_FOLDER;
            GraphDatabaseService graphDB = new GraphDatabaseFactory().newEmbeddedDatabase(neoPath);
            LuceneRetriever lucR = new LuceneRetriever(theme, hasRetweets, lucenePath, false);
            String timelinePath = FilePaths.DEFAULT_ROOT_PATH+FilePaths.DATA_PARENT_FOLDER+FilePaths.METAFILES_FOLDER+ theme +FilePaths.STATISTICS_FOLDER+FilePaths.TIMELINE_JSON;
            FileWriter fwriter = new FileWriter(timelinePath);
            BufferedWriter out = new BufferedWriter(fwriter);
            try  {
                out.write("{");
                out.newLine();
                out.write("\t\"timeline\" : [");
                out.newLine();
                
                for(i=Integer.parseInt(tfs.get(0));i<=Integer.parseInt(tfs.get(tfs.size()-1));i++) {
                    map = collectTweetsHashtagsTerms(1, theme, String.valueOf(i), graphDB, lucR, null, hasRetweets);
                    numOfTweets = map.get("tweets");
                    records++;
                    if(i != Integer.parseInt(tfs.get(tfs.size()-1))) {
                        out.write("\t\t{ \"timeframe\" : \""+i+"\", \"tweets\" : "+numOfTweets+", \"tooltip\" : \""+GraphBuilder.getTimeFrameDate(theme, String.valueOf(i))+" ("+i+")\" },");
                    }
                    else { //Final record, comma omitted
                        out.write("\t\t{ \"timeframe\" : \""+i+"\", \"tweets\" : "+numOfTweets+", \"tooltip\" : \""+GraphBuilder.getTimeFrameDate(theme, String.valueOf(i))+" ("+i+")\" }");
                    }
                    out.newLine();
                    map.clear();
                }
                lucR.closeConnection();
                graphDB.shutdown();
                records++; //First timeframe
                out.write("\t],");
                out.newLine();
                out.write("\t\"info\" : [");
                out.newLine();
                out.write("\t\t{ \"title\" : \""+chartTitle+"\" },");
                out.newLine();
                out.write("\t\t{ \"x\" : \""+xTitle+"\" },");
                out.newLine();
                out.write("\t\t{ \"y\" : \""+yTitle+"\" },");
                out.newLine();
                out.write("\t\t{ \"records\" : \""+records+"\" }");
                out.newLine();
                out.write("\t]");
                out.newLine();
                out.write("}");
                out.close();
                return true;
            }
            catch (IOException e) {
                e.printStackTrace();
                return false;
            }
            
        }
        else if(choice==2) {
            String lucDir = FilePaths.DEFAULT_ROOT_PATH+FilePaths.DATA_PARENT_FOLDER+FilePaths.DBs_FOLDER+ theme +FilePaths.LUCENE_FOLDER;
            LuceneRetriever lucR = new LuceneRetriever(theme, hasRetweets, lucDir, false);

            Map<Long, List<Integer>> usersInTimeFrames = new HashMap<Long, List<Integer>>();
            usersInTimeFrames = lucR.findTimeframesOfActivityPerUser(theme);

            lucR.closeConnection();

            Map<Integer, Integer> result = new HashMap<Integer, Integer>();
            int key;
            for(Map.Entry<Long, List<Integer>> entry : usersInTimeFrames.entrySet()){
                key = entry.getValue().size();
                if(!result.isEmpty()) {
                    if(result.containsKey(key)) {
                        result.put(key, result.get(key) + 1);
                    }
                    else {
                        result.put(key, 1);
                    }
                }
                else {
                    result.put(entry.getValue().size(), 1);
                }
            }

            result = sortByKey(result);

            String histogramPath = FilePaths.DEFAULT_ROOT_PATH+FilePaths.DATA_PARENT_FOLDER+FilePaths.METAFILES_FOLDER+ theme +FilePaths.STATISTICS_FOLDER+FilePaths.HISTOGRAM_CSV;
            File f = new File(histogramPath);
            if(!f.exists()) {
                f.createNewFile();
            }

            FileWriter fwriter = new FileWriter(histogramPath);
            BufferedWriter out = new BufferedWriter(fwriter);
            out.write("Timeframes,Users");
            out.newLine();
            out.write("0,0");
            int bin_lower = 1;
            int bin = 5;
            int bin_upper = bin;
            int count = 0;
            int modulo;
            int factor;
            int iterations = 0;
            int overlay;

            for(Map.Entry<Integer, Integer> entry : result.entrySet()){
                if( ((entry.getKey() >= bin_lower) && (entry.getKey() < bin_upper)) ) {
                    count += entry.getValue();
                }
                else {
                    if(entry.getKey() <= bin_upper+5) {
                        out.newLine();
                        out.write(bin_lower+","+count);
                        count = entry.getValue();
                        bin_lower = bin_upper;
                        bin_upper += bin;
                    }
                    else {
                        overlay = entry.getKey() - bin_lower;
                        modulo = entry.getKey() % bin;
                        factor = overlay - modulo;
                        iterations = factor / bin;
                        for(int i=0;i<iterations;i++) {
                            out.newLine();
                            out.write(bin_lower+",0");
                            bin_lower = bin_upper;
                            bin_upper += bin;
                        }
                        count = entry.getValue();
                    }
                }
            }
            out.newLine();
            out.write(bin_lower+","+count);
            for(int i=0;i<5;i++) {
                bin_lower+= bin;
                out.newLine();
                out.write(bin_lower+",0");
            }
            out.close();
            return true;
        }
        else if(choice==3) { //Size of communities
            GeneralStatistics gStat = new GeneralStatistics(theme, hasRetweets);
            Map<Integer, Integer> result;
            List<String> timeframes = GraphBuilder.getTimeFrameRange(theme);
            
            result = gStat.sizeOfCommunities(Long.parseLong(timeframes.get(0)), Long.parseLong(timeframes.get(timeframes.size()-1)));
            gStat.closeConnections();
            
            result = sortByKey(result);
            //Map<Integer, Integer> result = new HashMap<Integer, Integer>();
            
            // Reverse result1 map
            //for(Map.Entry<Integer, Integer> entry : result1.entrySet()) {
                //result.put(entry.getValue(), entry.getKey());
            //}
            
            //result = sortByKey(result);
            
            String sizeOfComPath = FilePaths.DEFAULT_ROOT_PATH+FilePaths.DATA_PARENT_FOLDER+FilePaths.METAFILES_FOLDER+ theme +FilePaths.STATISTICS_FOLDER+FilePaths.SIZE_OF_COMMUNITIES_CSV;
            File f = new File(sizeOfComPath);
            if(!f.exists()) {
                f.createNewFile();
            }

            FileWriter fwriter = new FileWriter(sizeOfComPath);
            BufferedWriter out = new BufferedWriter(fwriter);
            out.write("Users,Communities");
            out.newLine();
            out.write("0,0");
            int bin_lower = 1;
            int bin = 10;
            int bin_upper = bin;
            int count = 0;
            int modulo;
            int factor;
            int iterations = 0;
            int overlay;

            for(Map.Entry<Integer, Integer> entry : result.entrySet()){
                if( ((entry.getKey() >= bin_lower) && (entry.getKey() < bin_upper)) ) {
                    count += entry.getValue();
                }
                else {
                    if(entry.getKey() <= bin_upper+5) {
                        out.newLine();
                        out.write(bin_lower+","+count);
                        count = entry.getValue();
                        bin_lower = bin_upper;
                        bin_upper += bin;
                    }
                    else {
                        overlay = entry.getKey() - bin_lower;
                        modulo = entry.getKey() % bin;
                        factor = overlay - modulo;
                        iterations = factor / bin;
                        for(int i=0;i<iterations;i++) {
                            out.newLine();
                            out.write(bin_lower+",0");
                            bin_lower = bin_upper;
                            bin_upper += bin;
                        }
                        count = entry.getValue();
                    }
                }
            }
            out.newLine();
            out.write(bin_lower+","+count);
            for(int i=0;i<5;i++) {
                bin_lower+= bin;
                out.newLine();
                out.write(bin_lower+",0");
            }
            out.close();
            return true;
        }
        else { //choice == 4
            
            String lucenePath = FilePaths.DEFAULT_ROOT_PATH+FilePaths.DATA_PARENT_FOLDER+FilePaths.DBs_FOLDER+ theme +FilePaths.LUCENE_FOLDER;
            LuceneRetriever lucR = new LuceneRetriever(theme, hasRetweets, lucenePath, false);

            int MENTIONS = 0;   //Number of mentions
            int RETWEETS = 0;   //Number of retweets
            int REPLIES = 0;    //Number of replies

            List<String> list = GraphBuilder.getTimeFrameRange(theme);
            
            MENTIONS = calculatePie(db.neo.properties.RelTypes.MENTION, lucR, list);
            RETWEETS = calculatePie(db.neo.properties.RelTypes.RETWEET, lucR, list);
            REPLIES = calculatePie(db.neo.properties.RelTypes.REPLY, lucR, list);

            lucR.closeConnection();
            
            String piePath = FilePaths.DEFAULT_ROOT_PATH+FilePaths.DATA_PARENT_FOLDER+FilePaths.METAFILES_FOLDER+ theme +FilePaths.STATISTICS_FOLDER+FilePaths.PIE_JSON;
            File f = new File(piePath);
            if(!f.exists()) {
                f.createNewFile();
            }
            FileWriter fwriter = new FileWriter(piePath);
            BufferedWriter out = new BufferedWriter(fwriter);
            
            String title = "Pie of Mentions / retweets / replies";
            String x_title = "Measurements";
            String y_title = "Value";
            int numOfRecords = 3;
            String mentions_l = "Mentions";
            String retweets_l = "Retweets";
            String replies_l = "Replies";
            
            out.write("{");
            out.newLine();
            out.write("\t\"pie\" : [");
            out.newLine();
            out.write("\t\t{ \"value\" : "+MENTIONS+" },");
            out.newLine();
            out.write("\t\t{ \"value\" : "+RETWEETS+" },");
            out.newLine();
            out.write("\t\t{ \"value\" : "+REPLIES+" }");
            out.newLine();
            out.write("\t],");
            out.newLine();
            out.write("\t\"info\" : [");
            out.newLine();
            out.write("\t\t{ \"title\" : \""+title+"\" },");
            out.newLine();
            out.write("\t\t{ \"x\" : \""+x_title+"\" },");
            out.newLine();
            out.write("\t\t{ \"y\" : \""+y_title+"\" },");
            out.newLine();
            out.write("\t\t{ \"records\" : "+numOfRecords+" },");
            out.newLine();
            out.write("\t\t{ \"label\" : \""+mentions_l+"\" },");
            out.newLine();
            out.write("\t\t{ \"label\" : \""+retweets_l+"\" },");
            out.newLine();
            out.write("\t\t{ \"label\" : \""+replies_l+"\" }");
            out.newLine();
            out.write("\t]");
            out.newLine();
            out.write("}");
            out.close();
            return true;
        }
    }
}