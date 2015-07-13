package graphbuilder;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TimeZone;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.FSDirectory;
import org.neo4j.graphdb.GraphDatabaseService;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.index.lucene.QueryContext;

import entities.Cluster;
import db.neo.clusters.ClusterProperties;

/**
 * 
 * @author  Paraskevas Eleftherios
 * @version 2015.07.13_1405
 */
public class GraphBuilder {
    
    /**
     * This method replaces the underscores of string with white space.
     * @param str The string to be transformed
     * @return The transformed string
     */
    public static String trimUnderscores(String str) {
        char[] str_c = str.toCharArray();
        char[] str_new = new char[19];
        int j=0;
        for(int i=0;i<str.length();i++) {
            if(i==13 || i==15) {
                str_new[j] = ':';
                j++;
            }
            if(str_c[i]=='_') {
                if(i==10) {
                    str_new[j] = ' ';
                    j++;
                }
                else {
                    str_new[j] = '-';
                    j++;
                }
            }
            else { //Other than underscore
                str_new[j] = str_c[i];
                j++;
            }
        }
        String str_result = new String(str_new);
        return str_result;
    }
    
    /**
     * Auxiliary method that creates all needed metafiles in metafiles directory
     * @param theme The theme of the dataset
     * @return True if the process succeeds, false otherwise
     * @throws IOException 
     */
    public boolean generateTFListMetafiles(String theme) throws IOException {
        
        String filename = FilePaths.DEFAULT_ROOT_PATH+FilePaths.DATA_PARENT_FOLDER+FilePaths.METAFILES_FOLDER+ theme;
        String filename2 = FilePaths.DEFAULT_ROOT_PATH+FilePaths.DATA_PARENT_FOLDER+FilePaths.DBs_FOLDER+ theme;
        File f = new File(filename+FilePaths.TIMEFRAMES_LIST_FILE+FilePaths.METAFILES_EXT);
        if(!f.exists()) {
            f.createNewFile();
        }
        FileWriter fstream = new FileWriter(filename+FilePaths.TIMEFRAMES_LIST_FILE+FilePaths.METAFILES_EXT);
        BufferedWriter out = new BufferedWriter(fstream);
        try {
            FileInputStream fstream1 = new FileInputStream(filename2+FilePaths.TIMESTEP_INDEX_FILE);
            DataInputStream in = new DataInputStream(fstream1);
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            String strLine;
            int dashIndex;
            String temp, firstDate, lastDate, tf;
            boolean firstLine = true;
            while ((strLine = br.readLine()) != null)   {
                if(firstLine) {
                    firstLine = false;
                    continue;
                }
                dashIndex = strLine.indexOf("\t");
                temp = strLine.substring(dashIndex);
                tf = strLine.substring(0, dashIndex);
                temp = temp.substring(1, temp.length());
                dashIndex = temp.indexOf("\t");
                firstDate = temp.substring(0, dashIndex);
                lastDate = temp.substring(dashIndex+1, temp.length());
                firstDate = trimUnderscores(firstDate);
                lastDate = trimUnderscores(lastDate);
                out.write(firstDate+"&nbsp;&nbsp;&nbsp;to&nbsp;&nbsp;&nbsp;"+lastDate+" ("+tf+")");
                out.newLine();
            }
            in.close();
            out.close();
            return true;
        }catch (Exception e){//Catch exception if any
            return false;
        }
    }
    
    /**
     * 
     * @param theme
     * @param tf
     * @return 
     */
    public static String getTimeFrameDate(String theme, String tf) {
        
        String timestepPath = FilePaths.DEFAULT_ROOT_PATH+FilePaths.DATA_PARENT_FOLDER+FilePaths.DBs_FOLDER+ theme +FilePaths.TIMESTEP_INDEX_FILE;
         String first;
        String second;
       
        String returnVal1;
        String returnVal2;
        String returnVal;
        try {
            FileInputStream fstream = new FileInputStream(timestepPath);
            DataInputStream in = new DataInputStream(fstream);
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            String strLine;
            int separator = -1;
            boolean firstLine = true; //File starts with the first line
            while((strLine = br.readLine()) != null) {
                if(firstLine) {
                    //First line contains only text fields
                    firstLine = false;
                    continue;
                }
                separator = strLine.indexOf("\t");
                second = strLine.substring(0, separator);
                if(tf.equals(second)) {
                    second = strLine.substring(separator+1, strLine.length());
                    separator = second.indexOf("\t");
                    first = second.substring(0, separator);
                    second = second.substring(separator);
                    second = second.substring(1, second.length());
                    returnVal1 = trimUnderscores(first);
                    returnVal2 = trimUnderscores(second);
                    
                    //separator = returnVal2.indexOf(" ");
                    //returnVal2 = returnVal2.substring(0, separator);
                    return returnVal1+" - "+returnVal2;
                }
            }
            in.close();
        }
        catch (FileNotFoundException e) {
            e.printStackTrace();
            return null;
        }
        catch (IOException e) {
            e.printStackTrace();
            return null;
        }
        return null;
    }
    public static List<String> getTimeFrameRange(String theme) {
        
        String timestepPath = FilePaths.DEFAULT_ROOT_PATH+FilePaths.DATA_PARENT_FOLDER+FilePaths.DBs_FOLDER+ theme +FilePaths.TIMESTEP_INDEX_FILE;
        String timeframesPath = FilePaths.DEFAULT_ROOT_PATH+FilePaths.DATA_PARENT_FOLDER+FilePaths.DBs_FOLDER+ theme +FilePaths.TIMEFRAMES_FILE+FilePaths.METAFILES_EXT;
        List<String> timeSteps = new ArrayList<String>();
        try {
            FileInputStream fstream = new FileInputStream(timestepPath);
            DataInputStream in = new DataInputStream(fstream);
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            String strLine;
            int separator = -1;
            boolean firstLine = true; //File starts with the first line
            while((strLine = br.readLine()) != null) {
                if(firstLine) {
                    //First line contains only text fields
                    firstLine = false;
                    continue;
                }
                separator = strLine.indexOf("\t");
                timeSteps.add(strLine.substring(0, separator));
            }
            in.close();
            File f = new File(timeframesPath);
            BufferedWriter out = null;
            if(f.exists()) {
                f.delete();
            }
            if(!f.exists()) {
                try {
                    f.createNewFile();
                    FileWriter fstream1 = new FileWriter(timeframesPath);
                    out = new BufferedWriter(fstream1);
                }   
                catch (IOException e) {
                    e.printStackTrace();
                }
            }
            out.write(timeSteps.get(0));
            out.newLine();
            out.write(timeSteps.get(timeSteps.size()-1));
            out.close();
        }
        catch (FileNotFoundException e) {
            e.printStackTrace();
            return null;
        }
        catch (IOException e) {
            e.printStackTrace();
            return null;
        }
        return timeSteps;
    }
    
    public boolean communityEvolutionHandler(boolean choice, String from, String to, String theme, ArrayList allEvol) throws FileNotFoundException, IOException, StringIndexOutOfBoundsException {
        try {
            
            String comPath = FilePaths.DEFAULT_ROOT_PATH+FilePaths.DATA_PARENT_FOLDER+FilePaths.DBs_FOLDER+ theme +FilePaths.COMMUNITIES_EVOLUTION_FILE;
            Map<Integer, Integer> timeframeOfCom = new HashMap<Integer, Integer>();
            FileInputStream fstream = new FileInputStream(comPath);
            DataInputStream in = new DataInputStream(fstream);
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            String strLine;
            char[] line = new char[Integer.valueOf(to)+50];
            int range = Integer.parseInt(to) - Integer.parseInt(from);
            int parenthesis = -1;
            int dash = -1;
            String code = null;
            String cluster_id = null;
            List<Integer> tabsEvol = new ArrayList<Integer>(); //List with tabs per comEvolSingle
            int tabs = 0;
            boolean OOR = false; //Out of range
            boolean EOL = false; //End of line
            boolean firstLine = true;
            int pointer = 0; //Line pointer
            ArrayList comEvolAll = new ArrayList(); //Array list containing all cluster evolution
            int timeframe = 1; //For split usage -starts from 1st
            int i;
            String temp = null;
            boolean flag = false;
            List<String> cluster_id_temp = new ArrayList<String>(); //For split usage
            int pointerTemp = 0; //For split usage
            List<String> timeFrames = null;
            String firstTimeFrame = null;
            timeFrames = getTimeFrameRange(theme);
            firstTimeFrame = timeFrames.get(0).toString();

            //Iterate until end of file or out of range
            while(((strLine = br.readLine()) != null)&&(!OOR)) {
                ArrayList<String> comEvolSingle = new ArrayList<String>(); //Array list containg single cluster evolution
                if(firstLine) {
                    //First line contains only text fields
                    firstLine = false;
                    continue;
                }
                //If the timeframe does not start from first timeframe
                //and the lines don't start yet with a "\t"
                //initiate iteration to reach correct point
                if((!firstTimeFrame.equals(from))&&!flag) {
                    while(!strLine.startsWith("\t")) {
                        strLine = br.readLine();
                        continue;
                    }
                    flag = true; //Reached point of tabbed lines
                }
                if(strLine.startsWith("\t")) { //If the evolution doesn't start in the first timeframe
                    line = strLine.toCharArray(); //We handle the string as char array
                    i = 0;
                    while(line[i]=='\t'&&i<line.length) {
                        tabs++; //Count the tabs (timeframes)
                        i++;
                    }
                    //Flag reassures us that from is bigger than the lower timeframe
                    if(flag && (tabs < Integer.valueOf(from)-(Integer.valueOf(firstTimeFrame)))) {
                        tabs = 0;
                        i = 0;
                        continue;
                    }
                    if(tabs > Integer.valueOf(to)-(Integer.valueOf(firstTimeFrame))) {  
                        OOR = true; //We break the iteration
                        continue;
                    }
                    tabsEvol.add(tabs); //Store number of tabs
                }
                else {
                    tabsEvol.add(0); //0 tabs - evolution starts from the first timeframe
                }
                temp = strLine.trim(); //Remove all white spaces
                strLine = temp;
                timeframe+=tabs;
                while(!EOL) { //While end of line is not reached
                    try { 
                        parenthesis = strLine.indexOf("(", 0);
                        dash = strLine.indexOf("-", 0);
                        
                    
                        code = Character.toString(strLine.charAt(dash+1)); //D, C, S etc
                        cluster_id = strLine.substring(0, parenthesis); //Cluster_id
                        
                        timeframeOfCom.put(Integer.parseInt(cluster_id), ( (timeframe - 1) + Integer.parseInt(from)));
                        
                        comEvolSingle.add(cluster_id.trim()); //Stores only cluster_id, removing white space
                        comEvolSingle.add(Character.toString(strLine.charAt(dash+1))); //Stores code
                        
                        pointer = dash+2; //Move pointer
                        timeframe++; //New timeframe reached
                        temp = strLine.substring(pointer+1);
                        strLine = temp;
                        
                         
                        if("S".equals(code)) { //Handle split
                            parenthesis = strLine.indexOf("(", 0);
                            dash = strLine.indexOf("-", 0);
                            //temp = strLine;
                            while(parenthesis != -1) { //While we haven't reached the '-' character
                                cluster_id_temp.add(strLine.substring(0, parenthesis)); //Get cluster_id
                                pointerTemp = strLine.indexOf(")", 0);
                                temp = strLine.substring(pointerTemp+1);
                                strLine = temp;
                                parenthesis = temp.indexOf("(", 0); //Get new parenthesis
                                dash = strLine.indexOf("-", 0);
                            }
                            for(i=0;i<cluster_id_temp.size();i++) {
                                comEvolSingle.add(cluster_id_temp.get(i));
                            }
                            cluster_id_temp.clear(); //Clear old data
                            code = Character.toString(temp.charAt(dash+1));
                            comEvolSingle.add(code); //Store final code
                            strLine = temp;
                            continue;
                        }
                    }
                    catch (StringIndexOutOfBoundsException e) { //End of line  
                        EOL = true;
                    }
                }
                //Reset pointers
                EOL = false;
                pointer = 0; 
                tabs = 0;
                timeframe = 1;
                
                //Store community evolution
                comEvolAll.add(comEvolSingle);
            }
            in.close();

            if(!choice) {
                List<String> fullRange = new ArrayList<String>();
                fullRange = this.getTimeFrameRange(theme);
                ArrayList fEvol = new ArrayList();
                communityEvolutionHandler(true, fullRange.get(0), fullRange.get(fullRange.size()-1), theme, fEvol);
                FileBuilder fb = new FileBuilder();
                //if(fEvol.get(0).equals("75")) {
                    return fb.buildEvolJSON(comEvolAll, tabsEvol, theme, fEvol, firstTimeFrame, timeframeOfCom);
                //}
            }
            for(i=0;i<comEvolAll.size();i++) {
                allEvol.add(comEvolAll.get(i));
            }
            return true;
        }
        catch (FileNotFoundException e) {
            return false;
        }
        catch (IOException e) {
            return false;
        }
    }
    
    
    
    public boolean NeoHandlerPerTF(String theme, String tf, boolean hasMentions, boolean hasRetweets, boolean hasReplies) throws NullPointerException, ParseException, FileNotFoundException, IOException {
       
        String neoPath = FilePaths.DEFAULT_ROOT_PATH+FilePaths.DATA_PARENT_FOLDER+FilePaths.DBs_FOLDER+ theme +FilePaths.NEO_FOLDER;
        String entirePath = FilePaths.DEFAULT_ROOT_PATH+FilePaths.DATA_PARENT_FOLDER+FilePaths.METAFILES_FOLDER+ theme +FilePaths.ENTIRE_FOLDER;
        String lucenePath = FilePaths.DEFAULT_ROOT_PATH+FilePaths.DATA_PARENT_FOLDER+FilePaths.DBs_FOLDER+ theme +FilePaths.LUCENE_FOLDER;
        
        //Check if file exists
        File file = new File(entirePath+tf+"/"+tf+".gexf");
        if(file.exists()) {
            return true;
        }
        GraphDatabaseService graphDB = new GraphDatabaseFactory().newEmbeddedDatabase(neoPath); //Opening Neo4j DB
        IndexManager index1 = graphDB.index();
        Index<Node> index = index1.forNodes("clusternodes");
        Index<Node> hubIndex = index1.forNodes("hubnodes");
        Index<Node> outlierIndex = index1.forNodes("outliernodes");
        // Map<String,List<Cluster>> clusters = new HashMap<String,List<Cluster>>();
        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
	dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        IndexHits<Node> hits = index.query(QueryContext.numericRange("reference_time_frame", Long.valueOf(tf), Long.valueOf(tf), true, true));
        
        //get clusters
        List<entities.Cluster> clusters = new ArrayList<entities.Cluster>();
        for(Node cl : hits){
            try {
                clusters.add(new Cluster((Long)cl.getProperty("cluster_id"), (Integer)cl.getProperty("local_cluster_id"),Long.valueOf(tf), dateFormat.parse((String)cl.getProperty("from_date")), dateFormat.parse((String)cl.getProperty("to_date")),(String)cl.getProperty("rel_type")));
                //clusters.add(new Cluster((Long)cl.getProperty(ClusterProperties.ID), (Integer)cl.getProperty(ClusterProperties.LOCAL_ID),refTimeFrame, dateFormat.parse((String)cl.getProperty(ClusterProperties.FROM_DATE)), dateFormat.parse((String)cl.getProperty(ClusterProperties.TO_DATE))));

            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
        
        
        //get outliers
        entities.Cluster outlierCluster = getOutlierClusterInTimeFrame(Long.parseLong(tf), outlierIndex);
        
        //get hubs
        entities.Cluster hubCluster = getOutlierClusterInTimeFrame(Long.parseLong(tf), hubIndex);
        
        Node cnode;
        Iterable<Relationship> rels = null;
        Map<Long, Long> clustAss = new HashMap<Long, Long>();
        Map<Long, String> clustAssUserNames = new HashMap<Long, String>();
   
        for(entities.Cluster cl : clusters) {
            //Get cluster node
            cnode = index.get("cluster_id", cl.getId()).getSingle();
            try {
                //For each cluster, get all users
                rels = cnode.getRelationships(RelTypes.BELONGS_IN);
            }
            catch (NullPointerException e) {
                e.printStackTrace();
                graphDB.shutdown();
                return false;
            }
            for(Relationship r : rels) {
                //Store all user nodes to 'users' list
                clustAss.put(Long.parseLong(r.getOtherNode(cnode).getProperty("user_id").toString()),cl.getId());
                clustAssUserNames.put(Long.parseLong(r.getOtherNode(cnode).getProperty("user_id").toString()), r.getOtherNode(cnode).getProperty("username").toString());
            }
        }
        
        Set<Long> outliers = null;
        if(outlierCluster != null){
            outliers = new HashSet<Long>();
            cnode = outlierIndex.get("cluster_id", outlierCluster.getId()).getSingle();
            try {
                //For each cluster, get all users
                rels = cnode.getRelationships(RelTypes.BELONGS_IN);
            }
            catch (NullPointerException e) {
                e.printStackTrace();
                graphDB.shutdown();
                return false;
            }
            for(Relationship r : rels) {
                //Store all user nodes to 'users' list
                outliers.add(Long.parseLong(r.getOtherNode(cnode).getProperty("user_id").toString()));
            }
        }
        Set<Long> hubs = null;
        if(hubCluster != null){
            hubs = new HashSet<Long>();    
            cnode = hubIndex.get("cluster_id", hubCluster.getId()).getSingle();
            try {
                //For each cluster, get all users
                rels = cnode.getRelationships(RelTypes.BELONGS_IN);
            }
            catch (NullPointerException e) {
                e.printStackTrace();
                graphDB.shutdown();
                return false;
            }
            for(Relationship r : rels) {
                //Store all user nodes to 'users' list
                hubs.add(Long.parseLong(r.getOtherNode(cnode).getProperty("user_id").toString()));
            }
        
        }
        graphDB.shutdown();
        
        FileBuilder fBuilder = new FileBuilder();
        fBuilder.buildTFGEXF(clustAss, clustAssUserNames, hubs, outliers, theme, entirePath, tf, hasMentions, hasRetweets, hasReplies);
        
        return true; //Create GEXF files for all users
    }
    
	
	
    public entities.Cluster getOutlierClusterInTimeFrame(long refTimeFrame, Index outlierIndex){
        entities.Cluster cluster = null;
        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        //txManager.beginReadOperation();
        IndexHits<Node> hits = outlierIndex.query(QueryContext.numericRange(ClusterProperties.REFERENCE_TIME_FRAME, refTimeFrame, refTimeFrame, true, true));
        Node cl = hits.getSingle();
        if(cl != null){
            try {
                    cluster = new Cluster((Long)cl.getProperty(ClusterProperties.ID), refTimeFrame, dateFormat.parse((String)cl.getProperty(ClusterProperties.FROM_DATE)), dateFormat.parse((String)cl.getProperty(ClusterProperties.TO_DATE)),(String)cl.getProperty(ClusterProperties.REL_TYPE));
            } catch (ParseException e) {
                    e.printStackTrace();
            }

        }
        return cluster; 
    }

        
    public boolean NeoHandlerPerCommunity(String theme, boolean hasMentions, boolean hasRetweets, boolean hasReplies) throws NullPointerException, ParseException, FileNotFoundException, IOException {
        
        String neoPath = FilePaths.DEFAULT_ROOT_PATH+FilePaths.DATA_PARENT_FOLDER+FilePaths.DBs_FOLDER+ theme +FilePaths.NEO_FOLDER;
        String gexfPath = FilePaths.DEFAULT_ROOT_PATH+FilePaths.DATA_PARENT_FOLDER+FilePaths.METAFILES_FOLDER+ theme +FilePaths.GEXF_FOLDER;
        String lucenePath = FilePaths.DEFAULT_ROOT_PATH+FilePaths.DATA_PARENT_FOLDER+FilePaths.DBs_FOLDER+ theme +FilePaths.LUCENE_FOLDER;
        
        GraphDatabaseService graphDB = new GraphDatabaseFactory().newEmbeddedDatabase(neoPath); //Opening Neo4j DB
        
        //Getting indexes for "clusternodes" nodes
        IndexManager index1 = graphDB.index();
        Index<Node> index = index1.forNodes("clusternodes");
        Map<String,List<entities.Cluster>> clusters = new HashMap<String,List<entities.Cluster>>();
        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
	dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
	
        List<String> timeSteps = new ArrayList<String>();
        //timeSteps = null; //Range of timeframe
        
 
        timeSteps = this.getTimeFrameRange(theme); //Get timeframe ids' range
        if(timeSteps==null) {
            graphDB.shutdown();
            return false;
        }
  
        
        for(int i=0;i<timeSteps.size();i++) {
            //Get all cluster nodes between 'from' and 'to'
            IndexHits<Node> hits = index.query(QueryContext.numericRange("reference_time_frame", Long.valueOf(timeSteps.get(i)), Long.valueOf(timeSteps.get(i)), true, true));
            List<entities.Cluster> tmp = new ArrayList<entities.Cluster>();
            for(Node cl : hits){
                try {
                    //Store all clusters in 'clusters' list
                    tmp.add(new Cluster((Long)cl.getProperty("cluster_id"), (Integer)cl.getProperty("local_cluster_id"),Long.valueOf(timeSteps.get(i)), dateFormat.parse((String)cl.getProperty("from_date")), dateFormat.parse((String)cl.getProperty("to_date")),(String)cl.getProperty("rel_type")));
                    //clusters.add(new Cluster((Long)cl.getProperty(ClusterProperties.ID), (Integer)cl.getProperty(ClusterProperties.LOCAL_ID),refTimeFrame, dateFormat.parse((String)cl.getProperty(ClusterProperties.FROM_DATE)), dateFormat.parse((String)cl.getProperty(ClusterProperties.TO_DATE))));
		
                } catch (ParseException e) {
                    e.printStackTrace();
                }
            }
            clusters.put(timeSteps.get(i),tmp);
        }
        
        Node cnode;
        List<Node> users = new ArrayList<Node>();
        Iterable<Relationship> rels = null;
       
        //First, delete all old files
        File file = new File(gexfPath);        
        String[] myFiles;      
        if(file.isDirectory()) {  
            myFiles = file.list();  
            for(int d=0; d<myFiles.length; d++) {  
                File myFile = new File(file, myFiles[d]);   
                myFile.delete();  
            }  
        }
        for(Entry<String,List<entities.Cluster>> en : clusters.entrySet()) {
            List<entities.Cluster> curr = en.getValue();
            for(entities.Cluster cl : curr) {
                //Get cluster node
                cnode = index.get("cluster_id", cl.getId()).getSingle();
                try {
                    //For each cluster, get all users
                    rels = cnode.getRelationships(RelTypes.BELONGS_IN);
                }
                catch (NullPointerException e) {
                    e.printStackTrace();
                    graphDB.shutdown();
                    return false;
                }
                for(Relationship r : rels) {
                    //Store all user nodes to 'users' list
                    users.add(r.getOtherNode(cnode));
                }

                FileBuilder fBuilder = new FileBuilder();
                fBuilder.buildCommGEXF(users, cl, theme, en.getKey());
                users.clear();
            }
        }
        
        graphDB.shutdown();
               
        return true; //Create GEXF files for all users
    }
    public static void removeDirectory(File directory) {
        String[] list = directory.list();
        for(int j=0;j<list.length;j++) {
            File entry = new File(directory, list[j]);
            if(entry.isDirectory()) {
                removeDirectory(entry);
            }
            else {
                entry.delete();
            }
        }
        directory.delete();
    }
    
    public static void recDeleteDirectory(String theme) {
        
        String dbsPath = FilePaths.DEFAULT_ROOT_PATH+FilePaths.DATA_PARENT_FOLDER+FilePaths.DBs_FOLDER;
        String metaPath = FilePaths.DEFAULT_ROOT_PATH+FilePaths.DATA_PARENT_FOLDER+FilePaths.METAFILES_FOLDER;
        File dir = new File(dbsPath+theme);
        removeDirectory(dir);
        dir = new File(metaPath+theme);
        removeDirectory(dir);
    }
    
    public static void main(String[] args) throws IOException, ParseException {
        
        GraphBuilder files = new GraphBuilder();
        FileBuilder fb = new FileBuilder();
        
        //files.NeoHandlerPerCommunity("testworking@1372779412", true, true, true);
        //files.NeoHandlerPerTF("tds@1372934350", "4", true, true, true);
        //fb.buildTagCloudJSON("eurogroup4@1371203151", 5, true, true, true);
        //fb.buildStatistics(1, "test44@1372930777", true, true, true);
        //files.communityEvolutionHandler(false, "4", "166", "test44@1372930777", null);
        
        if(args[0].equals("1")) { //Case "Build evolution files"
            String from = args[1];
            String to = args[2];
            String theme = args[3];
            files.communityEvolutionHandler(false, from, to, theme, null);
        }
        
        else if(args[0].equals("2")) { //Case "Build GEXF files after update"
            String theme = args[1];
            String mentions = args[2];
            String retweets = args[3];
            String replies = args[4];
            boolean hasMentions = false;
            boolean hasRetweets = false;
            boolean hasReplies = false;
            if(mentions.equals("1")) {
                hasMentions = true;
            }
            if(retweets.equals("1")) {
                hasRetweets = true;
            }
            if(replies.equals("1")) {
                hasReplies = true;
            }
            files.NeoHandlerPerCommunity(theme, hasMentions, hasRetweets, hasReplies);
        }
        
        else if(args[0].equals("3")) { //Case "Entire graph"
            String theme = args[1];
            String tf = args[2];
            String mentions = args[3];
            String retweets = args[4];
            String replies = args[5];
            boolean hasMentions = false;
            boolean hasRetweets = false;
            boolean hasReplies = false;
            if(mentions.equals("1")) {
                hasMentions = true;
            }
            if(retweets.equals("1")) {
                hasRetweets = true;
            }
            if(replies.equals("1")) {
                hasReplies = true;
            }
            files.NeoHandlerPerTF(theme, tf, hasMentions, hasRetweets, hasReplies);
            fb.buildTagCloudJSON(theme, Integer.parseInt(tf), hasMentions, hasRetweets, hasReplies);
        }
        else if(args[0].equals("4")) { //EventWheel ('Automatic' Mode)
            String tfFrom = args[1];
            String tfTo = args[2];
            String themeA = args[3];
            String themeB = args[4];
            String nameA = args[5];
            String nameB = args[6];
            boolean hasLeft = Boolean.parseBoolean(args[7]);
            String nameOfEventWheel = args[8];
            String mentionsA = args[9];
            String retweetsA = args[10];
            String repliesA = args[11];
            String mentionsB = args[12];
            String retweetsB = args[13];
            String repliesB = args[14];
            boolean hasMentionsA = false;
            boolean hasRetweetsA = false;
            boolean hasRepliesA = false;
            boolean hasMentionsB = false;
            boolean hasRetweetsB = false;
            boolean hasRepliesB = false;
            if(mentionsA.equals("1")) {
                hasMentionsA = true;
            }
            if(retweetsA.equals("1")) {
                hasRetweetsA = true;
            }
            if(repliesA.equals("1")) {
                hasRepliesA = true;
            }
            if(mentionsB.equals("1")) {
                hasMentionsB = true;
            }
            if(retweetsB.equals("1")) {
                hasRetweetsB = true;
            }
            if(repliesB.equals("1")) {
                hasRepliesB = true;
            }

            //Build file
            fb.buildEventWheelJSON(tfFrom, tfTo, themeA, themeB, nameA, nameB, hasLeft, nameOfEventWheel, hasMentionsA, hasRetweetsA, hasRepliesA, hasMentionsB, hasRetweetsB, hasRepliesB);
        }
        else if(args[0].equals("5")) { //Timeframes list
            String theme = args[1];
            files.generateTFListMetafiles(theme);
        }
        else if(args[0].equals("6")) { //Build statistics
            String theme = args[1];
            String mentions = args[2];
            String retweets = args[3];
            String replies = args[4];
            boolean hasMentions = false;
            boolean hasRetweets = false;
            boolean hasReplies = false;
            if(mentions.equals("1")) {
                hasMentions = true;
            }
            if(retweets.equals("1")) {
                hasRetweets = true;
            }
            if(replies.equals("1")) {
                hasReplies = true;
            }
            fb.buildStatistics(1, theme, hasMentions, hasRetweets, hasReplies); //Timeline of tweets
            fb.buildStatistics(2, theme, hasMentions, hasRetweets, hasReplies); //User activity histogram
            fb.buildStatistics(3, theme, hasMentions, hasRetweets, hasReplies); //Size of communities chart
            fb.buildStatistics(4, theme, hasMentions, hasRetweets, hasReplies); //Pie of mentions / retweets / replies
        }
        else if(args[0].equals("7")) { //Delete folders
            String theme = args[1];
            recDeleteDirectory(theme);
        }
    }
}


