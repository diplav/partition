package graph.dataimport;

import graph.analysis.PartitionAnalysis;

import java.util.ArrayList;
import java.util.HashMap;

import com.mongodb.*;
/*Data 1:
 * No Of edges will be loaded are: 21693
 * No. of Nodes will be: 19596
 * Data 2:
 * No Of edges will be loaded are: 65078
 * No. of Nodes will be: 21048
 */

public class ImportData {
	//this location where node related data is stored
	String node_file_location="/home/diplav/workspace/DataImport/DataSet/cal.cnode";
	//this location where edge data is stored
	String edge_file_location="/home/diplav/workspace/DataImport/DataSet/cal.cedge";

	public static void main(String[] args) {
		ImportData importdata=new ImportData();
		PartitionAnalysis analysis=null;
		
		DBCollection coll_nodedata = importdata.getNewCollection("graph","nodedata");
		importdata.loadNodeData(coll_nodedata);
		
		//importdata.loadEdgeData(); //this function is to load edges in a single partition
		
		ArrayList<DBCollection> coll_list_edgedata =importdata.initializeDataObjects();
		importdata.loadPartitionEdgeData(coll_list_edgedata);
		
		DBCollection coll_analysis = importdata.getNewCollection("graph","analysis");
		analysis=new PartitionAnalysis(coll_analysis);
		
		//this will be used to store analysis regarding the partition scores
		DBCollection stat = importdata.getNewCollection("graph","stat");
		analysis.setStatisticsCollection(stat);
		
		System.out.println("Initial Score: "+ analysis.getCompleteScore(coll_list_edgedata,true));
		analysis.stabilizePartition(importdata.getMapOfCollection(coll_list_edgedata));
		
		
		
		long before=10000,after=-1;
		
		//while((Math.abs(before-after)) > 0)
		while(true)
		{
	    	coll_analysis.drop();
		
	    	coll_analysis = importdata.getNewCollection("graph","analysis");
	    	analysis.updateCollection(coll_analysis);
		
		 before= analysis.getCompleteScore(coll_list_edgedata,true);
		 analysis.stabilizePartition(importdata.getMapOfCollection(coll_list_edgedata));
		 after= analysis.getCompleteScore(coll_list_edgedata,true);
		 
		 System.out.println("Before: "+before+" After: "+after+" value:"+Math.abs(before-after));
		}
	} 

	//This function will import node data to the mongodb database into collection nodedata
	public void loadNodeData(DBCollection nodedata )
	{
		System.out.println("Loading Node Data into MongoDB");
		ExtractData.storeNodeData(node_file_location,nodedata);
	}
	//ThadEis function will import edge data to the mongodb database into collection edgedata
	public void loadEdgeData()
	{
		System.out.println("Loading Edge Data into MongoDB");
		MongoClient mongoclient=Database.connectMongoDB();
		DB db=Database.getDB("graph",mongoclient);
		Database.refreshSchema("edgedata",db);
		DBCollection edgedata=Database.getCollection("edgedata",db);
		ExtractData.storeEdgeData(edge_file_location,edgedata);
		
	}
	
	public void loadPartitionEdgeData(ArrayList<DBCollection> edgeDataList)
	{
		System.out.println("Loading Partitioned Edge Data into MongoDB");		
		ExtractData.storeEdgeData(edge_file_location,edgeDataList);
		
	}
	
	public ArrayList<DBCollection> initializeDataObjects()
	{
		MongoClient mongoclient=Database.connectMongoDB();
		DB db=Database.getDB("graph",mongoclient);
		
		Database.refreshSchema("pedgedata0",db);
		Database.refreshSchema("pedgedata1",db);
		Database.refreshSchema("pedgedata2",db);
		
		//here we have used 3 different partitions/collections to store data
		DBCollection edgedata0=Database.getCollection("pedgedata0",db);
		DBCollection edgedata1=Database.getCollection("pedgedata1",db);
		DBCollection edgedata2=Database.getCollection("pedgedata2",db);
		
		ArrayList<DBCollection> edgeDataList = new ArrayList<>();
		edgeDataList.add(edgedata0);
		edgeDataList.add(edgedata1);
		edgeDataList.add(edgedata2);
		return edgeDataList;
	}
	
	public DBCollection getNewCollection(String database_name,String collection_name)
	{
		MongoClient mongoclient=Database.connectMongoDB();
		DB db=Database.getDB(database_name,mongoclient);
		Database.refreshSchema(collection_name,db);
		return Database.getCollection(collection_name,db);
	}
	
	
	
	public HashMap<String,DBCollection> getMapOfCollection(ArrayList<DBCollection> coll)
	{
		HashMap<String,DBCollection> coll_map=new HashMap<>();
		
		for(DBCollection col:coll)
			coll_map.put(col.getName(), col);
		return coll_map;
		
	}

}
