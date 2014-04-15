//create table mapping(dst bigint(20),src bigint(20),primary key (dst ,src ));
//create table mapping(dst bigint(20),src bigint(20),src_edge bigint(20),primary key (dst ,src_edge ));


package graph.dataimport;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.IOUtils;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoException;
import com.mysql.jdbc.Statement;

public class ExtractData {
	
	static String insertIntoMapping = "INSERT INTO mapping(src,dst) VALUES (?,?)";
	
	
static private EdgeBean next=null;
	//This will extract All the nodes and their Long, Lat
	public static List<DBObject>  getNodeData(String data_file_path)
	{
		String [] arr=new String[3];      
		BufferedReader reader;
		String line;
		List<DBObject> documents = new ArrayList<>();
		DBObject doc;

		try {
			reader = new BufferedReader(new FileReader(data_file_path));
			while ((line = reader.readLine()) != null) {
				//split line by space and extract node_id,long and lat
				arr=line.split(" ");

				ArrayList<Float> loc = new ArrayList<>();
				loc.add(Float.parseFloat(arr[1]));        //long
				loc.add(Float.parseFloat(arr[2]));        //lat
				doc = new BasicDBObject("node_id", Long.parseLong(arr[0])).append("loc",loc); //add id,loc[long,lat]

				//add to the list of the document 
				documents.add(doc);

			}
			reader.close();
		} catch (Exception e) {
			e.printStackTrace();

		}
		return documents;
	}

	//store the node data in the given collection
	public static long  storeNodeData(String file_location,DBCollection collection)
	{
		List<DBObject> documents=getNodeData(file_location);
		collection.ensureIndex(new BasicDBObject("node_id",1), new BasicDBObject("unique", true));
		long temp=collection.count();	
		try{
			collection.insert(documents);
		}
		catch(MongoException e ){
			System.out.println(e.getMessage());
		}

		temp = collection.count() - temp;

		if(temp>0)
			System.out.println("INFO:"+ temp +" No of records successfully inserted");
		else
			System.out.println("INFO: None of the records added");


		return documents.size();
	}


	public static List<DBObject>  storeEdgeData(String data_file_path,DBCollection collection)
	{
		String [] arr=new String[4]; //[0]: edge_id,[1]: src_id,[2]:dst_id,[3]:distance     
		BufferedReader reader;
		String line;
		List<DBObject> documents = new ArrayList<>();
		BasicDBObject doc;

	
		
		collection.ensureIndex(new BasicDBObject("src",1), new BasicDBObject("unique", true));
		long temp=collection.count();
		long count=0;
		try {
			reader = new BufferedReader(new FileReader(data_file_path));
			while ((line = reader.readLine()) != null) {
				//split line by space and extract edge_id,src_id,dst_id,distance
				arr=line.split(" ");

				//speed attribute added in the data file and to the import utility

				doc = new BasicDBObject("edge_id", arr[0]).append("dst", Long.parseLong(arr[2])).append("distance",(Float.parseFloat(arr[3]))*100).append("speed", Integer.parseInt(arr[4]));
				
				DBObject query = new BasicDBObject("src", Long.parseLong(arr[1]));
				if( collection.find(query).count() == 0)//create new entry 
				{
					ArrayList<BasicDBObject> list= new ArrayList<BasicDBObject>();
					list.add(doc);
					doc=new BasicDBObject("src", Long.parseLong(arr[1])).append("edges",list);
					collection.insert(doc);
			
				}
				else  
				{
					DBObject upd=new BasicDBObject("$push",new BasicDBObject("edges", doc));
					collection.update(query, upd);
				}

				count++;
			}
			reader.close();
			temp = collection.count() - temp;

				System.out.println("INFO:"+ temp +" No of records successfully inserted");
				System.out.println("INFO:"+ count +" No of edges successfully inserted");
		} catch (Exception e) {
			e.printStackTrace();

		}
		return documents;
	}
	

	
	/*
	 * This function is used to store data in round robin fashion to different database
	 */
	
	
	/*
	 * Modification in this function to load the mapping data into mysql database
	 * MySQL Connection details are defined in Database.connect() function
	 */
	
	public static void storeEdgeData(String data_file_path,ArrayList<DBCollection> collections)
	{

		//Declare some useful variables
		String [] arr=new String[4]; //[0]: edge_id,[1]: src_id,[2]:dst_id,[3]:distance
		int marker=0;
		BufferedReader reader;
		String line;
		BasicDBObject doc;
		DBCollection collection=null;
		 Statement statement=null;
        //create index on each of the database based on source
		for(DBCollection coll:collections)
        	coll.ensureIndex(new BasicDBObject("src",1), new BasicDBObject("unique", true));
        System.out.println("No of databases"+collections.size());
        
        
        
        //mySQL setup
        
    	
		
		Connection mysqlConn=Database.connect();
        
      //This code is modified for bulk upload of data
      		// First create a statement off the connection and turn off unique checks and key creation
		     
		      
			   try {
				statement = (com.mysql.jdbc.Statement)mysqlConn.createStatement();
			
              statement.execute("SET UNIQUE_CHECKS=0; ");
              statement.execute("ALTER TABLE mapping DISABLE KEYS");
              System.out.println("Unset all the checks");
              
			   } catch (SQLException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
              
              // Define the query we are going to execute
              String statementText = "LOAD DATA LOCAL INFILE '/tmp/file1.txt' " +
                      "INTO TABLE mapping " +
                      "(dst, src, src_edge) ; " ;
        
              // Create StringBuilder to String that will become stream
              StringBuilder builder = new StringBuilder();
            
              
              
		
		long count=0;

		try {
			reader = new BufferedReader(new FileReader(data_file_path));
			while ((line = reader.readLine()) != null) {
				//split line by space and extract edge_id,src_id,dst_id,distance
				arr=line.split(" ");
			

				doc = new BasicDBObject("edge_id", arr[0]).append("dst", Long.parseLong(arr[2])).append("distance",(Float.parseFloat(arr[3]))*100).append("score", Integer.parseInt("0")).append("speed", Integer.parseInt(arr[4]));
				
				DBObject query = new BasicDBObject("src", Long.parseLong(arr[1]));
				
				  //this data will go into mysql mapping table
	              builder.append(Long.parseLong(arr[2]));  //dst
	              builder.append('\t');
	              builder.append(Long.parseLong(arr[1]));  //src
	              builder.append('\t');
	              builder.append(Long.parseLong(arr[0]));  //edge_id
	              builder.append('\n');
				
				collection=null;
				for(DBCollection coll:collections)
				{
					if( coll.find(query).count() != 0)
					{
					    collection=coll;	
					}
				}
				if(collection == null)
				{
					collection=collections.get(marker);
					if(++marker > collections.size()-1)
						marker=0;
				}
				
				if( collection.find(query).count() == 0)//create new entry 
				{
					ArrayList<BasicDBObject> list= new ArrayList<BasicDBObject>();
					list.add(doc);
					doc=new BasicDBObject("src", Long.parseLong(arr[1])).append("edges",list);
					collection.insert(doc);
			
				}
				else  
				{
					DBObject upd=new BasicDBObject("$push",new BasicDBObject("edges", doc));
					collection.update(query, upd);
				}

			count++;	
			}
			
			
				System.out.println("INFO:"+ count +" No of edges successfully inserted");
				count=0;
				
				for(DBCollection coll:collections)
					count+=coll.count();
				System.out.println("INFO:"+ count +" No of records successfully inserted");
		
				reader.close();		
				
				//mysql load code
				
				 // Create stream from String Builder
			       InputStream is = IOUtils.toInputStream(builder.toString());
			    // Setup our input stream as the source for the local infile
			       statement.setLocalInfileInputStream(is);

			       
			       // Execute the load infile
			       statement.execute(statementText);
			  
			       // Turn the checks back on
			       statement.execute("ALTER TABLE mapping ENABLE KEYS");
			       statement.execute("SET UNIQUE_CHECKS=1; ");
			       System.out.println("All check s enabled");
				
				
		} catch (Exception e) {
			e.printStackTrace();

		}	
		
		 
			
		
	}
/*
 * This function is incomplete.
 * Purpose: Collect edge data and store them in different partitions
 * Cons: Most of the work will be in Java and interaction with database will slow 
 * the operation. 
    //this function will partition edge data and stores into different tables		
	public static void storeEdgeData(String data_file_path,ArrayList<DBCollection> collection)
	{
		ArrayList<Long> collection_size=computeSize(collection);
		BufferedReader reader=initializeDataReader(data_file_path);
		EdgeBean e=null;
		
		while( e=getNextEdge(reader) != null)
		{
			
		}
	}
*/	
	public static EdgeBean getNextEdge(BufferedReader reader)
	{
		String [] arr=new String[4];
		String line = null; 
		EdgeBean bean=null;
		while(true)
		{
		
		try {
			line = reader.readLine();
		} catch (IOException e) {
			
			e.printStackTrace();
		}
		
		
		if(line == null) //if reached to the end of file
			return next;
		
		arr=line.split(" "); 
	
		
		if(next==null)
		{
			 next=new EdgeBean();
		     next.setSrc(Long.parseLong(arr[1]));
		     next.addEdge(new Edge(Long.parseLong(arr[0]),Long.parseLong(arr[2]),Float.parseFloat(arr[3]),Integer.parseInt(arr[4])));
		}
		else if(next.getSrc()==Long.parseLong(arr[1]))
		{	
		  next.addEdge(new Edge(Long.parseLong(arr[0]),Long.parseLong(arr[2]),Float.parseFloat(arr[3]),Integer.parseInt(arr[4])));
		}
		else
		{
			bean=next;
			 next=new EdgeBean();
		     next.setSrc(Long.parseLong(arr[1]));
		     next.addEdge(new Edge(Long.parseLong(arr[0]),Long.parseLong(arr[2]),Float.parseFloat(arr[3]),Integer.parseInt(arr[4])));
		     break;
		}
		//split line by space and extract edge_id,src_id,dst_id,distance
		}
		return bean;
	}
	
	public static BufferedReader initializeDataReader(String data_file_path)
	{
		BufferedReader reader=null;
		try {
			reader= new BufferedReader(new FileReader(data_file_path));
		} catch (FileNotFoundException e) {
			
			e.printStackTrace();
		}
	    return reader;
	}

	public static ArrayList<Long> computeSize(ArrayList<DBCollection> collection)
	{
		ArrayList<Long> collection_size=new ArrayList<Long>();
		//iterate all the collection
		for(DBCollection temp:collection) 
			collection_size.add(temp.count());
		return collection_size;
		
	}
	
}
