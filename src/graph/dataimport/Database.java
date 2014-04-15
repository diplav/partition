package graph.dataimport;

import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;

public class Database {
	static HashMap <String, MongoClient> inventory = new HashMap<>();
	
	
	public static  MongoClient connectMongoDB(String host,int IP)
	{
		 MongoClient mongoClient=null;
		 
		if(host.equals("127.0.0.1"))
			host="localhost";
		
		if(inventory.containsKey(host+IP))
			return inventory.get(host+IP);
	
		try {
			 mongoClient = new MongoClient( host , IP);
			 inventory.put(host+IP, mongoClient);
			 System.out.println("New Database Connection added in the inventory");
			 return mongoClient;
		} catch (UnknownHostException e) {
			
			e.printStackTrace();
		}
		
		return null;
	}
	public static  MongoClient connectMongoDB()
	{
		if(inventory.containsKey("localhost27017"))
			return inventory.get("localhost27017");
		
		 return connectMongoDB("localhost",27017);
	}
	
	
	public static DB getDB(String db_name,MongoClient mongo)
	{
		return mongo.getDB(db_name);
	}
	
	public static DBCollection getCollection(String collection_name,DB db)
	{
		
		DBCollection collection;
		if (db.collectionExists(collection_name)) {
		        collection = db.getCollection(collection_name);
		    } else {
		      //  DBObject options = BasicDBObjectBuilder.start().add("capped", true).add("size", 2000000000l).get();
		        collection = db.createCollection(collection_name, null);
		    }
	return collection;
	}
	
	public static  void refreshSchema(String collection_name,DB db)
	{
		db.getCollection(collection_name).drop();
	}
	
	
	
	/*
	 * Connect to the MySQL database
	 */
	public static Connection connect(){
		 Connection conn=null;
	    try {
	      Class.forName("com.mysql.jdbc.Driver").newInstance();
	      System.out.println("test");
	      conn = DriverManager.getConnection("jdbc:mysql://192.168.18.109:3306/graph","diplav","diplav");
	      System.out.println("Connected to mysql database");


	    } catch (SQLException ex) {
	      // handle any errors
	      System.out.println("SQLException: " + ex.getMessage());
	      System.out.println("SQLState: "  + ex.getSQLState());
	      System.out.println("VendorError: " +  ex.getErrorCode());
	    }catch(Exception e){e.printStackTrace();}   
	  
	return conn;
	}
}


