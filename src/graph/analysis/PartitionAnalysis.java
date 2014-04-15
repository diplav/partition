package graph.analysis;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class PartitionAnalysis {
	
	//this collection will be used to store temporary analysis database
		DBCollection collection=null;
    
	//this collection is used to store statistics
		DBCollection stat=null;
		
		public void updateCollection(DBCollection collection)
		{
			this.collection=collection;
		}
		
		public void setStatisticsCollection(DBCollection stat)
		{
			this.stat=stat;
		}
		
		/*
		 * Modifications:
		 * 1. Changes made to only process non-zero scores (14/01/2013)
		 * 2. After stabilizing the partition, there will be a call to function 
		 *    which will stabilize the different partitions based on their sizes (14/01/2013)
		 */
		
	public void stabilizePartition(HashMap<String,DBCollection> coll_map)
	{
		//result: this map is used to track overall score of particular partition
		//HashMap<String,Long> result=new HashMap<String,Long>();
		long total_size=0;
		ArrayList<DBCollection> coll_array = new ArrayList<DBCollection>(coll_map.values());

		//count total records(sum of all the partitions
		for(DBCollection coll:coll_array)
		    total_size+=coll.count();
		
		
		/*This loop will iterate through all the collections i.e. all the partitions in our case pedgedata0/1/2 */
		for(DBCollection coll:coll_array)
		{
			/*This query will get all the rows that is of current collection(i.e. either from 0,1 or 2 partition) 
			 * in descending order of their scores*/
			BasicDBObject query = new BasicDBObject();
			query.put("partition",coll.getName());
			DBCursor cursor = collection.find(query);
			
			//sort cursor in the descending order of score_fraction and the total_edges
			//fraction changed from -1 to 1
			cursor.sort(new BasicDBObject("score_fraction", 1).append("total_edges", -1));
			
	
			int count=0,threshold=(int)((total_size/3)*0.2); //only move 20% of records
			
			 while(cursor.hasNext() && count<threshold) {
			    	DBObject temp_dbo=cursor.next();
			    	//System.out.println("Score: "+temp_dbo.get("score"));
			    	 getMinScore((Long)temp_dbo.get("src"),coll,coll_array);
			    	 
			    	 count++;

			 } //this will end stabilizing of single partition. Now we should stabilize according to the size
			 
			 stablizeSize(coll_array,total_size/coll_array.size());
			
		}
		//System.out.println("Result Summary "+result.get("pedgedata0")+" ==> "+result.get("pedgedata1")+" ==> "+result.get("pedgedata2"));
		
		 
	}
	
	private void stablizeSize(ArrayList<DBCollection> coll_array,long size) {
			//Define some delta for this count values we will treat it 5% of actual partition size
		int delta=(int)(size*0.05); 
		boolean balance=false;
		System.out.println("Stablize Size called: size=> "+size);
		
		while(balance==false)
		{
			balance=true;
			for(DBCollection coll:coll_array)
			{
				if(size-coll.count()>delta) //imbalance found: difference is greater that delta
				{
					System.out.println("Move data called: "+coll.getName() +" Count: "+coll.count());
					moveData(coll, coll_array, size);
					balance=false;
				}
				
			}
			
			if(balance==true) System.out.println("Everything is balanced");
			
		} //while loop
			
	}//stablizeSize
	
	/*
	 * 
	 */
	private void moveData(DBCollection dst_coll,ArrayList<DBCollection> coll_array,long size)
	{
		System.out.println("Move Data Called for Collection: "+dst_coll.getName());

		for(DBCollection coll:coll_array)
		{
			if(dst_coll.count()>=size)             //break if the processing collection is balanced
				break;
			
			if(!(coll.getName().equals(dst_coll.getName())) && coll.count()>size)
			{
				System.out.println("Processing high collection: "+coll.getName() +" Count: "+coll.getCount());
				getCompleteScore(coll_array,true);   //this will recalculate the scores
				
				BasicDBObject query = new BasicDBObject();
				query.put("partition",coll.getName());
				DBCursor cursor = collection.find(query);
				
				//sort cursor in the descending order of score_fraction and the total_edges
				cursor.sort(new BasicDBObject("score_fraction ", -1).append("total_edges", -1));
				
				int i=0;
				System.out.println("Moving Records: ");
				
				 while(cursor.hasNext() && coll.count() > size) {
						DBObject temp_dbo=cursor.next();
					//get the object present in the cursor from exact partition
				    //the object present in the cursor and partition are not same
						
						BasicDBObject qry = new BasicDBObject();
						qry.put("src",temp_dbo.get("src"));
						DBCursor crs = coll.find(qry);
						if(!crs.hasNext())
							continue;
						
						temp_dbo=crs.next();
				    
				    	dst_coll.insert(temp_dbo);         //insert into unbalanced 
				    	coll.remove(temp_dbo);
				    	i++;
				 }
				 System.out.print(i);
				
				
			}
		} //for loop
		
	}
	
	/* 
	 * 1. This will move the record to appropriate partition. 
	 * 2. This function will return name of the partition where the current row will have minimum score and 
	 *    also the actual minimum score separated by ";".
	 */
	
	public String getMinScore(long src,DBCollection coll_src,ArrayList<DBCollection> coll_list)
	{
		int min=0,temp=0;
		String min_partition="";
		
		//The below 4 lines used to fetch the source record(vertex) from the partition.
		BasicDBObject query = new BasicDBObject();
		query.put("src",src);
		DBCursor cursor = coll_src.find(query);
		DBObject obj=cursor.next();                   //source object/vertex
		
		//compute score of this row 
		for(DBCollection coll:coll_list)
		{
			temp=getIndividualScore(obj,coll);
		//	System.out.println("Partition: "+coll.getName()+" Src: "+obj.get("src")+" Score: "+temp);
			if(min==0)
			{
				min=temp;
				min_partition=coll.getName();
				
			}
			else if(temp<min)
			{
				min=temp;
				min_partition=coll.getName();
			}
					
		}
		
		//lets move the row to appropriate partition
		if(!(coll_src.getName().equals(min_partition))) //if the correct partition is not the present one
		{
			for(DBCollection ll:coll_list)
			{
				if(!(min_partition.equals(ll.getName())))
				{
					
					coll_src.remove(obj);     //remove from current partition
					ll.insert(obj);           //add to the appropriate partition
					break;
					
				}
			}
			
		}
		
		return min_partition+";"+min;
		
	}
	
	/*
	 * DBObject src is the vertex and edge set for which we have to find the 
	 * best possible partition
	 * the vertex lies in a partition where it's score is minimum
	 * 
	 * Further Improvement: 
	 * 1. We fire query for edges for every partition we can 
	 *    compute this before calling this functions.(Remark: Imp)
	 */
	
	public int getIndividualScore(DBObject src,DBCollection coll)
	{
		BasicDBList e = (BasicDBList) src.get("edges");
		ArrayList<Long> edge_list=new ArrayList<Long>();
		BasicDBList or = new BasicDBList();
		
		int ret=0;
		
		/*
		 *This loop is used to get all the destinations from the current source(vertex)
		 *Which is stores in the "dst" field of the edges array
		 */
		for(Object obj:e)
    	{
			
    		DBObject dbo=(DBObject) obj;
    		edge_list.add((Long)dbo.get("dst"));
    		
    		//check if the entry of this edge is present in the same collection
    		BasicDBObject query = new BasicDBObject("src", dbo.get("dst"));
    		or.add(query);
    	
    	}
		/*
		 *	How many destination are present in this partition that 
		 *  will be counted as positive score.
		 */
		//fire query to find the positive score
		DBObject query = new BasicDBObject("$or", or);
		DBCursor cur = coll.find(query);
		ret=cur.count();    //get how many of destination vertex is in this partition
		cur.close();
		
		//How many other nodes have this node as a destination. This will count as negative score
		//Find Out the reverse score 
		DBObject queryForElem = new BasicDBObject("edges", new BasicDBObject("$elemMatch", new BasicDBObject("dst",src.get("src"))));
		DBCursor cur1 = coll.find(queryForElem);
		
		//Compute score based on the above two criteria.
		ret+=cur1.count();            //changed: Subtraction to addition
		
	
		return ret;
	}
	
	
	public PartitionAnalysis(DBCollection collection)
	{
		this.collection=collection;
		this.collection.ensureIndex(new BasicDBObject("src",1), new BasicDBObject("unique", true));
	}
	
	/*
	 * Get complete score by combining individual partition score
	 * using function getPartitionScore.
	 */	
	public long getCompleteScore(ArrayList<DBCollection> list,boolean record_data)
	{
		long count=0;
		
		if(record_data==true)
		{
			BasicDBObject dbo=new BasicDBObject();
			collection.remove(dbo);
		}
		Date date=new Date();
		for(DBCollection coll:list)
		{
			long temp_count=getPartitionScore(coll, record_data);
			System.out.println("Partition: "+coll.getName()+" Database Score: "+temp_count+"Records: "+coll.count());
			
			//This will insert statistics into database
			 DBObject doc=new BasicDBObject("timestamp", date).append("partition", coll.getName()).append("score",temp_count); 
		     stat.insert(doc);
		     
		     
			count+=temp_count;
			
			
			
		}
		//this is to store complete score
		DBObject doc=new BasicDBObject("timestamp", date).append("type", 1).append("score", count);
		stat.insert(doc);
		
		return count;
	}
	
/*
 * This function is used to get the partition score/cost.
 * If an referring edge is not in the current partition then the 
 * 1 will be added into score. 
 * The output score will be negative i.e. lower the score good is the partition.	
 */	
 public long getPartitionScore(DBCollection edgedata,boolean record_data)
 {
	 long pscore=0;
	 String partition_name=edgedata.getName();
	 //This will be used to iterate all the nodes in the edgedata collection
	 DBCursor cursor = edgedata.find();
	 
	 try {
	    while(cursor.hasNext()) {
	    	DBObject temp_dbo=cursor.next();
	    	//iterateArrayList of  edge objects 
	    	BasicDBList e = (BasicDBList) temp_dbo.get("edges");
	    	long temp_count=0;
	    	
	    	if(e==null)
	    	   continue;
	    	
	    		for(Object obj:e)
	    		{
	    			DBObject dbo=(DBObject) obj;
	    			//check if the entry of this edge is present in the same collection
	    			BasicDBObject query = new BasicDBObject("src",dbo.get("dst"));
	    			DBCursor cur = edgedata.find(query);
	    			if(cur.count()!=0)
	    				temp_count++;
	    			cur.close();
	    		}
	    	pscore+=temp_count;
	    	
	    	DBObject queryForElem = new BasicDBObject("edges", new BasicDBObject("$elemMatch", new BasicDBObject("dst",temp_dbo.get("src"))));
			DBCursor cur1 = edgedata.find(queryForElem);
			
			pscore+=cur1.count();
	    	
	    	
	    	double sf= (double) temp_count / e.size();
	    	
	    	//If we wish to store collection data
	    	if(record_data==true)	    		
	    		collection.insert(new BasicDBObject("src",temp_dbo.get("src")).append("partition",partition_name).append("total_edges",e.size()).append("score", temp_count).append("score_fraction", sf));
	    	
	    }
	 } finally {
	    cursor.close();
	 }
	 return pscore;
 }

 
}
