import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Properties;


public class WriteProperties {
	
	 public static void main(String[] args) {
		 
			Properties prop = new Properties();
			OutputStream output = null;
			
			String filePath = new File("").getAbsolutePath();
			System.out.println("Absolute path of the project: ");
		 
			try {
		 
				output = new FileOutputStream("config.properties");
		 
				// set the properties value
				prop.setProperty("node_file_location", filePath+"/DataSet/cal.cnode");
				prop.setProperty("edge_file_location",  filePath+"/DataSet/cal.cedge");
				prop.setProperty("ipaddress", getIPAddress());
				
				//these are the hazelcast details
				prop.setProperty("hazelcast_host1", "192.168.18.30:5701");
				prop.setProperty("hazelcast_host2", "192.168.18.30:5702");
				prop.setProperty("hazelcast_host3", "192.168.18.30:5703");
				prop.setProperty("hazelcast_cluster_name", "graph");
				prop.setProperty("hazelcast_cluster_pwd", "graph");
							
				prop.setProperty("mysql_host1", "192.168.18.30:3306");
				prop.setProperty("mysql_host2", "localhost:3306");
				
				// save properties to project root folder
				prop.store(output, null);
		 
			} catch (IOException io) {
				io.printStackTrace();
			} finally {
				if (output != null) {
					try {
						output.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
		 
			}
		  }
	 
	 private static String getIPAddress()
	 {

			Socket s = null;
			String ipaddress=null;
			try {
				s = new Socket("172.31.4.6", 80);
				ipaddress=s.getLocalAddress().getHostAddress();
				System.out.println("IP Address of the machine is: "+ipaddress);
				s.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		return ipaddress;
	 }
	 

}
