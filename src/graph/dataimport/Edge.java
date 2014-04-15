package graph.dataimport;

public class Edge {
	private long edgeid;
	private long dst;
	private float distance;
	private int speed;
	
	
	public int getSpeed() {
		return speed;
	}
	public void setSpeed(int speed) {
		this.speed = speed;
	}
	public Edge(long edgeid,long dst,float distance,int speed)
	{
		this.edgeid=edgeid;
		this.dst=dst;
		this.distance=distance;
		this.speed=speed;
	}
	public long getEdgeid() {
		return edgeid;
	}
	public void setEdgeid(long edgeid) {
		this.edgeid = edgeid;
	}
	public long getDst() {
		return dst;
	}
	public void setDst(long dst) {
		this.dst = dst;
	}
	public float getDistance() {
		return distance;
	}
	public void setDistance(float distance) {
		this.distance = distance;
	}
	
	


}
