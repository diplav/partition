package graph.dataimport;

import java.util.ArrayList;

public class EdgeBean {
	
	private long src;
	private ArrayList<Edge> edges= new ArrayList<Edge>();
	
	public long getSrc() {
		return src;
	}
	public void setSrc(long src) {
		this.src = src;
	}
	public ArrayList<Edge> getEdges() {
		return edges;
	}
	public void setEdges(ArrayList<Edge> edges) {
		this.edges = edges;
	} 
	public void addEdge(Edge e)
	{
		edges.add(e);
	}
	
	public void removeEdge(Edge e)
	{
		edges.remove(e);
	}
	

}
