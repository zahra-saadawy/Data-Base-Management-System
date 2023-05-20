package main.java;

import java.io.Serializable;
import java.util.ArrayList;

public class Node <T>  implements Serializable {
	ArrayList <OctPoint<T>> points= new ArrayList<OctPoint<T>>();
	OctPoint <T>maxXYZ, minXYZ;
	OctTreeIndex<T>[] children = new OctTreeIndex[8];

	 public Node(OctPoint minPoint, OctPoint maxPoint) {
	        this.minXYZ = minPoint;
	        this.maxXYZ = maxPoint;
	        this.points = new ArrayList<OctPoint<T>>();
	        this.children = null;
	    

}}
