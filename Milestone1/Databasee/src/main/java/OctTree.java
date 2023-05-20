package main.java;

import java.io.Serializable;

public class OctTree<T> implements Serializable {
	String xname;
	String yname;
	String zname;
	OctTreeIndex<T> octTreeIndex ;
	
	public OctTree(String x, String y, String z,Object minX, Object minY, Object minZ, Object maxX,
			Object maxY, Object maxZ,int n) throws DBAppException {
		xname = x;
		yname= y;
		zname = z;
		octTreeIndex = new OctTreeIndex<T>(minX,minY,minZ,maxX,maxY,maxZ,n);
		
	}
}
