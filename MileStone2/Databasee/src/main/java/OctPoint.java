package main.java;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Date;

public class OctPoint<T> implements Serializable {
	Object x;
	Object y;
	Object z;
	T object;
	ArrayList<OctPoint>duplicates=new ArrayList<OctPoint>();
	//boolean noParameters = false;

	public OctPoint(Object minX, Object minY, Object minZ,T object) {
		this.x = minX;
		this.y = minY;
		this.z = minZ;
		this.object=object;
	}

//	public OctPoint() {
//		noParameters = true;
//	}
	static String stringPlusOne(String S, int N)
    {  
		
	
        // Stores the base 26 digits after addition
        int[] a1 = new int[N + 1];
 
        for (int i = 0; i < N; i++) {
            a1[i + 1] = (int)S.charAt(i) - 97;
        }

        String result = "";
        a1[a1.length-1]++;
        if (a1[a1.length-1] > 25){
        	a1[a1.length-1] = 25;
        }
        for (int i = 1; i <= N; i++) {
           result +=((char)(a1[i] + 97));
        }
        return result;
    }
	
	public static void main(String[] args) throws ParseException{
//		Date o1 = (new SimpleDateFormat("yyyy-MM-dd").parse("2020-12-1"));
//		//o1.
////		Date o2 = (new SimpleDateFormat("yyyy-MM-dd").parse("2020-1-1"));
//		LocalDate localDateo1 = ((Date) o1).toInstant()
//				.atZone(ZoneId.systemDefault()).toLocalDate();
//		LocalDate addDay = (localDateo1).plusDays(1);
//		Date date = Date.from(addDay.atStartOfDay(
//				ZoneId.systemDefault()).toInstant());
//		
//		LocalDate localDateo1 = ((Date) o1).toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
//		LocalDate localDateo2 = ((Date) o2).toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
//		 long daysBetween = ChronoUnit.DAYS.between(localDateo1, localDateo2);
//	        LocalDate midpoint = (localDateo1).plusDays(arg0)(daysBetween / 2);
//	        Date date = Date.from(midpoint.atStartOfDay(ZoneId.systemDefault()).toInstant());
//	        
//	      System.out.println(date);
//	      System.out.println(o1);
//	      System.out.println(o2);
		System.out.println("bolty".compareTo("d"));
//		System.out.println(stringPlusOne("zzc",3));
//		 OctPoint<String> a= new OctPoint <String>(0,0,0,"1");
//		    a.duplicates.add(new OctPoint(a.x,a.y,a.z,a.object));
//		    System.out.print(a.duplicates.get(0).x);

	}
//	public boolean remove(Object x, Object y, Object z) throws DBAppException {
//	if (compareToo(x, minXYZ.x) < 0 || compareToo(x, maxXYZ.x) > 0
//			|| compareToo(y, minXYZ.y) < 0 || compareToo(y, maxXYZ.y) > 0
//			|| compareToo(z, minXYZ.z) < 0 || compareToo(z, maxXYZ.z) > 0)
//		return false;
//	Object midx = middle(minXYZ.x, maxXYZ.x);
//	Object midy = middle(minXYZ.y, maxXYZ.y);
//	Object midz = middle(minXYZ.z, maxXYZ.z);
//
//	int pos;
//
//	if (compareToo(x, midx) <= 0) {
//		if (compareToo(y, midy) <= 0) {
//			if (compareToo(z, midz) <= 0)
//				pos = OctTreeLocations.TopLeftFront.getNumber();
//			else
//				pos = OctTreeLocations.TopLeftBack.getNumber();
//		} else {
//			if (compareToo(z, midz) <= 0)
//				pos = OctTreeLocations.BottomLeftFront.getNumber();
//			else
//				pos = OctTreeLocations.BottomLeftBack.getNumber();
//		}
//	} else {
//		if (compareToo(y, midy) <= 0) {
//			if (compareToo(z, midz) <= 0)
//				pos = OctTreeLocations.TopRightFront.getNumber();
//			else
//				pos = OctTreeLocations.TopRightBack.getNumber();
//		} else {
//			if (compareToo(z, midz) <= 0)
//				pos = OctTreeLocations.BottomRightFront.getNumber();
//			else
//				pos = OctTreeLocations.BottomRightBack.getNumber();
//		}
//	}
//	// if root
//	if (root && this.point.noParameters) {
//		return false;
//	} else if (root && !this.point.noParameters) {
//		if (compareToo(this.point.x, x) == 0
//				&& compareToo(this.point.y, y) == 0
//				&& compareToo(this.point.z, z) == 0) {
//			this.point = new OctPoint();
//			this.object = null;
//			return false;
//		} else
//			throw new DBAppException("No Tuple with these values in index");
//	}
//	if (children[pos].point == null)
//		return children[pos].remove(x, y, z);
//	if (children[pos].point.noParameters)
//		return false;
//	if (compareToo(children[pos].point.x, x) == 0
//			&& compareToo(children[pos].point.y, y) == 0
//			&& compareToo(children[pos].point.z, z) == 0) {
//		children[pos] = new OctTreeIndex<>();
//	}
//	return false;
//}
//

}
