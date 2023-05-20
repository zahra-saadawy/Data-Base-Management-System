package main.java;

import java.io.Serializable;
import java.text.ParseException;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Date;

public class OctTreeIndex<T> implements Serializable {
	int n;
	Node root;

	public OctTreeIndex(Object minX, Object minY, Object minZ, Object maxX,
			Object maxY, Object maxZ, int n) {
		OctPoint min = new OctPoint(minX, minY, minZ, null);
		OctPoint max = new OctPoint(maxX, maxY, maxZ, null);
		root = new Node(min, max);
		this.n = n;
	}

	public void insert(Object x, Object y, Object z, T object)
			throws DBAppException {
		OctPoint<T> a = new OctPoint<T>(x, y, z, object);
		inserthelper(a, root, n);

	}

	public boolean checkExistsPoint(OctPoint<T> point, Node<T> node) {
		if (compareToo(point.x, node.minXYZ.x) >= 0
				&& compareToo(point.x, node.maxXYZ.x) <= 0
				&& compareToo(point.y, node.minXYZ.y) >= 0
				&& compareToo(point.y, node.maxXYZ.y) <= 0
				&& compareToo(point.z, node.minXYZ.z) >= 0
				&& compareToo(point.z, node.maxXYZ.z) <= 0) {
			Object midx = middle(node.minXYZ.x, node.maxXYZ.x);
			Object midy = middle(node.minXYZ.y, node.maxXYZ.y);
			Object midz = middle(node.minXYZ.z, node.maxXYZ.z);
			if (node.children == null) {
				for (int i = 0; i < node.points.size(); i++) {
					if (compareToo(node.points.get(i).x, point.x) == 0
							&& compareToo(node.points.get(i).y, point.y) == 0
							&& compareToo(node.points.get(i).z, point.z) == 0)
						return true;
				}
				return false;
			} else {
				int pos;
				if (compareToo(point.x, midx) <= 0) {
					if (compareToo(point.y, midy) <= 0) {
						if (compareToo(point.z, midz) <= 0) {
							pos = OctTreeLocations.TopLeftFront.getNumber();
						} else {
							pos = OctTreeLocations.TopLeftBack.getNumber();
						}
					} else if (compareToo(point.z, midz) <= 0) {
						pos = OctTreeLocations.BottomLeftFront.getNumber();
					} else {
						pos = OctTreeLocations.BottomLeftBack.getNumber();
					}
				} else if (compareToo(point.y, midy) <= 0) {
					if (compareToo(point.z, midz) <= 0) {
						pos = OctTreeLocations.TopRightFront.getNumber();
					} else {
						pos = OctTreeLocations.TopRightBack.getNumber();
					}
				} else if (compareToo(point.z, midz) <= 0) {
					pos = OctTreeLocations.BottomRightFront.getNumber();
				} else {
					pos = OctTreeLocations.BottomRightBack.getNumber();
				}

				return checkExistsPoint(point, node.children[pos].root);
			}
		}
		return false;
	}

	public void inserthelper(OctPoint<T> point, Node<T> node, int n)
			throws DBAppException {

		if (compareToo(point.x, node.minXYZ.x) < 0
				|| compareToo(point.x, node.maxXYZ.x) > 0
				|| compareToo(point.y, node.minXYZ.y) < 0
				|| compareToo(point.y, node.maxXYZ.y) > 0
				|| compareToo(point.z, node.minXYZ.z) < 0
				|| compareToo(point.z, node.maxXYZ.z) > 0) {
			System.out.println(point.x + " " + node.minXYZ.x + " / " + point.y
					+ " " + node.minXYZ.y + " / " + point.z + " "
					+ node.minXYZ.z + " / ");
			System.out.println(point.x + " " + node.maxXYZ.x + " / " + point.y
					+ " " + node.maxXYZ.y + " / " + point.z + " "
					+ node.maxXYZ.z + " / ");
			throw new DBAppException("Insertion point is out of bounds! X: "
					+ point.x + " Y: " + point.y + " Z: " + point.z
					+ " Object Name: " + point.object.getClass().getName());
		}

		if (node.children == null)// in case workin on root
		{
			if (node.points.size() < n) {// kant eh as8r mn el n
				if (checkExistsPoint(point, node)) {
					for (OctPoint<T> p : node.points) {
						if ((compareToo(p.x, point.x) == 0
								&& compareToo(p.y, point.y) == 0 && compareToo(
									p.z, point.z) == 0))
							p.duplicates.add(point);

					}
				} else
					node.points.add(point);
			} else if (checkExistsPoint(point, node)) {
				for (OctPoint<T> p : node.points) {
					if ((compareToo(p.x, point.x) == 0
							&& compareToo(p.y, point.y) == 0 && compareToo(p.z,
								point.z) == 0))
						p.duplicates.add(point);

				}
			} else {
				ArrayList<OctPoint<T>> temp = new ArrayList<OctPoint<T>>();
				for (OctPoint<T> p : node.points)
					temp.add(p);

				node.points = null;

				Object midx = middle(node.minXYZ.x, node.maxXYZ.x);
				Object midy = middle(node.minXYZ.y, node.maxXYZ.y);
				Object midz = middle(node.minXYZ.z, node.maxXYZ.z);

				node.children = new OctTreeIndex[8];

				node.children[0] = new OctTreeIndex<>(node.minXYZ.x,
						node.minXYZ.y, node.minXYZ.z, midx, midy, midz, n);

				node.children[1] = new OctTreeIndex<>(midx,
						node.minXYZ.y, node.minXYZ.z, node.maxXYZ.x, midy,
						midz, n);

				node.children[2] = new OctTreeIndex<>(midx,
						midy, node.minXYZ.z, node.maxXYZ.x,
						node.maxXYZ.y, midz, n);

				node.children[3] = new OctTreeIndex<>(node.minXYZ.x,
						midy, node.minXYZ.z, midx, node.maxXYZ.y, midz,
						n);

				node.children[4] = new OctTreeIndex<>(node.minXYZ.x,
						node.minXYZ.y, midz, midx, midy, node.maxXYZ.z,
						n);

				node.children[5] = new OctTreeIndex<>(midx,
						node.minXYZ.y, midz, node.maxXYZ.x, midy,
						node.maxXYZ.z, n);

				node.children[6] = new OctTreeIndex<>(midx,
						midy, midz, node.maxXYZ.x,
						node.maxXYZ.y, node.maxXYZ.z, n);

				node.children[7] = new OctTreeIndex<>(node.minXYZ.x,
						midy, midz, midx, node.maxXYZ.y,
						node.maxXYZ.z, n);

				for (OctPoint<T> p : temp)
					inserthelper(p, root, n);

				inserthelper(point, root, n);
			}
		} else {
		
			Object midx = middle(node.minXYZ.x, node.maxXYZ.x);
			Object midy = middle(node.minXYZ.y, node.maxXYZ.y);
			Object midz = middle(node.minXYZ.z, node.maxXYZ.z);

			int pos;

			if (compareToo(point.x, midx) <= 0) {
				if (compareToo(point.y, midy) <= 0) {
					if (compareToo(point.z, midz) <= 0)
						pos = OctTreeLocations.TopLeftFront.getNumber();
					else
						pos = OctTreeLocations.TopLeftBack.getNumber();
				} else {
					if (compareToo(point.z, midz) <= 0)
						pos = OctTreeLocations.BottomLeftFront.getNumber();
					else
						pos = OctTreeLocations.BottomLeftBack.getNumber();
				}
			} else {
				if (compareToo(point.y, midy) <= 0) {
					if (compareToo(point.z, midz) <= 0)
						pos = OctTreeLocations.TopRightFront.getNumber();
					else
						pos = OctTreeLocations.TopRightBack.getNumber();
				} else {
					if (compareToo(point.z, midz) <= 0)
						pos = OctTreeLocations.BottomRightFront.getNumber();
					else
						pos = OctTreeLocations.BottomRightBack.getNumber();
				}
			}
	

			inserthelper(point, node.children[pos].root, n);
		}

	}

	public boolean remove(OctPoint<T> point, Node<T> node)
			throws DBAppException {

		if (compareToo(point.x, node.minXYZ.x) < 0
				|| compareToo(point.x, node.maxXYZ.x) > 0
				|| compareToo(point.y, node.minXYZ.y) < 0
				|| compareToo(point.y, node.maxXYZ.y) > 0
				|| compareToo(point.z, node.minXYZ.z) < 0
				|| compareToo(point.z, node.maxXYZ.z) > 0)

			return false;

		if (node.children == null) {
			for (OctPoint<T> p : node.points){
				if (compareToo(p.x, point.x) == 0
						&& compareToo(p.y, point.y) == 0
						&& compareToo(p.z, point.z) == 0&&compareToo(p.object, point.object)==0) {
					if (p.duplicates.size() != 0)// nshil mn duplicates lw feh
					{  OctPoint<T> t=null;
						for (OctPoint<T> po :p.duplicates)
							if (compareToo(po.x, point.x) == 0
									&& compareToo(po.y, point.y) == 0
									&& compareToo(po.z, point.z) == 0&&compareToo(po.object, point.object)==0){
								t=po;}
						if(t!=null)
								 {((OctPoint<T>) p).duplicates.remove(t);
					    	return true;}
					}
					node.points.remove(p);
					return true;
				}}
			return false;}
		

		//lw lsa bdwr ana fen fe eltree
		else  {
			int pos = findChildPosition(point, node);
		    return remove(point, node.children[pos].root);
		
		
		
		}

	}

	public void print() {
		if (root.children == null) {
			if (root.points.size() == 0)
				System.out.println("(0,0,0)");
			for (int i = 0; i < root.points.size(); i++) {
				System.out.println("Point: ("
						+ ((OctPoint<T>) root.points.get(i)).x + ", "
						+ ((OctPoint<T>) root.points.get(i)).y + ", "
						+ ((OctPoint<T>) root.points.get(i)).z + ","
						+ ((OctPoint<T>) root.points.get(i)).object + ")");
				for (int j = 0; j < ((OctPoint) root.points.get(i)).duplicates
						.size(); j++)
					System.out.println("Point: ("
							+ ((OctPoint<T>) root.points.get(i)).duplicates
									.get(j).x
							+ ", "
							+ ((OctPoint<T>) root.points.get(i)).duplicates
									.get(j).y
							+ ", "
							+ ((OctPoint<T>) root.points.get(i)).duplicates
									.get(j).z
							+ ","
							+ ((OctPoint<T>) root.points.get(i)).duplicates
									.get(j).object + ")");

			}
		} else {

			// System.out.println("Bounds: (" + root.minXYZ.x + ", " +
			// root.minXYZ.y + ", "
			// +root. minXYZ.z + ") - (" + root.maxXYZ.x + ", " +root. maxXYZ.y
			// + ", "
			// + root.maxXYZ.z + ")");
			for (OctTreeIndex<T> child : root.children) {

				child.print();

			}
		}
	}public int findChildPosition(OctPoint<T> point, Node<T> node) {
	    Object midx = middle(node.minXYZ.x, node.maxXYZ.x);
	    Object midy = middle(node.minXYZ.y, node.maxXYZ.y);
	    Object midz = middle(node.minXYZ.z, node.maxXYZ.z);
	    
	    if (compareToo(point.x, midx) <= 0) {
	        if (compareToo(point.y, midy) <= 0) {
	            if (compareToo(point.z, midz) <= 0) {
	                return OctTreeLocations.TopLeftFront.getNumber();
	            } else {
	                return OctTreeLocations.TopLeftBack.getNumber();
	            }
	        } else if (compareToo(point.z, midz) <= 0) {
	            return OctTreeLocations.BottomLeftFront.getNumber();
	        } else {
	            return OctTreeLocations.BottomLeftBack.getNumber();
	        }
	    } else if (compareToo(point.y, midy) <= 0) {
	        if (compareToo(point.z, midz) <= 0) {
	            return OctTreeLocations.TopRightFront.getNumber();
	        } else {
	            return OctTreeLocations.TopRightBack.getNumber();
	        }
	    } else if (compareToo(point.z, midz) <= 0) {
	        return OctTreeLocations.BottomRightFront.getNumber();
	    } else {
	        return OctTreeLocations.BottomRightBack.getNumber();
	    }
	}

	// public boolean isEmpty() {
	// if (root.point != null) {
	// return false;
	// }
	// for (int i = 0; i <= 7; i++) {
	// if (!root.children[i].isEmpty()) {
	// return false;
	// }
	// }
	// return true;
	// }

	static String middleString(String S, String T, int N) {
		// Stores the base 26 digits after addition
		int sLength = S.length();
		int tLength = T.length();
		if (sLength > tLength) {
			T += S.substring(tLength);
		} else if (sLength < tLength) {
			S += T.substring(sLength);
		}
		N = S.length();
		int[] a1 = new int[N + 1];

		for (int i = 0; i < N; i++) {
			a1[i + 1] = (int) S.charAt(i) - 97 + (int) T.charAt(i) - 97;
		}

		// Iterate from right to left
		// and add carry to next position
		for (int i = N; i >= 1; i--) {
			a1[i - 1] += (int) a1[i] / 26;
			a1[i] %= 26;
		}

		// Reduce the number to find the middle
		// string by dividing each position by 2
		for (int i = 0; i <= N; i++) {

			// If current value is odd,
			// carry 26 to the next index value
			if ((a1[i] & 1) != 0) {

				if (i + 1 <= N) {
					a1[i + 1] += 26;
				}
			}

			a1[i] = (int) a1[i] / 2;
		}
		String result = "";
		for (int i = 1; i <= N; i++) {
			result += ((char) (a1[i] + 97));
		}
		return result;
	}

	public static String checkType(Object key) throws ParseException {

		String type = key.getClass().toString();
		// System.out.println(primaryKey);

		String typeString = "";
		if (type.substring(16).equals("Integer")) {
			typeString = "java.lang.Integer";
		}
		if (type.substring(16).equals("Double")) {
			typeString = "java.lang.Double";
		}

		if (type.substring(16).equals("String")) {
			typeString = "java.lang.String";
		}
		if (type.substring(16).equals("Date")) {
			typeString = "java.util.Date";
		}
		return typeString;
	}

	public static int compareToo(Object o1, Object o2) {
		String dataType;
		try {
			dataType = checkType(o1);
			// System.out.println(dataType);

			switch (dataType) {
			case "java.lang.Integer":

				return (((Integer) o1).compareTo((Integer) o2));
			case "java.lang.Double":
				return ((Double) o1).compareTo((Double) o2);
			case "java.lang.String":
				return ((String) o1).compareTo((String) o2);
			case "java.util.Date":
				return ((Date) o1).compareTo((Date) o2);

			}
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return 0;
	}

	public static Object middle(Object o1, Object o2) {
		String dataType;
		try {
			dataType = checkType(o1);
			// System.out.println(dataType);

			switch (dataType) {
			case "java.lang.Integer":
				return (((Integer) o1) + ((Integer) o2)) / 2;
			case "java.lang.Double":
				return (((Double) o1) + ((Double) o2)) / 2;
			case "java.lang.String":
				return middleString((String) o1, (String) o2,
						((String) o1).length());
			case "java.util.Date":
				LocalDate localDateo1 = ((Date) o1).toInstant()
						.atZone(ZoneId.systemDefault()).toLocalDate();
				LocalDate localDateo2 = ((Date) o2).toInstant()
						.atZone(ZoneId.systemDefault()).toLocalDate();
				long daysBetween = ChronoUnit.DAYS.between(localDateo1,
						localDateo2);
				LocalDate midpoint = (localDateo1).plusDays(daysBetween / 2);
				Date date = Date.from(midpoint.atStartOfDay(
						ZoneId.systemDefault()).toInstant());
				return (date);

			}
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return 0;
	}

	public static Object addOne(Object o1) {
		String dataType;
		try {
			dataType = checkType(o1);
			// System.out.println(dataType);

			switch (dataType) {
			case "java.lang.Integer":
				return (((Integer) o1) + 1);
			case "java.lang.Double":
				return ((Double) o1) + 0.1;
			case "java.lang.String":
				return stringPlusOne(((String) o1), ((String) o1).length());
			case "java.util.Date":
				LocalDate localDateo1 = ((Date) o1).toInstant()
						.atZone(ZoneId.systemDefault()).toLocalDate();
				LocalDate addDay = (localDateo1).plusDays(1);
				Date date = Date.from(addDay.atStartOfDay(
						ZoneId.systemDefault()).toInstant());
				return (date);

			}
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return 0;

	}

	static String stringPlusOne(String S, int N) {
		// Stores the base 26 digits after addition
		int[] a1 = new int[N + 1];

		for (int i = 0; i < N; i++) {
			a1[i + 1] = (int) S.charAt(i) - 97;
		}

		String result = "";
		a1[a1.length - 1]++;
		if (a1[a1.length - 1] > 25) {
			a1[a1.length - 1] = 25;

		}
		for (int i = 1; i <= N; i++) {
			result += ((char) (a1[i] + 97));
		}
		return result;
	}

	public static void main(String[] args) throws DBAppException {
		//
		OctTreeIndex<Object> tree = new OctTreeIndex<Object>(0, 0, 0, 16, 16,
				16, 1);
		tree.insert(1, 9, 1, "test ");
		
		tree.insert(8, 8, 9, "test ");
		//System.out.print(((OctPoint)(tree.root.children[0].root.children[0].root.points.get(0))).x);
	   //tree.insert(9, 9, 9, "test " );
//		tree.insert(9, 9, 9, "test ", 1);
//
//		// tree.insert(15, "bolty", 1.0, "test " + 4);
//		OctPoint P=new OctPoint(8,8,8,"test ");
//	    tree.remove(P,tree.root);
//	    tree.remove(P,tree.root);
//	    OctPoint P2=new OctPoint(1,1,1,"test ");
//	    tree.remove(P2,tree.root);
//	    P2=new OctPoint(10, 2, 10, "test ");
//	    tree.remove(P2,tree.root);
		// tree.insert(15, "bolty", 1.0, "test " + 5);
		// tree.insert(7, "orange", 2.7, "test " + 4);

		tree.print();
		// tree.insert(9, "f", 9, "test " + 1 + 5 + 1);
		// tree.insert(9,"x" ,9 , "test " + 1 + 5 + 1);
		// tree.insert(7,7 , , "test " + 1 + 5 + 1);
		// for(int x = 0; x < 3; x++){
		// for(int y = 0; y < 3; y++){
		// for(int z = 0; z < 3; z++){
		// tree.insert(x, y, z, "test " + x + y + z);
		// }
		// }
		//
		//
		// [1, 4.0, tofa7a]
		// [10, 2.0, samaka]
		// [13, 3.5, boat]
		// [15, 1.0, bolty]

		//
		//
		// }
		// Object midx = middle(tree.minXYZ.x, tree.maxXYZ.x);
		// Object midy = middle(tree.minXYZ.y,tree. maxXYZ.y);
		// Object midz = middle(tree.minXYZ.z, tree.maxXYZ.z);

		// System.out.print(midx+" "+midy+" "+midz);
		// System.out.println((char)(('a'+'l')/2));
		// middleString("asgg", "lm", 2);
		// System.out.println(compareToo("zz","zz"));

	}
}
