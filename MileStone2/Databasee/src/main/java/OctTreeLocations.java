package main.java;

public enum OctTreeLocations {
    TopLeftFront(0),
    TopRightFront(1),
    BottomRightFront(2),
    BottomLeftFront(3),
    TopLeftBack(4),
    TopRightBack(5),
    BottomRightBack(6),
    BottomLeftBack(7);

    int num;

    OctTreeLocations(int num){
        this.num = num;
    }

    int getNumber(){
        return num;
    }
}
