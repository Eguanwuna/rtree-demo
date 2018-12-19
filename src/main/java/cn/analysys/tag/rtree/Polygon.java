package cn.analysys.tag.rtree;

import com.github.davidmoten.rtree.geometry.Rectangle;

import java.io.Serializable;
import java.util.List;

public class Polygon implements Serializable {

    private static final long serialVersionUID = -8800566886189916550L;

    private List<Point> pointList;

    private  List<double[]> pointListDouble;

    private Rectangle rectangle;

    double[] VertexRectangle;

    private String key;

    public static long getSerialVersionUID() {
        return serialVersionUID;
    }

    public List<Point> getPointList() {
        return pointList;
    }

    public Polygon setPointList(List<Point> pointList) {
        this.pointList = pointList;
        return this;
    }
    public List<double[]> getPointListDouble(){
        return pointListDouble;
    }

    public List<double[]> setPointListDouble(List<double[]> pointListDouble){
        this.pointListDouble = pointListDouble ;
        return pointListDouble;
    }


    public Rectangle getRectangle() {
        return rectangle;
    }

    public Polygon setRectangle(Rectangle rectangle) {
        this.rectangle = rectangle;
        return this;
    }

    public String getKey() {
        return key;
    }

    public Polygon setKey(String key) {
        this.key = key;
        return this;
    }
    public double[] getLowHigh(){
        return VertexRectangle;
    }
    public double[] setLowHigh(double xL,double yL,double xH,double yH){
        double[] VertexRectangle = {xL,yL,xH,yH};
        this.VertexRectangle=VertexRectangle;
        return VertexRectangle;
    }


}
