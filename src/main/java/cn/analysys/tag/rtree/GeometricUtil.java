package cn.analysys.tag.rtree;

import com.github.davidmoten.rtree.geometry.Geometries;
import com.github.davidmoten.rtree.geometry.Rectangle;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class GeometricUtil implements Serializable {

    public  static Polygon string2Polygon(String pointStr) {
        // TODO 字符串转成多边形
        List<Point> polygonVertexList = string2List(pointStr);
        double minLon = polygonVertexList.get(1).getDoubleCoordinate(0);
        double maxLon = polygonVertexList.get(1).getDoubleCoordinate(0);
        double minLat = polygonVertexList.get(1).getDoubleCoordinate(1);
        double maxLat = polygonVertexList.get(1).getDoubleCoordinate(1);
        for(int i=1;i<polygonVertexList.size();i++){
            minLon = Math.min(minLon,polygonVertexList.get(i).getDoubleCoordinate(0));
            maxLon = Math.max(maxLon,polygonVertexList.get(i).getDoubleCoordinate(0));
            minLat = Math.min(minLat,polygonVertexList.get(i).getDoubleCoordinate(1));
            maxLat = Math.max(maxLat,polygonVertexList.get(i).getDoubleCoordinate(1));
        }
        Rectangle rectangle = Geometries.rectangle(minLon, minLat, maxLon, maxLat);
        Polygon polygon = new Polygon();
        polygon.setPointList(polygonVertexList);
        polygon.setLowHigh(minLon, minLat, maxLon, maxLat);
        polygon.setKey(minLon+","+minLat+";"+maxLon+","+maxLat);
        return polygon.setRectangle(rectangle);
    }

    public static List string2List(String pointStr){
        List<Point> polygonVertexList = new ArrayList<>();
        List<double[]> polygonVertexListDouble = new ArrayList<>();
        List<String> polygonVertexListStr = new ArrayList<>(Arrays.asList(pointStr.split(";")));

        for(int i =0;i<polygonVertexListStr.size();i++){
            double[] vertexD = new double[2];
            String[] vertexStr = polygonVertexListStr.get(i).split(",");
            for(int j=0;j<vertexStr.length;j++){
                vertexD[j] = Double.valueOf(vertexStr[j]);
            }
            polygonVertexListDouble.add(vertexD);
            polygonVertexList.add(new Point(vertexD));
        }
        Polygon polygon = new Polygon();
        polygon.setPointListDouble(polygonVertexListDouble);
        return polygonVertexList;
    }

}
