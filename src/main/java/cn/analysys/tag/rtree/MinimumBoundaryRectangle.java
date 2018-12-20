package cn.analysys.tag.rtree;

import java.util.ArrayList;
import java.util.List;

public class MinimumBoundaryRectangle {
    //TODO 查找围栏的最小外围矩形，返回类型:List{double minLon,double minLat,double maxLon,double maxLat}
    public List<Double> MinimumBoundaryRectangleLowHigh(String polygonStr){
        if(polygonStr.contains("|")){
            polygonStr = polygonStr.replace("|",";");
        }

        List<Double> mbrLowHigh = new ArrayList<Double>();
        String[] polygonVertex = polygonStr.split(";");
        double vertex[][] = new double[polygonVertex.length][2];
        for(int i=0;i<polygonVertex.length;i++){
            String vertexXY[] =polygonVertex[i].split(",");
            vertex[i][0] = Double.valueOf(vertexXY[0]);
            vertex[i][1] = Double.valueOf(vertexXY[1]);
        }
        double maxLon = vertex[0][0];
        double minLon = vertex[0][0];
        double maxLat = vertex[0][1];
        double minLat = vertex[0][1];
        for(int m=0;m<vertex.length;m++){
            maxLon = Math.max(maxLon,vertex[m][0]);
            maxLat = Math.max(maxLat,vertex[m][1]);
            minLon = Math.min(minLon,vertex[m][0]);
            minLat = Math.min(minLat,vertex[m][1]);
        }
        mbrLowHigh.add(minLon);
        mbrLowHigh.add(minLat);
        mbrLowHigh.add(maxLon);
        mbrLowHigh.add(maxLat);
        return mbrLowHigh;
    }
}
