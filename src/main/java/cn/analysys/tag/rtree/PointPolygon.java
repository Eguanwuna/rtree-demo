package cn.analysys.tag.rtree;

import com.github.davidmoten.rtree.Entry;
import com.github.davidmoten.rtree.RTree;
import com.github.davidmoten.rtree.geometry.Geometries;
import com.github.davidmoten.rtree.geometry.Geometry;
import rx.Observable;

import java.util.*;

public class PointPolygon {
    static Map<String, Map<String,String>> polygonListMap = new HashMap<>();
    public static void main(String[] args){
        Map<String,String> polygonMapA = new HashMap<>();
        polygonMapA.put("A1","1,5-5,5-6,6-5,7-2,7-1,6-1,5");
        polygonMapA.put("A2","1,2-2,1-3,1-4,2-4,3-3,4-2,4-1,3-1,2");
        polygonMapA.put("A3","1,0-7,0-7,5-1,5-1,0");
        polygonListMap.put("A",polygonMapA);
        Map<String,String> polygonMapB = new HashMap<>();
        polygonMapB.put("B1","10,3-13,3-13,6-10,6-10,3");
        polygonMapB.put("B2","9,0-12,0-12,3-9,3-9,0");
        polygonMapB.put("B3","13,0-15,0-15,5-13,5-13,0");
        polygonListMap.put("B",polygonMapB);
        double[] p = {12D,3D};//被查询的点
        for(String key:polygonListMap.keySet()){//遍历国家，对每个国家执行点查询
            List<Map<String,Integer>> resultCity = ifIn(p[0],p[1],key);
            System.out.println(resultCity);
        }
    }

    /**
     * 传入多个围栏，判断该点再哪个围栏当中
     */
    //确定点所落的区域
    public static List<Map<String,Integer>> ifIn(Double longitude, Double latitude,String key){
        Map<String,String> polygonMap =  polygonListMap.get(key);
        List<Map<String,Integer>> results = new ArrayList<>();

        //TODO 1.根据(市）围栏建立RTree
         RTree<String, Geometry> newTree = RTree.star().create();//初始化一棵树
        for(String k:polygonMap.keySet()){//遍历每一个（市）围栏
            //ToDo 1.1.获取围栏字符串
            String polygonStr = polygonMap.get(k);
            //TODO 1.2.获取围栏的最小矩形
            List<Double> mbrLowHigh=new MinimumBoundaryRectangle().MinimumBoundaryRectangleLowHigh(polygonStr);
            Double maxLongitude = mbrLowHigh.get(0);
            Double maxLatitude = mbrLowHigh.get(1);
            Double minLongitude = mbrLowHigh.get(2);
            Double minLatitude = mbrLowHigh.get(3);
            //TODO 1.3.将矩形添加到RTree中
            newTree = newTree.add(k, Geometries.rectangle(maxLongitude, maxLatitude, minLongitude, minLatitude));
        }

        //TODO 2.根据输入的点坐标执行查询操作
        Map<String,Integer> result = new HashMap<>();
        //TODO 2.1 找出点落在那些矩形中
        double[] point = {longitude,latitude};

        Observable<Entry<String, Geometry>> resultRec = newTree.search(Geometries.point(longitude,latitude));

        Iterable<List<Entry<String, Geometry>>> iterable = resultRec.toList().toBlocking().toIterable();
        Iterator<List<Entry<String, Geometry>>> it =iterable.iterator();
        while(it.hasNext()) {
            List<Entry<String, Geometry>> list = it.next();
            //TODO 2.2 判断点是否在围栏内
            for(Entry<String, Geometry> entry:list) {//遍历点所落在的矩形
                List<Point> pts = new GeometricUtil().string2List(polygonMap.get(entry.value()));//获取与矩形相对应的围栏
                boolean ifInPolygon = new IsPointInPolygon().isPtInPoly( new Point(point),pts);//判断点是否在围栏内
                if(ifInPolygon){//点在围栏内
                    result.put(entry.value(),1);
                }else{//点在围栏之外
                    result.put(entry.value(),0);
                }
            }
            results.add(result);
        }
        return results;
    }
}
