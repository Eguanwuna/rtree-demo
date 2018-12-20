package cn.analysys.tag.rtree;

import com.github.davidmoten.rtree.Entry;
import com.github.davidmoten.rtree.RTree;
import com.github.davidmoten.rtree.geometry.Geometries;
import com.github.davidmoten.rtree.geometry.Geometry;
import rx.Observable;
import java.sql.*;
import java.util.*;
import java.util.logging.Logger;

public class LbsPointInBatchUtil {
    static Map<String, Map<String, String>> polygonListMap = new HashMap<>();

    public static void main(String[] args) {
       /* Map<String, String> polygonMapA = new HashMap<>();
        polygonMapA.put("A1", "1,5;5,5;6,6;5,7;2,7;1,6;1,5");
        polygonMapA.put("A2", "1,2;2,1;3,1;4,2;4,3;3,4;2,4;1,3;1,2");
        polygonMapA.put("A3", "1,0;7,0;7,5;1,5;1,0");
        polygonListMap.put("A", polygonMapA);
        Map<String, String> polygonMapB = new HashMap<>();
        polygonMapB.put("B1", "10,3;13,3;13,6;10,6;10,3|10,6;13,6;13,8;10,8;10,6");
        polygonMapB.put("B2", "9,0;12,0;12,3;9,3;9,0");
        polygonMapB.put("B3", "13,0;15,0;15,5;13,5;13,0");
        polygonListMap.put("B", polygonMapB);
        double[] p = {12D, 3D};//被查询的点
        for (String key : polygonListMap.keySet()) {//遍历国家，对每个国家执行点查询
            Map<String, Integer> resultCity = ifIn(p[0], p[1], polygonListMap.get(key));
            System.out.println(resultCity);
        }*/
        //TODO 数据库连接测试
        Map<String,String> polygonMap = new HashMap<>();
         //final String JDBC_DRIVER = "com.mysql.jdbc.Driver";

        Connection conn = null;
        Statement stat = null;
        try{
            // 注册 JDBC 驱动
            java.sql.DriverManager.registerDriver(new Driver() {
                @Override
                public Connection connect(String url, Properties info) throws SQLException {
                    return null;
                }

                @Override
                public boolean acceptsURL(String url) throws SQLException {
                    return false;
                }

                @Override
                public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
                    return new DriverPropertyInfo[0];
                }

                @Override
                public int getMajorVersion() {
                    return 0;
                }

                @Override
                public int getMinorVersion() {
                    return 0;
                }

                @Override
                public boolean jdbcCompliant() {
                    return false;
                }

                @Override
                public Logger getParentLogger() throws SQLFeatureNotSupportedException {
                    return null;
                }
            });
            // 打开链接
            System.out.println("连接数据库...");
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection("jdbc:mysql://192.168.60.252:3306/analysys_multipolygon","root", "root");
            // 执行查询
            System.out.println(" 实例化Statement对象...");
            stat = conn.createStatement();
            String sql = "SELECT * FROM polygon";
            ResultSet rs = stat.executeQuery(sql);

            // 展开结果集数据库
            while(rs.next()){

                // 通过字段检索
                int id  = rs.getInt("id");
                String key = rs.getString("key");
                String polygon = rs.getString("polygon");
                polygonMap.put(key,polygon);
            }
            // 完成后关闭
            rs.close();
            stat.close();
            conn.close();
        }catch(SQLException se){
            // 处理 JDBC 错误
            se.printStackTrace();
        }catch(Exception e){
            // 处理 Class.forName 错误
            e.printStackTrace();
        }finally{
            // 关闭资源
            try{
                if(stat!=null) stat.close();
            }catch(SQLException se2){
            }// 什么都不做
            try{
                if(conn!=null) conn.close();
            }catch(SQLException se){
                se.printStackTrace();
            }
        }
        double[] p = {46.057633,-21.174992};//被查询的点
        Map<String, Integer> resultCity = ifIn(p[0], p[1], polygonMap);
        System.out.println(resultCity);
    }





//    110.339188,18.678395;109.47521,18.197701;108.655208,18.507682;108.626217,19.367888;109.119056,19.821039;110.211599,20.101254;110.786551,20.077534;111.010051,19.69593;110.570647,19.255879;110.339188,18.678395

    /**
     * @param longitude  经度
     * @param latitude   纬度
     * @param polygonMap 围栏Map
     * @return Map<围栏名称 ， 1 / 0>
     */
    public static Map<String, Integer> ifIn(Double longitude, Double latitude, Map<String, String> polygonMap) {
        Map<String, Integer> results = new HashMap<>();

        //TODO 1.根据(市）围栏建立RTree
        RTree<String, Geometry> newTree = RTree.star().create();//初始化一棵树
        for (String k : polygonMap.keySet()) {//遍历每一个（市）围栏
            //ToDo 1.1.获取围栏字符串
            String polygonStr = polygonMap.get(k);
            //TODO 1.2.获取围栏的最小矩形
            List<Double> mbrLowHigh = new MinimumBoundaryRectangle().MinimumBoundaryRectangleLowHigh(polygonStr);
            Double minLongitude = mbrLowHigh.get(0);//
            Double minLatitude = mbrLowHigh.get(1);
            Double maxLongitude = mbrLowHigh.get(2);
            Double maxLatitude = mbrLowHigh.get(3);
            //TODO 1.3.将矩形添加到RTree中
            newTree = newTree.add(k, Geometries.rectangle( minLongitude,minLatitude,maxLongitude, maxLatitude ));
        }

        //TODO 2.根据输入的点坐标执行查询操作

        //TODO 2.1 找出点落在那些矩形中
        double[] point = {longitude, latitude};

        Observable<Entry<String, Geometry>> resultRec = newTree.search(Geometries.point(longitude, latitude));
        Iterable<List<Entry<String, Geometry>>> iterable = resultRec.toList().toBlocking().toIterable();
        Iterator<List<Entry<String, Geometry>>> iterator = iterable.iterator();
        List<String> resultList = new ArrayList<>();//包含点的矩形List
        while (iterator.hasNext()) {
            List<Entry<String, Geometry>> list = iterator.next();
            for (Entry<String, Geometry> entry : list) {
                resultList.add(entry.value());
            }
        }
        for (String city : polygonMap.keySet()) {//遍历每一个被查询的矩形
            if (resultList.contains(city)) {//当前矩形被包含在查询出的矩形List中
                //TODO 2.2 判断点是否在围栏内
                boolean ifInPolygon = false;
                if(polygonMap.get(city).contains("|")){
                    String[] polStr = polygonMap.get(city).split("\\|");
                    for(int p=0;p<polStr.length;p++){
                        List<Point> pts = new GeometricUtil().string2List(polStr[p]);
                        ifInPolygon = new IsPointInPolygon().isPtInPoly(new Point(point),pts);
                        if(ifInPolygon){
                            break;
                        }
                    }
                }else{
                    List<Point> pts = new GeometricUtil().string2List(polygonMap.get(city));//获取与矩形相对应的围栏
                    ifInPolygon = new IsPointInPolygon().isPtInPoly(new Point(point), pts);//判断点是否在围栏内
                }

                if (ifInPolygon) {//点在围栏内
                    results.put(city, 1);
                } else {//点在围栏之外
                    results.put(city, 0);
                }
            } else {//当前矩形不在查询出的矩形List中
                results.put(city, 0);
            }
        }
        return results;
    }
}

