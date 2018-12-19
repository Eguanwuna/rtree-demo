package cn.analysys.tag.rtree;

//import com.ispyinpoly.Point;

import java.io.Serializable;
import java.util.List;

/**
 * java判断某个点是否在所画范围内(多边形【isPtInPoly】/圆形【distencePC】)
 * @return      点在多边形内返回true,否则返回false
 * @author 		ardo
 */
public class IsPointInPolygon implements Serializable {
    /**
     * 判断点是否在多边形内
     * @param point 检测点
     * @param pts   多边形的顶点
     * @return      点在多边形内返回true,否则返回false
     */
    public static boolean isPtInPoly(Point point, List<Point> pts){

        int N = pts.size();
        boolean boundOrVertex = true; //如果点位于多边形的顶点或边上，也算做点在多边形内，直接返回true
        int intersectCount = 0;//射线与多边形的相交点的数量
        double precision = 2e-10; //浮点类型计算时候与0比较时候的容差
        Point p1, p2;//neighbour bound vertices
        Point p = point; //当前点

        p1 = pts.get(0);//左顶点
        for(int i = 1; i <= N; ++i){//遍历所有顶点
            if(p.equals(p1)){
                return boundOrVertex;//p 为落在边界或顶点上
            }

            p2 = pts.get(i % N);//右顶点
            double px = p.getDoubleCoordinate(0);
            double py = p.getDoubleCoordinate(1);
            double p1x = p1.getDoubleCoordinate(0);
            double p1y = p1.getDoubleCoordinate(1);
            double p2x = p2.getDoubleCoordinate(0);
            double p2y = p2.getDoubleCoordinate(1);

            if(px< Math.min(p1x, p2x) || px > Math.max(p1x, p2x)){//射线超出范围
                p1 = p2;
                continue;//下一条射线
            }

            if(px > Math.min(p1x, p2x) && px < Math.max(p1x, p2x)){//ray is crossing over by the algorithm (common part of)
                if(py <= Math.max(p1y, p2y)){//x is before of ray
                    if(p1x == p2x && py >= Math.min(p1y, p2y)){//overlies on a horizontal ray
                        return boundOrVertex;
                    }

                    if(p1y == p2y){//ray is vertical
                        if(p1y == py){//overlies on a vertical ray
                            return boundOrVertex;
                        }else{//before ray
                            ++intersectCount;
                        }
                    }else{//cross point on the left side
                        double xinters = (px - p1x) * (p2y - p1y) / (p2x - p1x) + p1y;//cross point of y
                        if(Math.abs(py - xinters) < precision){//overlies on a ray
                            return boundOrVertex;
                        }

                        if(py < xinters){//before ray
                            ++intersectCount;
                        }
                    }
                }
            }else{//射线穿过顶点
                if(px == p2x && py <= p2y){//p crossing over p2
                    Point p3 = pts.get((i+1) % N); //next vertex
                    double p3x = p3.getDoubleCoordinate(0);
                    if(px >= Math.min(p1x, p3x) && px <= Math.max(p1x, p3x)){//p.x lies between p1.x & p3.x
                        ++intersectCount;
                    }else{
                        intersectCount += 2;
                    }
                }
            }
            p1 = p2;//next ray left point
        }

        if(intersectCount % 2 == 0){//偶数在多边形外
            return false;
        } else { //奇数在多边形内
            return true;
        }

    }

    }

