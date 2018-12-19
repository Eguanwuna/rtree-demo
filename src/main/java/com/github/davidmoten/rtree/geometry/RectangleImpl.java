//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package com.github.davidmoten.rtree.geometry;

import com.github.davidmoten.guavamini.Objects;
import com.github.davidmoten.guavamini.Optional;
import com.github.davidmoten.guavamini.Preconditions;
import com.github.davidmoten.rtree.internal.util.ObjectsHelper;

import java.io.Serializable;

class RectangleImpl implements Rectangle,Serializable {
    private final float x1;
    private final float y1;
    private final float x2;
    private final float y2;

    private RectangleImpl(float x1, float y1, float x2, float y2) {
        Preconditions.checkArgument(x2 >= x1);
        Preconditions.checkArgument(y2 >= y1);
        this.x1 = x1;
        this.y1 = y1;
        this.x2 = x2;
        this.y2 = y2;
    }

    static Rectangle create(double x1, double y1, double x2, double y2) {
        return new RectangleImpl((float)x1, (float)y1, (float)x2, (float)y2);
    }

    static Rectangle create(float x1, float y1, float x2, float y2) {
        return new RectangleImpl(x1, y1, x2, y2);
    }

    public float x1() {
        return this.x1;
    }

    public float y1() {
        return this.y1;
    }

    public float x2() {
        return this.x2;
    }

    public float y2() {
        return this.y2;
    }

    public float area() {
        return (this.x2 - this.x1) * (this.y2 - this.y1);
    }

    public Rectangle add(Rectangle r) {
        return new RectangleImpl(min(this.x1, r.x1()), min(this.y1, r.y1()), max(this.x2, r.x2()), max(this.y2, r.y2()));
    }

    public boolean contains(double x, double y) {
        return x >= (double)this.x1 && x <= (double)this.x2 && y >= (double)this.y1 && y <= (double)this.y2;
    }

    public boolean intersects(Rectangle r) {
        return intersects(this.x1, this.y1, this.x2, this.y2, r.x1(), r.y1(), r.x2(), r.y2());
    }

    public double distance(Rectangle r) {
        return distance(this.x1, this.y1, this.x2, this.y2, r.x1(), r.y1(), r.x2(), r.y2());
    }

    public static double distance(float x1, float y1, float x2, float y2, float a1, float b1, float a2, float b2) {
        if(intersects(x1, y1, x2, y2, a1, b1, a2, b2)) {
            return 0.0D;
        } else {
            boolean xyMostLeft = x1 < a1;
            float mostLeftX1 = xyMostLeft?x1:a1;
            float mostRightX1 = xyMostLeft?a1:x1;
            float mostLeftX2 = xyMostLeft?x2:a2;
            double xDifference = (double)max(0.0F, mostLeftX1 == mostRightX1?0.0F:mostRightX1 - mostLeftX2);
            boolean xyMostDown = y1 < b1;
            float mostDownY1 = xyMostDown?y1:b1;
            float mostUpY1 = xyMostDown?b1:y1;
            float mostDownY2 = xyMostDown?y2:b2;
            double yDifference = (double)max(0.0F, mostDownY1 == mostUpY1?0.0F:mostUpY1 - mostDownY2);
            return Math.sqrt(xDifference * xDifference + yDifference * yDifference);
        }
    }

    private static boolean intersects(float x1, float y1, float x2, float y2, float a1, float b1, float a2, float b2) {
        return x1 <= a2 && a1 <= x2 && y1 <= b2 && b1 <= y2;
    }

    public Rectangle mbr() {
        return this;
    }

    public String toString() {
        return "Rectangle [x1=" + this.x1 + ", y1=" + this.y1 + ", x2=" + this.x2 + ", y2=" + this.y2 + "]";
    }

    public int hashCode() {
        return Objects.hashCode(new Object[]{Float.valueOf(this.x1), Float.valueOf(this.y1), Float.valueOf(this.x2), Float.valueOf(this.y2)});
    }

    public boolean equals(Object obj) {
        Optional<RectangleImpl> other = ObjectsHelper.asClass(obj, RectangleImpl.class);
        return !other.isPresent()?false:Objects.equal(Float.valueOf(this.x1), Float.valueOf(((RectangleImpl)other.get()).x1)) && Objects.equal(Float.valueOf(this.x2), Float.valueOf(((RectangleImpl)other.get()).x2)) && Objects.equal(Float.valueOf(this.y1), Float.valueOf(((RectangleImpl)other.get()).y1)) && Objects.equal(Float.valueOf(this.y2), Float.valueOf(((RectangleImpl)other.get()).y2));
    }

    public float intersectionArea(Rectangle r) {
        return !this.intersects(r)?0.0F:create(max(this.x1, r.x1()), max(this.y1, r.y1()), min(this.x2, r.x2()), min(this.y2, r.y2())).area();
    }

    public float perimeter() {
        return 2.0F * (this.x2 - this.x1) + 2.0F * (this.y2 - this.y1);
    }

    public Geometry geometry() {
        return this;
    }

    private static float max(float a, float b) {
        return a < b?b:a;
    }

    private static float min(float a, float b) {
        return a < b?a:b;
    }
}
