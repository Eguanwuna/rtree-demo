//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package com.github.davidmoten.rtree.geometry;

import java.io.Serializable;

public final class Point implements Rectangle, Serializable {
    private final float x;
    private final float y;

    private Point(float x, float y) {
        this.x = x;
        this.y = y;
    }

    static Point create(double x, double y) {
        return new Point((float)x, (float)y);
    }

    static Point create(float x, float y) {
        return new Point(x, y);
    }

    public Rectangle mbr() {
        return this;
    }

    public double distance(Rectangle r) {
        return RectangleImpl.distance(this.x, this.y, this.x, this.y, r.x1(), r.y1(), r.x2(), r.y2());
    }

    public double distance(Point p) {
        return Math.sqrt(this.distanceSquared(p));
    }

    public double distanceSquared(Point p) {
        float dx = this.x - p.x;
        float dy = this.y - p.y;
        return (double)(dx * dx + dy * dy);
    }

    public boolean intersects(Rectangle r) {
        return r.x1() <= this.x && this.x <= r.x2() && r.y1() <= this.y && this.y <= r.y2();
    }

    public float x() {
        return this.x;
    }

    public float y() {
        return this.y;
    }

    public int hashCode() {
        int result1 = 1;
        int result = 31 * result1 + Float.floatToIntBits(this.x);
        result = 31 * result + Float.floatToIntBits(this.y);
        return result;
    }

    public boolean equals(Object obj) {
        if(this == obj) {
            return true;
        } else if(obj == null) {
            return false;
        } else if(this.getClass() != obj.getClass()) {
            return false;
        } else {
            Point other = (Point)obj;
            return Float.floatToIntBits(this.x) != Float.floatToIntBits(other.x)?false:Float.floatToIntBits(this.y) == Float.floatToIntBits(other.y);
        }
    }

    public String toString() {
        return "Point [x=" + this.x() + ", y=" + this.y() + "]";
    }

    public Geometry geometry() {
        return this;
    }

    public float x1() {
        return this.x;
    }

    public float y1() {
        return this.y;
    }

    public float x2() {
        return this.x;
    }

    public float y2() {
        return this.y;
    }

    public float area() {
        return 0.0F;
    }

    public Rectangle add(Rectangle r) {
        return RectangleImpl.create(Math.min(this.x, r.x1()), Math.min(this.y, r.y1()), Math.max(this.x, r.x2()), Math.max(this.y, r.y2()));
    }

    public boolean contains(double x, double y) {
        return (double)this.x == x && (double)this.y == y;
    }

    public float intersectionArea(Rectangle r) {
        return 0.0F;
    }

    public float perimeter() {
        return 0.0F;
    }
}
