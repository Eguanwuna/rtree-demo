//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package com.github.davidmoten.rtree.geometry;

import java.io.Serializable;

public interface Rectangle extends Geometry, HasGeometry, Serializable{
    float x1();

    float y1();

    float x2();

    float y2();

    float area();

    Rectangle add(Rectangle var1);

    boolean contains(double var1, double var3);

    float intersectionArea(Rectangle var1);

    float perimeter();
}
