package cn.analysys.tag.rtree;

/**
 * @ClassName Point
 * @Description n维空间中的点，所有的维度被存储在一个double数组中
 */
public class Point implements Cloneable {
    private double[] data;

    public Point(double[] data) {
        this.data = new double[data.length];
        System.arraycopy(data, 0, this.data, 0, data.length); // 复制数组
    }

   /* public Point(int[] data) {
        this.data = new double[data.length];
        for (int i = 0; i < data.length; i++) {
            this.data[i] = data[i]; // 复制数组
        }
    }*/

    @Override // 重写clone接口
    protected Object clone() {
        double[] copy = new double[data.length];
        System.arraycopy(data, 0, copy, 0, data.length);
        return new Point(copy);
    }

    @Override // 重写tostring（）方法
    public String toString() {
        //StringBuffer sBuffer = new StringBuffer("(");
        StringBuffer sBuffer = new StringBuffer();
        for (int i = 0; i < data.length - 1; i++) {
            sBuffer.append(data[i]).append(",");
        }
        //sBuffer.append(data[data.length - 1]).append(")"); // 最后一位数据后面不再添加逗号，追加放在循环外面
        sBuffer.append(data[data.length - 1]);
        return sBuffer.toString();
    }


    public int getDimension() {
        return data.length;
    }

    /**
     * @param index
     * @return 返回Point坐标第i位的Double值
     */
    public double getDoubleCoordinate(int index) {
        return data[index];
    }

    /**
     * @param index
     * @return 返回Point坐标第i位的int值
     */
    public int getIntCoordinate(int index) {
        return (int) data[index];
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Point) // 如果obj是point的实例
        {
            Point point = (Point) obj;

            if (point.getDimension() != getDimension()) // 维度相同的点才能比较
                throw new IllegalArgumentException("Points must be of equal dimensions to be compared.");

            for (int i = 0; i < getDimension(); i++) {
                if (getDoubleCoordinate(i) != point.getDoubleCoordinate(i))
                    return false;
            }
        }

        if (!(obj instanceof Point))
            return false;

        return true;
    }
}
