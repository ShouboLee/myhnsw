package com.shoubo.model;

/**
 * Author: shoubo
 * Date: 2023/5/27
 * Desc: 距离函数，计算两向量之间的距离
 */
public enum DistanceTypeImpls {
    INSTANCE;

    /**
     * 计算两个稀疏向量之间的距离
     */
    static class FloatSparseVectorInnerProduct implements DistanceType<SparseVector<float[]>, Float> {
        /**
         * 计算两个稀疏向量之间的距离
         *
         * @param u 向量 u
         * @param v 向量 v
         * @return 向量 u 和 v 之间的距离
         */
        @Override
        public Float distance(SparseVector<float[]> u, SparseVector<float[]> v) {

            // 获取稀疏向量的非零元素的索引和值
            int[] uIndices = u.getIndices();
            float[] uValues = u.getValues();
            int[] vIndices = v.getIndices();
            float[] vValues = v.getValues();
            float dot = 0.0f;
            int i = 0;
            int j = 0;

            // 计算两个稀疏向量的内积
            while (i < uIndices.length && j < vIndices.length) {
                if (uIndices[i] == vIndices[j]) {
                    dot += uValues[i] * vValues[j];
                    i++;
                    j++;
                } else if (uIndices[i] < vIndices[j]) {
                    i++;
                } else {
                    j++;
                }
            }
            return 1 - dot;
        }
    }

    /**
     * 计算两个稀疏向量之间的距离
     */
    static class DoubleSparseVectorInnerProduct implements DistanceType<SparseVector<double[]>, Double> {

        /**
         * 计算两个稀疏向量之间的距离
         *
         * @param u 向量 u
         * @param v 向量 v
         * @return 向量 u 和 v 之间的距离
         */
        @Override
        public Double distance(SparseVector<double[]> u, SparseVector<double[]> v) {
            int[] uIndices = u.getIndices();
            double[] uValues = u.getValues();
            int[] vIndices = v.getIndices();
            double[] vValues = v.getValues();
            double dot = 0.0;
            int i = 0;
            int j = 0;

            while (i < uIndices.length && j < vIndices.length) {
                if (uIndices[i] == vIndices[j]) {
                    dot += uValues[i] * vValues[j];
                    i++;
                    j++;
                } else if (uIndices[i] < vIndices[j]) {
                    i++;
                } else {
                    j++;
                }
            }
            return 1 - dot;
        }
    }

    /**
     * 计算两向量之间的余弦距离
     */
    static class FloatCosineDistance implements DistanceType<float[], Float> {
        // 序列化版本号
        private static final long serialVersionUID = 1L;

        /**
         * 计算两向量之间的余弦距离
         *
         * @param u 向量 u
         * @param v 向量 v
         * @return 向量 u 和 v 之间的距离
         */
        @Override
        public Float distance(float[] u, float[] v) {
            float dot = 0.0f;
            float normU = 0.0f;
            float normV = 0.0f;
            for (int i = 0; i < u.length; i++) {
                dot += u[i] * v[i];
                normU += u[i] * u[i];
                normV += v[i] * v[i];
            }
            // 余弦距离
            return 1 - dot / (float) (Math.sqrt(normU) * Math.sqrt(normV));
        }
    }

    /**
     * 计算两向量之间的余弦距离
     */
    static class DoubleCosineDistance implements DistanceType<double[], Double> {
        // 序列化版本号
        private static final long serialVersionUID = 1L;

        /**
         * 计算两向量之间的余弦距离
         *
         * @param u 向量 u
         * @param v 向量 v
         * @return 向量 u 和 v 之间的距离
         */
        @Override
        public Double distance(double[] u, double[] v) {
            double dot = 0.0;
            double normU = 0.0;
            double normV = 0.0;
            for (int i = 0; i < u.length; i++) {
                dot += u[i] * v[i];
                normU += u[i] * u[i];
                normV += v[i] * v[i];
            }
            // 余弦距离
            return 1 - dot / (Math.sqrt(normU) * Math.sqrt(normV));
        }
    }

    /**
     * 计算两向量之间的内积
     */
    static class FloatInnerProduct implements DistanceType<float[], Float> {
        // 序列化版本号
        private static final long serialVersionUID = 1L;

        /**
         * 计算两向量之间的内积
         *
         * @param u 向量 u
         * @param v 向量 v
         * @return 向量 u 和 v 之间的距离
         */
        @Override
        public Float distance(float[] u, float[] v) {
            float dot = 0.0f;
            for (int i = 0; i < u.length; i++) {
                dot += u[i] * v[i];
            }
            return 1 - dot;
        }
    }

    /**
     * 计算两向量之间的内积
     */
    static class DoubleInnerProduct implements DistanceType<double[], Double> {
        // 序列化版本号
        private static final long serialVersionUID = 1L;

        /**
         * 计算两向量之间的内积
         *
         * @param u 向量 u
         * @param v 向量 v
         * @return 向量 u 和 v 之间的距离
         */
        @Override
        public Double distance(double[] u, double[] v) {
            double dot = 0.0;
            for (int i = 0; i < u.length; i++) {
                dot += u[i] * v[i];
            }
            return 1 - dot;
        }
    }

    /**
     * 计算两向量之间的欧氏距离
     */
    static class FloatEuclideanDistance implements DistanceType<float[], Float> {
        // 序列化版本号
        private static final long serialVersionUID = 1L;

        /**
         * 计算两向量之间的欧式距离
         *
         * @param u 向量 u
         * @param v 向量 v
         * @return 向量 u 和 v 之间的距离
         */
        @Override
        public Float distance(float[] u, float[] v) {
            float sum = 0.0f;
            for (int i = 0; i < u.length; i++) {
                float dp = u[i] - v[i];
                sum += dp * dp;
            }
            return (float) Math.sqrt(sum);
        }
    }

    /**
     * 计算两向量之间的欧氏距离
     */
    static class DoubleEuclideanDistance implements DistanceType<double[], Double> {
        // 序列化版本号
        private static final long serialVersionUID = 1L;

        /**
         * 计算两向量之间的欧式距离
         *
         * @param u 向量 u
         * @param v 向量 v
         * @return 向量 u 和 v 之间的距离
         */
        @Override
        public Double distance(double[] u, double[] v) {
            double sum = 0.0;
            for (int i = 0; i < u.length; i++) {
                double dp = u[i] - v[i];
                sum += dp * dp;
            }
            return Math.sqrt(sum);
        }
    }

    /**
     * 计算两向量之间的坎贝拉距离
     */
    static class FloatCanberraDistance implements DistanceType<float[], Float> {
        // 序列化版本号
        private static final long serialVersionUID = 1L;

        /**
         * 计算两向量之间的Canberra距离
         *
         * @param u 向量 u
         * @param v 向量 v
         * @return 向量 u 和 v 之间的距离
         */
        @Override
        public Float distance(float[] u, float[] v) {
            float sum = 0.0f;
            for (int i = 0; i < u.length; i++) {
                float dp = Math.abs(u[i] - v[i]);
                sum += (u[i] == 0 && v[i] == 0) ? 0 : dp / (Math.abs(u[i]) + Math.abs(v[i]));
            }
            return sum;
        }
    }

    static class DoubleCanberraDistance implements DistanceType<double[], Double> {
        // 序列化版本号
        private static final long serialVersionUID = 1L;

        /**
         * 计算两向量之间的Canberra距离
         *
         * @param u 向量 u
         * @param v 向量 v
         * @return 向量 u 和 v 之间的距离
         */
        @Override
        public Double distance(double[] u, double[] v) {
            double sum = 0.0;
            for (int i = 0; i < u.length; i++) {
                double dp = Math.abs(u[i] - v[i]);
                sum += (u[i] == 0 && v[i] == 0) ? 0 : dp / (Math.abs(u[i]) + Math.abs(v[i]));
            }
            return sum;
        }
    }

    /**
     * 计算两向量之间的BrayCurtis距离
     */
    static class FloatBrayCurtisDistance implements DistanceType<float[], Float> {
        // 序列化版本号
        private static final long serialVersionUID = 1L;

        /**
         * 计算两向量之间的BrayCurtis距离
         *
         * @param u 向量 u
         * @param v 向量 v
         * @return 向量 u 和 v 之间的距离
         */
        @Override
        public Float distance(float[] u, float[] v) {
            float sum1 = 0.0f;
            float sum2 = 0.0f;
            for (int i = 0; i < u.length; i++) {
                sum1 += Math.abs(u[i] - v[i]);
                sum2 += Math.abs(u[i] + v[i]);
            }
            return sum1 / sum2;
        }
    }

    /**
     * 计算两向量之间的BrayCurtis距离
     */
    static class DoubleBrayCurtisDistance implements DistanceType<double[], Double> {
        // 序列化版本号
        private static final long serialVersionUID = 1L;

        /**
         * 计算两向量之间的BrayCurtis距离
         *
         * @param u 向量 u
         * @param v 向量 v
         * @return 向量 u 和 v 之间的距离
         */
        @Override
        public Double distance(double[] u, double[] v) {
            double sum1 = 0.0;
            double sum2 = 0.0;
            for (int i = 0; i < u.length; i++) {
                sum1 += Math.abs(u[i] - v[i]);
                sum2 += Math.abs(u[i] + v[i]);
            }
            return sum1 / sum2;
        }
    }

    /**
     * 计算两向量之间的相关系数距离
     */
    static class FloatCorrelationDistance implements DistanceType<float[], Float> {
        // 序列化版本号
        private static final long serialVersionUID = 1L;

        /**
         * 计算两向量之间的相关系数距离
         *
         * @param u 向量 u
         * @param v 向量 v
         * @return 向量 u 和 v 之间的相关系数距离
         */
        @Override
        public Float distance(float[] u, float[] v) {
            float x = 0.0f;
            float y = 0.0f;

            for (int i = 0; i < u.length; ++i) {
                x += -u[i];
                y += -v[i];
            }

            x /= u.length;
            y /= v.length;

            float sum = 0.0f;
            float den1 = 0.0f;
            float den2 = 0.0f;

            for (int i = 0; i < u.length; i++) {
                sum += (u[i] + x) * (v[i] + y);

                den1 += Math.abs(Math.pow(u[i] + x, 2));
                den2 += Math.abs(Math.pow(v[i] + y, 2));
            }

            return 1f - sum / (float) Math.sqrt(den1) * (float) Math.sqrt(den2);
        }
    }

    /**
     * 计算两向量之间的相关系数距离
     */
    static class DoubleCorrelationDistance implements DistanceType<double[], Double> {
        // 序列化版本号
        private static final long serialVersionUID = 1L;

        /**
         * 计算两向量之间的相关系数距离
         *
         * @param u 向量 u
         * @param v 向量 v
         * @return 向量 u 和 v 之间的相关系数距离
         */
        @Override
        public Double distance(double[] u, double[] v) {
            double x = 0.0;
            double y = 0.0;

            for (int i = 0; i < u.length; ++i) {
                x += -u[i];
                y += -v[i];
            }

            x /= u.length;
            y /= v.length;

            double sum = 0.0;
            double den1 = 0.0;
            double den2 = 0.0;

            for (int i = 0; i < u.length; ++i) {
                sum += (u[i] + x) * (v[i] + y);

                den1 += Math.abs(Math.pow(u[i] + x, 2));
                den2 += Math.abs(Math.pow(v[i] + y, 2));
            }

            return 1d - sum / Math.sqrt(den1) * Math.sqrt(den2);
        }
    }

    /**
     * 计算两向量之间的曼哈顿距离
     */
    static class FloatManhattanDistance implements DistanceType<float[], Float> {
        // 序列化版本号
        private static final long serialVersionUID = 1L;

        /**
         * 计算两向量之间的曼哈顿距离
         *
         * @param u 向量 u
         * @param v 向量 v
         * @return 向量 u 和 v 之间的曼哈顿距离
         */
        @Override
        public Float distance(float[] u, float[] v) {
            float sum = 0.0f;
            for (int i = 0; i < u.length; i++) {
                sum += Math.abs(u[i] - v[i]);
            }
            return sum;
        }
    }

    /**
     * 计算两向量之间的曼哈顿距离
     */
    static class DoubleManhattanDistance implements DistanceType<double[], Double> {
        // 序列化版本号
        private static final long serialVersionUID = 1L;

        /**
         * 计算两向量之间的曼哈顿距离
         *
         * @param u 向量 u
         * @param v 向量 v
         * @return 向量 u 和 v 之间的曼哈顿距离
         */
        @Override
        public Double distance(double[] u, double[] v) {
            double sum = 0.0;
            for (int i = 0; i < u.length; i++) {
                sum += Math.abs(u[i] - v[i]);
            }
            return sum;
        }
    }

    /**
     * 计算两向量之间的余弦距离
     */
    public static final DistanceType<float[], Float> FLOAT_COSINE_DISTANCE = new FloatCosineDistance();
    public static final DistanceType<double[], Double> DOUBLE_COSINE_DISTANCE = new DoubleCosineDistance();

    /**
     * 计算两向量之间的点积距离
     */
    public static final DistanceType<float[], Float> FLOAT_INNER_PRODUCT = new FloatInnerProduct();
    public static final DistanceType<double[], Double> DOUBLE_INNER_PRODUCT = new DoubleInnerProduct();

    /**
     * 计算两向量之间的欧氏距离
     */
    public static final DistanceType<float[], Float> FLOAT_EUCLIDEAN_DISTANCE = new FloatEuclideanDistance();
    public static final DistanceType<double[], Double> DOUBLE_EUCLIDEAN_DISTANCE = new DoubleEuclideanDistance();

    /**
     * 计算两向量之间的BrayCurtis距离
     */
    public static final DistanceType<float[], Float> FLOAT_BRAY_CURTIS_DISTANCE = new FloatBrayCurtisDistance();
    public static final DistanceType<double[], Double> DOUBLE_BRAY_CURTIS_DISTANCE = new DoubleBrayCurtisDistance();

    /**
     * 计算两向量之间的Canberra距离
     */
    public static final DistanceType<float[], Float> FLOAT_CANBERRA_DISTANCE = new FloatCanberraDistance();
    public static final DistanceType<double[], Double> DOUBLE_CANBERRA_DISTANCE = new DoubleCanberraDistance();

    /**
     * 计算两向量之间的相关系数距离
     */
    public static final DistanceType<float[], Float> FLOAT_CORRELATION_DISTANCE = new FloatCorrelationDistance();
    public static final DistanceType<double[], Double> DOUBLE_CORRELATION_DISTANCE = new DoubleCorrelationDistance();

    /**
     * 计算两向量之间的曼哈顿距离
     */
    public static final DistanceType<float[], Float> FLOAT_MANHATTAN_DISTANCE = new FloatManhattanDistance();
    public static final DistanceType<double[], Double> DOUBLE_MANHATTAN_DISTANCE = new DoubleManhattanDistance();

    /**
     * 计算两稀疏向量之间的内积距离
     */
    public static final DistanceType<SparseVector<float[]>, Float> FLOAT_SPARSE_VECTOR_INNER_PRODUCT = new FloatSparseVectorInnerProduct();
    public static final DistanceType<SparseVector<double[]>, Double> DOUBLE_SPARSE_VECTOR_INNER_PRODUCT = new DoubleSparseVectorInnerProduct();

}
