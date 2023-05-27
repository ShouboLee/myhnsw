package com.shoubo.model;

/**
 * Author: shoubo
 * Date: 2023/5/27
 * Desc: 距离函数，计算两向量之间的距离
 */
public final class DistanceImpls {
    /**
     * 计算两个稀疏向量之间的距离
     */
    static class FloatSparseVectorInnerProduct implements Distance<SparseVector<float[]>, Float> {
        /**
         * 计算两个稀疏向量之间的距离
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
            return 1-dot;
        }
    }

    /**
     * 计算两个稀疏向量之间的距离
     */
    static class DoubleSparseVectorInnerProduct implements Distance<SparseVector<double[]>, Double> {

        /**
         * 计算两个稀疏向量之间的距离
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
            return 1-dot;
        }
    }

    /**
     * 计算两向量之间的余弦距离
     */
    static class FloatCosineDistance implements Distance<float[], Float> {
        // 序列化版本号
        private static final long serialVersionUID = 1L;

        /**
         * 计算两向量之间的余弦距离
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
            return 1-dot/(float)(Math.sqrt(normU)*Math.sqrt(normV));
        }
    }

    /**
     * 计算两向量之间的余弦距离
     */
    static class DoubleCosineDistance implements Distance<double[], Double> {
        // 序列化版本号
        private static final long serialVersionUID = 1L;

        /**
         * 计算两向量之间的余弦距离
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
            return 1-dot/(Math.sqrt(normU)*Math.sqrt(normV));
        }
    }

    /**
     * 计算两向量之间的内积
     */
    static class FloatInnerProduct implements Distance<float[], Float> {
        // 序列化版本号
        private static final long serialVersionUID = 1L;

        /**
         * 计算两向量之间的内积
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

    static class FloatEuclideanDistance implements Distance<float[], Float> {
        // 序列化版本号
        private static final long serialVersionUID = 1L;

        /**
         * 计算两向量之间的欧式距离
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

    static class DoubleEuclideanDistance implements Distance<double[], Double> {
        // 序列化版本号
        private static final long serialVersionUID = 1L;

        /**
         * 计算两向量之间的欧式距离
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


}
