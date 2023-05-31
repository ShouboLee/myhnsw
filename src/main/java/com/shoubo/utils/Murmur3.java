package com.shoubo.utils;

/**
 * @author shoubo
 * @date 031 23/5/31
 * @desc 实现了 Murmur3 哈希算法的 32 位变体。
 * Murmur3 是一种快速的非加密哈希算法，用于生成给定输入数据的哈希码。
 * Murmur3有32位和128位两种变体。
 * 32位的Java版本参考自https://code.google.com/p/smhasher/source/browse/trunk/MurmurHash3.cpp#94
 * 128位的Java版本参考自https://code.google.com/p/smhasher/source/browse/trunk/MurmurHash3.cpp#255
 */
public class Murmur3 {
    // 32bit变体的常量
    private static final int C1_32 = 0xcc9e2d51;
    private static final int C2_32 = 0x1b873593;
    private static final int R1_32 = 15;
    private static final int R2_32 = 13;
    private static final int M_32 = 5;
    private static final int N_32 = 0xe6546b64;

    private static final int DEFAULT_SEED = 104729;

    /**
     * Murmur3的32位变体。
     * @param data - 输入字节数组
     * @return - 哈希码
     */
    public static int hash32(byte[] data) {
        return hash32(data, 0, data.length, DEFAULT_SEED);
    }

    /**
     * Murmur3的32位变体。
     * @param data - 输入字节数组
     * @param offset - 数据偏移量
     * @param length - 数组长度
     * @param seed - 种子。 (默认 0)
     * @return - 哈希码
     */
    public static int hash32(byte[] data, int offset, int length, int seed) {
        int hash = seed;
        final int nblocks = length >> 2;

        // body 处理4字节的快
        for (int i = 0; i < nblocks; i++) {
            int i_4 = i << 2;
            int k = (data[offset + i_4] & 0xff)
                    | ((data[offset + i_4 + 1] & 0xff) << 8)
                    | ((data[offset + i_4 + 2] & 0xff) << 16)
                    | ((data[offset + i_4 + 3] & 0xff) << 24);

            hash = mix32(k, hash);
        }

        // tail 处理剩余的字节
        int idx = nblocks << 2;
        int k1 = 0;
        switch (length - idx) {
            case 3:
                k1 ^= data[offset + idx + 2] << 16;
            case 2:
                k1 ^= data[offset + idx + 1] << 8;
            case 1:
                k1 ^= data[offset + idx];

                // mix 执行混合操作
                k1 *= C1_32;
                k1 = Integer.rotateLeft(k1, R1_32);
                k1 *= C2_32;
                hash ^= k1;
        }
        return fmix32(hash, length);
    }

    /**
     * 对一个32位整数进行混合操作，生成新的哈希值。
     * @param k - 输入的32位整数
     * @param hash - 当前的哈希值
     * @return - 新的哈希值
     */
    private static int mix32(int k, int hash) {
        k *= C1_32; // 乘以常数C1
        k = Integer.rotateLeft(k, R1_32); // 循环左移R1位
        k *= C2_32; // 乘以常数C2
        hash ^= k; // 异或
        return Integer.rotateLeft(hash, R2_32) * M_32 + N_32; // 循环左移R2位，乘以M，加上常数N
    }

    /**
     * 对最终的哈希值进行混合操作。
     * @param length - 输入数据的长度
     * @param hash - 当前的哈希值
     * @return - 新的哈希值
     */
    private static int fmix32(int length, int hash) {
        // 将长度与哈希值进行异或
        hash ^= length;
        // 右循环移位
        hash ^= (hash >>> 16);
        // 乘以常数
        hash *= 0x85ebca6b;
        // 右循环移位
        hash ^= (hash >>> 13);
        // 乘以常数
        hash *= 0xc2b2ae35;
        // 右循环移位
        hash ^= (hash >>> 16);

        return hash;
    }

}
