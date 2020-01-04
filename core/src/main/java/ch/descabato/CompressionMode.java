package ch.descabato;

public enum CompressionMode {
    none(0, 52),
    @Deprecated
    gzip(1, 1800),
    lzma1(31, 3100),
    lzma3(33, 3400),
    lzma6(3, 25000),
    @Deprecated
    snappy(4, 351),
    @Deprecated
    deflate(5, 1500),
    @Deprecated
    lz4(6, 546),
    @Deprecated
    lz4hc(7, 3900),
    zstd1(11, 215),
    zstd5(15, 550),
    zstd9(19, 852),
    smart;

    private final int val;
    private final boolean isCompressionAlgorithm;
    private final long estimatedTime;

    CompressionMode(int v, long estimatedTime) {
        val = v;
        isCompressionAlgorithm = true;
        this.estimatedTime = estimatedTime;
    }

    CompressionMode() {
        val = 255;
        isCompressionAlgorithm = false;
        this.estimatedTime = -1;
    }

    public byte getByte() {
        return (byte) val;
    }

    public static CompressionMode getByByte(int b) {
        for (CompressionMode c : CompressionMode.values()) {
            if (c.getByte() == b) {
                return c;
            }
        }
        throw new IllegalArgumentException("Did not find compression mode for " + b);
    }

    public boolean isCompressionAlgorithm() {
        return isCompressionAlgorithm;
    }

    /**
     * Experimentally determined values for the estimated time for a 100kb block.
     * Used to seed the speed comparison, will be replaced with currently measured values later
     */
    public long getEstimatedTime() {
        return estimatedTime;
    }
}
