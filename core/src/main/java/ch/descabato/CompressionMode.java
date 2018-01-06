package ch.descabato;

public enum CompressionMode {
	none(0, 		366),
	gzip(1,		5682770),
	bzip2(2,   27457575),
	lzma(3,    31656340),
    snappy(4,    185912),
    deflate(5,  5549557),
    lz4(6,       412813),
    lz4hc(7,    3673233),
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
		throw new IllegalArgumentException("Did not find compression mode for "+b);
	}
	
	public boolean isCompressionAlgorithm() {
		return isCompressionAlgorithm;
	}

    /**
     *  Experimentally determined values for the estimated time for a 100kb block.
     *  Used to seed the speed comparison, will be replaced with currently measured values later
     */
    public long getEstimatedTime() {
        return estimatedTime;
    }
}
