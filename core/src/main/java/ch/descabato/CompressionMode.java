package ch.descabato;

public enum CompressionMode {
	none(0), 
	gzip(1), 
	bzip2(2), 
	lzma(3),
    snappy(4),
    deflate(5),
    lz4(6),
    lz4hc(7),
    smart;

	private final int val;
	private final boolean isCompressionAlgorithm;

	private CompressionMode(int v) {
		val = v;
		isCompressionAlgorithm = true;
	}

	private CompressionMode() {
		val = 255;
		isCompressionAlgorithm = false;
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
}
