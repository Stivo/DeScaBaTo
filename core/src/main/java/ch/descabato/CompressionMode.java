package ch.descabato;

public enum CompressionMode {
	none(0), 
	gzip(1), 
	bzip2(2), 
	lzma(3);

	private final int val;

	private CompressionMode(int v) {
		val = v;
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
}
