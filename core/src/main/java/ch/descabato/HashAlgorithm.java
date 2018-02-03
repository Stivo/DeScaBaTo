package ch.descabato;

import org.bouncycastle.crypto.Digest;
import org.bouncycastle.crypto.digests.Blake2bDigest;
import org.bouncycastle.crypto.digests.SHA3Digest;

public enum HashAlgorithm {

	blake2b_160("Blake2b_160", 20),
	blake2b_256("Blake2b_256", 32),
	blake2b_384("Blake2b_384", 48),
	blake2b_512("Blake2b_512", 64),
	sha3_256("SHA3-256", 32),
	sha3_384("SHA3-384", 48),
	sha3_512("SHA3-512", 64);

	private final String name;
	private int digestLength;

	HashAlgorithm(String s, int digestLength) {
		this.name = s;
		this.digestLength = digestLength;
	}

	public String getName() {
		return name;
	}

	public Digest newInstance() {
		switch(this) {
			case sha3_256:
			case sha3_384:
			case sha3_512:
				return new SHA3Digest(digestLength * 8);
			case blake2b_160:
			case blake2b_256:
			case blake2b_384:
			case blake2b_512:
				return new Blake2bDigest(digestLength * 8);
		}
		throw new IllegalArgumentException("Should not get here");
	}

	public int getDigestLength() {
		return digestLength;
	}
}
