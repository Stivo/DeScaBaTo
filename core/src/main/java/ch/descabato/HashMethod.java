package ch.descabato;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public enum HashMethod {

	sha256("SHA-256", 32),
	sha384("SHA-384", 48),
	sha512("SHA-512", 64);

	private final String name;
	private int digestLength;

	HashMethod(String s, int digestLength) {
		this.name = s;
		this.digestLength = digestLength;
	}

	public String getName() {
		return name;
	}

	public MessageDigest newInstance() {
		try {
			return MessageDigest.getInstance(name);
		} catch (NoSuchAlgorithmException e) {
			throw new IllegalStateException("All these should exist");
		}
	}

	public int getDigestLength() {
		return digestLength;
	}
}
