package backup;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.KeySpec;

import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.CipherOutputStream;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;

public class AES {
	/*
	static String IV = "AAAAAAAAAAAAAAAA";
	static String plaintext = "test text 123\0\0\0"; /* Note null padding 
	static String encryptionKey = "0123456789abcdef";
	static byte[] salt = null;
	*/
	
	public static SecretKey deriveKey(String password, int keylength) throws NoSuchAlgorithmException, InvalidKeySpecException {
		SecureRandom sr = new SecureRandom("test".getBytes());
	    byte[] salt = new byte[1024];
		sr.nextBytes(salt);
	    SecretKeyFactory kf = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA1");
	    KeySpec specs = new PBEKeySpec(password.toCharArray(), salt, 1024, keylength);
	    SecretKey generateSecret = kf.generateSecret(specs);
	    return new SecretKeySpec(generateSecret.getEncoded(), "AES");
	}
	
	public static InputStream wrapStreamWithDecryption(InputStream input, String encryptionKey, int keylength)
			throws Exception {
		Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding", "SunJCE");
		SecretKey deriveKey = deriveKey(encryptionKey, keylength);
		byte[] buf = new byte[16];
		input.read(buf);
		cipher.init(Cipher.DECRYPT_MODE, deriveKey,
				new IvParameterSpec(buf));
		return new CipherInputStream(input, cipher);
	}
	
	public static OutputStream wrapStreamWithEncryption(OutputStream cipherText, String encryptionKey, int keylength)
			throws Exception {
		Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding", "SunJCE");
		SecretKey deriveKey = deriveKey(encryptionKey, keylength);
		SecureRandom secureRandom = new SecureRandom();
		byte[] buf = new byte[16];
		secureRandom.nextBytes(buf);
		cipher.init(Cipher.ENCRYPT_MODE, deriveKey,
				new IvParameterSpec(buf));
		cipherText.write(buf);
		return new CipherOutputStream(cipherText, cipher);
	}
	
}