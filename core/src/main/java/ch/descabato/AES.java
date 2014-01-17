package ch.descabato;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.KeySpec;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.CipherOutputStream;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;

public class AES {

	public static boolean testMode = false;
	
	public static ThreadLocal<Map<String, SecretKeySpec>> local = new ThreadLocal<>();
	
	public static SecretKey deriveKey(String password, int keylength) throws NoSuchAlgorithmException, InvalidKeySpecException {
		if (local.get()== null) {
			local.set(new HashMap<String, SecretKeySpec>());
		}
		String key = password+keylength;
		SecretKeySpec keySpec = local.get().get(password+keylength);
		if (keySpec != null) {
			return keySpec;
		}
		Random sr = new Random(6877347960117378046L);
	    byte[] salt = new byte[1024];
		sr.nextBytes(salt);
	    SecretKeyFactory kf = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA1");
	    KeySpec specs = new PBEKeySpec(password.toCharArray(), salt, 1024, keylength);
	    SecretKey generateSecret = kf.generateSecret(specs);
	    keySpec = new SecretKeySpec(generateSecret.getEncoded(), "AES");
	    local.get().put(key, keySpec);
	    return keySpec;
	}
	
	public static InputStream wrapStreamWithDecryption(InputStream input, String encryptionKey, int keylength)
			throws Exception {
		Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding", "SunJCE");
		byte[] buf = new byte[16];
		input.read(buf);
		SecretKey deriveKey = deriveKey(encryptionKey, keylength);
		cipher.init(Cipher.DECRYPT_MODE, deriveKey,
				new IvParameterSpec(buf));
		return new CipherInputStream(input, cipher);
	}
	
	public static OutputStream wrapStreamWithEncryption(OutputStream cipherText, String encryptionKey, int keylength)
			throws Exception {
		Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding", "SunJCE");
		SecretKey deriveKey = deriveKey(encryptionKey, keylength);
		byte[] buf = new byte[16];
		if (testMode) {
			Random random = new Random(25335235);
			random.nextBytes(buf);
		} else {
			SecureRandom secureRandom = new SecureRandom();
			secureRandom.nextBytes(buf);
		}
		cipher.init(Cipher.ENCRYPT_MODE, deriveKey,
				new IvParameterSpec(buf));
		cipherText.write(buf);
		return new CipherOutputStream(cipherText, cipher);
	}
	
}