package ch.descabato;

import java.util.Map;

import javax.inject.Provider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.java.truecommons.key.spec.common.AesKeyStrength;
import net.java.truevfs.access.TArchiveDetector;
import net.java.truevfs.driver.zip.raes.SafeZipRaesDriver;
import net.java.truevfs.driver.zip.raes.crypto.RaesKeyException;
import net.java.truevfs.driver.zip.raes.crypto.RaesParameters;
import net.java.truevfs.driver.zip.raes.crypto.Type0RaesParameters;
import net.java.truevfs.kernel.spec.FsController;
import net.java.truevfs.kernel.spec.FsDriver;
import net.java.truevfs.kernel.spec.FsModel;
import net.java.truevfs.kernel.spec.FsScheme;

class TrueVfs {

	static Logger log = LoggerFactory.getLogger(TrueVfs.class);
	
	public static TArchiveDetector newArchiveDetector1(
			Provider<Map<FsScheme, FsDriver>> provider, String extensions,
			char[] password, int keylength) {
		return new TArchiveDetector(provider, extensions,
				new CustomZipRaesDriver1(password, keylength));
	}

	private static final class CustomZipRaesDriver1 extends SafeZipRaesDriver {

		final RaesParameters param;
		private int keylength;

		CustomZipRaesDriver1(char[] password, int keylength) {
			this.keylength = keylength;
			param = new CustomRaesParameters(password, keylength);
		}

		@Override
		protected RaesParameters raesParameters(FsModel model) {
			// If you need the URI of the particular archive file, then call
			// model.getMountPoint().toUri().
			// If you need a more user friendly form of this URI, then call
			// model.getMountPoint().toHierarchicalUri().

			// Let's not use the key manager but instead our custom parameters.
			return param;
		}

		@Override
		public FsController decorate(FsController controller) {
			// This is a minor improvement: The default implementation decorates
			// the default file system controller chain with a package private
			// file system controller which uses the key manager to keep track
			// of the encryption parameters.
			// Because we are not using the key manager, we don't need this
			// special purpose file system controller and can simply return the
			// given file system controller chain instead.
			return controller;
		}
	} // CustomZipRaesDriver

	private static final class CustomRaesParameters implements
			Type0RaesParameters {

		final char[] password;
		private int keylength;

		CustomRaesParameters(final char[] password, int keylength) {
			this.keylength = keylength;
			this.password = password.clone();
		}

		@Override
		public char[] getPasswordForWriting() throws RaesKeyException {
			return password.clone();
		}

		@Override
		public char[] getPasswordForReading(boolean invalid)
				throws RaesKeyException {
			if (invalid)
				throw new RaesKeyException("Invalid password!");
			return password.clone();
		}

		@Override
		public AesKeyStrength getKeyStrength() throws RaesKeyException {
			try {
				return AesKeyStrength.valueOf("BITS_"+keylength);
			} catch (IllegalArgumentException iae){
				log.warn("Invalied keylength "+keylength+", using 128");
			}
			return AesKeyStrength.BITS_128;
		}

		@Override
		public void setKeyStrength(AesKeyStrength keyStrength)
				throws RaesKeyException {
			// ignore?
		}
	} // CustomRaesParameters

}