package ch.descabato;

import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;

/**
 * Needed because scala doesnt use the same visibility rules as Java.
 * Java makes the constructor public from another package, sclaa can use it.
 */
public class FileVisitorHelper extends SimpleFileVisitor<Path> {
	
	public FileVisitorHelper() {
		
	}

}
