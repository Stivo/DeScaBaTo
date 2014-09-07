package ch.descabato;

import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;

/**
 * Needed because scala doesn't use the same visibility rules as Java.
 * Java makes the constructor public from another package, scala can use it.
 */
public class FileVisitorHelper extends SimpleFileVisitor<Path> {
	
	public FileVisitorHelper() {
		
	}

}
