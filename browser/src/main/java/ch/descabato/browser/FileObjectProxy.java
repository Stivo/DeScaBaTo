package ch.descabato.browser;

import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.provider.AbstractFileName;
import org.apache.commons.vfs2.provider.AbstractFileObject;
import org.apache.commons.vfs2.provider.AbstractFileSystem;

public abstract class FileObjectProxy extends AbstractFileObject {
    /**
     * @param name the file name - muse be an instance of {@link AbstractFileName}
     * @param fs   the file system
     * @throws ClassCastException if {@code name} is not an instance of {@link AbstractFileName}
     */
    public FileObjectProxy(AbstractFileName name, AbstractFileSystem fs) {
        super(name, fs);
    }
}
