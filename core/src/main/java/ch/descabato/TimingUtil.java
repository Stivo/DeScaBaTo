package ch.descabato;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;

public class TimingUtil {
    private static ThreadLocal<ThreadMXBean> beans = new ThreadLocal<ThreadMXBean>();
    /** Get CPU time in nanoseconds. */
    public static long getCpuTime() {
        return System.nanoTime();
//        if (beans.get() == null) {
//            beans.set(ManagementFactory.getThreadMXBean());
//        }
//        ThreadMXBean bean = beans.get();
//        if ( ! bean.isCurrentThreadCpuTimeSupported() )
//            return 0L;
//        return bean.getCurrentThreadCpuTime();
    }

}
