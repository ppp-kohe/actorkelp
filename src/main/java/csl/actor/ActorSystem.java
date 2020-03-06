package csl.actor;

import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public interface ActorSystem extends Executor, AutoCloseable {
    void register(Actor actor);
    void unregister(String actorName);
    void send(Message<?> message);

    Actor resolveActorLocalNamed(ActorRefLocalNamed ref);
    void close();

    int getThreads();
    ScheduledExecutorService getScheduledExecutor();

    void awaitClose(long time, TimeUnit unit) throws InterruptedException;

    SystemLogger getLogger();

    interface SystemLogger {
        default void log(String fmt, Object... args) {
            log(true, -1, fmt, args);
        }
        void log(boolean flag, int color, String fmt, Object... args);
        void log(boolean flag, int color, Throwable ex, String fmt, Object... args);

        default SystemLogToStringLimit toStringLimit(Object o) {
            return new SystemLogToStringLimit(o);
        }
    }

    static int systemPropertyColor(String name, int defVal) {
        String n = System.getProperty(name);
        if (n == null) {
            return defVal;
        } else {
            try {
                return Integer.parseInt(n);
            } catch (Exception ex) {
                System.err.println("systemPropertyColor(" + name + ", " + defVal + "): " + ex);
                ex.printStackTrace();
                return defVal;
            }
        }
    }

    class SystemLogToStringLimit {
        protected Object o;

        public SystemLogToStringLimit(Object o) {
            this.o = o;
        }

        @Override
        public String toString() {
            String s = Objects.toString(o);
            if (s.length() > 128) {
                return s.substring(0, 120) + String.format("...[%,d]", s.length());
            } else {
                return s;
            }
        }
    }
}
