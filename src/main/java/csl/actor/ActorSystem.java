package csl.actor;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public interface ActorSystem extends Executor, AutoCloseable {
    boolean register(Actor actor);
    boolean unregister(Actor actor);
    void send(Message<?> message);

    Actor resolveActorLocalNamed(ActorRefLocalNamed ref);
    void close();

    int getThreads();
    ScheduledExecutorService getScheduledExecutor();

    /**
     * awaiting termination of executors. it needs to call {@link #close()}
     * @param time the time amount
     * @param unit the unit of the time
     * @throws InterruptedException caused by time-out
     */
    void awaitClose(long time, TimeUnit unit) throws InterruptedException;

    SystemLogger getLogger();

    interface SystemLogger {
        default void log(String fmt, Object... args) {
            log( -1, fmt, args);
        }
        default void log(int color, String fmt, Object... args) {
            log(true, color, fmt, args);
        }
        void log(boolean flag, int color, String fmt, Object... args);
        void log(boolean flag, int color, Throwable ex, String fmt, Object... args);

        default void log(Throwable ex, String fmt, Object... args) {
            log( -1, ex, fmt, args);
        }

        default void log(int color, Throwable ex, String fmt, Object... args) {
            log(true, color, ex, fmt, args);
        }


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

    static OffsetDateTime timeForLog(Instant t) {
        return OffsetDateTime.ofInstant(t, ZoneOffset.systemDefault());
    }

    default boolean isSpecialMessage(Message<?> message) {
        if (message != null) {
            return isSpecialMessageData(message.getData());
        } else {
            return false;
        }
    }

    default boolean isSpecialMessageData(Object data) {
        return data instanceof Message.MessageDataSpecial ||
                isSpecialMessageClocked(data) ||
                (data instanceof Message.MessageDataHolder<?> &&
                        isSpecialMessageData(((Message.MessageDataHolder<?>) data).getData()));
    }

    default boolean isSpecialMessageClocked(Object data) {
        if (data instanceof Message.MessageDataClock<?>) {
            Object clockData = ((Message.MessageDataClock<?>) data).body;
            if (clockData instanceof Message<?>) {
                return isSpecialMessage((Message<?>) clockData);
            }
        }
        return false;
    }

    ProcessMessage createProcessMessageSubsequently(Actor target, boolean special);

    interface ProcessMessage extends Runnable {
        void submit(Message<?> msg);
    }
}
