package csl.actor.util;

import csl.actor.ActorSystem;
import csl.actor.remote.ActorAddress;
import csl.actor.remote.ActorSystemRemote;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.regex.Pattern;

public interface PathModifier {
    Path get(String path);

    default Path getExpanded(String path) {
        return get(expandPath(path));
    }

    default String expandPath(String path) {
        return path;
    }

    class PathModifierHost implements PathModifier {
        protected String baseDir;

        protected String host;
        protected String app;

        public PathModifierHost(String baseDir) {
            this.baseDir = baseDir;
        }

        public PathModifierHost setHost(String host) {
            this.host = host;
            return this;
        }

        public PathModifierHost setHost(String host, int port) {
            String hostId = toOutputFileComponent(false, 18, host) + "-" +
                    toOutputFileComponent(false, 8, Integer.toString(port));
            return setHost(hostId);
        }

        public PathModifierHost setApp(String app) {
            this.app = app;
            return this;
        }

        public String getBaseDir() {
            return baseDir;
        }

        public String getHost() {
            return host;
        }

        public String getApp() {
            return app;
        }

        @Override
        public Path get(String path) {
            return Paths.get(baseDir, expandPath(path)).normalize();
        }

        @Override
        public String expandPath(String path) {
            return path.replaceAll(Pattern.quote("%h"), host)
                       .replaceAll(Pattern.quote("%a"), app);
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() +
                    "(baseDir=" + baseDir + ", host=" + host + ", app=" + app + ")";
        }
    }

    static String toOutputFileComponent(boolean preferRight, int max, String str) {
        return toOutputFileComponentMinus(preferRight, max, str.replaceAll("\\W+", "-"));
    }

    private static String toOutputFileComponentMinus(boolean preferRight, int max, String str) {
        if (str.length() > max) {
            int n = (preferRight ? str.lastIndexOf("-") : str.indexOf("-"));
            if (n >= 0 && max > 8) {
                int nextMax = Math.max(max / 2 - 1, 8);
                String pre = toOutputFileComponentMinus(preferRight, nextMax, str.substring(0, n));
                String pos = toOutputFileComponentMinus(preferRight, nextMax, str.substring(n + 1));
                if (pre.length() + pos.length() + 1 > max) {
                    if (pre.length() + 1 + 8 <= max) {
                        return pre + "-" + Integer.toHexString(pos.hashCode());
                    } else if (8 + 1 + pos.length() <= max) {
                        return Integer.toHexString(pre.hashCode()) + "-" + pos;
                    } else {
                        return Integer.toHexString(str.hashCode());
                    }
                } else {
                    return pre + "-" + pos;
                }
            } else {
                return Integer.toHexString(str.hashCode());
            }
        } else {
            return str;
        }
    }


    static PathModifier.PathModifierHost createDefaultPathModifier(ActorSystem system) {
        PathModifier.PathModifierHost pm = new PathModifier.PathModifierHost(".");
        ActorAddress.ActorAddressRemote addr;
        if (system instanceof ActorSystemRemote &&
                (addr = ((ActorSystemRemote) system).getServerAddress()) != null) {
            pm.setHost(addr.getHost(), addr.getPort());
        } else {
            pm.setHost("local");
        }
        pm.setApp(getAppName("app"));
        return pm;
    }

    static String getAppName(String head) {
        Instant now = Instant.now();
        OffsetDateTime time = OffsetDateTime.ofInstant(now, ZoneOffset.UTC);
        int milli = (time.getHour() * 60 * 60 + time.getMinute() * 60 + time.getSecond()) * 1000
                + (time.getNano() / 1000_000);
        //nano max: 999,999,999 /1m ->         999
        //hour max: 23 * 60^2 *1k ->    82,800,000
        //min max : 59 * 60   *1k ->     3,540,000
        //sec max : 59        *1k ->        59,000
        //total max:                    86,399,999 -hex-> 5265bff -len-> 7
        String milliStr = String.format("%h", milli);
        while (milliStr.length() < 7) {
            milliStr = "0" + milliStr;
        }
        return String.format("%s-%s-%s", head,
                time.format(DateTimeFormatter.ofPattern("uu-MM-dd")), milliStr);
    }

    Map<ActorSystem, PathModifier> pathModifiers = new WeakHashMap<>();

    static PathModifier getPathModifier(ActorSystem system) {
        synchronized (pathModifiers) {
            return pathModifiers.computeIfAbsent(system, PathModifier::createDefaultPathModifier);
        }
    }

    static void setPathModifier(ActorSystem system, PathModifier m) {
        synchronized (pathModifiers) {
            pathModifiers.put(system, m);
        }
    }

    static PathModifier.PathModifierHost setPathModifierWithBaseDir(ActorSystem system, String baseDir) {
        PathModifier.PathModifierHost h = new PathModifier.PathModifierHost(baseDir);
        setPathModifier(system, h);
        return h;
    }
}
