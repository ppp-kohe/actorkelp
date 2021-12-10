package csl.actor.util;

import csl.actor.ActorSystem;
import csl.actor.remote.ActorAddress;
import csl.actor.remote.ActorSystemRemote;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.regex.Pattern;

/**
 * {@link #setPathModifierWithBaseDir(ActorSystem, String)} :
 * baseDir + (subPath with replacing "%a" or "%h")
 *  <ul>
 *      <li><code>%a</code> : <code>app-uu-MM-dd-%07h</code>.
 *         the string "app" and the UTC date uu-MM-dd and the UTC time (milliseconds) of the day as hex</li>
 *      <li><code>%h</code> : <code>host-port</code>.
 *        the host name up to 18 chars and the port is up to 8 chars. Non word chars are replaced to "-"</li>
 *
 *     <li>absolute paths ({@link Path#isAbsolute()}): the default path-modifier created by {@link #setPathModifierWithBaseDir(ActorSystem, String)}
 *          can ignore the baseDir if the expanded path is an absolute path.
 *          Som custom path-modifiers for clusters might disallow it.</li>
 *      <li>top <code>%l</code> : <code>local-only-base-path</code>. another baseDir intended use for local only files.
 *         if no path is set, it will be ignored.
 *      </li>
 *  </ul>
 *
 *  example:
 *  <pre>
 *      PathModifier pm = PathModifier.setPathModifierWithBaseDir("/mnt/mydir_on_h1")
 *                        .setHost("h1", 3000).setApp("myapp");
 *      String path = pm.expandPath("%a/%h/out.txt"); //=&gt; "myapp-20-07-26-5265bff/h1-3000/out.txt"
 *        //the expandPath does not include the baseDir
 *        //So it can transfer to another host and apply the path to get(path)
 *      ...
 *      //in another host
 *      PathModifier pm = PathModifier.setPathModifierWithBaseDir("/mnt/mydir_on_h2")
 *                        .setHost("h2", 3000).setApp("myapp");
 *      String path = ...; //transferred from the h1 as "myapp-20-07-26-5265bff/h1-3000/out.txt"
 *      String p2 = pm.expandPath(path); //p2.equals(path)
 *
 *      pm.get(path); //=&gt; "/mnt/mydir_on_h2/myapp-20-07-26-5265bff/h1-3000/out.txt"
 *  </pre>
 */
public interface PathModifier {
    Path get(String expandedPath);

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
        protected String localOnlyBaseDir;

        protected boolean allowAbsolutePath = false;

        public PathModifierHost(String baseDir) {
            if (baseDir == null) {
                baseDir = "";
            }
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

        public PathModifierHost setLocalOnlyBaseDir(String localOnlyBaseDir) {
            if (localOnlyBaseDir != null && localOnlyBaseDir.endsWith("/")) {
                return setLocalOnlyBaseDir(localOnlyBaseDir.substring(0, localOnlyBaseDir.length() - 1));
            } else {
                this.localOnlyBaseDir = localOnlyBaseDir;
                return this;
            }
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

        public String getLocalOnlyBaseDir() {
            return localOnlyBaseDir;
        }

        public boolean isAllowAbsolutePath() {
            return allowAbsolutePath;
        }

        public PathModifierHost setAllowAbsolutePath(boolean allowAbsolutePath) {
            this.allowAbsolutePath = allowAbsolutePath;
            return this;
        }

        @Override
        public Path get(String expandedPath) {
            String ep = expandPath(expandedPath);
            boolean localOnlyAbs = false;
            if (ep.startsWith(localOnlyBaseDir + "/")) {
                localOnlyAbs = true;
            }
            String baseDir = this.baseDir.replaceAll("/", File.separator);
            ep = ep.replaceAll("/", File.separator);
            if (allowAbsolutePath || localOnlyAbs) {
                Path p = Paths.get(ep);
                if (p.isAbsolute() || localOnlyAbs) {
                    return p.normalize();
                } else {
                    return Paths.get(baseDir, ep).normalize();
                }
            } else {
                return Paths.get(baseDir, ep).normalize();
            }
        }

        @Override
        public String expandPath(String path) {
            if (path.startsWith("%l")) {
                if (localOnlyBaseDir == null || localOnlyBaseDir.isEmpty()) {
                    path = path.substring(2);
                    if (path.startsWith("/")) {
                        path = path.substring(1);
                    }
                } else {
                    path = path.substring(2);
                    path = localOnlyBaseDir + (path.startsWith("/") ? "" : "/") + path;
                }
            }
            return path.replaceAll(Pattern.quote("%h"), host)
                       .replaceAll(Pattern.quote("%a"), app);
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() +
                    "(baseDir=" + baseDir + ", host=" + host + ", app=" + app +
                        ", localBase=" + localOnlyBaseDir +
                        ", allowAbs=" + allowAbsolutePath + ")";
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
        return PathModifier.createDefaultPathModifier(system, ".");
    }

    static PathModifier.PathModifierHost createDefaultPathModifier(ActorSystem system, String baseDir) {
        PathModifier.PathModifierHost pm = new PathModifier.PathModifierHost(baseDir)
                .setAllowAbsolutePath(true);
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
        PathModifier.PathModifierHost h = PathModifier.createDefaultPathModifier(system, baseDir);
        setPathModifier(system, h);
        return h;
    }
}
