package csl.actor.remote;

import csl.actor.Actor;
import csl.actor.ActorRefLocalNamed;
import csl.actor.ActorSystem;

import java.io.Serializable;
import java.util.Objects;

public abstract class ActorAddress {

    public Object getKey() {
        return this;
    }

    public abstract ActorAddressRemote getHostAddress();

    public static ActorAddressRemote get(String hostAndPort) {
        String[] cols = hostAndPort.split(":");
        if (cols.length >= 3) {
            return get(cols[0], Integer.parseInt(cols[1]), cols[2]);
        } else {
            return get(cols[0], Integer.parseInt(cols[1]));
        }
    }

    public static ActorAddressRemote get(String host, int port) {
        return new ActorAddressRemote(host, port);
    }

    public static ActorAddressRemoteActor get(String host, int port, String name) {
        return get(host, port).getActor(name);
    }

    public ActorRefLocalNamed toLocal(ActorSystem system) {
        return new ActorRefLocalNamed.ActorRefLocalNamedNoName(system, this);
    }

    public abstract ActorAddressRemoteActor getActor(String name);

    public static class ActorAddressRemote extends ActorAddress implements Serializable {
        public static final long serialVersionUID = 1L;
        protected String host;
        protected int port;

        public ActorAddressRemote(String host, int port) {
            this.host = host;
            this.port = port;
        }

        public ActorAddressRemote() {}

        public String getHost() {
            return host;
        }

        public int getPort() {
            return port;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ActorAddressRemote that = (ActorAddressRemote) o;
            return port == that.port &&
                    Objects.equals(host, that.host);
        }

        @Override
        public ActorAddressRemote getHostAddress() {
            return this;
        }

        @Override
        public int hashCode() {
            return Objects.hash(host, port);
        }

        @Override
        public String toString() {
            return host + ":" + port;
        }

        @Override
        public ActorAddressRemoteActor getActor(String name) {
            return new ActorAddressRemoteActor(host, port, name);
        }

        public boolean equalsHost(ActorAddress addr) {
            if (addr instanceof ActorAddressRemote) {
                ActorAddressRemote r = (ActorAddressRemote) addr;
                return Objects.equals(this.host, r.getHost())
                        && this.port == r.port;
            } else {
                return false;
            }
        }

        public ActorAddressRemoteActor getActor(Actor a) {
            String n = a.getName();
            if (n == null) {
                return new ActorAddressAnonymousActor(host, port, a.getClass().getName(), System.identityHashCode(a));
            } else {
                return getActor(n);
            }
        }

        public ActorAddressRemoteActor getActor(ActorRefLocalNamed local) {
            if (local instanceof ActorRefLocalNamed.ActorRefLocalNamedNoName) {
                ActorAddress addr = ((ActorRefLocalNamed.ActorRefLocalNamedNoName) local).getOrigin();
                if (addr instanceof ActorAddressRemoteActor) {
                    return (ActorAddressRemoteActor) addr;
                } else if (addr instanceof ActorAddressRemote) {
                    ActorAddressRemote remote = (ActorAddressRemote) addr;
                    return new ActorAddressAnonymousActor(remote.getHost(), remote.getPort(), "?", -1);
                } else {
                    return new ActorAddressError("localhost", -1, Objects.toString(addr));
                }
            } else {
                String n = local.getName();
                if (n == null) {
                    return new ActorAddressError(host, port, local.toString());
                } else {
                    return getActor(n);
                }
            }
        }
    }

    public static class ActorAddressRemoteActor extends ActorAddressRemote {
        public static final long serialVersionUID = 1L;
        protected String actorName;

        public ActorAddressRemoteActor(String host, int port, String actorName) {
            super(host, port);
            this.actorName = actorName;
        }

        public ActorAddressRemoteActor() {}

        public String getActorName() {
            return actorName;
        }

        @Override
        public ActorAddressRemote getHostAddress() {
            return new ActorAddressRemote(host, port);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (!super.equals(o)) return false;
            ActorAddressRemoteActor that = (ActorAddressRemoteActor) o;
            return Objects.equals(actorName, that.actorName);
        }

        public ActorRefRemote ref(ActorSystem system) {
            return ActorRefRemote.get(system, this);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), actorName);
        }

        @Override
        public String toString() {
            return super.toString() + "/" + actorName;
        }

        @Override
        public ActorRefLocalNamed toLocal(ActorSystem system) {
            return new ActorRefLocalNamed(system, actorName);
        }
    }

    public static class ActorAddressAnonymousActor extends ActorAddressRemoteActor {
        public static final long serialVersionUID = 1L;
        protected String typeName;
        protected int identityHashCode;

        public ActorAddressAnonymousActor(String host, int port, String typeName, int identityHashCode) {
            super(host, port, null);
            this.typeName = typeName;
            this.identityHashCode = identityHashCode;
        }

        @Override
        public String toString() {
            return super.toString() + "#anon:" + typeName + "@" + Integer.toHexString(identityHashCode);
        }

        @Override
        public ActorRefLocalNamed toLocal(ActorSystem system) {
            return new ActorRefLocalNamed.ActorRefLocalNamedNoName(system, this);
        }
    }

    public static class ActorAddressError extends ActorAddressRemoteActor {
        public static final long serialVersionUID = 1L;
        protected String info;

        public ActorAddressError(String host, int port, String info) {
            super(host, port, null);
            this.info = info;
        }

        @Override
        public String toString() {
            return super.toString() + "#" + info;
        }
    }
}
