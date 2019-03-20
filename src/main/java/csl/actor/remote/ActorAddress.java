package csl.actor.remote;

import java.io.Serializable;
import java.util.Objects;

public class ActorAddress {

    public Object getKey() {
        return this;
    }

    public static ActorAddressRemote create(String host, int port) {
        return new ActorAddressRemote(host, port);
    }

    public static ActorAddressRemoteActor create(String host, int port, String name) {
        return create(host, port).getActor(name);
    }

    public static class ActorAddressRemote extends ActorAddress implements Serializable {
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
        public int hashCode() {
            return Objects.hash(host, port);
        }

        @Override
        public String toString() {
            return host + ":" + port;
        }

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
    }

    public static class ActorAddressRemoteActor extends ActorAddressRemote {
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
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (!super.equals(o)) return false;
            ActorAddressRemoteActor that = (ActorAddressRemoteActor) o;
            return Objects.equals(actorName, that.actorName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), actorName);
        }

        @Override
        public String toString() {
            return super.toString() + "/" + actorName;
        }
    }
}
