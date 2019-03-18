package csl.actor.remote;

import java.nio.channels.SocketChannel;
import java.util.Objects;

public class ActorAddress {

    public Object getKey() {
        return this;
    }

    public ActorAddressDefault create(String host, int port) {
        return new ActorAddressDefault(host, port);
    }

    public static class ActorAddressDefault extends ActorAddress {
        protected String host;
        protected int port;

        public ActorAddressDefault(String host, int port) {
            this.host = host;
            this.port = port;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ActorAddressDefault that = (ActorAddressDefault) o;
            return port == that.port &&
                    Objects.equals(host, that.host);
        }

        @Override
        public int hashCode() {
            return Objects.hash(host, port);
        }

        public SocketChannel createChannel(ActorSystemRemote system) {
            return system.createChannel(host, port);
        }
    }
}
