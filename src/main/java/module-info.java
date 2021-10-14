open module actorkelp {
    exports csl.actor;
    exports csl.actor.cluster;
    exports csl.actor.kelp;
    exports csl.actor.kelp.behavior;
    exports csl.actor.kelp.shuffle;
    exports csl.actor.persist;
    exports csl.actor.remote;

    requires java.base;
    requires java.sql;

    requires transitive com.esotericsoftware.kryo;
    requires transitive com.esotericsoftware.reflectasm;
    requires transitive org.objenesis;
    requires transitive io.netty.transport;
    requires transitive io.netty.buffer;
    requires transitive io.netty.handler;
    requires transitive io.netty.codec.http;
    requires transitive io.netty.codec;
    requires transitive io.netty.common;
}
