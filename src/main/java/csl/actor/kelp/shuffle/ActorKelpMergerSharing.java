package csl.actor.kelp.shuffle;

import csl.actor.ActorRef;
import csl.actor.ActorSystem;
import csl.actor.kelp.ActorKelp;
import csl.actor.kelp.ActorKelpSerializable;
import csl.actor.kelp.ConfigKelp;

import java.util.List;
import java.util.UUID;

public class ActorKelpMergerSharing<ActorType extends ActorKelp<ActorType>> extends ActorKelpStateSharing<ActorType, ActorKelpSerializable<ActorType>>
        implements AutoCloseable {

    public ActorKelpMergerSharing(ActorSystem system, ConfigKelp config) {
        this(system, config, UUID.randomUUID());
    }

    public ActorKelpMergerSharing(ActorSystem system, ConfigKelp config, UUID id) {
        super(system, config, id);
    }

    @Override
    public ToStateFunction<ActorType, ActorKelpSerializable<ActorType>> getToState() {
        return (k) -> {
            ActorKelpSerializable<ActorType> s = k.toSerializable(false);
            s.internalStateUsed = true;
            return s;
        };
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    public SetStateFunction<ActorType, ActorKelpSerializable<ActorType>> getSetState() {
        return (k,state) ->
            k.setSerializable((ActorKelpSerializable) state);
    }

    public ActorType mergeToLocalSync(List<? extends ActorRef> members) {
        try {
            share = false;
            ActorKelpSerializable<ActorType> k = mergeSync(members);
            return (k == null ? null : k.restoreMerge(system, config));
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public ActorKelpSerializable<ActorType> merge(ActorKelpSerializable<ActorType> l, ActorKelpSerializable<ActorType> r) {
        try {
            ActorType tmp = temporaryActor(l);
            tmp.merge(r);
            return tmp.toSerializable(false);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public ActorType temporaryActor(ActorKelpSerializable<ActorType> state) throws Exception {
        ActorType tmp = state.create(system, null, config);
        tmp.setNameRandom();
        return tmp;
    }

}
