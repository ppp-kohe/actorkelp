package csl.actor.kelp.shuffle;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import csl.actor.Actor;
import csl.actor.ActorRef;
import csl.actor.ActorSystem;
import csl.actor.Message;
import csl.actor.kelp.ActorKelp;
import csl.actor.kelp.ConfigKelp;
import csl.actor.kelp.KelpStage;
import csl.actor.kelp.behavior.KelpDispatcher;
import csl.actor.util.Staging;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ActorRefCombinedKelp<ActorType extends Actor> implements KelpStage<ActorType>, Serializable, KryoSerializable,
        Staging.StagingPointComposite {
    public static final long serialVersionUID = -1;
    protected transient ActorSystem system;
    protected transient List<DispatchUnitMember> memberUnits;
    /** {@link Staging.StagingPoint} or {@link DispatchUnitMember} */
    protected List<ActorRef> members;
    protected String name;

    public static ActorRef combine(ActorSystem system, String name, ActorRef existing, ActorRef next) {
        if (next == null) { //this means clearing as null
            return null; //TODO ?
        } else if (existing instanceof ActorRefCombinedKelp<?>) {
            ((ActorRefCombinedKelp<?>) existing).addMember(next);
            return existing;
        } else if (existing == null) {
            return next;
        } else {
            return new ActorRefCombinedKelp<>(system, name, Arrays.asList(existing, next));
        }
    }

    public ActorRefCombinedKelp() {}

    public ActorRefCombinedKelp(ActorSystem system) {
        this.system = system;
        this.name = Staging.stageNameArray("combine");
    }

    public ActorRefCombinedKelp(ActorSystem system, String name, List<? extends ActorRef> members) {
        this.system = system;
        this.members = new ArrayList<>();
        this.name = name;
        members.forEach(this::addMember);
    }

    public void addMember(ActorRef ref) {
        if (ref instanceof Staging.StagingPoint) {
            members.add(ref);
        } else {
            members.add(new DispatchUnitMember(ref, members.size()));
        }
    }

    @Override
    public void tellMessage(Message<?> message) {
        for (ActorRef member : members) {
            member.tellMessage(message);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public Class<ActorType> getActorType() {
        return (Class<ActorType>) ActorKelp.class;
    }

    @Override
    public <NextActorType extends Actor> KelpStage<NextActorType> connects(Class<NextActorType> actorType, ActorRef ref) {
        return new ActorRefCombinedKelp<>(system, name, members.stream()
                .map(m -> connectMember(m, actorType, ref))
                .collect(Collectors.toList()));
    }

    public <NextActorType extends Actor> KelpStage<?> connectMember(ActorRef member, Class<NextActorType> actorType, ActorRef ref) {
        if (member instanceof KelpStage<?>) {
            return ((KelpStage<?>) member).connects(actorType, ref);
        } else if (member instanceof DispatchUnitMember) {
            return connectMember(((DispatchUnitMember) member).getMember(), actorType, ref);
        } else {
            return ActorKelp.toKelpStage(getSystem(), actorType, ref, 0); //TODO
        }
    }

    /**
     * @return list of {@link Staging.StagingPoint} or internally created {@link DispatchUnitMember}.
     *  Also, the latter is {@link Staging.StagingPointMembers}, thus all of members are {@link Staging.StagingPoint}
     */
    public List<ActorRef> getMembers() {
        return members;
    }

    @Override
    public List<ActorRef> getMemberActors() {
        return new ArrayList<>(getStagingSubjectActors());
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public ActorSystem getSystem() {
        return system;
    }


    @Override
    public ActorType merge() {
        return null; //TODO
    }

    @Override
    public ActorType getMergedState() {
        return null; //TODO
    }

    @Override
    public <StateType> StateType merge(BiFunction<ActorSystem, ConfigKelp, ? extends ActorKelpStateSharing<ActorType, StateType>> factory) {
        return null; //TODO
    }
    @Override
    public void forEach(Consumer<KelpDispatcher.DispatchUnit> task) {
        getDispatchUnits().forEach(task);
    }

    @Override
    public List<? extends KelpDispatcher.DispatchUnit> getDispatchUnits() {
        if (memberUnits == null) {
            memberUnits = IntStream.range(0, members.size())
                    .mapToObj(i -> toMember(members.get(i), i))
                    .collect(Collectors.toList());
        }
        return memberUnits;
    }

    protected DispatchUnitMember toMember(ActorRef member, int i) {
        if (member instanceof DispatchUnitMember) {
            return (DispatchUnitMember) member;
        } else {
            return new DispatchUnitMember(member, i);
        }
    }

    public static class DispatchUnitMember implements KelpDispatcher.DispatchUnit, Staging.StagingPointMembers, Serializable {
        public static final long serialVersionUID = -1;
        public ActorRef member;
        public int index;
        public String name;

        public DispatchUnitMember() {}

        public DispatchUnitMember(ActorRef member, int index) {
            this.member = member;
            this.index = index;
            this.name = Staging.name(member);
        }

        @Override
        public String getName() {
            return name;
        }

        public ActorRef getMember() {
            return member;
        }

        @Override
        public List<ActorRef> getMemberActors() {
            if (member instanceof Staging.StagingNonSubject) {
                return new ArrayList<>(((Staging.StagingNonSubject) member).getStagingSubjectActors());
            } else {
                return Collections.singletonList(member);
            }
        }

        @Override
        public boolean hasRemainingMessage() {
            if (member instanceof KelpDispatcher.DispatchUnit) {
                return ((KelpDispatcher.DispatchUnit) member).hasRemainingMessage();
            } else if (member instanceof KelpStage<?>) {
                return ((KelpStage<?>) member).hasRemainingMessage();
            } else {
                return false;
            }
        }

        @Override
        public void tell(Object data) {
            member.tell(data);
        }

        @Override
        public void tellMessage(Message<?> message) {
            member.tellMessage(message);
        }

        @Override
        public Actor asLocal() {
            return member.asLocal();
        }

        @Override
        public int getIndex() {
            return index;
        }

        @Override
        public void flush() {
            ActorRefShuffle.flush(member);
        }
    }

    @Override
    public KelpDispatcher.DispatchUnit getDispatchUnit(int index) {
        return getDispatchUnits().get(index);
    }

    @Override
    public void write(Kryo kryo, Output output) {
        output.writeString(name);
        kryo.writeObject(output, members);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void read(Kryo kryo, Input input) {
        name = input.readString();
        members = (List<ActorRef>) kryo.readObject(input, List.class);
    }

    @Override
    public void flush() {
        getDispatchUnits()
                .forEach(KelpDispatcher.DispatchUnit::flush);
    }

    @Override
    public List<? extends Staging.StagingPoint> getSubPoints() {
        return this.members.stream()
                .map(Staging.StagingPoint.class::cast)
                .collect(Collectors.toList());
    }

}
