package csl.actor.util;

import csl.actor.Actor;
import csl.actor.ActorRef;
import csl.actor.ActorRefLocalNamed;
import csl.actor.Message;
import csl.actor.remote.ActorAddress;

import java.io.Serializable;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class Staging {

    public static String name(ActorRef ref) {
        String head2 = null;
        if (ref instanceof Actor) {
            head2 = ((Actor) ref).getName();
        } else if (ref instanceof ActorRefLocalNamed) {
            head2 = ((ActorRefLocalNamed) ref).getName();
        } else if (ref instanceof ActorAddress.ActorAddressRemoteActor) {
            head2 = ((ActorAddress.ActorAddressRemoteActor) ref).getActorName();
        } else if (ref instanceof StagingPoint) {
            head2 = ((StagingPoint) ref).getName();
        }
        return head2;
    }

    public static Pattern PATTERN_ID = Pattern.compile(Pattern.quote(Actor.NAME_ID_SEPARATOR) + "([0-9A-Za-z]+)-([0-9A-Za-z-]+)");

    public static String shortenId(String n) {
        return PATTERN_ID.matcher(n == null ? "" : n).replaceAll(
                Matcher.quoteReplacement(Actor.NAME_ID_SEPARATOR) + "$1"); //replace T@123ab-cdef012 -> T@123ab
    }

    public static String stageNameArray(String... names) {
        StringBuilder buf = new StringBuilder();
        for (String n : names) {
            if (n != null) {
                if (buf.length() != 0) {
                    buf.append(Actor.NAME_SYSTEM_SEPARATOR);
                }
                n = shortenId(n);
                buf.append(n);
            }
        }
        if (buf.length() == 0) {
            buf.append(Actor.NAME_SYSTEM_SEPARATOR)
                    .append("stage");
        }
        return buf.append(Actor.NAME_ID_SEPARATOR)
                .append(UUID.randomUUID())
                .toString();
    }

    /**
     * interface for actors
     */
    public interface StagingSupported {
        String getName();
        default Iterable<? extends ActorRef> nextStageActors() {
            ActorRef ref = nextStageActor();
            if (ref == null) {
                return Collections.emptyList();
            } else if (ref instanceof StagingNonSubject) {
                return ((StagingNonSubject) ref).getStagingSubjectActors();
            } else {
                return Collections.singletonList(ref);
            }
        }

        default ActorRef nextStageActor() {
            return null;
        }

        /**
         * optional operation
         * @param ref the next stage reference
         */
        default void setNextStage(ActorRef ref) { }

        /**
         * invoked when a stating watcher exits the actor.
         * @param taskKey a key object indicating a path on the graph
         * @return false means it has remaining processes and the watcher cannot finish
         */
        default boolean processStageExited(Object taskKey) {
            return true;
        }

        /**
         * invoked when all path of the stage that the actor belongs are exited
         */
        default void stageEnd() {}
    }

    /**
     * the interface indicates that an {@link ActorRef} is not a subject,
     */
    public interface StagingNonSubject {
        default boolean isNonSubject() {
            return !(this instanceof Actor);
        }

        /**
         * @return non {@link StagingNonSubject} refs or refs whose {@link #isNonSubject()} returns true
         */
        List<? extends ActorRef> getStagingSubjectActors();
    }

    /**
     * the interface indicates that an {@link ActorRef} is an outgoing port for the next stage.
     * It might be a point containing multiple actors ({@link StagingPointMembers}), or
     *    a collection of sub-points ({@link StagingPointComposite})
     *   <p>
     * it does not include DispatcherUnit
     */
    public interface StagingPoint extends StagingNonSubject {
        String getName();

        default Map<String, List<ActorRef>> getStageNameToMemberActors() {
            Map<String, List<ActorRef>> refs = new LinkedHashMap<>();
            collectStageNameToMemberActors(refs, this);
            return refs;
        }

        @Override
        default List<? extends ActorRef> getStagingSubjectActors() {
            return getStageNameToMemberActors().values().stream()
                    .flatMap(List::stream)
                    .collect(Collectors.toList());
        }
    }

    public interface StagingPointComposite extends StagingPoint {
        List<? extends StagingPoint> getSubPoints();
    }

    public interface StagingPointMembers extends StagingPoint {
        List<ActorRef> getMemberActors();

        @Override
        default List<? extends ActorRef> getStagingSubjectActors() {
            return getMemberActors();
        }
    }

    public static void collectStageNameToMemberActors(Map<String, List<ActorRef>> map, StagingPoint point) {
        if (point instanceof StagingPointMembers) {
            String name = point.getName();
            map.put(name, ((StagingPointMembers) point).getMemberActors());
        }
        if (point instanceof StagingPointComposite) {
            ((StagingPointComposite) point).getSubPoints()
                    .forEach(p -> collectStageNameToMemberActors(map, p));
        }
        if (!(point instanceof StagingPointComposite) && !(point instanceof StagingPointMembers) &&
            point instanceof ActorRef) {
            map.put(point.getName(), Collections.singletonList((ActorRef) point));
        }
    }

    public static ActorRefCombined combine(ActorRef... refs) {
        return new ActorRefCombined(UUID.randomUUID().toString(),
                Arrays.asList(refs));
    }

    public static ActorRefCombined combine(Iterable<ActorRef> refs) {
        List<ActorRef> list = new ArrayList<>();
        refs.forEach(list::add);
        return new ActorRefCombined(stageNameArray("combine"),
                list);
    }

    /**
     * {@link ActorRef} for broadcasting messages to all combined actors
     */
    public static class ActorRefCombined implements ActorRef, StagingPointMembers, Serializable {
        public static final long serialVersionUID = -1;
        public String name;
        public List<ActorRef> members;
        public ActorRefCombined() {}

        public ActorRefCombined(String name) {
            this.name = name;
            members = new ArrayList<>();
        }

        public ActorRefCombined(String name, List<ActorRef> members) {
            this.name = name;
            this.members = members;
        }

        public ActorRefCombined addMember(ActorRef member) {
            members.add(member);
            return this;
        }

        @Override
        public List<ActorRef> getMemberActors() {
            return members;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public void tellMessage(Message<?> message) {
            members.forEach(m ->
                    m.tellMessage(message));
        }

        @Override
        public Map<String, List<ActorRef>> getStageNameToMemberActors() {
            LinkedHashMap<String, List<ActorRef>> nameToMems = new LinkedHashMap<>(members.size());
            members.forEach(m -> {
                if (m instanceof StagingPoint) {
                    ((StagingPoint) m).getStageNameToMemberActors()
                            .forEach((n, pm) ->
                                    nameToMems.computeIfAbsent(n, _n -> new ArrayList<>())
                                            .addAll(pm));
                } else {
                    nameToMems.computeIfAbsent(memberName(m), _k -> new ArrayList<>())
                            .add(m);
                }
            });
            return nameToMems;
        }

        protected String memberName(ActorRef ref) {
            return name(ref);
        }

        @Override
        public String toString() {
            return "{" +
                     name +
                    ", " + members +
                    '}';
        }
    }

}
