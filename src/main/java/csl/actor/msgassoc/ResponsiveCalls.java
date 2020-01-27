package csl.actor.msgassoc;

import csl.actor.*;
import csl.actor.remote.ActorAddress;
import csl.actor.remote.ActorRefRemote;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

public class ResponsiveCalls {

    protected static AtomicLong id = new AtomicLong();

    public static final String CALLABLE_NAME = ResponsiveCalls.class.getName() + ".callable";

    public static String nextId() {
        return ResponsiveCalls.class.getName() + "." + id.getAndIncrement();
    }

    /**
     *  The target must be an actor like the following
     * <pre>
     *    class RemoteTargetActor extends Actor {
     *        protected void processMessage(Message m) {
     *            Object msg = m.getData();
     *            Object result;
     *            try {
     *              result = processMsg(msg);
     *            } catch (Throwable e) {
     *              result = new CallableFailure(e);
     *            }
     *            m.getSender().tell(result, this);
     *        }
     *    }
     * </pre>
     *  The result Future receives the result.
     *
     *  Please take care the usage of the mechanism.
     *   The following example will become deadlock.
     *     (starting by new Sender(system).tell("start", null).
     *        this is a meaningless example just for explanation)
     *  <pre>
     *      class Sender extends Actor {
     *          ActorRef target;
     *          Sender(ActorSystem s) { super(s); target = new Target(s, this); }
     *          protected void processMessage(Message m) {
     *              if (m.getData().equals("start")) { //1.
     *                try {
     *                  String res = ResponsiveCalls.send(getSystem(), target, "msg") //2. send and wait
     *                            .get();
     *                  System.out.println(res);
     *                } catch (Exception ex) {}
     *              } else if (m.getData().equals("getFromTarget")) { //5. it cannot run the code:
     *                  m.getSender().tell("backToTarget", this);      //   above Future.get() possesses the actor
     *              }
     *          }
     *      }
     *      class Target extends Actor {
     *          Sender owner;
     *          Target backTarget;
     *          Target(ActorSystem s, Sender s) { super(s); owner = s; }
     *          protected void processMessage(Message m) {
     *              if (m.getData().equals("msg")) { //3. receive from ResponsiveCall
     *                  backTarget = m.getSender();
     *                  owner.tell("getFromTarget", this); //4. not a message sender but the owner
     *              } else if (m.getData().equals("backToTarget")) {
     *                  backTarget.tell("result", this);
     *              }
     *          }
     *      }
     *  </pre>
     *
     * @param system a local system holding a sender actor
     * @param target the target actor which must response the msg and send back a result to the sender
     * @param msg a sending message
     * @param <T> the result type
     * @return a future of the result
     */
    public static <T> Future<T> send(ActorSystem system, ActorRef target, Object msg) {
        return new ResponsiveSenderActor<T>(system).send(target, msg);
    }

    /**
     * send a task to a remote actor and execute it on the actor.
     *  the target actor must be exist as an instanceof {@link ResponsiveCallableActor}
     * @param system a local system which will hold a sender actor
     * @param target the target host address
     * @param name the target actor name within the host described by the target address
     * @param task the task generating a result in the target actor
     * @param <T> the result type
     * @return a future of the result
     */
    public static <T> Future<T> send(ActorSystem system, ActorAddress.ActorAddressRemote target, String name, CallableMessage<T> task) {
        return send(system, ActorRefRemote.get(system, target.getActor(name)), task);
    }

    /**
     * run a task within a remote host.
     *   The system on the remote host must have the {@link ResponsiveCallableActor} named as {@link #CALLABLE_NAME}.
     *     This can be done by {@link #initCallableTarget(ActorSystem)}.
     *
     *    For calling to the local target:
     *   <pre>
     *       send(system, ActorRefLocalNamed.get(system, CALLABLE_NAME), task)
     *   </pre>
     * @param system a local system which will hold a sender actor
     * @param target the target host address
     * @param task the task generating a result in the target host
     * @param <T> the result type
     * @return a future of the result
     */
    public static <T> Future<T> send(ActorSystem system, ActorAddress.ActorAddressRemote target, CallableMessage<T> task) {
        return send(system, target, CALLABLE_NAME, task);
    }

    public static void initCallableTarget(ActorSystem system) {
        new ResponsiveCallableActor(system);
    }

    public static class ResponsiveSenderActor<T> extends ActorDefault {
        protected CompletableFuture<T> resultHolder;

        public ResponsiveSenderActor(ActorSystem system) {
            super(system, nextId());
            resultHolder = new CompletableFuture<>();
        }

        public Future<T> send(ActorRef target, Object data) {
            target.tell(data, this);
            return resultHolder;
        }

        @Override
        protected ActorBehavior initBehavior() {
            return behaviorBuilder()
                    .match(ActorSystemDefault.DeadLetter.class, this::fail)
                    .matchWithSender(Object.class, this::receive)
                    .build();
        }

        @SuppressWarnings("unchecked")
        public void receive(Object v, ActorRef sender) {
            getSystem().unregister(getName());
            if (v instanceof CallableMessage.CallableFailure) {
                resultHolder.completeExceptionally(((CallableMessage.CallableFailure) v).getError());
            } else {
                try {
                    resultHolder.complete((T) v);
                } catch (Throwable ce) {
                    resultHolder.completeExceptionally(ce);
                }
            }
        }

        public void fail(ActorSystemDefault.DeadLetter l) {
            resultHolder.completeExceptionally(new DeadLetterException(l));
        }

        public CompletableFuture<T> getResultHolder() {
            return resultHolder;
        }
    }

    public static class DeadLetterException extends RuntimeException {
        protected ActorSystemDefault.DeadLetter letter;

        public DeadLetterException(ActorSystemDefault.DeadLetter letter) {
            super(Objects.toString(letter));
            this.letter = letter;
        }

        public ActorSystemDefault.DeadLetter getLetter() {
            return letter;
        }
    }

    public static class ResponsiveCallableActor extends ActorDefault {
        public ResponsiveCallableActor(ActorSystem system, String name) {
            super(system, name);
        }

        public ResponsiveCallableActor(ActorSystem system) {
            this(system, CALLABLE_NAME);
        }

        @Override
        protected ActorBehavior initBehavior() {
            return behaviorBuilder()
                    .build();
        }
    }
}
