# ActorKelp



## Actor

```java
import csl.actor.*;

public class ExampleActor extends ActorDefault {
    public static void main(String[] args) {
        ActorSystem system = new ActorSystemDefault();
        ExampleActor a = new ExampleActor(system, "a");
        a.tell("hello");
    }
    public ExampleActor(ActorSystem system, String name) {
        super(system, name);
    }
    protected ActorBehavior initBehavior() {
        return behaviorBuilder()
                .match(String.class, this::receive)
                .build();
    }
    void receive(String msg) {
        System.out.println("receive " + msg + " @ " + getName());
        getSystem().close();
    }
}
```

* `ActorDefault` is the base class for actors: you can define your actors as sub-classes of the base class.
    * define a constructor with a super call of `(ActorSystem system)` or `(ActorSystem system, String name)`
        * The string `name` is an optional parameter, which will be required for remote actors.
    * override `protected ActorBehavior initBehavior()` and write a call chain like `return behaviorBuilder().match(...).build()`: The method sets up behaviors of the actor as `ActorBehavior`. You can use `ActorBehaviorBuilder` returned by `behaviorBuilder()` , which is a fluent API class for constructing the behaviors.
        * `.match(T.class, Consumer<T> b)` : With the actor model, any type of object can be a message. A message is asynchronous invocation to an actor and the received actor synchronously processes each messages. To accept multiple messages, an actor internally has a concurrent queue called "mailbox". A dequeued message from the mailbox will be tested by the list of behaviors declared by `.match(T1.class, ...).match(T2.class,...)...`. A top matched behavior handles the message.
* In the top of the main method, it creates `ActorSystemDefault` , which is a shared instance for managing message dispatching with thread-pools and named actors.
    * The created system has a `ExecutorService` for hosting threads of executing message behaviors of actors.
    * Any actors are associated with a system which can be obtained by `getSystem()`.
    * So, the system needs to be explicitly `.close()` for shutting down.
* In order to send a message to an actor, `.tell(msg)` can be used. The call is asynchronous and thus it immediately returns
    * The method `tell` is declared in `ActorRef` which is the base interface of `ActorDefault`. The type is also used as the remote proxy for remote actors.
    * In the library, an actor instance is ordinary object. So it can directly invoke a regular method  to the actor instance. However, it will collapse safety benefited from the actor model. 

## Remote Actor

The library supports remote actors. This can launch multiple processes on different hosts and communicates each other.

```java
import csl.actor.*;
import csl.actor.remote.*;

public class ExampleRemote extends ActorDefault {
    public static void main(String[] args) {
        int port = Integer.parseInt(args[0]);
        ActorSystemRemote system = new ActorSystemRemote();
        system.startWithoutWait(port);

        if (port == 3000) {
            new ExampleRemote(system, "a");
        } else {
            ExampleRemote b = new ExampleRemote(system, "b");
            b.tell(ActorAddress.get("localhost", 3000, "a"));
        }
    }

    public ExampleRemote(ActorSystem system, String name) {
        super(system, name);
    }

    protected ActorBehavior initBehavior() {
        return behaviorBuilder()
                .match(ActorAddress.class, this::start)
                .matchWithSender(String.class, this::receive)
                .build();
    }

    void start(ActorAddress target) {
        ActorRefRemote.get(getSystem(), target)
                .tell("hello", this);
    }

    void receive(String msg, ActorRef sender) {
        System.out.println(this + " receive " + msg + " from " + sender);
        if (sender != null) {
            sender.tell(msg);
        } else {
            getSystem().close();
        }
    }
}
```

```bash
 mvn exec:java -Dexec.mainClass=ExampleRemote -Dexec.args=3000 & 
 mvn exec:java -Dexec.mainClass=ExampleRemote -Dexec.args=3001
```

* `ActorSystemRemote` is the remote version of `ActorSystem`
    * `startWithoutWait(port)` starts listening TCP connections on the port.
* `new ExampleRemote(system, "a")` is the actor creation with registering it as the name `"a"` , then another process can specify the actor as the address `localhost:3000/a`.
* In another process, `ActorAddress.get("localhost", 3000, "a")` is the address specification for the remote actor. 
    * In the above example, `localhost:3001/b` receives the target address `localhost:3000/a` and sends `"hello"` to the remote actor `"a"`. 
        * The type `ActorAddress` is just an address representation of an actor without binding a system, and
        * `ActorRefRemote` is a sub-type of `ActorRef` derived from the `ActorAddress`. 
        * `.tell("hello", this)` can send the string `"hello"` as the message to the target actor with specifying `this` (`localhost:3001/b`) as the sender actor.
    * The remote actor receiving `"hello"` can match with `.matchWithSender(String.class, this::receive)` pattern, and then the send parameter `sender` becomes `ActorRefRemote` of `localhost:3001/b`. 
        * In the `receive` method, the sender is non null and thus send back to the string to the sender.

* Messages sent between remote actors must be `Serializable`. The library relies on [Kryo](https://github.com/EsotericSoftware/kryo) as serialization framework.
    * The network transportation relies on [Netty](https://netty.io)

