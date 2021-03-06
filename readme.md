# ActorKelp

ActorKelp is a library for the actor model in the modern style of Java. 

The library includes the following features

* A classical actor mechanism with employing lambda expressions
* Remote actors
* Cluster supports with the key-value message aggregation mechanism for scalable data processing

## Installation

* Install JDK (11 or later)

* Install [Apache Maven](http://maven.apache.org)

* Run `mvn package` under the project directory

* After that, `mvn install` can install the compiled jar into the your local repository, and then you can use it by appending the following dependency to your `pom.xml`:

    ```xml
    <dependency>
        <groupId>csl</groupId>
        <artifactId>actorkelp</artifactId>
        <version>1.0-SNAPSHOT</version>
    </dependency>
    ```

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
    * override `protected ActorBehavior initBehavior()` and write a call chain like `return behaviorBuilder().match(...).build()`: The method sets up behaviors of the actor as `ActorBehavior`. You can use `ActorBehaviorBuilder` returned by `behaviorBuilder()` , which is the class for a set of fluent APIs  for constructing behaviors.
        * `.match(T.class, Consumer<T> b)` : With the actor model, any type of an object can be a message. A message is asynchronous invocation to an actor and the received actor synchronously processes each messages. To accept multiple messages, an actor internally has a concurrent queue called "mailbox". A dequeued message from the mailbox will be tested by the list of behaviors declared by `.match(T1.class, ...).match(T2.class,...)...`. A top matched behavior handles the message.
* In the top of the main method, it creates `ActorSystemDefault` , which is a shared instance for managing message dispatching with thread-pools and named actors.
    * The created system has an `ExecutorService` for hosting threads of executing message behaviors of actors.
    * Any actors are associated with a system which can be obtained by `getSystem()` from the actor.
    * The system needs to be explicitly `.close()` for shutting down.
* In order to send a message to an actor, `.tell(msg)` can be used. The call is asynchronous and thus it immediately returns
    * The method `tell` is declared in `ActorRef` which is the base interface of `ActorDefault`. The type is also used as the remote proxy for remote actors.
    * In the library, an actor instance is an ordinary object. So it can directly invoke a regular method  to the actor instance. However, it will collapse safety benefited from the actor model. 

## Remote Actor

The library supports remote actors for distribution. It can launch multiple processes on different hosts and communicating each other.

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

The above program can be executed as two separated processes:

```bash
 mvn exec:java -Dexec.mainClass=ExampleRemote -Dexec.args=3000 & 
 mvn exec:java -Dexec.mainClass=ExampleRemote -Dexec.args=3001
```

* `ActorSystemRemote` is the remote version of `ActorSystem`
    * `startWithoutWait(port)` starts listening TCP connections on the port.
* `new ExampleRemote(system, "a")` is the actor creation with registering it as the name `"a"` , and then another process can specify the actor as the address `localhost:3000/a`.
* In another process, `ActorAddress.get("localhost", 3000, "a")` is the address specification for the remote actor. 
    * In the above example, `localhost:3001/b` receives the target address `localhost:3000/a` and sends `"hello"` to the remote actor `"a"`. 
        * The type `ActorAddress` is just an address representation of an actor without binding a system, and
        * `ActorRefRemote` is a sub-type of `ActorRef` derived from the `ActorAddress ` with binding a system. 
        * `.tell("hello", this)` can send the string `"hello"` as the message to the target actor with specifying `this` (`localhost:3001/b`) as the sender actor.
    * The remote actor receiving `"hello"` can match with the behavior `.matchWithSender(String.class, this::receive)`, and then the parameter `sender` becomes `ActorRefRemote` of `localhost:3001/b`. 
        * In the `receive` method, the sender is non null and thus send back to the string to the sender.
* Messages sent between remote actors must be `Serializable`. 
    * Any types of actors are serialized as `ActorRefRemote`.
    * The library relies on [Kryo](https://github.com/EsotericSoftware/kryo) as serialization framework.
    * The network transportation relies on [Netty](https://netty.io)

## Key-Value Data Aggregation Processing

```java
import csl.actor.ActorBehavior;
import csl.actor.ActorSystem;
import csl.actor.kelp.ActorKelp;
import csl.actor.kelp.ConfigKelp;

import java.util.Arrays;

public class ExampleLearnerKelp extends ActorKelp<ExampleLearnerKelp> {
    @TransferredState(mergeType = MergerOpType.Add) int[] counts;
    public ExampleLearnerKelp(ActorSystem system, ConfigKelp config, int size) {
        super(system, config);
        this.counts = new int[size];
    }

    public ExampleLearnerKelp(ActorSystem system, String name, ConfigKelp config) {
        super(system, name, config);
    }

    @Override
    protected ActorBehavior initBehavior() {
        return behaviorBuilder()
                .matchKey(Integer.class, identity())
                .forEach(this::train)
                .build();
    }

    public void train(Integer i) {
        counts[i]++;
    }

    public double[] dist() {
        double total = Arrays.stream(counts).sum();
        double[] d = new double[counts.length];
        Arrays.setAll(d, i -> d[i] = counts[i] / total);
        return d;
    }
}
```

* The class extends `ActorKelp` with supplying the type argument as recursive self-type: `class A extends ActorKelp<A>`.
* The field `int[] counts`  is the actor's internal state as the learning model, and annotated by `@TransferredState`.
    * The annotation takes `mergeType=MergerOpType.Add `. The actor supports distributed processing by multiple copies, and the `Add` can aggregate all internal state of the copies by summing each element value after processing (`actor[0].counts[0]+actor[1].counts[0]+actor[2].counts[0]+..., ...`).
* The first constructor is specific for the class, which can initialize the internal state. But it can take `ConfigKelp` which has a set of configuration parameters of the mechanism.
* The second constructor `(ActorSystem, String, ConfigKelp)` is the default constructor which is used at creating distributed copies.
* The behavior uses `.matchKey(...).forEach(...)`
    * `.matchKey(Integer.class, identity())` specifies the actor receives `Integer` as message and extracts the int value as key for the message (`identity() ` is ` i -> i`)
    * The mechanism delivers the message to a distributed copy of the actor based on the key
    * `.forEach(this::train)` specifies that after delivering, the received copy of the actor process each message by `train(i)`. 
* The above class is executed by the following runner class 

```java
import csl.actor.ActorSystem;
import csl.actor.kelp.ClusterKelp;
import csl.actor.kelp.ConfigKelp;
import csl.actor.kelp.KelpStage;

import java.util.Arrays;
import java.util.Random;

public class ExampleKelpRunner {
    public static void main(String[] args) throws Exception {
        try (ClusterKelp<ConfigKelp> k = ClusterKelp.create()) {
            ActorSystem system = k.deploy();
            int max = 10;
            KelpStage<ExampleLearnerKelp> learners = 
                new ExampleLearnerKelp(system, k.getPrimaryConfig(), max).shuffle();

            Random random = new Random();
            for (int i = 0; i < 1000; ++i) {
                learners.tell(random.nextInt(max));
            }
            learners.sync().get();

            ExampleLearnerKelp merged = learners.merge();
            System.out.println(Arrays.toString(merged.dist()));
        }
    }
}
```

* `ClusterKelp` is a cluster manager instance
    * by default, the class copies class-path jars and saves config files into `target/kelp` directory (under the default build directory of maven), but wors only on the running process.
    * config file specified by `-Dcsl.actor.cluster.conf=<confFilePath>` can changes the settings as running on multiple processes.
* `k.deploy()` starts the system
    * The try-resource block can automatically shutting down the clsuter
* `new ExampleLearnerKelp(system, k.getPrimaryConfig(), max)` creates the temporary learner actor instance as starting point.
* `.shuffle()` creates a special kind of `ActorRef` , `KelpStage`, which can holds multiple distributed copies of the actor and deliver messages followed by `matchKey` behaviors.
* `learners.tell(...)` do the delivering; dispatch each message to one of copy of distributed actors. Also the mechanism has buffers of messages and thus the message will not be immediately sent to the target.
* `leraners.sync()` causes flusing all messages on the buffers and returns a `CompletableFuture` object which can await finishing all processes of the messages by `.get()`. 
* After processing finished, `leraners.merge()` aggregates all distributed copies as a local actor. The internal state of the actor will be merged followed by `@TransferredState`