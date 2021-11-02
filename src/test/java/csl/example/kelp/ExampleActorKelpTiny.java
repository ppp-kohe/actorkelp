package csl.example.kelp;

import csl.actor.ActorBehaviorBuilder;
import csl.actor.ActorSystem;
import csl.actor.CallableMessage;
import csl.actor.kelp.ActorKelp;
import csl.actor.kelp.ActorSystemKelp;
import csl.actor.kelp.ConfigKelp;
import csl.actor.kelp.behavior.ActorBehaviorBuilderKelp;
import csl.example.TestTool;

import java.util.concurrent.atomic.AtomicReference;

public class ExampleActorKelpTiny {
    public static void main(String[] args) throws Exception {
        new ExampleActorKelpTiny().run();
        new ExampleActorKelpTiny().runSpecial();
    }

    public void run() throws Exception {
        ActorSystem system = new ActorSystemKelp.ActorSystemDefaultForKelp();
        MyActorTiny a = new MyActorTiny(system);
        a.tell("hello");
        Thread.sleep(100);
        TestTool.assertEquals("value", "hello", a.lastValue.get());
        system.close();
    }

    public void runSpecial() throws Exception {
        ActorSystem system = new ActorSystemKelp.ActorSystemDefaultForKelp();
        MyActorTiny a = new MyActorTiny(system);
        a.tell(CallableMessage.callableMessageConsumer(_a -> {
            ((MyActorTiny) _a).lastValue.set("finish");
        }).withSender(null));
        Thread.sleep(100);
        TestTool.assertEquals("value", "finish", a.lastValue.get());
        system.close();
    }

    public static class MyActorTiny extends ActorKelp<MyActorTiny> {
        public AtomicReference<String> lastValue = new AtomicReference<>();
        public MyActorTiny(ActorSystem system, String name, ConfigKelp config) {
            super(system, name, config);
        }

        public MyActorTiny(ActorSystem system) {
            super(system, new ConfigKelp());
        }

        @Override
        protected ActorBehaviorBuilder initBehavior(ActorBehaviorBuilderKelp builder) {
            return builder.match(String.class, this::process);
        }
        public void process(String value) {
            System.out.println("receive " + value);
            this.lastValue.set(value);
        }
    }

}
