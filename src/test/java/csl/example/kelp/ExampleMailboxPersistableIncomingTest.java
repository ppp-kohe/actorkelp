package csl.example.kelp;

import csl.actor.Message;
import csl.actor.kelp.ActorSystemKelp;
import csl.actor.persist.MailboxPersistableIncoming;
import csl.actor.persist.PersistentFileManager;
import csl.actor.remote.KryoBuilder;
import csl.example.TestTool;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;

public class ExampleMailboxPersistableIncomingTest {
    public static void main(String[] args) {
        new ExampleMailboxPersistableIncomingTest().runOfferPoll();
        new ExampleMailboxPersistableIncomingTest().runOfferWithPoll();
    }

    public void runOfferPoll() {
        System.err.println("------------------ runOfferPoll");
        test((system, manager) -> {
            MailboxPersistableIncoming mbox = new MailboxPersistableIncoming(manager, 100);


            TestTool.assertEquals("before offer: sizeOnMem", 0L, mbox.getSizeOnMemory());
            TestTool.assertEquals("before offer: size", 0L, mbox.getSize());
            TestTool.assertEquals("before offer: prevSizeOnMem", 0L, mbox.getPreviousSizeOnMemory());
            TestTool.assertEquals("before offer: empty", true, mbox.isEmpty());

            List<Object> values = new ArrayList<>();
            for (int i = 0; i < 110; ++i) {
                Object data = "m" + i;
                values.add(data);
                mbox.offer(new Message<>(null, data));
            }

            TestTool.assertEquals("after offer: sizeOnMem", 100L, mbox.getSizeOnMemory());
            TestTool.assertEquals("after offer: size", 110L, mbox.getSize());
            TestTool.assertEquals("after offer: prevSizeOnMem", 100L, mbox.getPreviousSizeOnMemory());
            TestTool.assertEquals("after offer: empty", false, mbox.isEmpty());

            List<Object> polled = new ArrayList<>();
            for (int i = 0; i < 110; ++i) {
                Message<?> m = mbox.poll();
                polled.add(m.getData());
            }
            TestTool.assertEquals("after poll", null, mbox.poll());
            TestTool.assertEquals("after poll: data", values, polled);

            TestTool.assertEquals("after poll: sizeOnMem", 0L, mbox.getSizeOnMemory());
            TestTool.assertEquals("after poll: size", 0L, mbox.getSize());
            TestTool.assertEquals("after poll: prevSizeOnMem", 0L, mbox.getPreviousSizeOnMemory());
            TestTool.assertEquals("after poll: empty", true, mbox.isEmpty());

            List<Object> values2 = new ArrayList<>();
            for (int i = 0; i < 110; ++i) {
                Object data = "p" + i;
                values2.add(data);
                mbox.offer(new Message<>(null, data));
            }
            TestTool.assertEquals("after offer2: sizeOnMem", 100L, mbox.getSizeOnMemory());
            TestTool.assertEquals("after offer2: size", 110L, mbox.getSize());
            TestTool.assertEquals("after offer2: prevSizeOnMem", 100L, mbox.getPreviousSizeOnMemory());
            TestTool.assertEquals("after offer2: empty", false, mbox.isEmpty());

            List<Object> polled2 = new ArrayList<>();
            for (int i = 0; i < 110; ++i) {
                Message<?> m = mbox.poll();
                polled2.add(m.getData());
            }
            TestTool.assertEquals("after poll2", null, mbox.poll());
            TestTool.assertEquals("after poll2: data", values2, polled2);

            TestTool.assertEquals("after poll2: sizeOnMem", 0L, mbox.getSizeOnMemory());
            TestTool.assertEquals("after poll2: size", 0L, mbox.getSize());
            TestTool.assertEquals("after poll2: prevSizeOnMem", 0L, mbox.getPreviousSizeOnMemory());
            TestTool.assertEquals("after poll2: empty", true, mbox.isEmpty());

            mbox.delete();
        });
    }

    public void runOfferWithPoll() {
        MailboxPersistableIncoming.storageClearSize = 100;
        System.err.println("------------------ runOfferWithPoll");
        test((system, manager) -> {
            MailboxPersistableIncoming mbox = new MailboxPersistableIncoming(manager, 100);


            TestTool.assertEquals("before offer: sizeOnMem", 0L, mbox.getSizeOnMemory());
            TestTool.assertEquals("before offer: size", 0L, mbox.getSize());
            TestTool.assertEquals("before offer: prevSizeOnMem", 0L, mbox.getPreviousSizeOnMemory());
            TestTool.assertEquals("before offer: empty", true, mbox.isEmpty());

            List<Object> values = new ArrayList<>();
            for (int i = 0; i < 100; ++i) {
                Object data = "m" + i;
                values.add(data);
                mbox.offer(new Message<>(null, data));
            }

            TestTool.assertEquals("after offer: sizeOnMem", 100L, mbox.getSizeOnMemory());
            TestTool.assertEquals("after offer: size", 100L, mbox.getSize());
            TestTool.assertEquals("after offer: prevSizeOnMem", 100L, mbox.getPreviousSizeOnMemory());
            TestTool.assertEquals("after offer: empty", false, mbox.isEmpty());

            for (int i = 0; i < 110; ++i) {
                Object data = "p" + i;
                values.add(data);
                mbox.offer(new Message<>(null, data));
            }

            TestTool.assertEquals("after offer persist: sizeOnMem", 100L, mbox.getSizeOnMemory());
            TestTool.assertEquals("after offer persist: size", 210L, mbox.getSize());
            TestTool.assertEquals("after offer persist: prevSizeOnMem", 100L, mbox.getPreviousSizeOnMemory());
            TestTool.assertEquals("after offer persist: empty", false, mbox.isEmpty());

            List<Object> polled = new ArrayList<>();
            for (int i = 0; i < 105; ++i) {
                Message<?> m = mbox.poll();
                polled.add(m.getData());
            }
            TestTool.assertEquals("after poll 105: data", values.subList(0, 105), polled);
            TestTool.assertEquals("after poll 105: sizeOnMem", 0L, mbox.getSizeOnMemory());
            TestTool.assertEquals("after poll 105: size", 105L, mbox.getSize());
            TestTool.assertEquals("after poll 105: prevSizeOnMem", 0L, mbox.getPreviousSizeOnMemory());
            TestTool.assertEquals("after poll 105: empty", false, mbox.isEmpty());

            List<Object> values2 = new ArrayList<>(values.subList(105, values.size()));
            for (int i = 0; i < 50; ++i) {
                Object data = "M" + i;
                values2.add(data);
                mbox.offer(new Message<>(null, data));
            }

            TestTool.assertEquals("after offer 50: sizeOnMem", 0L, mbox.getSizeOnMemory());
            TestTool.assertEquals("after offer 50: size", 155L, mbox.getSize());
            TestTool.assertEquals("after offer 50: prevSizeOnMem", 0L, mbox.getPreviousSizeOnMemory());
            TestTool.assertEquals("after offer 50: empty", false, mbox.isEmpty());

            List<Object> polled2 = new ArrayList<>();
            for (int i = 0; i < 155; ++i) {
                Message<?> m = mbox.poll();
                polled2.add(m.getData()); //after poll all storage items, source and writer will be cleared thanks to storageClearSize=100
            }
            TestTool.assertEquals("after poll all: poll", null, mbox.poll());
            TestTool.assertEquals("after poll all: data", values2, polled2);
            TestTool.assertEquals("after poll all: sizeOnMem", 0L, mbox.getSizeOnMemory());
            TestTool.assertEquals("after poll all: size", 0L, mbox.getSize());
            TestTool.assertEquals("after poll all: prevSizeOnMem", 0L, mbox.getPreviousSizeOnMemory());
            TestTool.assertEquals("after poll all: empty", true, mbox.isEmpty());

            List<Object> values3 = new ArrayList<>();
            for (int i = 0; i < 200; ++i) {
                Object data = "P" + i;
                values3.add(data);
                mbox.offer(new Message<>(null, data)); //create new file at i=100 thanks to storageClearSize=100
            }

            TestTool.assertEquals("after offer 200: sizeOnMem", 100L, mbox.getSizeOnMemory());
            TestTool.assertEquals("after offer 200: size", 200L, mbox.getSize());
            TestTool.assertEquals("after offer 200: prevSizeOnMem", 100L, mbox.getPreviousSizeOnMemory());
            TestTool.assertEquals("after offer 200: empty", false, mbox.isEmpty());

            List<Object> polled3 = new ArrayList<>();
            for (int i = 0; i < 200; ++i) {
                Message<?> m = mbox.poll();
                polled3.add(m.getData());
            }

            TestTool.assertEquals("after poll all: poll", null, mbox.poll());
            TestTool.assertEquals("after poll all: data", values3, polled3);
            TestTool.assertEquals("after poll all: sizeOnMem", 0L, mbox.getSizeOnMemory());
            TestTool.assertEquals("after poll all: size", 0L, mbox.getSize());
            TestTool.assertEquals("after poll all: prevSizeOnMem", 0L, mbox.getPreviousSizeOnMemory());
            TestTool.assertEquals("after poll all: empty", true, mbox.isEmpty());
            mbox.delete();
        });
    }

    public void test(BiConsumer<ActorSystemKelp.ActorSystemDefaultForKelp, PersistentFileManager> r) {
        try (ActorSystemKelp.ActorSystemDefaultForKelp system = ActorSystemKelp.createLocal()) {
            KryoBuilder.SerializerPool p = new KryoBuilder.SerializerPoolDefault(system);
            PersistentFileManager manager = new PersistentFileManager(
                    "target/debug-persist", p, Paths::get, system.getLogger());

            r.accept(system, manager);
        }
    }
}
