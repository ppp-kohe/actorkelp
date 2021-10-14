package csl.example.util;

import csl.example.TestTool;
import csl.actor.util.ConfigBase;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;

public class ExampleConfigBase {
    public static void main(String[] args) throws Exception {
        new ExampleConfigBase().testConfigProps();
        new ExampleConfigBase().testLog();
    }

    public void testConfigProps() throws Exception {
        System.setProperty("csl.actor.example.i", "1234");
        System.setProperty("csl.actor.example.f", "1234.5");
        System.setProperty("csl.actor.example.l", "123_456");
        System.setProperty("csl.actor.example.d", "1_234.56");
        System.setProperty("csl.actor.example.flag", "true");
        System.setProperty("csl.actor.example.s", "hello-world");

        ConfigExample ex = ConfigBase.readConfig(ConfigExample.class, "csl.actor.example", System.getProperties());
        System.out.println(ex);

        TestTool.assertEquals("int", 1234, ex.i);
        TestTool.assertEquals("float", 1234.5f, ex.f, (e,a) -> Math.abs(e - a) < 0.00001);
        TestTool.assertEquals("long", 123456L, ex.l);
        TestTool.assertEquals("double", 1234.56, ex.d, (e,a) -> Math.abs(e - a) < 0.00001);
        TestTool.assertEquals("boolean",  true, ex.flag);
        TestTool.assertEquals("String",  "hello-world", ex.s);

        ex.showHelp();

        List<String> un = ex.readArgs("--argIntVal", "54321", "--argStrVal", "HELLOWORLD", "-i", "6789",
                "--unknownOption", "unknownOptionValue", "unknownOptionValue2");
        TestTool.assertEquals("readArgs remaining", un,
                Arrays.asList("--unknownOption", "unknownOptionValue", "unknownOptionValue2"));
        TestTool.assertEquals("readArgs", 54321, ex.argIntVal);
        TestTool.assertEquals("readArgs", "HELLOWORLD", ex.argStrVal);
        TestTool.assertEquals("readArgs", 6789, ex.i);

        ex.set("d", 987.654);
        TestTool.assertEquals("set&get", 987.654, ex.get("d"));
    }

    public static class ConfigExample extends ConfigBase {
        public static final long serialVersionUID = -1L;
        @CommandArgumentOption(value = "--intValue", abbrev = "-i", help = "an integer value")
        public int i;
        public float f;
        public long l;
        public double d;
        public boolean flag;
        public String s;


        public int argIntVal;
        public String argStrVal;

        @Override
        public int getLogColorDefault() {
            return 60;
        }

        @Override
        protected FormatAndArgs logMessageHeader() {
            return super.logMessageHeader().append(new FormatAndArgs("<Example> "));
        }
    }

    public void testLog() {
        ConfigBase base = new ConfigExample();
        base.log("log: hello %d", 123);
        base.log(123, "log: hello colored");
        base.log(new RuntimeException(), "log: hello exception");
        base.log(180, new RuntimeException(), "log: hello exception with color");

        ConfigBase.FormatAndArgs fa = base
                .logMessage("log: format %s", Instant.now())
                .append(new ConfigBase.FormatAndArgs("format2 %d", 123));
        System.err.println(fa.format());
    }
}
