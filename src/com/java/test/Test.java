package com.java.test;

import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Properties;

public class Test {

    public static void main(String[] args) throws IOException, ParseException {

//        long l = Runtime.getRuntime().maxMemory();
//        double v = l / (double) 1024 / 1024 / 1024;
//        System.out.println("v = " + v);
//
//
//        l = Runtime.getRuntime().totalMemory();
//        v = l / (double) 1024 / 1024 ;
//        System.out.println("v = " + v);
//
//        System.out.println(241.5 * 64);


//        D:\DevInstall\Java\jdk1.8.0_211\bin\java.exe -Xms8m -Xmx8m -XX:+PrintGCDetails "-javaagent:D:\DevInstall\JetBrains\IntelliJ IDEA 2019.1.3\lib\idea_rt.jar=7492:D:\DevInstall\JetBrains\IntelliJ IDEA 2019.1.3\bin" -Dfile.encoding=UTF-8 -classpath D:\DevInstall\Java\jdk1.8.0_211\jre\lib\charsets.jar;D:\DevInstall\Java\jdk1.8.0_211\jre\lib\deploy.jar;D:\DevInstall\Java\jdk1.8.0_211\jre\lib\ext\access-bridge-64.
//        jar;D:\DevInstall\Java\jdk1.8.0_211\jre\lib\ext\cldrdata.jar;D:\DevInstall\Java\jdk1.8.0_211\jre\lib\ext\dnsns.jar;D:\DevInstall\Java\jdk1.8.0_211\jre\lib\ext\jaccess.jar;D:\DevInstall\Java\jdk1.8.0_211\jre\lib\ext\jfxrt.jar;D:\DevInstall\Java\jdk1.8.0_211\jre\lib\ext\localedata.jar;D:\DevInstall\Java\jdk1.8.0_211\jre\lib\ext\nashorn.jar;D:\DevInstall\Java\jdk1.8.0_211\jre\lib\ext\sunec.jar;D:\DevInstall\Java\jdk1.8.0_211\jre\lib\ext\sunjce_provider.jar;D:\DevInstall\Java\jdk1.8.0_211\jre\lib\ext\sunmscapi.jar;D:\DevInstall\Java\jdk1.8.0_211\jre\lib\ext\sunpkcs11.jar;D:\DevInstall\Java\jdk1.8.0_211\jre\lib\ext\zipfs.jar;D:\DevInstall\Java\jdk1.8.0_211\jre\lib\javaws.jar;D:\DevInstall\Java\jdk1.8.0_211\jre\lib\jce.jar;D:\DevInstall\Java\jdk1.8.0_211\jre\lib\jfr.jar;D:\DevInstall\Java\jdk1.8.0_211\jre\lib\jfxswt.jar;D:\DevInstall\Java\jdk1.8.0_211\jre\lib\jsse.jar;D:\DevInstall\Java\jdk1.8.0_211\jre\lib\management- agent.jar;D:\DevInstall\Java\jdk1.8.0_211\jre\lib\plugin.jar;D:\DevInstall\Java\jdk1.8.0_211\jre\lib\resources.jar;D:\DevInstall\Java\jdk1.8.0_211\jre\lib\rt.jar;D:\Workspace\Idea-project\HadoopEcosystem\target\classes com.java.test.Test[GC (Allocation Failure) [PSYoungGen: 1526K->488K(2048K)] 1526K->656K(7680K), 0.0007773 secs] [Times: user=0.00 sys=0.00, real=0.00 secs]
//        [GC (Allocation Failure) [PSYoungGen: 1865K->503K(2048K)] 2033K->1087K(7680K), 0.0007372 secs] [Times: user=0.00 sys=0.00, real=0.00 secs]
//        [GC (Allocation Failure) [PSYoungGen: 1821K->407K(2048K)] 2405K->1630K(7680K), 0.0007250 secs] [Times: user=0.00 sys=0.00, real=0.00 secs]
//        [GC (Allocation Failure) [PSYoungGen: 1769K->280K(2048K)] 4270K->3100K(7680K), 0.0006450 secs] [Times: user=0.00 sys=0.00, real=0.00 secs]
//        [GC (Allocation Failure) [PSYoungGen: 1588K->280K(2048K)] 5687K->5018K(7680K), 0.0007391 secs] [Times: user=0.00 sys=0.00, real=0.00 secs]
//        [Full GC (Ergonomics) [PSYoungGen: 280K->0K(2048K)] [ParOldGen: 4738K->2623K(5632K)] 5018K->2623K(7680K), [Metaspace: 3231K->3231K(1056768K)], 0.0024915 secs] [Times: user=0.00 sys=0.00, real=0.00 secs]
//        [Full GC (Ergonomics) [PSYoungGen: 1339K->0K(2048K)] [ParOldGen: 5180K->1901K(5632K)] 6519K->1901K(7680K), [Metaspace: 3231K->3231K(1056768K)], 0.0063703 secs] [Times: user=0.09 sys=0.00, real=0.01 secs]
//        [GC (Allocation Failure) [PSYoungGen: 1308K->0K(2048K)] 5766K->4458K(7680K), 0.0003458 secs] [Times: user=0.00 sys=0.00, real=0.00 secs]
//        [GC (Allocation Failure) [PSYoungGen: 0K->0K(2048K)] 4458K->4458K(7680K), 0.0002930 secs] [Times: user=0.00 sys=0.00, real=0.00 secs]
//        [Full GC (Allocation Failure) [PSYoungGen: 0K->0K(2048K)] [ParOldGen: 4458K->4458K(5632K)] 4458K->4458K(7680K), [Metaspace: 3231K->3231K(1056768K)], 0.0022180 secs] [Times: user=0.00 sys=0.00, real=0.00 secs]
//        [GC (Allocation Failure) [PSYoungGen: 0K->0K(2048K)] 4458K->4458K(7680K), 0.0002414 secs] [Times: user=0.00 sys=0.00, real=0.00 secs]
//        [Full GC (Allocation Failure) [PSYoungGen: 0K->0K(2048K)] [ParOldGen: 4458K->4438K(5632K)] 4458K->4438K(7680K), [Metaspace: 3231K->3231K(1056768K)], 0.0060759 secs] [Times: user=0.00 sys=0.00, real=0.01 secs]
//                Exception in thread "main" java.lang.OutOfMemoryError: Java heap space
//                at java.util.Arrays.copyOf(Arrays.java:3332)
//                at java.lang.AbstractStringBuilder.ensureCapacityInternal(AbstractStringBuilder.java:124)
//                at java.lang.AbstractStringBuilder.append(AbstractStringBuilder.java:674)
//                at java.lang.StringBuilder.append(StringBuilder.java:208)
//                at com.java.test.Test.main(Test.java:24)
//                Heap
//                PSYoungGen      total 2048K, used 46K [0x00000000ffd80000, 0x0000000100000000, 0x0000000100000000)
//                eden space 1536K, 3% used [0x00000000ffd80000,0x00000000ffd8b8c8,0x00000000fff00000)
//                from space 512K, 0% used [0x00000000fff80000,0x00000000fff80000,0x0000000100000000)
//                to   space 512K, 0% used [0x00000000fff00000,0x00000000fff00000,0x00000000fff80000)
//                ParOldGen       total 5632K, used 4438K [0x00000000ff800000, 0x00000000ffd80000, 0x00000000ffd80000)
//                object space 5632K, 78% used [0x00000000ff800000,0x00000000ffc55998,0x00000000ffd80000)
//                Metaspace       used 3262K, capacity 4496K, committed 4864K, reserved 1056768K
//                class space    used 353K, capacity 388K, committed 512K, reserved 1048576K
//
//                Process finished with exit code 1
        InputStream is = Test.class.getClassLoader().getResourceAsStream("");
        Properties prop = new Properties();
        prop.load(is);


        System.out.println(ZoneId.getAvailableZoneIds());

        LocalDateTime now = LocalDateTime.now(ZoneId.of("Asia/Aden"));
        System.out.println(now);

        System.out.println(0.05 + 0.01);

//        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
//        SimpleDateFormat sdf1 = new SimpleDateFormat("YYYY");
//        System.out.println(sdf1.format(sdf1.parse("2019-12-01")));
//        System.out.println(sdf1.format("2019-12-30"));
//        System.out.println(sdf1.format(sdf1.parse("2020-01-01")));


    }

}