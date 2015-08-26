package org.sample;

import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

import java.util.concurrent.TimeUnit;

/**
 * Created by rob on 26/08/2015.
 */
public class Main {


    public static void main(String[] args) throws RunnerException {
        int time = Boolean.getBoolean("longTest") ? 30 : 2;
        System.out.println("measurementTime: " + time + " secs");
        Options opt = new OptionsBuilder()
                .include(MyBenchmark.class.getSimpleName())
//                    .warmupIterations(5)
                .measurementIterations(5)
                .forks(10)
                .mode(Mode.SampleTime)
                .measurementTime(TimeValue.seconds(time))
                .timeUnit(TimeUnit.NANOSECONDS)
                .build();

        new Runner(opt).run();
    }
}
