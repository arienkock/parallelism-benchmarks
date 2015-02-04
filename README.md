To run:
`mvn install`

`java -Dco.paralleluniverse.fibers.verifyInstrumentation -javaagent:target/dependency/quasar-core-0.6.2-jdk8.jar -jar target/benchmarks.jar ParallelismBenchmark -wi 1 -i 1 -f 1`