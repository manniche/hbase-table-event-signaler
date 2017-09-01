package net.nzcorp.rmq;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.apache.commons.cli.*;
import org.json.JSONObject;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.*;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Copyright 2017 NovoZymes A/S
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
public class SpeedTest {
    /*
     */
    public static void main(String[] args) {
        Options options = getOptions();
        CommandLineParser parser = new GnuParser();
        try {
            CommandLine cmd = parser.parse(options, args);

            if (cmd.hasOption('?')) {
                usage(options);
                System.exit(0);
            }
            int iterations = intArg(cmd, 'i', 1);
            int numberThreads = intArg(cmd, 't', 1);
            String host = strArg(cmd, 'h', "amqp://localhost");
            String queueName = "rmq_speed_test";

            System.out.println(String.format("Sending %d messages to %s using %d threads", iterations, host, numberThreads));
            int split = (iterations / numberThreads);
            System.out.println(String.format("split: %d", split));

            ExecutorService exService = Executors.newFixedThreadPool(numberThreads);
            Set<Callable<List<Long>>> callables = new HashSet<>();

            for (int i = 0; i < numberThreads; i++) {
                callables.add(new MessageSender(split, split*i, host, queueName));
            }
            List<Future<List<Long>>> futures = exService.invokeAll(callables);


            for(Future<List<Long>> future : futures){
                System.out.println(String.format("Getting from future %s, is ready %s", future.toString(), future.isDone()));
                List<Long> longs = future.get();
                System.out.println(longs.size());
                printStats(longs);
            }

            exService.shutdown();

            //System.exit(0);
        } catch (ParseException | InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

    static class MessageSender implements Callable<List<Long>>{

        private final int iterations;
        private final int start;
        private final String host;
        private final String queueName;

        MessageSender(int iterations, int start, String host, String queueName){
            this.iterations = iterations;
            this.start = start;
            this.host = host;
            this.queueName = queueName;
        }

        @Override
        public List<Long> call() throws Exception {
            return sendMessages(this.iterations, this.start, this.host, this.queueName);
        }

        private List<Long> sendMessages(int iterations, int start, String host, String queueName) throws NoSuchAlgorithmException, KeyManagementException, URISyntaxException, IOException, TimeoutException {
            System.out.println(String.format("Starting from %d", start));
            long outer = System.nanoTime();
            Connection amqpConn = getRMQConnection(host);
            System.out.println(String.format("Obtained connection for %s in %d ms", host, NANOSECONDS.toMillis(System.nanoTime() - outer)));
            long inner, innerEnd = 0L;
            List<Long> timings = new ArrayList<>();
            for (int j = start; iterations > j; j++) {
                if (j % 100 == 0) {
                    System.out.print(j + ".");
                }
                inner = System.nanoTime();
                final AMQP.BasicProperties headers = constructBasicProperties();
                String message = constructJsonObject(j);
                final Channel channel = amqpConn.createChannel();
                channel.queueDeclare(queueName, true, false, false, null);

                ensureQueue(channel, queueName);
                channel.basicPublish(
                        "",
                        queueName,
                        headers,
                        message.getBytes());
                innerEnd = System.nanoTime();
                timings.add(innerEnd - inner);
                channel.close();
            }
            amqpConn.close();
            long outerEnd = System.nanoTime();
            System.out.println();
            System.out.println(String.format("Finished speedtest in %d ms", NANOSECONDS.toMillis(outerEnd - outer)));
            return timings;
        }

        private AMQP.BasicProperties constructBasicProperties() {
            Map<String, Object> customHeader = new HashMap<>();
            customHeader.put("action", "PUT");
            return new AMQP.BasicProperties.Builder().
                    contentType("application/json").
                    priority(1).
                    headers(customHeader).
                    timestamp(new Date()).
                    deliveryMode(1).build();
        }
        private Connection getRMQConnection(String uri) throws NoSuchAlgorithmException, KeyManagementException, URISyntaxException, IOException, TimeoutException {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setShutdownTimeout(0); // So we still shut down even with slow consumers
            factory.setUri(uri);
            factory.setRequestedFrameMax(0);
            factory.setRequestedHeartbeat(0);
            return factory.newConnection();
        }

        private final ConcurrentHashMap<String, Boolean> queuesCreated = new ConcurrentHashMap<>();

        private void ensureQueue(final Channel channel, final String queueName) throws IOException {
            if (!queuesCreated.getOrDefault(queueName, false)) {
                channel.queueDeclare(queueName, true, false, false, null);
                queuesCreated.put(queueName, true);
            }
        }

        private String constructJsonObject(int i) throws UnsupportedEncodingException {
            final JSONObject jo = new JSONObject();

            jo.put("row_key", "EFG" + i);
            jo.put("column_family", "e");
            jo.put("column_qualifier", "dna_accession_number");
            jo.put("column_value", "EFN" + Integer.toString(i));
            jo.put("secondary_index", "secondaryIndexTable");
            jo.put("secondary_index_cf", "secondaryIndexCF");
            jo.put("destination_table", "destinationTable");

            return jo.toString();
        }
    }

    private static void printStats(List<Long> timings) {
        Map<String, Long> stats = getOutliers(timings);

        System.out.println(String.format("Publish times range (min-max) (#%d)%d-(#%d)%d ms",
                stats.get("min_at"),
                NANOSECONDS.toMillis(stats.get("min")),
                stats.get("max_at"),
                NANOSECONDS.toMillis(stats.get("max"))));
        System.out.println(String.format("Sum: %d ms", NANOSECONDS.toMillis(stats.get("sum"))));
        System.out.println(String.format("Size: %d", stats.get("size")));
        System.out.println(String.format("Mean publish time %d ms", NANOSECONDS.toMillis(stats.get("sum") / timings.size())));
    }


    private static Map<String, Long> getOutliers(List<Long> list) {
        Map<String, Long> v = new HashMap<>();
        long max = 0;
        long min = Long.MAX_VALUE;
        long sum = 0;
        // remove the first (highest reported timing) since it will always be the first iteration that takes the hit of declaring a queue
        // we just start at 1
        for (int i = 1; i < list.size(); i++) {
            if (list.get(i) > max) {
                max = list.get(i);
                v.put("max", max);
                v.put("max_at", (long) i);
            }
            if (list.get(i) < min) {
                min = list.get(i);
                v.put("min", min);
                v.put("min_at", (long) i);
            }
            sum += list.get(i);
        }
        v.put("sum", sum);
        v.put("size", (long) list.size());
        return v;
    }


    private static void usage(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("<program>", options);
    }

    private static Options getOptions() {
        Options options = new Options();
        options.addOption(new Option("?", "help", false, "show usage"));
        options.addOption(new Option("h", "uri", true, "connection URI"));
        options.addOption(new Option("i", "iterations", true, "number of iterations to run (msgs to send)"));
        options.addOption(new Option("t", "threadNumber", true, "number of threadsto run"));

        return options;
    }

    private static int intArg(CommandLine cmd, char opt, int def) {
        return Integer.parseInt(cmd.getOptionValue(opt, Integer.toString(def)));
    }

    private static String strArg(CommandLine cmd, char opt, String def) {
        return cmd.getOptionValue(opt, def);
    }



}
