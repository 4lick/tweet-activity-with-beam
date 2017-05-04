package fr.ippon.beam.demo;

import org.apache.beam.sdk.Pipeline;
//import org.apache.beam.sdk.coders.StringUtf8Coder;

import org.apache.beam.sdk.coders.BigEndianLongCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.repackaged.com.google.common.collect.ImmutableMap;
import org.apache.beam.sdk.transforms.*;

import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
//import org.apache.beam.sdks.java.core.repackaged.com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringEscapeUtils;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

import org.apache.beam.sdk.io.kafka.KafkaIO;

public class KafkaTopTweet {

    static final long WINDOW_SIZE = 20;  // Default window duration in seconds
    static final String KAFKA_TOPIC = "tweets-topic";  // Default kafka topic to read from
    static final String KAFKA_BROKER = "localhost:9092";  // Default kafka broker to contact
    static final long SLIDE_SIZE = 10;  // Default window slide in seconds
    static final String KAFKA_RESULT = "result";

    static class ExtractEntitiesFn extends DoFn<String, String> {

        private static final Logger LOG = LoggerFactory.getLogger(ExtractEntitiesFn.class);

        @ProcessElement
        public void processElement(ProcessContext c) {

            String in = c.element();

            if (!in.isEmpty() && isJSONValid(in)) {
                JSONObject tweet = new JSONObject(in);
                Instant timestamp = Instant.now(); //TODO: Ugly, replace timestamp by current timestamp when empty

                if (!tweet.isNull("timestamp_ms")) {
                    timestamp = new Instant(tweet.getLong("timestamp_ms"));
                    LOG.info("Timestamp: " + timestamp);
                    this.processTweet(c, tweet, timestamp);
                }

                if (!tweet.isNull("quoted_status")) {
                    LOG.debug("Found quoted_status");
                    JSONObject quotedTweet = tweet.getJSONObject("quoted_status");
                    this.processTweet(c, quotedTweet, timestamp);
                }
            }
        }

        private boolean isJSONValid(String in) {
            try {
                new JSONObject(in);
            } catch (JSONException ex) {
                return false;
            }
            return true;
        }

        protected void processTweet(ProcessContext c, JSONObject tweet, Instant timestamp) {

            Instant now = Instant.now(); //TODO: Use real timestamp (add watermark delay)

            JSONObject entities = tweet.getJSONObject("entities");

            entities.getJSONArray("user_mentions").forEach((mention) -> {
                LOG.debug("Mention: " + "@" + ((JSONObject) mention).getString("screen_name").toLowerCase());
                //c.outputWithTimestamp("@" + ((JSONObject) mention).getString("screen_name").toLowerCase(), timestamp);
                c.outputWithTimestamp("@" + ((JSONObject) mention).getString("screen_name").toLowerCase(), now);
            });

            entities.getJSONArray("hashtags").forEach((hashtag) -> {
                LOG.debug("Hashtag: " + "#" + ((JSONObject) hashtag).getString("text").toLowerCase());
                //c.outputWithTimestamp("#" + ((JSONObject) hashtag).getString("text").toLowerCase(), timestamp);
                c.outputWithTimestamp("#" + ((JSONObject) hashtag).getString("text").toLowerCase(), now);
            });
        }
    }

    static class mentionHashTagSpliting extends DoFn<KV<String, Long>, KV<String, KV<String, Long>>> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            if (c.element().getKey().startsWith("@"))
                c.output(KV.<String, KV<String, Long>>of("mention", c.element()));
            else
                c.output(KV.<String, KV<String, Long>>of("hashtag", c.element()));
        }
    }

    static class TransformForPrint extends DoFn<KV<String, List<KV<String, Long>>>, String> {

        private static final long serialVersionUID = -7661898092810755425L;

        @ProcessElement
        public void processElement(ProcessContext c) {
            String output = "" + c.element().getKey() + "," + c.timestamp();
            for (KV<String, Long> e : c.element().getValue()) {
                output += "," + e.getKey() + "," + e.getValue();
                System.out.println("=> " + "," + e.getKey() + "," + e.getValue());
            }
            c.output(output);
        }
    }

    static class BytesToString extends DoFn<byte[], String> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            byte[] bytes = c.element();
            String str = new String(bytes, StandardCharsets.UTF_8);

            String in = StringEscapeUtils.unescapeJava(str);
            String s = in.substring(1, in.length() - 1);
            c.output(s);
        }
    }

    public static interface KafkaToTopicOptions extends PipelineOptions {

        @Description("The Kafka topic to read from")
        @Default.String(KAFKA_TOPIC)
        String getKafkaTopic();

        void setKafkaTopic(String value);

        @Description("The Kafka Broker to read from")
        @Default.String(KAFKA_BROKER)
        String getBroker();

        void setBroker(String value);

        @Description("Sliding window duration, in seconds")
        @Default.Long(WINDOW_SIZE)
        Long getWindowSize();

        void setWindowSize(Long value);

        @Description("Window slide, in seconds")
        @Default.Long(SLIDE_SIZE)
        Long getSlide();

        void setSlide(Long value);
    }

    public static void main(String[] args) {

        KafkaToTopicOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(KafkaToTopicOptions.class);
        options.setJobName("LeadDataflow:KafkaTopTweet - WindowSize: " + options.getWindowSize() + " seconds");

        // result by add .withKeyCoder(BigEndianLongCoder.of()), a la place de <Void, String>
        final PTransform<PCollection<String>, PDone> kafkaSink =
                KafkaIO.<Long, String>write()
                        .withBootstrapServers(KAFKA_BROKER)
                        .withTopic(KAFKA_RESULT)
                        .updateProducerProperties(ImmutableMap.of("serializer.class", "kafka.serializer.StringEncoder"))
                        .withKeyCoder(BigEndianLongCoder.of())
                        .withValueCoder(StringUtf8Coder.of())
                        .values();

        Pipeline p = Pipeline.create(options);

        // Apache Beam reading from Kafka gives CoderException: java.io.EOFException : http://stackoverflow.com/questions/43209743/apache-beam-reading-from-kafka-gives-coderexception-java-io-eofexception/43233399
        p.apply(KafkaIO.<Long, String>readBytes()
                .withBootstrapServers(KAFKA_BROKER)
                .withTopics(Arrays.asList(KAFKA_TOPIC))
                .withoutMetadata())
                .apply(Values.<byte[]>create())
                .apply(ParDo.of(new BytesToString()))
                .apply("Extract", ParDo.of(new ExtractEntitiesFn()))
                .apply("Window", Window.<String>into(SlidingWindows.of(Duration.standardSeconds(WINDOW_SIZE))
                        .every(Duration.standardSeconds(WINDOW_SIZE)))
                        .triggering(AfterProcessingTime.pastFirstElementInPane()
                                .plusDelayOf(Duration.standardMinutes(1))).withAllowedLateness(Duration.ZERO)
                        .discardingFiredPanes())
                .apply("Count", Count.<String>perElement())
                .apply("Split Mention / Hash", ParDo.of(new mentionHashTagSpliting()))
                .apply("Top 3", Top.<String, KV<String, Long>, KV.OrderByValue<String, Long>>perKey(3, new KV.OrderByValue<String, Long>()))
                .apply(ParDo.of(new TransformForPrint()))
                .apply(kafkaSink);

        p.run().waitUntilFinish();
    }
}
