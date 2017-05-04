package fr.ippon.beam.demo;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Top;

import org.apache.beam.sdk.values.KV;

import org.joda.time.Instant;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class TopTweet {

    static class ExtractEntitiesFn extends DoFn<String, String> {

        private static final Logger LOG = LoggerFactory.getLogger(ExtractEntitiesFn.class);

        @ProcessElement
        public void processElement(ProcessContext c) {

            String in = c.element();

            if (!in.isEmpty() && isJSONValid(in)) {
                JSONObject tweet = new JSONObject(in);
                Instant timestamp = new Instant(tweet.getLong("timestamp_ms"));
                LOG.info("Timestamp: " + timestamp);

                this.processTweet(c, tweet, timestamp);

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

            JSONObject entities = tweet.getJSONObject("entities");

            entities.getJSONArray("user_mentions").forEach((mention) -> {
                LOG.debug("Mention: " + "@" + ((JSONObject) mention).getString("screen_name").toLowerCase());
                c.outputWithTimestamp("@" + ((JSONObject) mention).getString("screen_name").toLowerCase(), timestamp);
            });

            entities.getJSONArray("hashtags").forEach((hashtag) -> {
                LOG.debug("Hashtag: " + "#" + ((JSONObject) hashtag).getString("text").toLowerCase());
                c.outputWithTimestamp("#" + ((JSONObject) hashtag).getString("text").toLowerCase(), timestamp);
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
        @ProcessElement
        public void processElement(ProcessContext c) {
            String output = "" + c.element().getKey() + "," + c.timestamp();
            for (KV<String, Long> e : c.element().getValue()) {
                output += "," + e.getKey() + "," + e.getValue();
            }
            c.output(output);
        }
    }


    public static interface TweetBeamOptions extends PipelineOptions {
        @Description("Path of the file to read from")
        @Default.String("/tmp/tweets.json")
        String getInput();

        void setInput(String value);

        @Description("Path of the file to write to")
        @Default.String("/tmp/lead/tweets.csv")
        String getOutput();

        void setOutput(String value);
    }

    public static void main(String[] args) {
        TweetBeamOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(TweetBeamOptions.class);
        options.setJobName("TweetBeam:TopTweet");

        Pipeline p = Pipeline.create(options);

        p.apply(TextIO.Read.from(options.getInput()))
                .apply("Extract", ParDo.of(new ExtractEntitiesFn()))
                .apply("Count", Count.<String>perElement())
                .apply("Mention / Hash Spliting", ParDo.of(new mentionHashTagSpliting()))

                // Top 3 of Mention / Hash ordered by Value.
                .apply("Top 3", Top.<String, KV<String, Long>, KV.OrderByValue<String, Long>>perKey(3, new KV.OrderByValue<String, Long>()))
                .apply(ParDo.of(new TransformForPrint()))
                .apply(TextIO.Write.to(options.getOutput()).withoutSharding());

        p.run().waitUntilFinish();
    }
}
