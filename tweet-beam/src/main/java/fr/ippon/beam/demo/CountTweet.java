package fr.ippon.beam.demo;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;

import org.joda.time.Instant;

import org.json.JSONException;
import org.json.JSONObject;

public class CountTweet {

    // Extracts Hashtags and Mentions from Tweet
    static class ExtractEntitiesFn extends DoFn<String, String> {

        @ProcessElement
        public void processElement(ProcessContext c) {

            String in = c.element();

            if (!in.isEmpty() && isJSONValid(in)) {
                JSONObject tweet = new JSONObject(in);
                Instant timestamp = new Instant(tweet.getLong("timestamp_ms"));

                this.processTweet(c, tweet, timestamp);

                if (!tweet.isNull("quoted_status")) {
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

        // Parsing Json for user_mentions and hashtags
        protected void processTweet(ProcessContext c, JSONObject tweet, Instant timestamp) {

            JSONObject entities = tweet.getJSONObject("entities");

            entities.getJSONArray("user_mentions").forEach((mention) -> {
                c.outputWithTimestamp("@" + ((JSONObject) mention).getString("screen_name").toLowerCase(), timestamp);
            });

            entities.getJSONArray("hashtags").forEach((hashtag) -> {
                c.outputWithTimestamp("#" + ((JSONObject) hashtag).getString("text").toLowerCase(), timestamp);
            });
        }
    }

    // Other tech for formating output with ParDo (without MapElements)
    public static class FormatAsTextFn extends DoFn<KV<String, Long>, String> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            String text = c.element().getKey() + "," + c.element().getValue();
            c.output(text);
        }
    }

    public static interface TwitterDataflowOptions extends PipelineOptions {
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
        TwitterDataflowOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(TwitterDataflowOptions.class);
        options.setJobName("TweetBeam:CountTweet");

        Pipeline p = Pipeline.create(options);

        // Read tweets file where each element is one line from input text (json tweet).
        p.apply(TextIO.Read.from(options.getInput()))

                // Apply a ParDo transform to the PCollection of text lines.
                // ParDo invokes a DoFn on each element that extracts Hashtags and Mentions.
                // The ParDo returns a PCollection<String>, where each element is an Hashtags or Mentions.
                .apply("Extract", ParDo.of(new ExtractEntitiesFn()))

                // Count transform returns a new PCollection of key/value pairs, where each key represents a unique
                // hashtags and mentions. The associated value is the occurrence count for the
                // string represented hashtag or mention.
                .apply("Count", Count.<String>perElement())

                // Apply a MapElements transform that formats the PCollection of hashtag or mention counts
                // into a printable string, suitable for writing to an output file.
                .apply("FormatResults", MapElements.via(new SimpleFunction<KV<String, Long>, String>() {
                    @Override
                    public String apply(KV<String, Long> input) {
                        return input.getKey() + "," + input.getValue();
                    }
                }))
                //.apply("Format", ParDo.of(new FormatAsTextFn()))

                // Write the contents of the PCollection to a single file (with withoutSharding) as output.
                .apply(TextIO.Write.to(options.getOutput()).withoutSharding());

        // Run the pipeline.
        p.run().waitUntilFinish();
    }
}
