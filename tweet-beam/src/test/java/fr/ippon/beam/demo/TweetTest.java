package fr.ippon.beam.demo;

import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.RunnableOnService;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Arrays;
import java.util.List;

@RunWith(JUnit4.class)
public class TweetTest {

    @Test
    public void testExtractTweetsFn() throws Exception {
        DoFnTester<String, String> extractFn =
                DoFnTester.of(new CountTweet.ExtractEntitiesFn());

        Assert.assertThat(extractFn.processBundle("{\"text\": \"RT @HelloWord: This is a test. #Test\", \"id\": 1234567890112, \"timestamp_ms\": \"1487171207\", \"entities\": {\"user_mentions\": [{\"id\": 11111111, \"indices\": [3, 13], \"id_str\": \"777925\", \"screen_name\": \"HelloWord\", \"name\": \"Hello Word\"}], \"symbols\": [], \"hashtags\": [{\"indices\": [15, 36], \"text\": \"Test\"}], \"urls\": []}, \"id_str\": \"12738165059\"}"),
                CoreMatchers.hasItems("@helloword", "#test"));
    }

    static final String[] TWEETS_ARRAY = new String[]{
            "{\"text\": \"RT @HelloWord: This is a test. #Test\", \"id\": 1234567890112, \"timestamp_ms\": \"1487171207\", \"entities\": {\"user_mentions\": [{\"id\": 11111111, \"indices\": [3, 13], \"id_str\": \"777925\", \"screen_name\": \"HelloWord\", \"name\": \"Hello Word\"}], \"symbols\": [], \"hashtags\": [{\"indices\": [15, 36], \"text\": \"Test\"}], \"urls\": []}, \"id_str\": \"12738165059\"}",
            "{\"text\": \"@HelloWord: A other #Test from @HelloWord\", \"id\": 33333333333, \"timestamp_ms\": \"1487257607\", \"entities\": {\"user_mentions\": [{\"id\": 2222222, \"indices\": [0, 10], \"id_str\": \"444444444\", \"screen_name\": \"HelloWord\", \"name\": \"Hello Word\"}], \"symbols\": [], \"hashtags\": [{\"indices\": [20, 25], \"text\": \"Test\"}], \"urls\": []}, \"id_str\": \"11111111\"}",
            "{\"id\":859404888274587649,\"id_str\":\"859404888274587649\",\"text\":\"RT @JavaScriptDaily: Node.js 8.0 Delayed Till End of May, and Here's Why: https:\\/\\/t.co\\/dT6GGKVhkf\",\"source\":\"\\u003ca href=\\\"http:\\/\\/twitter.com\\/download\\/iphone\\\" rel=\\\"nofollow\\\"\\u003eTwitter for iPhone\\u003c\\/a\\u003e\",\"truncated\":false,\"in_reply_to_status_id\":null,\"in_reply_to_status_id_str\":null,\"in_reply_to_user_id\":null,\"in_reply_to_user_id_str\":null,\"in_reply_to_screen_name\":null,\"user\":{\"id\":807623037004587008,\"id_str\":\"807623037004587008\",\"name\":\"LaBrightWebDev\",\"screen_name\":\"LaBrightWebDev\",\"location\":\"Massachusetts, USA\",\"url\":null,\"description\":\"Hi, I'm Laura. I'm a techie who loves training users how to use apps better & more efficiently. I also love web development.\",\"protected\":false,\"verified\":false,\"followers_count\":51,\"friends_count\":127,\"listed_count\":4,\"favourites_count\":47,\"statuses_count\":134,\"created_at\":\"Sat Dec 10 16:28:31 +0000 2016\",\"utc_offset\":-25200,\"time_zone\":\"Pacific Time (US & Canada)\",\"geo_enabled\":false,\"lang\":\"en\",\"contributors_enabled\":false,\"is_translator\":false,\"profile_background_color\":\"F5F8FA\",\"profile_background_image_url\":\"\",\"profile_background_image_url_https\":\"\",\"profile_background_tile\":false,\"profile_link_color\":\"1DA1F2\",\"profile_sidebar_border_color\":\"C0DEED\",\"profile_sidebar_fill_color\":\"DDEEF6\",\"profile_text_color\":\"333333\",\"profile_use_background_image\":true,\"profile_image_url\":\"http:\\/\\/pbs.twimg.com\\/profile_images\\/807632184093745152\\/s_2FW4Fv_normal.jpg\",\"profile_image_url_https\":\"https:\\/\\/pbs.twimg.com\\/profile_images\\/807632184093745152\\/s_2FW4Fv_normal.jpg\",\"default_profile\":true,\"default_profile_image\":false,\"following\":null,\"follow_request_sent\":null,\"notifications\":null},\"geo\":null,\"coordinates\":null,\"place\":null,\"contributors\":null,\"retweeted_status\":{\"created_at\":\"Tue May 02 12:21:27 +0000 2017\",\"id\":859382325288808448,\"id_str\":\"859382325288808448\",\"text\":\"Node.js 8.0 Delayed Till End of May, and Here's Why: https:\\/\\/t.co\\/dT6GGKVhkf\",\"source\":\"\\u003ca href=\\\"http:\\/\\/bufferapp.com\\\" rel=\\\"nofollow\\\"\\u003eBuffer\\u003c\\/a\\u003e\",\"truncated\":false,\"in_reply_to_status_id\":null,\"in_reply_to_status_id_str\":null,\"in_reply_to_user_id\":null,\"in_reply_to_user_id_str\":null,\"in_reply_to_screen_name\":null,\"user\":{\"id\":459275531,\"id_str\":\"459275531\",\"name\":\"JavaScript Daily\",\"screen_name\":\"JavaScriptDaily\",\"location\":null,\"url\":\"http:\\/\\/jslive.com\",\"description\":\"Daily JavaScript \\/ JS community news, links and events. Go to @reactdaily for React news.\",\"protected\":false,\"verified\":false,\"followers_count\":255117,\"friends_count\":192,\"listed_count\":5410,\"favourites_count\":451,\"statuses_count\":7729,\"created_at\":\"Mon Jan 09 13:43:05 +0000 2012\",\"utc_offset\":-25200,\"time_zone\":\"Pacific Time (US & Canada)\",\"geo_enabled\":false,\"lang\":\"en\",\"contributors_enabled\":false,\"is_translator\":false,\"profile_background_color\":\"51CBBF\",\"profile_background_image_url\":\"http:\\/\\/pbs.twimg.com\\/profile_background_images\\/558331836\\/jsbg.png\",\"profile_background_image_url_https\":\"https:\\/\\/pbs.twimg.com\\/profile_background_images\\/558331836\\/jsbg.png\",\"profile_background_tile\":true,\"profile_link_color\":\"D4BB00\",\"profile_sidebar_border_color\":\"F0EDDA\",\"profile_sidebar_fill_color\":\"FDFFF0\",\"profile_text_color\":\"333333\",\"profile_use_background_image\":true,\"profile_image_url\":\"http:\\/\\/pbs.twimg.com\\/profile_images\\/710428065369890816\\/ZaHBODSJ_normal.jpg\",\"profile_image_url_https\":\"https:\\/\\/pbs.twimg.com\\/profile_images\\/710428065369890816\\/ZaHBODSJ_normal.jpg\",\"profile_banner_url\":\"https:\\/\\/pbs.twimg.com\\/profile_banners\\/459275531\\/1458214369\",\"default_profile\":false,\"default_profile_image\":false,\"following\":null,\"follow_request_sent\":null,\"notifications\":null},\"geo\":null,\"coordinates\":null,\"place\":null,\"contributors\":null,\"is_quote_status\":false,\"retweet_count\":28,\"favorite_count\":26,\"entities\":{\"hashtags\":[],\"urls\":[{\"url\":\"https:\\/\\/t.co\\/dT6GGKVhkf\",\"expanded_url\":\"https:\\/\\/medium.com\\/the-node-js-collection\\/node-js-8-0-0-has-been-delayed-and-will-ship-on-or-around-may-30th-cd38ba96980d\",\"display_url\":\"medium.com\\/the-node-js-co\\u2026\",\"indices\":[53,76]}],\"user_mentions\":[],\"symbols\":[]},\"favorited\":false,\"retweeted\":false,\"possibly_sensitive\":false,\"filter_level\":\"low\",\"lang\":\"en\"},\"is_quote_status\":false,\"retweet_count\":0,\"favorite_count\":0,\"entities\":{\"hashtags\":[],\"urls\":[{\"url\":\"https:\\/\\/t.co\\/dT6GGKVhkf\",\"expanded_url\":\"https:\\/\\/medium.com\\/the-node-js-collection\\/node-js-8-0-0-has-been-delayed-and-will-ship-on-or-around-may-30th-cd38ba96980d\",\"display_url\":\"medium.com\\/the-node-js-co\\u2026\",\"indices\":[74,97]}],\"user_mentions\":[{\"screen_name\":\"JavaScriptDaily\",\"name\":\"JavaScript Daily\",\"id\":459275531,\"id_str\":\"459275531\",\"indices\":[3,19]}],\"symbols\":[]},\"favorited\":false,\"retweeted\":false,\"possibly_sensitive\":false,\"filter_level\":\"low\",\"lang\":\"en\",\"timestamp_ms\":\"1493733066992\"}"
    };

    static final List<String> TWEETS = Arrays.asList(TWEETS_ARRAY);

    static final String[] EXPECTED_RESULT = new String[]{
            "@helloword,2", "#test,2", "@javascriptdaily,1"};

    @Rule
    public TestPipeline p = TestPipeline.create();

    @Test
    @Category(RunnableOnService.class)
    public void testCountTweetPipeline() throws Exception {
        PCollection<String> input = p.apply(Create.of(TWEETS).withCoder(StringUtf8Coder.of()));

        PCollection<String> output = input.apply(ParDo.of(new CountTweet.ExtractEntitiesFn()))
                .apply("Count", Count.<String>perElement())
                .apply("Format", ParDo.of(new CountTweet.FormatAsTextFn()));

        PAssert.that(output).containsInAnyOrder(EXPECTED_RESULT);
        p.run().waitUntilFinish();
    }
}
