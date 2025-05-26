package com.pinterest.secor.parser;

import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.message.Message;
import junit.framework.TestCase;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.TimeZone;

@RunWith(PowerMockRunner.class)
public class PatternDateMessageParserTest extends TestCase {

    private SecorConfig mConfig;
    private Message mFormat1;
    private Message mFormat2;
    private Message mFormat3;
    private Message mInvalidDate;
    private Message mISOFormat;
    private Message mNanosecondISOFormat;
    private Message mNestedISOFormat;
    private long timestamp;

    @Override
    public void setUp() throws Exception {
        mConfig = Mockito.mock(SecorConfig.class);
        Mockito.when(mConfig.getTimeZone()).thenReturn(TimeZone.getTimeZone("Asia/Kolkata"));
        Mockito.when(mConfig.getMessageTimeZone()).thenReturn(TimeZone.getTimeZone("UTC"));
        Mockito.when(mConfig.isPartitionPrefixEnabled()).thenReturn(true);
        Mockito.when(mConfig.getPartitionPrefixIdentifier()).thenReturn("eid");

        timestamp = System.currentTimeMillis();

        byte format1[] = "{\"timestamp\":\"2014-07-30 22:53:20\",\"eid\":\"ME_WORKFLOW_SUMMARY\",\"id\":0,\"guid\":\"0436b17b-e78a-4e82-accf-743bf1f0b884\",\"isActive\":false,\"balance\":\"$3,561.87\",\"picture\":\"http://placehold.it/32x32\",\"age\":23,\"eyeColor\":\"green\",\"name\":\"Mercedes Brewer\",\"gender\":\"female\",\"company\":\"MALATHION\",\"email\":\"mercedesbrewer@malathion.com\",\"phone\":\"+1 (848) 471-3000\",\"address\":\"786 Gilmore Court, Brule, Maryland, 3200\",\"about\":\"Quis nostrud Lorem deserunt esse ut reprehenderit aliqua nisi et sunt mollit est. Cupidatat incididunt minim anim eiusmod culpa elit est dolor ullamco. Aliqua cillum eiusmod ullamco nostrud Lorem sit amet Lorem aliquip esse esse velit.\\r\\n\",\"registered\":\"2014-01-14T13:07:28 +08:00\",\"latitude\":47.672012,\"longitude\":102.788623,\"tags\":[\"amet\",\"amet\",\"dolore\",\"eu\",\"qui\",\"fugiat\",\"laborum\"],\"friends\":[{\"id\":0,\"name\":\"Rebecca Hardy\"},{\"id\":1,\"name\":\"Sutton Briggs\"},{\"id\":2,\"name\":\"Dena Campos\"}],\"greeting\":\"Hello, Mercedes Brewer! You have 7 unread messages.\",\"favoriteFruit\":\"strawberry\"}"
                .getBytes("UTF-8");
        mFormat1 = new Message("test", 0, 0, null, format1, timestamp, null);

        byte format2[] = "{\"timestamp\":\"2014/10/25\",\"eid\":\"IMPRESSION\",\"id\":0,\"guid\":\"0436b17b-e78a-4e82-accf-743bf1f0b884\",\"isActive\":false,\"balance\":\"$3,561.87\",\"picture\":\"http://placehold.it/32x32\",\"age\":23,\"eyeColor\":\"green\",\"name\":\"Mercedes Brewer\",\"gender\":\"female\",\"company\":\"MALATHION\",\"email\":\"mercedesbrewer@malathion.com\",\"phone\":\"+1 (848) 471-3000\",\"address\":\"786 Gilmore Court, Brule, Maryland, 3200\",\"about\":\"Quis nostrud Lorem deserunt esse ut reprehenderit aliqua nisi et sunt mollit est. Cupidatat incididunt minim anim eiusmod culpa elit est dolor ullamco. Aliqua cillum eiusmod ullamco nostrud Lorem sit amet Lorem aliquip esse esse velit.\\r\\n\",\"registered\":\"2014-01-14T13:07:28 +08:00\",\"latitude\":47.672012,\"longitude\":102.788623,\"tags\":[\"amet\",\"amet\",\"dolore\",\"eu\",\"qui\",\"fugiat\",\"laborum\"],\"friends\":[{\"id\":0,\"name\":\"Rebecca Hardy\"},{\"id\":1,\"name\":\"Sutton Briggs\"},{\"id\":2,\"name\":\"Dena Campos\"}],\"greeting\":\"Hello, Mercedes Brewer! You have 7 unread messages.\",\"favoriteFruit\":\"strawberry\"}"
                .getBytes("UTF-8");
        mFormat2 = new Message("test", 0, 0, null, format2, timestamp, null);

        byte format3[] = "{\"timestamp\":\"02001.July.04 AD 12:08 PM\",\"eid\":\"ME_WORKFLOW_SUMMARY\",\"id\":0,\"guid\":\"0436b17b-e78a-4e82-accf-743bf1f0b884\",\"isActive\":false,\"balance\":\"$3,561.87\",\"picture\":\"http://placehold.it/32x32\",\"age\":23,\"eyeColor\":\"green\",\"name\":\"Mercedes Brewer\",\"gender\":\"female\",\"company\":\"MALATHION\",\"email\":\"mercedesbrewer@malathion.com\",\"phone\":\"+1 (848) 471-3000\",\"address\":\"786 Gilmore Court, Brule, Maryland, 3200\",\"about\":\"Quis nostrud Lorem deserunt esse ut reprehenderit aliqua nisi et sunt mollit est. Cupidatat incididunt minim anim eiusmod culpa elit est dolor ullamco. Aliqua cillum eiusmod ullamco nostrud Lorem sit amet Lorem aliquip esse esse velit.\\r\\n\",\"registered\":\"2014-01-14T13:07:28 +08:00\",\"latitude\":47.672012,\"longitude\":102.788623,\"tags\":[\"amet\",\"amet\",\"dolore\",\"eu\",\"qui\",\"fugiat\",\"laborum\"],\"friends\":[{\"id\":0,\"name\":\"Rebecca Hardy\"},{\"id\":1,\"name\":\"Sutton Briggs\"},{\"id\":2,\"name\":\"Dena Campos\"}],\"greeting\":\"Hello, Mercedes Brewer! You have 7 unread messages.\",\"favoriteFruit\":\"strawberry\"}"
                .getBytes("UTF-8");
        mFormat3 = new Message("test", 0, 0, null, format3, timestamp, null);

        byte invalidDate[] = "{\"timestamp\":\"11111111\",\"eid\":\"ME_WORKFLOW_SUMMARY\",\"id\":0,\"guid\":\"0436b17b-e78a-4e82-accf-743bf1f0b884\",\"isActive\":false,\"balance\":\"$3,561.87\",\"picture\":\"http://placehold.it/32x32\",\"age\":23,\"eyeColor\":\"green\",\"name\":\"Mercedes Brewer\",\"gender\":\"female\",\"company\":\"MALATHION\",\"email\":\"mercedesbrewer@malathion.com\",\"phone\":\"+1 (848) 471-3000\",\"address\":\"786 Gilmore Court, Brule, Maryland, 3200\",\"about\":\"Quis nostrud Lorem deserunt esse ut reprehenderit aliqua nisi et sunt mollit est. Cupidatat incididunt minim anim eiusmod culpa elit est dolor ullamco. Aliqua cillum eiusmod ullamco nostrud Lorem sit amet Lorem aliquip esse esse velit.\\r\\n\",\"registered\":\"2014-01-14T13:07:28 +08:00\",\"latitude\":47.672012,\"longitude\":102.788623,\"tags\":[\"amet\",\"amet\",\"dolore\",\"eu\",\"qui\",\"fugiat\",\"laborum\"],\"friends\":[{\"id\":0,\"name\":\"Rebecca Hardy\"},{\"id\":1,\"name\":\"Sutton Briggs\"},{\"id\":2,\"name\":\"Dena Campos\"}],\"greeting\":\"Hello, Mercedes Brewer! You have 7 unread messages.\",\"favoriteFruit\":\"strawberry\"}"
                .getBytes("UTF-8");
        mInvalidDate = new Message("test", 0, 0, null, invalidDate, timestamp, null);

        byte isoFormat[] = "{\"timestamp\":\"2006-01-02T15:04:05Z\",\"eid\":\"ME_WORKFLOW_SUMMARY\",\"id\":0,\"guid\":\"0436b17b-e78a-4e82-accf-743bf1f0b884\",\"isActive\":false,\"balance\":\"$3,561.87\",\"picture\":\"http://placehold.it/32x32\",\"age\":23,\"eyeColor\":\"green\",\"name\":\"Mercedes Brewer\",\"gender\":\"female\",\"company\":\"MALATHION\",\"email\":\"mercedesbrewer@malathion.com\",\"phone\":\"+1 (848) 471-3000\",\"address\":\"786 Gilmore Court, Brule, Maryland, 3200\",\"about\":\"Quis nostrud Lorem deserunt esse ut reprehenderit aliqua nisi et sunt mollit est. Cupidatat incididunt minim anim eiusmod culpa elit est dolor ullamco. Aliqua cillum eiusmod ullamco nostrud Lorem sit amet Lorem aliquip esse esse velit.\\r\\n\",\"registered\":\"2014-01-14T13:07:28 +08:00\",\"latitude\":47.672012,\"longitude\":102.788623,\"tags\":[\"amet\",\"amet\",\"dolore\",\"eu\",\"qui\",\"fugiat\",\"laborum\"],\"friends\":[{\"id\":0,\"name\":\"Rebecca Hardy\"},{\"id\":1,\"name\":\"Sutton Briggs\"},{\"id\":2,\"name\":\"Dena Campos\"}],\"greeting\":\"Hello, Mercedes Brewer! You have 7 unread messages.\",\"favoriteFruit\":\"strawberry\"}"
                .getBytes("UTF-8");
        mISOFormat = new Message("test", 0, 0, null, isoFormat, timestamp, null);

        byte nanosecondISOFormat[] = "{\"timestamp\":\"2006-01-02T23:59:59.999999999Z\",\"eid\":\"ME_WORKFLOW_SUMMARY\"}"
                .getBytes("UTF-8");
        mNanosecondISOFormat = new Message("test", 0, 0, null, nanosecondISOFormat, timestamp, null);

        byte nestedISOFormat[] = "{\"meta_data\":{\"created\":\"2016-01-11T11:50:28.647Z\"},\"eid\":\"ME_WORKFLOW_SUMMARY\",\"id\":0,\"guid\":\"0436b17b-e78a-4e82-accf-743bf1f0b884\",\"isActive\":false,\"balance\":\"$3,561.87\",\"picture\":\"http://placehold.it/32x32\",\"age\":23,\"eyeColor\":\"green\",\"name\":\"Mercedes Brewer\",\"gender\":\"female\",\"company\":\"MALATHION\",\"email\":\"mercedesbrewer@malathion.com\",\"phone\":\"+1 (848) 471-3000\",\"address\":\"786 Gilmore Court, Brule, Maryland, 3200\",\"about\":\"Quis nostrud Lorem deserunt esse ut reprehenderit aliqua nisi et sunt mollit est. Cupidatat incididunt minim anim eiusmod culpa elit est dolor ullamco. Aliqua cillum eiusmod ullamco nostrud Lorem sit amet Lorem aliquip esse esse velit.\\r\\n\",\"registered\":\"2014-01-14T13:07:28 +08:00\",\"latitude\":47.672012,\"longitude\":102.788623,\"tags\":[\"amet\",\"amet\",\"dolore\",\"eu\",\"qui\",\"fugiat\",\"laborum\"],\"friends\":[{\"id\":0,\"name\":\"Rebecca Hardy\"},{\"id\":1,\"name\":\"Sutton Briggs\"},{\"id\":2,\"name\":\"Dena Campos\"}],\"greeting\":\"Hello, Mercedes Brewer! You have 7 unread messages.\",\"favoriteFruit\":\"strawberry\"}"
                .getBytes("UTF-8");
        mNestedISOFormat = new Message("test", 0, 0, null, nestedISOFormat, timestamp, null);
    }

    @Test
    public void testExtractDateUsingInputPattern() throws Exception {
        Mockito.when(mConfig.getMessageTimestampName()).thenReturn("timestamp");
        Mockito.when(mConfig.getPartitionPrefixMapping()).thenReturn("{\"ME_WORKFLOW_SUMMARY\":\"summary\",\"DEFAULT\":\"raw\"}");

        Mockito.when(mConfig.getMessageTimestampInputPattern()).thenReturn("yyyy-MM-dd HH:mm:ss");
        assertEquals("summary/2014-07-31", new PatternDateMessageParser(mConfig).extractPartitions(mFormat1)[0]);

        Mockito.when(mConfig.getMessageTimestampInputPattern()).thenReturn("yyyy/MM/d");
        assertEquals("raw/2014-10-25", new PatternDateMessageParser(mConfig).extractPartitions(mFormat2)[0]);

        Mockito.when(mConfig.getMessageTimestampInputPattern()).thenReturn("yyyyy.MMMMM.dd GGG hh:mm aaa");
        assertEquals("summary/2001-07-04", new PatternDateMessageParser(mConfig).extractPartitions(mFormat3)[0]);

        Mockito.when(mConfig.getMessageTimestampInputPattern()).thenReturn("yyyy-MM-dd'T'HH:mm:ss'Z'");
        assertEquals("summary/2006-01-02", new PatternDateMessageParser(mConfig).extractPartitions(mISOFormat)[0]);

        Mockito.when(mConfig.getMessageTimestampInputPattern()).thenReturn("yyyy-MM-dd'T'HH:mm:ss");
        assertEquals("summary/2006-01-02", new PatternDateMessageParser(mConfig).extractPartitions(mISOFormat)[0]);

        Mockito.when(mConfig.getMessageTimestampInputPattern()).thenReturn("yyyy-MM-dd'T'HH:mm:ss");
        assertEquals("summary/2006-01-03", new PatternDateMessageParser(mConfig).extractPartitions(mNanosecondISOFormat)[0]);
    }

    @Test
    public void testExtractDateWhenPrefixIsNotSet() throws Exception {
        Mockito.when(mConfig.getMessageTimestampName()).thenReturn("timestamp");
        Mockito.when(mConfig.isPartitionPrefixEnabled()).thenReturn(false);

        Mockito.when(mConfig.getMessageTimestampInputPattern()).thenReturn("yyyy-MM-dd HH:mm:ss");
        assertEquals("2014-07-31", new PatternDateMessageParser(mConfig).extractPartitions(mFormat1)[0]);
    }

    @Test
    public void testExtractDateWithWrongEntries() throws Exception {
        Mockito.when(mConfig.getMessageTimestampName()).thenReturn("timestamp");
        Mockito.when(mConfig.getString("secor.partition.output_dt_format", "yyyy-MM-dd")).thenReturn("yyyy-MM-dd");
        Mockito.when(mConfig.getMessageTimestampName()).thenReturn("timestamp");
        Mockito.when(mConfig.getPartitionPrefixMapping()).thenReturn("{\"ME_WORKFLOW_SUMMARY\":\"summary\",\"DEFAULT\":\"raw\"}");

        // invalid date
        Mockito.when(mConfig.getMessageTimestampInputPattern()).thenReturn("yyyy-MM-dd HH:mm:ss"); // any pattern
        assertEquals("raw/" + PatternDateMessageParser.defaultDate, new PatternDateMessageParser(
                mConfig).extractPartitions(mInvalidDate)[0]);

        // invalid pattern
        Mockito.when(mConfig.getMessageTimestampInputPattern()).thenReturn("yyy-MM-dd :s");
        assertEquals("raw/" + PatternDateMessageParser.defaultDate, new PatternDateMessageParser(
                mConfig).extractPartitions(mFormat1)[0]);
    }

    @Test
    public void testDatePrefix() throws Exception {
        Mockito.when(mConfig.getMessageTimestampName()).thenReturn("timestamp");
        Mockito.when(mConfig.getPartitionPrefixMapping()).thenReturn("{\"ME_WORKFLOW_SUMMARY\":\"summary\",\"DEFAULT\":\"raw\"}");
        Mockito.when(mConfig.getMessageTimestampInputPattern()).thenReturn("yyyy-MM-dd HH:mm:ss");
        Mockito.when(mConfig.getString("secor.partition.output_dt_format", "yyyy-MM-dd")).thenReturn("yyyy-MM-dd");

        assertEquals("summary/2014-07-31", new PatternDateMessageParser(mConfig).extractPartitions(mFormat1)[0]);
    }

    @Test
    public void testCustomDateFormat() throws Exception {
        Mockito.when(mConfig.getMessageTimestampName()).thenReturn("timestamp");
        Mockito.when(mConfig.getMessageTimestampInputPattern()).thenReturn("yyyy-MM-dd HH:mm:ss");
        Mockito.when(mConfig.getPartitionPrefixMapping()).thenReturn("{\"ME_WORKFLOW_SUMMARY\":\"summary\",\"DEFAULT\":\"raw\"}");
        Mockito.when(mConfig.getPartitionOutputDtFormat()).thenReturn("'yr='yyyy'/mo='MM'/dy='dd'/hr='HH");

        assertEquals("summary/yr=2014/mo=07/dy=31/hr=04", new PatternDateMessageParser(mConfig).extractPartitions(mFormat1)[0]);
    }
}
