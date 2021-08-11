package com.screwdriver.aws.lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import mockit.integration.junit4.JMockit;
import org.apache.commons.io.FileUtils;
import org.apache.http.client.methods.HttpGet;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.junit.Assert.assertEquals;

@RunWith(JMockit.class)
public class S3EventProcessorTest {
    private static S3Event s3Event;

    @Mocked
    private Context mockedContext;

    @Mocked
    private AmazonS3 s3Client;

    @Mocked
    AmazonS3ClientBuilder amazonS3ClientBuilder;

    @Mocked
    private GetObjectRequest getObjectRequest;

    @Mocked
    private S3Object s3Object;

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Before
    public void setUp() throws IOException {
        s3Event = TestUtils.parse("s3-event.json", S3Event.class);
    }

    @Test
    public void testHandleRequestSuccess() throws Exception {
        final File tempFile = tempFolder.newFile("archive.zip");
        final StringBuilder sb = new StringBuilder();
        sb.append("Test String");

        final ZipOutputStream out = new ZipOutputStream(new FileOutputStream(tempFile));
        final ZipEntry e = new ZipEntry("test-object.txt");
        out.putNextEntry(e);

        byte[] data = sb.toString().getBytes();
        out.write(data, 0, data.length);
        out.closeEntry();
        out.close();

        S3ObjectInputStream inputStream = new S3ObjectInputStream(FileUtils.openInputStream(tempFile), new HttpGet());

        new Expectations() {{
            s3Client.getObject((GetObjectRequest) any);
            result = s3Object;
            s3Object.getObjectContent();
            result = inputStream;
            mockedContext.getFunctionName();
            result="s3-lambda-event-processor-prod";
        }};

        String lambdaResult = new S3EventProcessor().handleRequest(s3Event, mockedContext);
        assertEquals("ok", lambdaResult);
    }

    @Test
    public void testHandleRequestError() {
        new MockUp<URLDecoder>() {
            @Mock
            String decode(String s, String enc) throws IOException {
                throw new UnsupportedEncodingException();
            }
        };
        new Expectations() {{
            mockedContext.getFunctionName();
            result="s3-lambda-event-processor-beta";
        }};
        new S3EventProcessor().handleRequest(s3Event, mockedContext);
    }

    @After
    public void tearDown() {
        tempFolder.delete();
    }
}
