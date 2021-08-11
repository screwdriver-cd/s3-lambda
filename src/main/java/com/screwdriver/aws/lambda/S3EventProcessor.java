package com.screwdriver.aws.lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.event.S3EventNotification.S3EventNotificationRecord;
import com.amazonaws.services.s3.model.*;
import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URLDecoder;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class S3EventProcessor implements RequestHandler<S3Event, String> {

    private final AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
    private static final Logger log = LoggerFactory.getLogger(S3EventProcessor.class);
    private static final int BUFFER_SIZE = 3008;
    private static final String betaKMSKeyID="ADD_ME";
    private static final String prodKMSKeyID="ADD_ME";
    private String fileName;

    @Override
    public String handleRequest(S3Event s3Event, Context context) {

        String serverSideKMSKeyID = context.getFunctionName().equals("s3-lambda-event-processor-prod")? prodKMSKeyID:betaKMSKeyID;
        for (S3EventNotificationRecord record : s3Event.getRecords()) {
            try {
                String srcBucket = record.getS3().getBucket().getName();
                String srcKey = record.getS3().getObject().getKey().replace('+', ' ');
                srcKey = URLDecoder.decode(srcKey, "UTF-8");
                log.info("Extracting zip file {}/{}", srcBucket, srcKey);
                byte[] buffer = new byte[BUFFER_SIZE];
                S3Object s3Object = s3Client.getObject(new GetObjectRequest(srcBucket, srcKey));

                try (ZipInputStream zis = new ZipInputStream(s3Object.getObjectContent())) {
                    ZipEntry entry = zis.getNextEntry();
                    while (entry != null) {
                        fileName = entry.getName();
                        log.debug("Extracting " + fileName + ", compressed: " + entry.getCompressedSize() + " bytes, extracted: " + entry.getSize() + " bytes");
                        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
                            int len;
                            while ((len = zis.read(buffer)) > 0) {
                                outputStream.write(buffer, 0, len);
                            }
                            try (InputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray())) {
                                ObjectMetadata meta = new ObjectMetadata();
                                meta.setContentLength(outputStream.size());
                                PutObjectRequest putObjectRequest = new PutObjectRequest(srcBucket, FilenameUtils.getFullPath(srcKey) + fileName,
                                        inputStream, meta).withSSEAwsKeyManagementParams(new SSEAwsKeyManagementParams(serverSideKMSKeyID));
                                s3Client.putObject(putObjectRequest);
                                entry = zis.getNextEntry();
                            }
                        }
                    }
                }
                log.info("Deleting zip file {}/{}", srcBucket, srcKey);
                s3Client.deleteObject(new DeleteObjectRequest(srcBucket,srcKey));
            } catch (Exception e) {
                log.error("Extracting {} failed. error{} ", fileName, e.getMessage());
            }
        }
        return "ok";
    }
}
