package com.ggc.aws.lambda;

import com.amazonaws.services.lambda.runtime.events.SNSEvent;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.event.S3EventNotification;
import com.amazonaws.services.s3.event.S3EventNotification.*;
import com.amazonaws.services.s3.model.S3Object;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.mockito.Mockito.mock;

public class S3SnsHandlerTest extends HandlerTestBase {

    private ObjectMapper mapper;
    private AmazonS3 amazonS3;
    private IObjectHandler handler;

    @Before
    public void setup() {
        amazonS3 = mock(AmazonS3.class);
        mapper = new ObjectMapper();
        handler = mock(IObjectHandler.class);
    }

    private S3SnsHandler getMockTarget() throws Exception {
        S3SnsHandler target = Mockito.spy(new S3SnsHandler());
        Mockito.doReturn(amazonS3).when(target).getS3Client();
        return target;
    }

    @Test
    public void testHandleRequest() throws Exception {

        S3SnsHandler target = getMockTarget();
        Mockito.doReturn(handler).when(target).getHandler();

        target.handleRequest(this.getSNSEvent("key"), null);

        Mockito.verify(handler, Mockito.times(1)).handle(Mockito.anyObject());
    }

    @Test
    public void testHandleRequestNullHandler() throws Exception {
        S3SnsHandler target = getMockTarget();
        Mockito.doReturn("").when(target).getEnvironmentVariable(Mockito.anyString());

        String key = "key";

        // setup mock
        S3Object object = this.getS3ObjectMock(this.amazonS3, null, key, new Date(), null);

        target.handleRequest(this.getSNSEvent(key), null);

        // validate default handler pulled object content
        Mockito.verify(object, Mockito.times(1)).getObjectContent();

    }

    private SNSEvent getSNSEvent(String key) throws JsonProcessingException {

        SNSEvent.SNS sns = new SNSEvent.SNS();
        sns.setMessage(mapper.writeValueAsString(getNotification(key)));

        SNSEvent.SNSRecord record = new SNSEvent.SNSRecord();
        record.setSns(sns);

        List<SNSEvent.SNSRecord> records = new ArrayList<>();
        records.add(record);

        SNSEvent event = new SNSEvent();
        event.setRecords(records);

        return event;
    }

    /**
     * This method constructs an S3Notification dummy notification to satisfy
     * handler
     *
     * @param key the key of the object
     * @return Returns an S3Notification with the specified key
     */
    private S3EventNotification getNotification(String key) {

        UserIdentityEntity owner = new UserIdentityEntity("owner");
        S3BucketEntity bucket = new S3BucketEntity("name", owner, "arn");
        S3ObjectEntity object = new S3ObjectEntity(key, 0L, "eTag", "versionId");
        S3Entity s3 = new S3Entity("configurationId", bucket, object, "s3SchemaVersion");
        S3EventNotificationRecord record = new S3EventNotificationRecord(null, null, null, null, null, null, null, s3,
                owner);
        List<S3EventNotificationRecord> records = new ArrayList<>();
        records.add(record);

        return new S3EventNotification(records);
    }

}
