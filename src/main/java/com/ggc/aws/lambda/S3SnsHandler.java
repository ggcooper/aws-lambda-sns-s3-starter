package com.ggc.aws.lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SNSEvent;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.event.S3EventNotification;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class S3SnsHandler implements RequestHandler<SNSEvent, String> {

    private static final Logger _log = LoggerFactory.getLogger(S3SnsHandler.class);
    private static final String ENV_CLASS_NAME = "ENV_CLASS_NAME";


    private AmazonS3 s3Client;
    private Class handlerClass;

    @Override
    public String handleRequest(SNSEvent input, Context context) {

        _log.debug("Invoke started");

        try {

            if (null != input && null != input.getRecords() && !input.getRecords().isEmpty()) {
                for (SNSEvent.SNSRecord record : input.getRecords()) {
                    if (null != record.getSNS() && !StringUtils.isNullOrEmpty(record.getSNS().getMessage())) {

                        // extract notification
                        S3EventNotification event = S3EventNotification.parseJson(record.getSNS().getMessage());

                        List<GetObjectRequest> requests = this.getObjects(event);
                        for (GetObjectRequest request : requests) {
                            _log.debug("Retrieving [bucket, key]: " + request.getBucketName() + " " + request.getKey());
                            S3Object object = getS3Client().getObject(request);
                            try {

                                //
                                // Instaniate handler and call handler
                                //
                                IObjectHandler handler = getHandler();
                                handler.handle(object);

                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        }
                    }
                }
            }
            return null;
        } finally {
            _log.debug("Invoke returning");
        }
    }

    AmazonS3 getS3Client() {
        if (null == this.s3Client) {
            this.s3Client = new AmazonS3Client();
        }
        return this.s3Client;
    }

    private List<GetObjectRequest> getObjects(S3EventNotification event) {

        List<GetObjectRequest> requests = new ArrayList<>();

        // validate there are records
        if (null != event && null != event.getRecords() && !event.getRecords().isEmpty()) {
            for (S3EventNotification.S3EventNotificationRecord record : event.getRecords()) {

                // validate the bucket and key are non-null
                if (null != record.getS3() && null != record.getS3().getBucket()
                        && !StringUtils.isNullOrEmpty(record.getS3().getBucket().getName())
                        && null != record.getS3().getObject()
                        && !StringUtils.isNullOrEmpty(record.getS3().getObject().getKey())) {

                    // create request
                    requests.add(new GetObjectRequest(record.getS3().getBucket().getName(),
                            record.getS3().getObject().getKey()));
                }
            }
        }
        return requests;
    }

    String getEnvironmentVariable(String envVar) {
        return System.getenv(envVar);
    }

    IObjectHandler getHandler() throws IllegalAccessException, InstantiationException, ClassNotFoundException {
        if (null == handlerClass) {
            String name = getEnvironmentVariable(ENV_CLASS_NAME);
            if (StringUtils.isNullOrEmpty(name)) {
                name = HandlerStringFormatter.class.getName();
            }
            handlerClass = Class.forName(name);
        }
        return (IObjectHandler) handlerClass.newInstance();
    }
}
