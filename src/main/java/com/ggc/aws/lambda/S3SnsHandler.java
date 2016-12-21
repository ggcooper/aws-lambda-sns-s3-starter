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
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

//import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
//import com.amazonaws.services.cloudwatch.AmazonCloudWatchClient;
//import com.amazonaws.services.cloudwatch.model.Dimension;
//import com.amazonaws.services.cloudwatch.model.MetricDatum;
//import com.amazonaws.services.cloudwatch.model.PutMetricDataRequest;
//import com.amazonaws.services.cloudwatch.model.StandardUnit;

public class S3SnsHandler implements RequestHandler<SNSEvent, String> {

    static final Logger _log = LoggerFactory.getLogger(S3SnsHandler.class);
    static final String ENV_CLASS_NAME = "ENV_CLASS_NAME";


    private ObjectMapper mapper = new ObjectMapper();
    private AmazonS3 s3Client;
    //	private AmazonCloudWatch cloudWatchClient;
    private Class handlerClass;

    public S3SnsHandler() {

    }

    @Override
    public String handleRequest(SNSEvent input, Context context) {

        _log.debug("Invoke started");

        // _log.info("Info: " + System.getenv("AWS_REGION"));
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

        List<GetObjectRequest> requests = new ArrayList<GetObjectRequest>();

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

    private void writeCloudWatch(String namespace, String prefix, String serial, double value, Date timestamp) {
        if (!StringUtils.isNullOrEmpty(serial) && !serial.startsWith("X")) {
            try {
//				AmazonCloudWatch client = getCwClient();
//
//				// set timestamp to zero midnight
//				Calendar now = Calendar.getInstance();
//				now.setTime(timestamp);
//				now.set(Calendar.HOUR, 0);
//				now.set(Calendar.MINUTE, 0);
//				now.set(Calendar.SECOND, 0);
//				timestamp = now.getTime();
//
//				PutMetricDataRequest request = new PutMetricDataRequest()
//						.withMetricData(new MetricDatum().withMetricName(prefix + "-" + serial).withValue(value)
//								.withTimestamp(timestamp).withUnit(StandardUnit.Count)
//								.withDimensions(new Dimension().withName(prefix).withValue(serial)))
//						.withNamespace(namespace);
//
//				client.putMetricData(request);

                _log.info("Put Metric: " + namespace + ":" + prefix + ":" + serial + ":" + timestamp + ":" + value);
            } catch (Exception e) {
                _log.error("Exception: ", e);
            }
        } else {
            _log.error("Serial Number empty or null or XXXXXX = " + serial);
        }
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
