package com.ggc.aws.lambda;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;


/**
 * This is a simple handler to log the results of reading and S3 Object
 */
public class HandlerStringFormatter implements IObjectHandler {
    final static Logger _log = LoggerFactory.getLogger(HandlerStringFormatter.class);

    public static final String METADATA_EXAMPLE = "metadata_example";

    @Override
    public void handle(S3Object object) {
        BufferedReader reader = null;
        try {
            InputStream stream = object.getObjectContent();
            if (null != stream) {
                reader = new BufferedReader(new InputStreamReader(stream));

                try {

                    //
                    // Read object contents
                    //
                    _log.info(String.format("Hello, %1$s!", reader.readLine()));

                    ObjectMetadata metadata = object.getObjectMetadata();
                    if (null != metadata) {
                        //
                        // example read user metadata
                        //
                        _log.info("User Metadata: " +
                                metadata.getUserMetaDataOf(METADATA_EXAMPLE));

                        //
                        // example read object metadata
                        //
                        _log.info("Last Modified: " + metadata.getLastModified());
                    }
                } catch (IOException e) {
                    // eat exception
                }
            }
        } finally {
            if (null != reader) {
                try {
                    reader.close();
                } catch (IOException e) {
                    // eat exception
                }
            }
        }

    }
}
