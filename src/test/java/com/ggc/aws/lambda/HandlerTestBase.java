package com.ggc.aws.lambda;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import org.apache.http.client.methods.HttpGet;
import org.mockito.Mockito;

import java.io.InputStream;
import java.util.Date;
import java.util.Map;

import static org.mockito.Mockito.mock;

/**
 * Base class to provide helper methods to create mocks
 */
class HandlerTestBase {

    S3Object getS3ObjectMock(AmazonS3 client, InputStream stream, String key, Date lastModified, ObjectMetadata metadata) {

        // setup s3 object
        S3ObjectInputStream s3Stream = null;
        if(null != stream) {
            s3Stream = new S3ObjectInputStream(stream, new HttpGet());
        }

        final S3Object s3Object = mock(S3Object.class);
        Mockito.doReturn(s3Stream).when(s3Object).getObjectContent();
        Mockito.doReturn(key).when(s3Object).getKey();

        if (null != client) {
            Mockito.doReturn(s3Object).when(client).getObject(Mockito.any(GetObjectRequest.class));
        }

        //
        // setup user metadata
        //
        Mockito.doReturn(metadata).when(s3Object).getObjectMetadata();

        if (null != metadata) {
            //
            // setup amazon builtin metadata
            //
            Mockito.doReturn(lastModified).when(metadata).getLastModified();
        }

        return s3Object;
    }

    ObjectMetadata getMetadataMock(Map<String, String> data) {
        ObjectMetadata metadata = Mockito.spy(new ObjectMetadata());
        metadata.setUserMetadata(data);

        return metadata;
    }

}
