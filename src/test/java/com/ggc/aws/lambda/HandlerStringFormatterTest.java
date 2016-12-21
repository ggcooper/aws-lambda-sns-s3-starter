package com.ggc.aws.lambda;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class HandlerStringFormatterTest extends HandlerTestBase{
    @Test
    public void testHandle() throws Exception {
        HandlerStringFormatter target = new HandlerStringFormatter();

        String string = new String(Files.readAllBytes(Paths.get("src", "test", "resources", "s3Object.txt")));
        InputStream stream = new ByteArrayInputStream(string.getBytes(StandardCharsets.UTF_8));

        // setup mock
        Map<String, String> metadata = new HashMap<>();
        metadata.put(HandlerStringFormatter.METADATA_EXAMPLE, "metadata");
        ObjectMetadata metadataMock = getMetadataMock(metadata);
        S3Object object = this.getS3ObjectMock(null, stream, "key", new Date(), metadataMock);

        target.handle(object);

        // validate default handler pulled object content
        Mockito.verify(object, Mockito.times(1)).getObjectContent();
        Mockito.verify(object, Mockito.times(1)).getObjectMetadata();
        Mockito.verify(object.getObjectMetadata(), Mockito.times(1)).getLastModified();
        Mockito.verify(object.getObjectMetadata(), Mockito.times(1))
                .getUserMetaDataOf(HandlerStringFormatter.METADATA_EXAMPLE);
    }
}