package com.ggc.aws.lambda;

import com.amazonaws.services.s3.model.S3Object;

/**
 * This class interface is used to handle objects retreived by S3SnsHandler.  To utilize
 * S3SnsHanlder you <b>MUST</b> provide and implementation of this interface in
 * your package.
 */
public interface IObjectHandler {
    public void handle(S3Object object);
}

