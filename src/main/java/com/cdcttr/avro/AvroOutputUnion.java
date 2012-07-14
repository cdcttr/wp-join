/*
 */
package com.cdcttr.avro;

import org.apache.avro.reflect.Nullable;

/**
 *
 * @author cdcttr
 */
public class AvroOutputUnion {

    @Nullable
    public WikiPage wp = null;
    @Nullable
    public WikiCategoryLink wcl = null;
}
