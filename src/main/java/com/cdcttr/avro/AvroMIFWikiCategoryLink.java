/*
 * 
 */
package com.cdcttr.avro;

import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroInputFormat;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.reflect.ReflectData;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

/**
 *
 * @author cdcttr
 */
public class AvroMIFWikiCategoryLink extends AvroInputFormat {
    @Override
    public RecordReader getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException {
        // Set the input schema right before the retrieving the record reader
        Schema schema = ReflectData.get().getSchema(WikiCategoryLink.class);
        AvroJob.setInputSchema(job, schema);

        return super.getRecordReader(split, job, reporter);
    }
}
