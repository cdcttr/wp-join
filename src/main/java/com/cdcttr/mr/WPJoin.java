/*
 */
package com.cdcttr.mr;

import com.cdcttr.avro.AvroMIFWikiPage;
import com.cdcttr.avro.AvroMIFWikiCategoryLink;
import com.cdcttr.avro.AvroOutputUnion;
import com.cdcttr.avro.WikiCategoryLink;
import com.cdcttr.avro.AvroUnionConstructorVisitor;
import com.cdcttr.avro.WikiPage;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.avro.Schema;
import org.apache.avro.mapred.*;
import org.apache.avro.reflect.ReflectData;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author cdcttr
 */
public class WPJoin extends Configured implements Tool {

    public static void main(String [] args) throws Exception {
        ToolRunner.run(new WPJoin(), args);
    }
    
    public int run(String[] args) throws IOException {
        JobConf jc = new JobConf(getConf(), getClass());
        AvroJob.setReflect(jc);
        
        MultipleInputs.addInputPath(jc, new Path(args[0]), AvroMIFWikiPage.class, WPPageJoinMapper.class);
        MultipleInputs.addInputPath(jc, new Path(args[1]), AvroMIFWikiCategoryLink.class, WPCategoryLinkJoinMapper.class);

        // map output: key and value schemas
        Schema mokeyschema = ReflectData.get().getSchema(JoinKey.class);
        Schema moschema = ReflectData.get().getSchema(AvroOutputUnion.class);
        AvroJob.setMapOutputSchema(jc, Pair.getPairSchema(mokeyschema, moschema));
        
        AvroJob.setReducerClass(jc, WPJoinReducer.class);
        
        Schema oschema = ReflectData.get().getSchema(WikiTitleCategoryLink.class);
        AvroJob.setOutputSchema(jc, oschema);
        
        jc.setPartitionerClass(PartitionJoinKey.class);
        jc.setOutputValueGroupingComparator(GroupingComparator.class);
        
        FileOutputFormat.setOutputPath(jc, new Path(args[2]));
        
        JobClient.runJob(jc);
        
        return 0;
    }
        
    
    public static class WikiTitleCategoryLink extends WikiCategoryLink {
        public String pagetitle;
        
        public WikiTitleCategoryLink() {}
        
        public WikiTitleCategoryLink(WikiCategoryLink wcl, String title) {
            set(wcl, title);
        }
        
        public void set(WikiCategoryLink wcl, String title) {
            this.id = wcl.id;
            this.to = wcl.to;
            this.sortkey = wcl.sortkey;
            this.timestamp = wcl.timestamp;
            this.sortkey_prefix = wcl.sortkey_prefix;
            this.collation = wcl.collation;
            this.type = wcl.type;
            this.pagetitle = title;            
        }
    }
    
    
    
    public static class JoinKey {
        public Long key;
        public int position;
    }
    
    
    static public class PartitionJoinKey implements Partitioner<AvroKey<JoinKey>, AvroValue> {

        public int getPartition(AvroKey<JoinKey> k2, AvroValue v2, int i) {
            return k2.datum().key.hashCode() % i;
        }

        public void configure(JobConf jc) {
        }
    }

    public static class GroupingComparator extends AvroKeyComparator<JoinKey> {

        @Override
        public int compare(AvroWrapper<JoinKey> x, AvroWrapper<JoinKey> y) {
            JoinKey xk, yk;
            xk = x.datum();
            yk = y.datum();

            return xk.key.compareTo(yk.key);
        }
    }
    
    /**
     * Base class for the mappers - uses reflection to get fields and values
     */
    public static class WPJoinMapper extends MapReduceBase implements Mapper<AvroWrapper, NullWritable, AvroKey<JoinKey>, AvroValue<AvroOutputUnion>> {
        protected Class cls;
        protected String joinfield;
        protected int position;
                
        public void map(AvroWrapper k1, NullWritable v1, OutputCollector<AvroKey<JoinKey>, AvroValue<AvroOutputUnion>> oc, Reporter rprtr) throws IOException {
            try {
                JoinKey jk = new JoinKey();
                
                AvroUnionConstructorVisitor obj = (AvroUnionConstructorVisitor)k1.datum();

                jk.key = (Long)cls.getField(joinfield).get(obj);
                jk.position = position;
                
                oc.collect(new AvroKey(jk), new AvroValue(obj.getOutputUnion()));
            } catch (IllegalArgumentException ex) {
                Logger.getLogger(WPJoin.class.getName()).log(Level.SEVERE, null, ex);
            } catch (IllegalAccessException ex) {
                Logger.getLogger(WPJoin.class.getName()).log(Level.SEVERE, null, ex);
            } catch (NoSuchFieldException ex) {
                Logger.getLogger(WPJoin.class.getName()).log(Level.SEVERE, null, ex);
            } catch (SecurityException ex) {
                Logger.getLogger(WPJoin.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }
    
    public static class WPPageJoinMapper extends WPJoinMapper {
        public WPPageJoinMapper() {
            cls = WikiPage.class;
            position = 0;
            joinfield = "id";
        }
    }
    
    public static class WPCategoryLinkJoinMapper extends WPJoinMapper {
        public WPCategoryLinkJoinMapper() {
            cls = WikiCategoryLink.class;
            position = 1;
            joinfield = "id";
        }
    }

    public static class WPJoinReducer extends AvroReducer<JoinKey, AvroOutputUnion, WikiTitleCategoryLink> {
        @Override
        public void reduce(JoinKey key, Iterable<AvroOutputUnion> values, AvroCollector<WikiTitleCategoryLink> collector, Reporter reporter) throws IOException {
            WikiPage wp = null;
            WikiTitleCategoryLink wtcl = new WikiTitleCategoryLink();
            
            for(AvroOutputUnion aou:values) {
                if(aou.wp != null) {
                    // should have one and only one WikiPage per JoinKey
                    assert wp == null;
                    
                    wp = aou.wp;
                } else {
                    if(wp == null) {
                        reporter.incrCounter(getClass().getSimpleName(), "no_page", 1);
                        return;
                    }
                    wtcl.set(aou.wcl, wp.title);
                    collector.collect(wtcl);
                }
            }
        }
    }
}
