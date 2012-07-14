/*
 */
package com.cdcttr.avro;

/**
 *
 * @author cdcttr
 */
public class WikiPage implements AvroUnionConstructorVisitor {

    public long id;
    public String namespace;
    public String title;
    public String restrictions;
    public String counter;
    public String is_redirect;
    public String is_new;
    public String random;
    public String touched;
    public String latest;
    public String len;

    public AvroOutputUnion getOutputUnion() {
        AvroOutputUnion aou = new AvroOutputUnion();

        aou.wp = this;

        return aou;
    }
}
