/*
 */
package com.cdcttr.avro;

/**
 *
 * @author cdcttr
 */
public class WikiCategoryLink implements AvroUnionConstructorVisitor {

    public long id;
    public String to;
    public String sortkey;
    public String timestamp;
    public String sortkey_prefix;
    public String collation;
    public String type;

    public AvroOutputUnion getOutputUnion() {
        AvroOutputUnion aou = new AvroOutputUnion();
        aou.wcl = this;

        return aou;
    }
}
