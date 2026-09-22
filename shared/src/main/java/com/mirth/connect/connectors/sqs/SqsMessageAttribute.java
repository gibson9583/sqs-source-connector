/*
 * SPDX-License-Identifier: MPL-2.0
 */
package com.mirth.connect.connectors.sqs;

import java.io.Serializable;
import java.util.Objects;

/** A configurable SQS message attribute. Binary values are stored as Base64 text. */
public class SqsMessageAttribute implements Serializable {
    private static final long serialVersionUID = 1L;

    private String name;
    private String dataType;
    private String value;

    public SqsMessageAttribute() {
        this("", "String", "");
    }

    public SqsMessageAttribute(String name, String dataType, String value) {
        this.name = name;
        this.dataType = dataType;
        this.value = value;
    }

    public SqsMessageAttribute(SqsMessageAttribute attribute) {
        this(attribute.getName(), attribute.getDataType(), attribute.getValue());
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDataType() {
        return dataType;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof SqsMessageAttribute)) return false;
        SqsMessageAttribute that = (SqsMessageAttribute) other;
        return Objects.equals(name, that.name) && Objects.equals(dataType, that.dataType)
                && Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, dataType, value);
    }
}
