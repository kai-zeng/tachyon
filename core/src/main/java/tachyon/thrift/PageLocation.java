/**
 * Autogenerated by Thrift Compiler (0.9.1)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package tachyon.thrift;

import org.apache.thrift.scheme.IScheme;
import org.apache.thrift.scheme.SchemeFactory;
import org.apache.thrift.scheme.StandardScheme;

import org.apache.thrift.scheme.TupleScheme;
import org.apache.thrift.protocol.TTupleProtocol;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.EncodingUtils;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.server.AbstractNonblockingServer.*;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.EnumMap;
import java.util.Set;
import java.util.HashSet;
import java.util.EnumSet;
import java.util.Collections;
import java.util.BitSet;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PageLocation implements org.apache.thrift.TBase<PageLocation, PageLocation._Fields>, java.io.Serializable, Cloneable, Comparable<PageLocation> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("PageLocation");

  private static final org.apache.thrift.protocol.TField PAGE_ID_FIELD_DESC = new org.apache.thrift.protocol.TField("pageId", org.apache.thrift.protocol.TType.I64, (short)1);
  private static final org.apache.thrift.protocol.TField STORAGE_ID_FIELD_DESC = new org.apache.thrift.protocol.TField("storageId", org.apache.thrift.protocol.TType.I64, (short)2);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new PageLocationStandardSchemeFactory());
    schemes.put(TupleScheme.class, new PageLocationTupleSchemeFactory());
  }

  public long pageId; // required
  public long storageId; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    PAGE_ID((short)1, "pageId"),
    STORAGE_ID((short)2, "storageId");

    private static final Map<String, _Fields> byName = new HashMap<String, _Fields>();

    static {
      for (_Fields field : EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    public static _Fields findByThriftId(int fieldId) {
      switch(fieldId) {
        case 1: // PAGE_ID
          return PAGE_ID;
        case 2: // STORAGE_ID
          return STORAGE_ID;
        default:
          return null;
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, throwing an exception
     * if it is not found.
     */
    public static _Fields findByThriftIdOrThrow(int fieldId) {
      _Fields fields = findByThriftId(fieldId);
      if (fields == null) throw new IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /**
     * Find the _Fields constant that matches name, or null if its not found.
     */
    public static _Fields findByName(String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final String _fieldName;

    _Fields(short thriftId, String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    public short getThriftFieldId() {
      return _thriftId;
    }

    public String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments
  private static final int __PAGEID_ISSET_ID = 0;
  private static final int __STORAGEID_ISSET_ID = 1;
  private byte __isset_bitfield = 0;
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.PAGE_ID, new org.apache.thrift.meta_data.FieldMetaData("pageId", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64)));
    tmpMap.put(_Fields.STORAGE_ID, new org.apache.thrift.meta_data.FieldMetaData("storageId", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64)));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(PageLocation.class, metaDataMap);
  }

  public PageLocation() {
  }

  public PageLocation(
    long pageId,
    long storageId)
  {
    this();
    this.pageId = pageId;
    setPageIdIsSet(true);
    this.storageId = storageId;
    setStorageIdIsSet(true);
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public PageLocation(PageLocation other) {
    __isset_bitfield = other.__isset_bitfield;
    this.pageId = other.pageId;
    this.storageId = other.storageId;
  }

  public PageLocation deepCopy() {
    return new PageLocation(this);
  }

  @Override
  public void clear() {
    setPageIdIsSet(false);
    this.pageId = 0;
    setStorageIdIsSet(false);
    this.storageId = 0;
  }

  public long getPageId() {
    return this.pageId;
  }

  public PageLocation setPageId(long pageId) {
    this.pageId = pageId;
    setPageIdIsSet(true);
    return this;
  }

  public void unsetPageId() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __PAGEID_ISSET_ID);
  }

  /** Returns true if field pageId is set (has been assigned a value) and false otherwise */
  public boolean isSetPageId() {
    return EncodingUtils.testBit(__isset_bitfield, __PAGEID_ISSET_ID);
  }

  public void setPageIdIsSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __PAGEID_ISSET_ID, value);
  }

  public long getStorageId() {
    return this.storageId;
  }

  public PageLocation setStorageId(long storageId) {
    this.storageId = storageId;
    setStorageIdIsSet(true);
    return this;
  }

  public void unsetStorageId() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __STORAGEID_ISSET_ID);
  }

  /** Returns true if field storageId is set (has been assigned a value) and false otherwise */
  public boolean isSetStorageId() {
    return EncodingUtils.testBit(__isset_bitfield, __STORAGEID_ISSET_ID);
  }

  public void setStorageIdIsSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __STORAGEID_ISSET_ID, value);
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case PAGE_ID:
      if (value == null) {
        unsetPageId();
      } else {
        setPageId((Long)value);
      }
      break;

    case STORAGE_ID:
      if (value == null) {
        unsetStorageId();
      } else {
        setStorageId((Long)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case PAGE_ID:
      return Long.valueOf(getPageId());

    case STORAGE_ID:
      return Long.valueOf(getStorageId());

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case PAGE_ID:
      return isSetPageId();
    case STORAGE_ID:
      return isSetStorageId();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof PageLocation)
      return this.equals((PageLocation)that);
    return false;
  }

  public boolean equals(PageLocation that) {
    if (that == null)
      return false;

    boolean this_present_pageId = true;
    boolean that_present_pageId = true;
    if (this_present_pageId || that_present_pageId) {
      if (!(this_present_pageId && that_present_pageId))
        return false;
      if (this.pageId != that.pageId)
        return false;
    }

    boolean this_present_storageId = true;
    boolean that_present_storageId = true;
    if (this_present_storageId || that_present_storageId) {
      if (!(this_present_storageId && that_present_storageId))
        return false;
      if (this.storageId != that.storageId)
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  @Override
  public int compareTo(PageLocation other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(isSetPageId()).compareTo(other.isSetPageId());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetPageId()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.pageId, other.pageId);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetStorageId()).compareTo(other.isSetStorageId());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetStorageId()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.storageId, other.storageId);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
    schemes.get(iprot.getScheme()).getScheme().read(iprot, this);
  }

  public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    schemes.get(oprot.getScheme()).getScheme().write(oprot, this);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("PageLocation(");
    boolean first = true;

    sb.append("pageId:");
    sb.append(this.pageId);
    first = false;
    if (!first) sb.append(", ");
    sb.append("storageId:");
    sb.append(this.storageId);
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    // check for sub-struct validity
  }

  private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
    try {
      write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, ClassNotFoundException {
    try {
      // it doesn't seem like you should have to do this, but java serialization is wacky, and doesn't call the default constructor.
      __isset_bitfield = 0;
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class PageLocationStandardSchemeFactory implements SchemeFactory {
    public PageLocationStandardScheme getScheme() {
      return new PageLocationStandardScheme();
    }
  }

  private static class PageLocationStandardScheme extends StandardScheme<PageLocation> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, PageLocation struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // PAGE_ID
            if (schemeField.type == org.apache.thrift.protocol.TType.I64) {
              struct.pageId = iprot.readI64();
              struct.setPageIdIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // STORAGE_ID
            if (schemeField.type == org.apache.thrift.protocol.TType.I64) {
              struct.storageId = iprot.readI64();
              struct.setStorageIdIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          default:
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();

      // check for required fields of primitive type, which can't be checked in the validate method
      struct.validate();
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot, PageLocation struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      oprot.writeFieldBegin(PAGE_ID_FIELD_DESC);
      oprot.writeI64(struct.pageId);
      oprot.writeFieldEnd();
      oprot.writeFieldBegin(STORAGE_ID_FIELD_DESC);
      oprot.writeI64(struct.storageId);
      oprot.writeFieldEnd();
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class PageLocationTupleSchemeFactory implements SchemeFactory {
    public PageLocationTupleScheme getScheme() {
      return new PageLocationTupleScheme();
    }
  }

  private static class PageLocationTupleScheme extends TupleScheme<PageLocation> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, PageLocation struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      BitSet optionals = new BitSet();
      if (struct.isSetPageId()) {
        optionals.set(0);
      }
      if (struct.isSetStorageId()) {
        optionals.set(1);
      }
      oprot.writeBitSet(optionals, 2);
      if (struct.isSetPageId()) {
        oprot.writeI64(struct.pageId);
      }
      if (struct.isSetStorageId()) {
        oprot.writeI64(struct.storageId);
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, PageLocation struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      BitSet incoming = iprot.readBitSet(2);
      if (incoming.get(0)) {
        struct.pageId = iprot.readI64();
        struct.setPageIdIsSet(true);
      }
      if (incoming.get(1)) {
        struct.storageId = iprot.readI64();
        struct.setStorageIdIsSet(true);
      }
    }
  }

}
