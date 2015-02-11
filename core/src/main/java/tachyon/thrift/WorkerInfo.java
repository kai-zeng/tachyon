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

public class WorkerInfo implements org.apache.thrift.TBase<WorkerInfo, WorkerInfo._Fields>, java.io.Serializable, Cloneable, Comparable<WorkerInfo> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("WorkerInfo");

  private static final org.apache.thrift.protocol.TField ADDRESS_FIELD_DESC = new org.apache.thrift.protocol.TField("address", org.apache.thrift.protocol.TType.STRUCT, (short)1);
  private static final org.apache.thrift.protocol.TField PAGES_FIELD_DESC = new org.apache.thrift.protocol.TField("pages", org.apache.thrift.protocol.TType.LIST, (short)2);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new WorkerInfoStandardSchemeFactory());
    schemes.put(TupleScheme.class, new WorkerInfoTupleSchemeFactory());
  }

  public NetAddress address; // required
  public List<PageLocation> pages; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    ADDRESS((short)1, "address"),
    PAGES((short)2, "pages");

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
        case 1: // ADDRESS
          return ADDRESS;
        case 2: // PAGES
          return PAGES;
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
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.ADDRESS, new org.apache.thrift.meta_data.FieldMetaData("address", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, NetAddress.class)));
    tmpMap.put(_Fields.PAGES, new org.apache.thrift.meta_data.FieldMetaData("pages", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST, 
            new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, PageLocation.class))));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(WorkerInfo.class, metaDataMap);
  }

  public WorkerInfo() {
  }

  public WorkerInfo(
    NetAddress address,
    List<PageLocation> pages)
  {
    this();
    this.address = address;
    this.pages = pages;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public WorkerInfo(WorkerInfo other) {
    if (other.isSetAddress()) {
      this.address = new NetAddress(other.address);
    }
    if (other.isSetPages()) {
      List<PageLocation> __this__pages = new ArrayList<PageLocation>(other.pages.size());
      for (PageLocation other_element : other.pages) {
        __this__pages.add(new PageLocation(other_element));
      }
      this.pages = __this__pages;
    }
  }

  public WorkerInfo deepCopy() {
    return new WorkerInfo(this);
  }

  @Override
  public void clear() {
    this.address = null;
    this.pages = null;
  }

  public NetAddress getAddress() {
    return this.address;
  }

  public WorkerInfo setAddress(NetAddress address) {
    this.address = address;
    return this;
  }

  public void unsetAddress() {
    this.address = null;
  }

  /** Returns true if field address is set (has been assigned a value) and false otherwise */
  public boolean isSetAddress() {
    return this.address != null;
  }

  public void setAddressIsSet(boolean value) {
    if (!value) {
      this.address = null;
    }
  }

  public int getPagesSize() {
    return (this.pages == null) ? 0 : this.pages.size();
  }

  public java.util.Iterator<PageLocation> getPagesIterator() {
    return (this.pages == null) ? null : this.pages.iterator();
  }

  public void addToPages(PageLocation elem) {
    if (this.pages == null) {
      this.pages = new ArrayList<PageLocation>();
    }
    this.pages.add(elem);
  }

  public List<PageLocation> getPages() {
    return this.pages;
  }

  public WorkerInfo setPages(List<PageLocation> pages) {
    this.pages = pages;
    return this;
  }

  public void unsetPages() {
    this.pages = null;
  }

  /** Returns true if field pages is set (has been assigned a value) and false otherwise */
  public boolean isSetPages() {
    return this.pages != null;
  }

  public void setPagesIsSet(boolean value) {
    if (!value) {
      this.pages = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case ADDRESS:
      if (value == null) {
        unsetAddress();
      } else {
        setAddress((NetAddress)value);
      }
      break;

    case PAGES:
      if (value == null) {
        unsetPages();
      } else {
        setPages((List<PageLocation>)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case ADDRESS:
      return getAddress();

    case PAGES:
      return getPages();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case ADDRESS:
      return isSetAddress();
    case PAGES:
      return isSetPages();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof WorkerInfo)
      return this.equals((WorkerInfo)that);
    return false;
  }

  public boolean equals(WorkerInfo that) {
    if (that == null)
      return false;

    boolean this_present_address = true && this.isSetAddress();
    boolean that_present_address = true && that.isSetAddress();
    if (this_present_address || that_present_address) {
      if (!(this_present_address && that_present_address))
        return false;
      if (!this.address.equals(that.address))
        return false;
    }

    boolean this_present_pages = true && this.isSetPages();
    boolean that_present_pages = true && that.isSetPages();
    if (this_present_pages || that_present_pages) {
      if (!(this_present_pages && that_present_pages))
        return false;
      if (!this.pages.equals(that.pages))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  @Override
  public int compareTo(WorkerInfo other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(isSetAddress()).compareTo(other.isSetAddress());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetAddress()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.address, other.address);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetPages()).compareTo(other.isSetPages());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetPages()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.pages, other.pages);
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
    StringBuilder sb = new StringBuilder("WorkerInfo(");
    boolean first = true;

    sb.append("address:");
    if (this.address == null) {
      sb.append("null");
    } else {
      sb.append(this.address);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("pages:");
    if (this.pages == null) {
      sb.append("null");
    } else {
      sb.append(this.pages);
    }
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    // check for sub-struct validity
    if (address != null) {
      address.validate();
    }
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
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class WorkerInfoStandardSchemeFactory implements SchemeFactory {
    public WorkerInfoStandardScheme getScheme() {
      return new WorkerInfoStandardScheme();
    }
  }

  private static class WorkerInfoStandardScheme extends StandardScheme<WorkerInfo> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, WorkerInfo struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // ADDRESS
            if (schemeField.type == org.apache.thrift.protocol.TType.STRUCT) {
              struct.address = new NetAddress();
              struct.address.read(iprot);
              struct.setAddressIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // PAGES
            if (schemeField.type == org.apache.thrift.protocol.TType.LIST) {
              {
                org.apache.thrift.protocol.TList _list0 = iprot.readListBegin();
                struct.pages = new ArrayList<PageLocation>(_list0.size);
                for (int _i1 = 0; _i1 < _list0.size; ++_i1)
                {
                  PageLocation _elem2;
                  _elem2 = new PageLocation();
                  _elem2.read(iprot);
                  struct.pages.add(_elem2);
                }
                iprot.readListEnd();
              }
              struct.setPagesIsSet(true);
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

    public void write(org.apache.thrift.protocol.TProtocol oprot, WorkerInfo struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.address != null) {
        oprot.writeFieldBegin(ADDRESS_FIELD_DESC);
        struct.address.write(oprot);
        oprot.writeFieldEnd();
      }
      if (struct.pages != null) {
        oprot.writeFieldBegin(PAGES_FIELD_DESC);
        {
          oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, struct.pages.size()));
          for (PageLocation _iter3 : struct.pages)
          {
            _iter3.write(oprot);
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class WorkerInfoTupleSchemeFactory implements SchemeFactory {
    public WorkerInfoTupleScheme getScheme() {
      return new WorkerInfoTupleScheme();
    }
  }

  private static class WorkerInfoTupleScheme extends TupleScheme<WorkerInfo> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, WorkerInfo struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      BitSet optionals = new BitSet();
      if (struct.isSetAddress()) {
        optionals.set(0);
      }
      if (struct.isSetPages()) {
        optionals.set(1);
      }
      oprot.writeBitSet(optionals, 2);
      if (struct.isSetAddress()) {
        struct.address.write(oprot);
      }
      if (struct.isSetPages()) {
        {
          oprot.writeI32(struct.pages.size());
          for (PageLocation _iter4 : struct.pages)
          {
            _iter4.write(oprot);
          }
        }
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, WorkerInfo struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      BitSet incoming = iprot.readBitSet(2);
      if (incoming.get(0)) {
        struct.address = new NetAddress();
        struct.address.read(iprot);
        struct.setAddressIsSet(true);
      }
      if (incoming.get(1)) {
        {
          org.apache.thrift.protocol.TList _list5 = new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, iprot.readI32());
          struct.pages = new ArrayList<PageLocation>(_list5.size);
          for (int _i6 = 0; _i6 < _list5.size; ++_i6)
          {
            PageLocation _elem7;
            _elem7 = new PageLocation();
            _elem7.read(iprot);
            struct.pages.add(_elem7);
          }
        }
        struct.setPagesIsSet(true);
      }
    }
  }

}

