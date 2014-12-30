// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: test.proto

package test;

public final class TestSchema {
  private TestSchema() {}
  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistry registry) {
  }
  public interface TestOrBuilder extends
      // @@protoc_insertion_point(interface_extends:test.Test)
      com.google.protobuf.MessageOrBuilder {

    /**
     * <code>optional int32 f1 = 1;</code>
     */
    boolean hasF1();
    /**
     * <code>optional int32 f1 = 1;</code>
     */
    int getF1();

    /**
     * <code>optional float f2 = 2;</code>
     */
    boolean hasF2();
    /**
     * <code>optional float f2 = 2;</code>
     */
    float getF2();

    /**
     * <code>optional string f3 = 3;</code>
     */
    boolean hasF3();
    /**
     * <code>optional string f3 = 3;</code>
     */
    java.lang.String getF3();
    /**
     * <code>optional string f3 = 3;</code>
     */
    com.google.protobuf.ByteString
        getF3Bytes();
  }
  /**
   * Protobuf type {@code test.Test}
   */
  public static final class Test extends
      com.google.protobuf.GeneratedMessage implements
      // @@protoc_insertion_point(message_implements:test.Test)
      TestOrBuilder {
    // Use Test.newBuilder() to construct.
    private Test(com.google.protobuf.GeneratedMessage.Builder<?> builder) {
      super(builder);
      this.unknownFields = builder.getUnknownFields();
    }
    private Test(boolean noInit) { this.unknownFields = com.google.protobuf.UnknownFieldSet.getDefaultInstance(); }

    private static final Test defaultInstance;
    public static Test getDefaultInstance() {
      return defaultInstance;
    }

    public Test getDefaultInstanceForType() {
      return defaultInstance;
    }

    private final com.google.protobuf.UnknownFieldSet unknownFields;
    @java.lang.Override
    public final com.google.protobuf.UnknownFieldSet
        getUnknownFields() {
      return this.unknownFields;
    }
    private Test(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      initFields();
      int mutable_bitField0_ = 0;
      com.google.protobuf.UnknownFieldSet.Builder unknownFields =
          com.google.protobuf.UnknownFieldSet.newBuilder();
      try {
        boolean done = false;
        while (!done) {
          int tag = input.readTag();
          switch (tag) {
            case 0:
              done = true;
              break;
            default: {
              if (!parseUnknownField(input, unknownFields,
                                     extensionRegistry, tag)) {
                done = true;
              }
              break;
            }
            case 8: {
              bitField0_ |= 0x00000001;
              f1_ = input.readInt32();
              break;
            }
            case 21: {
              bitField0_ |= 0x00000002;
              f2_ = input.readFloat();
              break;
            }
            case 26: {
              com.google.protobuf.ByteString bs = input.readBytes();
              bitField0_ |= 0x00000004;
              f3_ = bs;
              break;
            }
          }
        }
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        throw e.setUnfinishedMessage(this);
      } catch (java.io.IOException e) {
        throw new com.google.protobuf.InvalidProtocolBufferException(
            e.getMessage()).setUnfinishedMessage(this);
      } finally {
        this.unknownFields = unknownFields.build();
        makeExtensionsImmutable();
      }
    }
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return test.TestSchema.internal_static_test_Test_descriptor;
    }

    protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return test.TestSchema.internal_static_test_Test_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              test.TestSchema.Test.class, test.TestSchema.Test.Builder.class);
    }

    public static com.google.protobuf.Parser<Test> PARSER =
        new com.google.protobuf.AbstractParser<Test>() {
      public Test parsePartialFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws com.google.protobuf.InvalidProtocolBufferException {
        return new Test(input, extensionRegistry);
      }
    };

    @java.lang.Override
    public com.google.protobuf.Parser<Test> getParserForType() {
      return PARSER;
    }

    private int bitField0_;
    public static final int F1_FIELD_NUMBER = 1;
    private int f1_;
    /**
     * <code>optional int32 f1 = 1;</code>
     */
    public boolean hasF1() {
      return ((bitField0_ & 0x00000001) == 0x00000001);
    }
    /**
     * <code>optional int32 f1 = 1;</code>
     */
    public int getF1() {
      return f1_;
    }

    public static final int F2_FIELD_NUMBER = 2;
    private float f2_;
    /**
     * <code>optional float f2 = 2;</code>
     */
    public boolean hasF2() {
      return ((bitField0_ & 0x00000002) == 0x00000002);
    }
    /**
     * <code>optional float f2 = 2;</code>
     */
    public float getF2() {
      return f2_;
    }

    public static final int F3_FIELD_NUMBER = 3;
    private java.lang.Object f3_;
    /**
     * <code>optional string f3 = 3;</code>
     */
    public boolean hasF3() {
      return ((bitField0_ & 0x00000004) == 0x00000004);
    }
    /**
     * <code>optional string f3 = 3;</code>
     */
    public java.lang.String getF3() {
      java.lang.Object ref = f3_;
      if (ref instanceof java.lang.String) {
        return (java.lang.String) ref;
      } else {
        com.google.protobuf.ByteString bs = 
            (com.google.protobuf.ByteString) ref;
        java.lang.String s = bs.toStringUtf8();
        if (bs.isValidUtf8()) {
          f3_ = s;
        }
        return s;
      }
    }
    /**
     * <code>optional string f3 = 3;</code>
     */
    public com.google.protobuf.ByteString
        getF3Bytes() {
      java.lang.Object ref = f3_;
      if (ref instanceof java.lang.String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (java.lang.String) ref);
        f3_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }

    private void initFields() {
      f1_ = 0;
      f2_ = 0F;
      f3_ = "";
    }
    private byte memoizedIsInitialized = -1;
    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized == 1) return true;
      if (isInitialized == 0) return false;

      memoizedIsInitialized = 1;
      return true;
    }

    public void writeTo(com.google.protobuf.CodedOutputStream output)
                        throws java.io.IOException {
      getSerializedSize();
      if (((bitField0_ & 0x00000001) == 0x00000001)) {
        output.writeInt32(1, f1_);
      }
      if (((bitField0_ & 0x00000002) == 0x00000002)) {
        output.writeFloat(2, f2_);
      }
      if (((bitField0_ & 0x00000004) == 0x00000004)) {
        output.writeBytes(3, getF3Bytes());
      }
      getUnknownFields().writeTo(output);
    }

    private int memoizedSerializedSize = -1;
    public int getSerializedSize() {
      int size = memoizedSerializedSize;
      if (size != -1) return size;

      size = 0;
      if (((bitField0_ & 0x00000001) == 0x00000001)) {
        size += com.google.protobuf.CodedOutputStream
          .computeInt32Size(1, f1_);
      }
      if (((bitField0_ & 0x00000002) == 0x00000002)) {
        size += com.google.protobuf.CodedOutputStream
          .computeFloatSize(2, f2_);
      }
      if (((bitField0_ & 0x00000004) == 0x00000004)) {
        size += com.google.protobuf.CodedOutputStream
          .computeBytesSize(3, getF3Bytes());
      }
      size += getUnknownFields().getSerializedSize();
      memoizedSerializedSize = size;
      return size;
    }

    private static final long serialVersionUID = 0L;
    @java.lang.Override
    protected java.lang.Object writeReplace()
        throws java.io.ObjectStreamException {
      return super.writeReplace();
    }

    public static test.TestSchema.Test parseFrom(
        com.google.protobuf.ByteString data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static test.TestSchema.Test parseFrom(
        com.google.protobuf.ByteString data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static test.TestSchema.Test parseFrom(byte[] data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static test.TestSchema.Test parseFrom(
        byte[] data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static test.TestSchema.Test parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return PARSER.parseFrom(input);
    }
    public static test.TestSchema.Test parseFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseFrom(input, extensionRegistry);
    }
    public static test.TestSchema.Test parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      return PARSER.parseDelimitedFrom(input);
    }
    public static test.TestSchema.Test parseDelimitedFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseDelimitedFrom(input, extensionRegistry);
    }
    public static test.TestSchema.Test parseFrom(
        com.google.protobuf.CodedInputStream input)
        throws java.io.IOException {
      return PARSER.parseFrom(input);
    }
    public static test.TestSchema.Test parseFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseFrom(input, extensionRegistry);
    }

    public static Builder newBuilder() { return Builder.create(); }
    public Builder newBuilderForType() { return newBuilder(); }
    public static Builder newBuilder(test.TestSchema.Test prototype) {
      return newBuilder().mergeFrom(prototype);
    }
    public Builder toBuilder() { return newBuilder(this); }

    @java.lang.Override
    protected Builder newBuilderForType(
        com.google.protobuf.GeneratedMessage.BuilderParent parent) {
      Builder builder = new Builder(parent);
      return builder;
    }
    /**
     * Protobuf type {@code test.Test}
     */
    public static final class Builder extends
        com.google.protobuf.GeneratedMessage.Builder<Builder> implements
        // @@protoc_insertion_point(builder_implements:test.Test)
        test.TestSchema.TestOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor
          getDescriptor() {
        return test.TestSchema.internal_static_test_Test_descriptor;
      }

      protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
          internalGetFieldAccessorTable() {
        return test.TestSchema.internal_static_test_Test_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                test.TestSchema.Test.class, test.TestSchema.Test.Builder.class);
      }

      // Construct using test.TestSchema.Test.newBuilder()
      private Builder() {
        maybeForceBuilderInitialization();
      }

      private Builder(
          com.google.protobuf.GeneratedMessage.BuilderParent parent) {
        super(parent);
        maybeForceBuilderInitialization();
      }
      private void maybeForceBuilderInitialization() {
        if (com.google.protobuf.GeneratedMessage.alwaysUseFieldBuilders) {
        }
      }
      private static Builder create() {
        return new Builder();
      }

      public Builder clear() {
        super.clear();
        f1_ = 0;
        bitField0_ = (bitField0_ & ~0x00000001);
        f2_ = 0F;
        bitField0_ = (bitField0_ & ~0x00000002);
        f3_ = "";
        bitField0_ = (bitField0_ & ~0x00000004);
        return this;
      }

      public Builder clone() {
        return create().mergeFrom(buildPartial());
      }

      public com.google.protobuf.Descriptors.Descriptor
          getDescriptorForType() {
        return test.TestSchema.internal_static_test_Test_descriptor;
      }

      public test.TestSchema.Test getDefaultInstanceForType() {
        return test.TestSchema.Test.getDefaultInstance();
      }

      public test.TestSchema.Test build() {
        test.TestSchema.Test result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      public test.TestSchema.Test buildPartial() {
        test.TestSchema.Test result = new test.TestSchema.Test(this);
        int from_bitField0_ = bitField0_;
        int to_bitField0_ = 0;
        if (((from_bitField0_ & 0x00000001) == 0x00000001)) {
          to_bitField0_ |= 0x00000001;
        }
        result.f1_ = f1_;
        if (((from_bitField0_ & 0x00000002) == 0x00000002)) {
          to_bitField0_ |= 0x00000002;
        }
        result.f2_ = f2_;
        if (((from_bitField0_ & 0x00000004) == 0x00000004)) {
          to_bitField0_ |= 0x00000004;
        }
        result.f3_ = f3_;
        result.bitField0_ = to_bitField0_;
        onBuilt();
        return result;
      }

      public Builder mergeFrom(com.google.protobuf.Message other) {
        if (other instanceof test.TestSchema.Test) {
          return mergeFrom((test.TestSchema.Test)other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(test.TestSchema.Test other) {
        if (other == test.TestSchema.Test.getDefaultInstance()) return this;
        if (other.hasF1()) {
          setF1(other.getF1());
        }
        if (other.hasF2()) {
          setF2(other.getF2());
        }
        if (other.hasF3()) {
          bitField0_ |= 0x00000004;
          f3_ = other.f3_;
          onChanged();
        }
        this.mergeUnknownFields(other.getUnknownFields());
        return this;
      }

      public final boolean isInitialized() {
        return true;
      }

      public Builder mergeFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws java.io.IOException {
        test.TestSchema.Test parsedMessage = null;
        try {
          parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
          parsedMessage = (test.TestSchema.Test) e.getUnfinishedMessage();
          throw e;
        } finally {
          if (parsedMessage != null) {
            mergeFrom(parsedMessage);
          }
        }
        return this;
      }
      private int bitField0_;

      private int f1_ ;
      /**
       * <code>optional int32 f1 = 1;</code>
       */
      public boolean hasF1() {
        return ((bitField0_ & 0x00000001) == 0x00000001);
      }
      /**
       * <code>optional int32 f1 = 1;</code>
       */
      public int getF1() {
        return f1_;
      }
      /**
       * <code>optional int32 f1 = 1;</code>
       */
      public Builder setF1(int value) {
        bitField0_ |= 0x00000001;
        f1_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional int32 f1 = 1;</code>
       */
      public Builder clearF1() {
        bitField0_ = (bitField0_ & ~0x00000001);
        f1_ = 0;
        onChanged();
        return this;
      }

      private float f2_ ;
      /**
       * <code>optional float f2 = 2;</code>
       */
      public boolean hasF2() {
        return ((bitField0_ & 0x00000002) == 0x00000002);
      }
      /**
       * <code>optional float f2 = 2;</code>
       */
      public float getF2() {
        return f2_;
      }
      /**
       * <code>optional float f2 = 2;</code>
       */
      public Builder setF2(float value) {
        bitField0_ |= 0x00000002;
        f2_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional float f2 = 2;</code>
       */
      public Builder clearF2() {
        bitField0_ = (bitField0_ & ~0x00000002);
        f2_ = 0F;
        onChanged();
        return this;
      }

      private java.lang.Object f3_ = "";
      /**
       * <code>optional string f3 = 3;</code>
       */
      public boolean hasF3() {
        return ((bitField0_ & 0x00000004) == 0x00000004);
      }
      /**
       * <code>optional string f3 = 3;</code>
       */
      public java.lang.String getF3() {
        java.lang.Object ref = f3_;
        if (!(ref instanceof java.lang.String)) {
          com.google.protobuf.ByteString bs =
              (com.google.protobuf.ByteString) ref;
          java.lang.String s = bs.toStringUtf8();
          if (bs.isValidUtf8()) {
            f3_ = s;
          }
          return s;
        } else {
          return (java.lang.String) ref;
        }
      }
      /**
       * <code>optional string f3 = 3;</code>
       */
      public com.google.protobuf.ByteString
          getF3Bytes() {
        java.lang.Object ref = f3_;
        if (ref instanceof String) {
          com.google.protobuf.ByteString b = 
              com.google.protobuf.ByteString.copyFromUtf8(
                  (java.lang.String) ref);
          f3_ = b;
          return b;
        } else {
          return (com.google.protobuf.ByteString) ref;
        }
      }
      /**
       * <code>optional string f3 = 3;</code>
       */
      public Builder setF3(
          java.lang.String value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000004;
        f3_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional string f3 = 3;</code>
       */
      public Builder clearF3() {
        bitField0_ = (bitField0_ & ~0x00000004);
        f3_ = getDefaultInstance().getF3();
        onChanged();
        return this;
      }
      /**
       * <code>optional string f3 = 3;</code>
       */
      public Builder setF3Bytes(
          com.google.protobuf.ByteString value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000004;
        f3_ = value;
        onChanged();
        return this;
      }

      // @@protoc_insertion_point(builder_scope:test.Test)
    }

    static {
      defaultInstance = new Test(true);
      defaultInstance.initFields();
    }

    // @@protoc_insertion_point(class_scope:test.Test)
  }

  private static final com.google.protobuf.Descriptors.Descriptor
    internal_static_test_Test_descriptor;
  private static
    com.google.protobuf.GeneratedMessage.FieldAccessorTable
      internal_static_test_Test_fieldAccessorTable;

  public static com.google.protobuf.Descriptors.FileDescriptor
      getDescriptor() {
    return descriptor;
  }
  private static com.google.protobuf.Descriptors.FileDescriptor
      descriptor;
  static {
    java.lang.String[] descriptorData = {
      "\n\ntest.proto\022\004test\"*\n\004Test\022\n\n\002f1\030\001 \001(\005\022\n" +
      "\n\002f2\030\002 \001(\002\022\n\n\002f3\030\003 \001(\tB\022\n\004testB\nTestSche" +
      "ma"
    };
    com.google.protobuf.Descriptors.FileDescriptor.InternalDescriptorAssigner assigner =
        new com.google.protobuf.Descriptors.FileDescriptor.    InternalDescriptorAssigner() {
          public com.google.protobuf.ExtensionRegistry assignDescriptors(
              com.google.protobuf.Descriptors.FileDescriptor root) {
            descriptor = root;
            return null;
          }
        };
    com.google.protobuf.Descriptors.FileDescriptor
      .internalBuildGeneratedFileFrom(descriptorData,
        new com.google.protobuf.Descriptors.FileDescriptor[] {
        }, assigner);
    internal_static_test_Test_descriptor =
      getDescriptor().getMessageTypes().get(0);
    internal_static_test_Test_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessage.FieldAccessorTable(
        internal_static_test_Test_descriptor,
        new java.lang.String[] { "F1", "F2", "F3", });
  }

  // @@protoc_insertion_point(outer_class_scope)
}
