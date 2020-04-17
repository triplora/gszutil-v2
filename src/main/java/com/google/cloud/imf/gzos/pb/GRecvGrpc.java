package com.google.cloud.imf.gzos.pb;

import static io.grpc.MethodDescriptor.generateFullMethodName;
import static io.grpc.stub.ClientCalls.asyncBidiStreamingCall;
import static io.grpc.stub.ClientCalls.asyncClientStreamingCall;
import static io.grpc.stub.ClientCalls.asyncServerStreamingCall;
import static io.grpc.stub.ClientCalls.asyncUnaryCall;
import static io.grpc.stub.ClientCalls.blockingServerStreamingCall;
import static io.grpc.stub.ClientCalls.blockingUnaryCall;
import static io.grpc.stub.ClientCalls.futureUnaryCall;
import static io.grpc.stub.ServerCalls.asyncBidiStreamingCall;
import static io.grpc.stub.ServerCalls.asyncClientStreamingCall;
import static io.grpc.stub.ServerCalls.asyncServerStreamingCall;
import static io.grpc.stub.ServerCalls.asyncUnaryCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.23.0)",
    comments = "Source: grecv.proto")
public final class GRecvGrpc {

  private GRecvGrpc() {}

  public static final String SERVICE_NAME = "com.google.cloud.imf.gzos.GRecv";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<com.google.cloud.imf.gzos.pb.GRecvProto.WriteRequest,
      com.google.cloud.imf.gzos.pb.GRecvProto.GRecvResponse> getWriteMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Write",
      requestType = com.google.cloud.imf.gzos.pb.GRecvProto.WriteRequest.class,
      responseType = com.google.cloud.imf.gzos.pb.GRecvProto.GRecvResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.CLIENT_STREAMING)
  public static io.grpc.MethodDescriptor<com.google.cloud.imf.gzos.pb.GRecvProto.WriteRequest,
      com.google.cloud.imf.gzos.pb.GRecvProto.GRecvResponse> getWriteMethod() {
    io.grpc.MethodDescriptor<com.google.cloud.imf.gzos.pb.GRecvProto.WriteRequest, com.google.cloud.imf.gzos.pb.GRecvProto.GRecvResponse> getWriteMethod;
    if ((getWriteMethod = GRecvGrpc.getWriteMethod) == null) {
      synchronized (GRecvGrpc.class) {
        if ((getWriteMethod = GRecvGrpc.getWriteMethod) == null) {
          GRecvGrpc.getWriteMethod = getWriteMethod =
              io.grpc.MethodDescriptor.<com.google.cloud.imf.gzos.pb.GRecvProto.WriteRequest, com.google.cloud.imf.gzos.pb.GRecvProto.GRecvResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.CLIENT_STREAMING)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Write"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.google.cloud.imf.gzos.pb.GRecvProto.WriteRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.google.cloud.imf.gzos.pb.GRecvProto.GRecvResponse.getDefaultInstance()))
              .setSchemaDescriptor(new GRecvMethodDescriptorSupplier("Write"))
              .build();
        }
      }
    }
    return getWriteMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static GRecvStub newStub(io.grpc.Channel channel) {
    return new GRecvStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static GRecvBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new GRecvBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static GRecvFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new GRecvFutureStub(channel);
  }

  /**
   */
  public static abstract class GRecvImplBase implements io.grpc.BindableService {

    /**
     */
    public io.grpc.stub.StreamObserver<com.google.cloud.imf.gzos.pb.GRecvProto.WriteRequest> write(
        io.grpc.stub.StreamObserver<com.google.cloud.imf.gzos.pb.GRecvProto.GRecvResponse> responseObserver) {
      return asyncUnimplementedStreamingCall(getWriteMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getWriteMethod(),
            asyncClientStreamingCall(
              new MethodHandlers<
                com.google.cloud.imf.gzos.pb.GRecvProto.WriteRequest,
                com.google.cloud.imf.gzos.pb.GRecvProto.GRecvResponse>(
                  this, METHODID_WRITE)))
          .build();
    }
  }

  /**
   */
  public static final class GRecvStub extends io.grpc.stub.AbstractStub<GRecvStub> {
    private GRecvStub(io.grpc.Channel channel) {
      super(channel);
    }

    private GRecvStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected GRecvStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new GRecvStub(channel, callOptions);
    }

    /**
     */
    public io.grpc.stub.StreamObserver<com.google.cloud.imf.gzos.pb.GRecvProto.WriteRequest> write(
        io.grpc.stub.StreamObserver<com.google.cloud.imf.gzos.pb.GRecvProto.GRecvResponse> responseObserver) {
      return asyncClientStreamingCall(
          getChannel().newCall(getWriteMethod(), getCallOptions()), responseObserver);
    }
  }

  /**
   */
  public static final class GRecvBlockingStub extends io.grpc.stub.AbstractStub<GRecvBlockingStub> {
    private GRecvBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private GRecvBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected GRecvBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new GRecvBlockingStub(channel, callOptions);
    }
  }

  /**
   */
  public static final class GRecvFutureStub extends io.grpc.stub.AbstractStub<GRecvFutureStub> {
    private GRecvFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private GRecvFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected GRecvFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new GRecvFutureStub(channel, callOptions);
    }
  }

  private static final int METHODID_WRITE = 0;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final GRecvImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(GRecvImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        default:
          throw new AssertionError();
      }
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_WRITE:
          return (io.grpc.stub.StreamObserver<Req>) serviceImpl.write(
              (io.grpc.stub.StreamObserver<com.google.cloud.imf.gzos.pb.GRecvProto.GRecvResponse>) responseObserver);
        default:
          throw new AssertionError();
      }
    }
  }

  private static abstract class GRecvBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    GRecvBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return com.google.cloud.imf.gzos.pb.GRecvProto.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("GRecv");
    }
  }

  private static final class GRecvFileDescriptorSupplier
      extends GRecvBaseDescriptorSupplier {
    GRecvFileDescriptorSupplier() {}
  }

  private static final class GRecvMethodDescriptorSupplier
      extends GRecvBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    GRecvMethodDescriptorSupplier(String methodName) {
      this.methodName = methodName;
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.MethodDescriptor getMethodDescriptor() {
      return getServiceDescriptor().findMethodByName(methodName);
    }
  }

  private static volatile io.grpc.ServiceDescriptor serviceDescriptor;

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    io.grpc.ServiceDescriptor result = serviceDescriptor;
    if (result == null) {
      synchronized (GRecvGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new GRecvFileDescriptorSupplier())
              .addMethod(getWriteMethod())
              .build();
        }
      }
    }
    return result;
  }
}