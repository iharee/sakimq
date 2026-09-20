package com.arth.sakimq.broker.server;

import com.arth.sakimq.exception.DuplicateMessageException;
import com.arth.sakimq.exception.InvalidArgumentException;
import com.arth.sakimq.exception.QueueNotFoundException;
import com.arth.sakimq.exception.SakimqException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.ForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

/**
 * 把业务异常映射为语义化的 gRPC {@link Status}。
 * <p>未映射的 {@link RuntimeException} 默认会被 gRPC 转成 {@code UNKNOWN}，客户端无法区分
 * "队列不存在" / "重复消息" / "参数非法"，因此服务方法抛出的 {@code SakimqException} 在此统一转换。</p>
 * <p>注意：映射结果必须通过 {@link ServerCall#close} 下发，不能从 {@code onHalfClose} 抛出。
 * 任何逃逸出 listener 的异常都会被 grpc-core 的
 * {@code ServerImpl.JumpToApplicationThreadServerStreamListener.internalClose}
 * 统一转成 {@code UNKNOWN: Application error processing RPC}，客户端拿不到映射后的状态。</p>
 * <p>只覆盖 unary 调用（{@code onHalfClose}）；当前 MQService 全部为 unary。</p>
 */
public final class ExceptionMappingInterceptor implements ServerInterceptor {

    private static final Logger log = LoggerFactory.getLogger(ExceptionMappingInterceptor.class);

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
        ServerCall.Listener<ReqT> listener = next.startCall(call, headers);
        return new ForwardingServerCallListener.SimpleForwardingServerCallListener<>(listener) {
            @Override
            public void onHalfClose() {
                try {
                    super.onHalfClose();
                } catch (StatusRuntimeException e) {
                    close(call, e.getStatus(), e);
                } catch (RuntimeException e) {
                    close(call, toStatus(e), e);
                }
            }
        };
    }

    private static <ReqT, RespT> void close(ServerCall<ReqT, RespT> call, Status status, RuntimeException cause) {
        log.debug("RPC failed with {}: {}", status.getCode(), status.getDescription(), cause);
        try {
            call.close(status, new Metadata());
        } catch (IllegalStateException alreadyClosed) {
            // 业务侧已经结束调用（如先 onCompleted 再抛异常），状态已下发，无需处理
            log.warn("Call already closed, dropping mapped status {}", status.getCode());
        }
    }

    private static Status toStatus(RuntimeException e) {
        if (e instanceof QueueNotFoundException) {
            return Status.NOT_FOUND.withDescription(e.getMessage()).withCause(e);
        }
        if (e instanceof DuplicateMessageException) {
            return Status.ALREADY_EXISTS.withDescription(e.getMessage()).withCause(e);
        }
        if (e instanceof InvalidArgumentException) {
            return Status.INVALID_ARGUMENT.withDescription(e.getMessage()).withCause(e);
        }
        if (e instanceof SakimqException) {
            return Status.INTERNAL.withDescription(e.getMessage()).withCause(e);
        }
        return Status.UNKNOWN.withDescription(e.getMessage()).withCause(e);
    }
}
