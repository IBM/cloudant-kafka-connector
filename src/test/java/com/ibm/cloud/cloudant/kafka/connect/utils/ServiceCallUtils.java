package com.ibm.cloud.cloudant.kafka.connect.utils;

import com.ibm.cloud.sdk.core.http.Response;
import com.ibm.cloud.sdk.core.http.ServiceCall;
import com.ibm.cloud.sdk.core.http.ServiceCallback;
import io.reactivex.Single;
import org.powermock.api.easymock.PowerMock;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;

public class ServiceCallUtils {

    public static <T> ServiceCall<T> makeServiceCallWithResult(T result) {
        Response<T> mockResponse = PowerMock.createMock(Response.class);
        expect(mockResponse.getResult()).andReturn(result).anyTimes();
        replay(mockResponse);
        return new ServiceCall<T>() {
            @Override
            public ServiceCall<T> addHeader(String s, String s1) {
                return null;
            }

            @Override
            public Response<T> execute() throws RuntimeException {
                return mockResponse;
            }

            @Override
            public void enqueue(ServiceCallback<T> serviceCallback) {

            }

            @Override
            public Single<Response<T>> reactiveRequest() {
                return null;
            }

            @Override
            public void cancel() {

            }
        };
    }

}
