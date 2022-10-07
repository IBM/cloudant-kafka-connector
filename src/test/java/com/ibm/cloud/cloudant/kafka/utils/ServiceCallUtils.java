/*
 * Copyright © 2022 IBM Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */
package com.ibm.cloud.cloudant.kafka.utils;

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
                throw new UnsupportedOperationException("Unsupported in mock");
            }

            @Override
            public Response<T> execute() throws RuntimeException {
                return mockResponse;
            }

            @Override
            public void enqueue(ServiceCallback<T> serviceCallback) {
                throw new UnsupportedOperationException("Unsupported in mock");
            }

            @Override
            public Single<Response<T>> reactiveRequest() {
                throw new UnsupportedOperationException("Unsupported in mock");
            }

            @Override
            public void cancel() {
                throw new UnsupportedOperationException("Unsupported in mock");
            }
        };
    }

}
