/*
 * Copyright © 2016, 2022 IBM Corp. All rights reserved.
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

import com.ibm.cloud.cloudant.kafka.caching.CachedClientManager;
import com.ibm.cloud.cloudant.v1.Cloudant;
import com.ibm.cloud.cloudant.v1.model.DatabaseInformation;
import com.ibm.cloud.cloudant.v1.model.DeleteDatabaseOptions;
import com.ibm.cloud.cloudant.v1.model.GetDatabaseInformationOptions;
import com.ibm.cloud.cloudant.v1.model.Ok;
import com.ibm.cloud.sdk.core.http.Response;
import org.junit.Assert;

import java.util.Map;

public class CloudantDbUtils {

    public static void dropDatabase(Map<String, String> props) {
        Cloudant service = CachedClientManager.getInstance(props);
        String dbName = props.get(InterfaceConst.DB);
        DeleteDatabaseOptions deleteDbOptions = new DeleteDatabaseOptions.Builder()
                .db(dbName)
                .build();
        Response<Ok> result = service.deleteDatabase(deleteDbOptions).execute();
        Assert.assertTrue(result.getResult().isOk());
    }

    public static DatabaseInformation getDbInfo(String db, Cloudant service) {
        GetDatabaseInformationOptions dbInfoOptions;
        dbInfoOptions = new GetDatabaseInformationOptions.Builder()
                .db(db)
                .build();
        return service.getDatabaseInformation(dbInfoOptions).execute().getResult();
    }
}
