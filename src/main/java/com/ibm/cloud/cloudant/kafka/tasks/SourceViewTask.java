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

package com.ibm.cloud.cloudant.kafka.tasks;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Stream;
import org.apache.kafka.connect.source.SourceRecord;
import com.ibm.cloud.cloudant.kafka.mappers.ViewRowToSourceRecord;
import com.ibm.cloud.cloudant.kafka.utils.InterfaceConst;
import com.ibm.cloud.cloudant.v1.model.PostViewOptions;
import com.ibm.cloud.cloudant.v1.model.ViewResult;
import com.ibm.cloud.cloudant.v1.model.ViewResultRow;
import com.ibm.cloud.sdk.core.http.ServiceCall;

public class SourceViewTask extends AbstractSourceTask<ViewResult, ViewResultRow> {

    public static final String START_KEY = "cloudant.view.startKey";

    private String ddoc;
    private String view;

    SourceViewTask() {
        super(Collections.singletonList(START_KEY));
    }

    @Override
    public void start(Map<String, String> props) {
        super.start(props);
        this.ddoc = config.getString(InterfaceConst.DESIGN_DOC);
        this.view = config.getString(InterfaceConst.VIEW_NAME);

        // Add ddoc and view to source partition information
        this.sourcePartition.put(InterfaceConst.DESIGN_DOC, this.ddoc);
        this.sourcePartition.put(InterfaceConst.VIEW_NAME, this.view);
    }

    @Override
    protected BiFunction<String, ViewResultRow, Stream<SourceRecord>> getItemToRecordFunction() {
        final ViewRowToSourceRecord rowToRecord = new ViewRowToSourceRecord(this.sourcePartition);
        return (topic, row) -> {
            return Stream.of(rowToRecord.apply(topic, row));
        };
    }

    @Override
    protected ServiceCall<ViewResult> nextServiceCall() {
        PostViewOptions opts = new PostViewOptions.Builder(this.db, this.ddoc, this.view)
            .startKey(this.offset.get(START_KEY))
            .build();
        return service.postView(opts);
    }

    @Override
    protected Stream<ViewResultRow> resultToStream(ViewResult result) {
        // Paginating, so don't return the last row as it will be our next first row
        return result.getRows().stream().limit(result.getRows().size() - 1);
    }

    @Override
    protected void updateOffset(ViewResult result, List<SourceRecord> records) {
        ViewResultRow lastRow = result.getRows().get(result.getRows().size()-1);
        this.offset.put(START_KEY, lastRow.getKey());
    }
    
}
