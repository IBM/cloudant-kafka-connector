# Single Message Transforms

Single Message Transforms, or SMTs, can be used to customize fields or values of events during data flow.

## Sink
The examples below demonstrate modifying fields for events flowing from the Kafka topic to a Cloudant database using the sink connector.

1. If the event value contains an existing field, not called `_id`, that is suitable to use as the Cloudant document ID, then you can use the `RenameField` transform.
    ```
    transforms=RenameField
    transforms.RenameField.type=org.apache.kafka.connect.transforms.ReplaceField$Value 
    transforms.RenameField.renames=name:_id
    ```
1. If you have `_id` fields and would prefer to have Cloudant generate a UUID for the document ID, use the `ReplaceField` transform to exclude the existing `_id` field:
    ```
    transforms=ReplaceField
    transforms.ReplaceField.type=org.apache.kafka.connect.transforms.ReplaceField$Value 
    transforms.ReplaceField.exclude=_id
    ```
1. If you have events where there is no value ([_tombstone_ events](https://kafka.apache.org/documentation.html#compaction)), you may wish to filter these out.
    - In the Cloudant sink connector, these may be undesirable as they will generate an empty document.
    - In the Cloudant source connector, tombstone events are generated for deleted documents (in addition to the deleted document itself).
    - In either case, you can use the `RecordIsTombstone` predicate with a filter to remove these tombstone events as shown in this example:

    ```
    transforms=dropNullEvents
    transforms.dropNullEvents.type=org.apache.kafka.connect.transforms.Filter
    transforms.dropNullEvents.predicate=isNullEvent

    predicates=isNullEvent
    predicates.isNullEvent.type=org.apache.kafka.connect.transforms.predicates.RecordIsTombstone
    ```

1. If you want to use the event key or another custom value as the document ID then use the `cloudant_doc_id` custom header.
   The value set in this custom header will be added to the `_id` field.  If the `_id` field already exists then it will be overwritten
   with the value in this header.
   You can use the `HeaderFrom` SMT to move or copy a key to the custom header. The example config below adds the transform to move
   the `docid` event key to the `cloudant_doc_id` custom header and sets the header converter to string:
   ```
   transforms=moveFieldsToHeaders
   transforms.moveFieldsToHeaders.type=org.apache.kafka.connect.transforms.HeaderFrom$Key
   transforms.moveFieldsToHeaders.fields=docid
   transforms.moveFieldsToHeaders.headers=cloudant_doc_id
   transforms.moveFieldsToHeaders.operation=move
   
   header.converter=org.apache.kafka.connect.storage.StringConverter
   ```

   **Note**: The `header.converter` is required to be set to `StringConverter` since the document ID field only supports strings.

1. If you have events where the `_id` field is absent or `null` then Cloudant will generate
   a document ID. If you don't want this to happen then set an `_id` (see earlier examples).
   If you need to filter out those documents or drop `_id` fields when the value is `null` then you'll need to create a custom SMT.

**Note**: For any of the SMTs above, if the field does not exist it will leave the event unmodified and continue processing the next event.

## Source
The examples below demonstrate modifying events produced by the Cloudant source connector.

1. Flatten maps in the JSON document using the Kafka built-in `org.apache.kafka.connect.transforms.ReplaceField$Value`
    ```
    transforms=FlattenMaps
    transforms.FlattenMaps.type=org.apache.kafka.connect.transforms.Flatten
    ```

1. Flatten arrays in the JSON document using `com.ibm.cloud.cloudant.kafka.transforms.ArrayFlatten`. Note that this transform
   is only suitable for use with Map event values and will filter events that do not conform. As such if used in conjunction with the
   `MapToStruct` transform, this `ArrayFlatten` operation must precede `MapToStruct` in the SMT pipeline.
   The `delimiter` configuration property may be used to customize the delimiter, which defaults to `.`.
    ```
    transforms=FlattenArrays
    transforms.FlattenArrays.type=com.ibm.cloud.cloudant.kafka.transforms.ArrayFlatten
    ```

1. Convert schemaless `java.util.Map` values to `org.apache.kafka.connect.data.Struct` with an inferred schema. This transform is designed
   to improve compatibility with other connectors and converters that requires a `Struct` type event. For complex schemas a schema registry
   should be used.
    ```
    transforms=MapToStruct
    transforms.MapToStruct.type=com.ibm.cloud.cloudant.kafka.transforms.MapToStruct
    ```

1. Omit design documents from the produced events by using the Kafka built-in `org.apache.kafka.connect.transforms.Filter`
   in conjunction with the predicate `com.ibm.cloud.cloudant.kafka.transforms.predicates.IsDesignDocument`. Note that this
   predicate relies on the key format of the Cloudant source connector events so must be applied before any other transformations that
   alter the key format.
    ```
    transforms=omitDesignDocs
    transforms.omitDesignDocs.type=org.apache.kafka.connect.transforms.Filter
    transforms.omitDesignDocs.predicate=isDesignDoc

    predicates=isDesignDoc
    predicates.isDesignDoc.type=com.ibm.cloud.cloudant.kafka.transforms.predicates.IsDesignDocument
    ```
