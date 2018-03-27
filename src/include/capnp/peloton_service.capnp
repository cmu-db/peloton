@0xf3d342883f3f0344;

struct CreateIndexRequest {
  databaseName @0 :Text;
  tableName @1 :Text;

  keyAttrs @2 :List(Int32);
  indexName @3 :Text;
  uniqueKeys @4 :Bool;

  indexKeys @5 :Int32;
}

struct CreateIndexResponse {
  message @0 :Text;
}

interface PelotonService {
  createIndex @0 (request :CreateIndexRequest) -> (response :CreateIndexResponse);
}
