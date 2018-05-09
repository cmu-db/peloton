@0xf3d342883f3f0344;

struct CreateIndexRequest {
  databaseOid @0 :Int32;
  tableOid @1 :Int32;

  keyAttrOids @2 :List(Int32);
  indexName @3 :Text;
  uniqueKeys @4 :Bool;
}

struct CreateIndexResponse {
  message @0 :Text;
}

struct DropIndexRequest {
  databaseOid @0 :Int32;
  indexOid @1 :Int32;
}

struct DropIndexResponse {
  message @0 :Text;
}

interface PelotonService {
  createIndex @0 (request :CreateIndexRequest) -> (response :CreateIndexResponse);
  dropIndex @1 (request :DropIndexRequest) -> (response :DropIndexResponse);
}
