import gleam/function
import gleam/option
import offstage
import pont/serialize
import suweal/node as surreal_node
import suweal/surreal_ql
import suweal_service
import suweal_service/model

/// Represents a database query with its associated metadata and transformation logic.
/// - `resource` specifies the resource being queried.
/// - `operation_type` indicates the type of database operation.
/// - `query` is the list of SurrealDB nodes forming the query.
/// - `serializer` is used to serialize the query parameters.
/// - `parameters` is the list of query parameters.
/// - `transform` is a function to transform the query result.
pub type Query(a, b) {
  Query(
    resource: String,
    operation_type: suweal_service.OperationType,
    query: List(surreal_node.Node),
    serializer: serialize.Serializer(a),
    parameters: List(#(String, surreal_ql.SurrealQL)),
    transform: fn(Result(option.Option(a), suweal_service.DBExecutionError)) ->
      Result(b, suweal_service.DBExecutionError),
  )
}

/// Creates a new query with the specified resource, operation type, query nodes, serializer, and parameters.
pub fn query(
  resource resource: String,
  operation_type operation_type: suweal_service.OperationType,
  query query: List(surreal_node.Node),
  serializer serializer: serialize.Serializer(a),
  parameters parameters: List(#(String, surreal_ql.SurrealQL)),
) -> Query(a, option.Option(a)) {
  Query(
    resource: resource,
    operation_type: operation_type,
    query: query,
    serializer: serializer,
    parameters: parameters,
    transform: function.identity,
  )
}

/// Transforms a query that returns a list into a query that returns a single element.
pub fn single(query: Query(a, option.Option(List(b)))) -> Query(a, b) {
  Query(..query, transform: fn(value) {
    value
    |> query.transform()
    |> suweal_service.single()
  })
}

/// Transforms a query that returns an optional value into a query that returns the value directly.
pub fn some(query: Query(a, option.Option(b))) -> Query(a, b) {
  Query(..query, transform: fn(value) {
    value
    |> query.transform()
    |> suweal_service.some()
  })
}

/// Transforms a query that returns an optional list into a query that returns an optional single element.
pub fn optional_single(
  query: Query(a, option.Option(List(b))),
) -> Query(a, option.Option(b)) {
  Query(..query, transform: fn(value) {
    value
    |> query.transform()
    |> suweal_service.optional_single()
  })
}

/// Transforms a query that returns an optional list into a query that returns an empty list if the result is `None`.
pub fn empty_if_none(
  query: Query(a, option.Option(List(b))),
) -> Query(a, List(b)) {
  Query(..query, transform: fn(value) {
    value
    |> query.transform()
    |> suweal_service.empty_if_none()
  })
}

/// Transforms a query that returns an optional list into a query that ensures the list is not empty.
pub fn not_empty(query: Query(a, option.Option(List(b)))) -> Query(a, List(b)) {
  Query(..query, transform: fn(value) {
    value
    |> query.transform()
    |> suweal_service.not_empty()
  })
}

/// Executes the given query against the specified namespace and service, and processes the response using the provided operation response handler.
/// - `query` is the query to be executed.
/// - `namespace` specifies the database namespace.
/// - `service` is the process handling the database messages.
/// - `operation_response` is the handler for processing the operation response.
/// Returns a `Result` containing the query result or an error response.
pub fn run(
  query: Query(a, b),
  in namespace: String,
  service service: model.ProcessName,
  response operation_response: suweal_service.DbOperationErrorSet,
) -> Result(b, offstage.WispResponse) {
  suweal_service.query(namespace, query.query, query.serializer)
  |> suweal_service.with_params(query.parameters)
  |> suweal_service.run(service)
  |> query.transform()
  |> suweal_service.to_response(
    query.operation_type,
    query.resource,
    operation_response,
  )
}
