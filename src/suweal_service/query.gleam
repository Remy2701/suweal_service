import gleam/erlang/process
import gleam/function
import gleam/option
import hive
import offstage
import offstage/dynamic/serialize
import suweal/node as surreal_node
import suweal/surreal_ql
import suweal_service
import suweal_service/model

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

pub fn single(query: Query(a, option.Option(List(b)))) -> Query(a, b) {
  Query(..query, transform: fn(value) {
    value
    |> query.transform()
    |> suweal_service.single()
  })
}

pub fn some(query: Query(a, option.Option(b))) -> Query(a, b) {
  Query(..query, transform: fn(value) {
    value
    |> query.transform()
    |> suweal_service.some()
  })
}

pub fn optional_single(
  query: Query(a, option.Option(List(b))),
) -> Query(a, option.Option(b)) {
  Query(..query, transform: fn(value) {
    value
    |> query.transform()
    |> suweal_service.optional_single()
  })
}

pub fn empty_if_none(
  query: Query(a, option.Option(List(b))),
) -> Query(a, List(b)) {
  Query(..query, transform: fn(value) {
    value
    |> query.transform()
    |> suweal_service.empty_if_none()
  })
}

pub fn not_empty(query: Query(a, option.Option(List(b)))) -> Query(a, List(b)) {
  Query(..query, transform: fn(value) {
    value
    |> query.transform()
    |> suweal_service.not_empty()
  })
}

pub fn run(
  query: Query(a, b),
  in namespace: String,
  service service: process.Name(hive.DispatcherMessage(model.DBMessage)),
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
