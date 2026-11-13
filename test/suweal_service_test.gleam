import gleam/dynamic
import gleam/dynamic/decode
import gleam/erlang/process
import gleam/option
import gleam/otp/static_supervisor
import gleam/result
import gleeunit
import pont/serialize
import suweal
import suweal/node
import suweal/wss/surreal_wss
import suweal/wss/surreal_wss_mock
import suweal_service

pub fn main() -> Nil {
  gleeunit.main()
}

fn start_service_with_reply(
  name name: suweal_service.ProcessName,
  reply reply: surreal_wss.SurrealWssResponse(decode.Dynamic),
) {
  let connection = suweal.Connection("_", "main", "main", "")

  static_supervisor.new(static_supervisor.OneForOne)
  |> static_supervisor.add(
    suweal_service.default_config(name, connection)
    |> suweal_service.with_wss_connector(fn(connection) {
      surreal_wss_mock.start(connection, surreal_wss_mock.reply(_, reply))
      |> result.replace_error("Failed to start WSS mock")
    })
    |> suweal_service.dispatcher(),
  )
  |> static_supervisor.start()
}

fn start_service_with_value_reply(
  name name: suweal_service.ProcessName,
  value value: dynamic.Dynamic,
) {
  start_service_with_reply(name, value_reply(value))
}

fn value_reply(value: dynamic.Dynamic) {
  surreal_wss.QueryResponse(1, [
    suweal.SurrealResponse(
      time: "",
      type_: option.None,
      result: option.Some(value),
    ),
  ])
}

pub fn suweal_service_with_success_query_test() {
  let dispatcher_name = process.new_name("SUWEAL_SERVICE")

  let assert Ok(_supervisor) =
    start_service_with_value_reply(
      name: dispatcher_name,
      value: dynamic.string("Hello"),
    )

  let assert Ok(response) =
    suweal_service.query(
      namespace: "main",
      query: [node.Number(0.0)],
      serializer: serialize.string(),
    )
    |> suweal_service.run(dispatcher_name)

  assert response == option.Some("Hello")
}

pub fn suweal_service_with_failed_decoding_test() {
  let dispatcher_name = process.new_name("SUWEAL_SERVICE")

  let assert Ok(_supervisor) =
    start_service_with_value_reply(name: dispatcher_name, value: dynamic.int(0))

  let assert Error(error) =
    suweal_service.query(
      namespace: "main",
      query: [node.Number(0.0)],
      serializer: serialize.string(),
    )
    |> suweal_service.run(dispatcher_name)

  let assert suweal_service.FailedToDecode(..) = error
}

pub fn suweal_service_with_failed_query_test() {
  let dispatcher_name = process.new_name("SUWEAL_SERVICE")

  let assert Ok(_supervisor) =
    start_service_with_reply(
      name: dispatcher_name,
      reply: surreal_wss.ErrorResponse(
        0,
        surreal_wss.SurrealWssResponseError(
          option.None,
          1234,
          "kind",
          "message",
        ),
      ),
    )

  let assert Error(error) =
    suweal_service.query(
      namespace: "main",
      query: [node.Number(0.0)],
      serializer: serialize.string(),
    )
    |> suweal_service.run(dispatcher_name)

  let assert suweal_service.QueryError(..) = error
}

pub fn suweal_service_with_timeout_test() {
  let dispatcher_name = process.new_name("SUWEAL_SERVICE")

  let assert Ok(_supervisor) =
    start_service_with_value_reply(name: dispatcher_name, value: dynamic.nil())

  let assert Error(error) =
    suweal_service.query(
      namespace: "main",
      query: [node.Number(0.0)],
      serializer: serialize.string(),
    )
    |> suweal_service.with_timeout(0)
    |> suweal_service.run(dispatcher_name)

  let assert suweal_service.Timeout(..) = error
}
