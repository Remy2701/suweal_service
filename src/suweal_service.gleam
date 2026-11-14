import gleam/dynamic
import gleam/dynamic/decode
import gleam/erlang/process
import gleam/function
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/otp/static_supervisor
import gleam/otp/supervision
import gleam/result
import gleam/string
import gleam/time/duration
import gleam/time/timestamp
import hive
import offstage
import offstage/offstage_core
import pont/serialize
import pont/value
import suweal
import suweal/http/surreal_http
import suweal/node as surreal_node
import suweal/surreal_ql.{type SurrealQL}
import suweal/wss/surreal_wss
import suweal_service/model.{
  ExecuteQuery, ExecuteSignin, ExecuteSignup, WSSResponse,
}

pub type QueryData =
  model.QueryData

pub type SigninData =
  model.SigninData

pub type SignupData =
  model.SignupData

pub type WorkerMessage =
  model.WorkerMessage

pub type DBMessage =
  model.DBMessage

pub type ProcessName =
  model.ProcessName

pub type SuwealServiceConfig =
  model.SuwealServiceConfig

pub const default_config = model.default_config

pub const with_log_callback = model.with_log_callback

pub const without_queue = model.without_queue

pub const with_queue = model.with_queue

pub const with_strategy = model.with_strategy

pub const with_close_after = model.with_close_after

pub const with_wss_connector = model.with_wss_connector

//-----------------------------------------------------------------------------------------------//
//                                            Execute                                            //
//-----------------------------------------------------------------------------------------------//

pub type DBExecutionError {
  Timeout
  ServiceUnavailable
  QueryError(String)
  FailedToDecode(value: String, errors: List(decode.DecodeError))
  NotFound
}

pub type DBRequest(a) {
  DBQueryRequest(
    timeout: Int,
    namespace: String,
    query: List(surreal_node.Node),
    parameters: List(#(String, SurrealQL)),
    decoder: decode.Decoder(a),
  )
}

pub fn query(
  namespace namespace: String,
  query query: List(surreal_node.Node),
  serializer serializer: serialize.Serializer(a),
) -> DBRequest(a) {
  DBQueryRequest(
    timeout: 10_000,
    namespace: namespace,
    query: query,
    parameters: [],
    decoder: serializer.decoder,
  )
}

pub fn with_timeout(request: DBRequest(a), timeout: Int) -> DBRequest(a) {
  case request {
    DBQueryRequest(..) -> DBQueryRequest(..request, timeout: timeout)
  }
}

pub fn with_params(
  request: DBRequest(a),
  parameters: List(#(String, SurrealQL)),
) -> DBRequest(a) {
  case request {
    DBQueryRequest(..) -> DBQueryRequest(..request, parameters: parameters)
  }
}

pub fn with_param(
  request: DBRequest(a),
  name: String,
  value: SurrealQL,
) -> DBRequest(a) {
  case request {
    DBQueryRequest(..) ->
      DBQueryRequest(..request, parameters: [
        #(name, value),
        ..request.parameters
      ])
  }
}

pub fn run(
  request request: DBRequest(a),
  name name: ProcessName,
) -> Result(Option(a), DBExecutionError) {
  case request {
    DBQueryRequest(timeout:, namespace:, query:, parameters:, decoder:) -> {
      execute(timeout, namespace, name, query, parameters, decoder)
    }
  }
}

fn execute(
  timeout: Int,
  namespace: String,
  name: ProcessName,
  query: List(surreal_node.Node),
  parameters: List(#(String, SurrealQL)),
  decoder: decode.Decoder(a),
) -> Result(Option(a), DBExecutionError) {
  let subject = process.new_subject()
  let db = process.named_subject(name)
  let query =
    list.map(query, fn(node) {
      surreal_node.replace_variables(node, parameters)
      |> surreal_node.to_string()
      <> ";"
    })
    |> string.join("\n\n")
  use _ <- result.try(
    hive.send(db, ExecuteQuery(namespace, query, subject), 500)
    |> result.replace_error(ServiceUnavailable),
  )

  let response = process.receive(subject, timeout)
  case response {
    Error(_) -> Error(Timeout)
    Ok(Error(message)) -> Error(QueryError(message))
    Ok(Ok(result)) ->
      option.unwrap(result, dynamic.nil())
      |> decode.run(decode.optional(decoder))
      |> result.map_error(FailedToDecode(string.inspect(result), _))
  }
}

//-----------------------------------------------------------------------------------------------//
//                                        Authentication                                         //
//-----------------------------------------------------------------------------------------------//

pub fn signin(
  timeout: Int,
  namespace: String,
  name: ProcessName,
  collection: String,
  fields: List(#(String, SurrealQL)),
) -> Result(String, DBExecutionError) {
  let subject = process.new_subject()
  let db = process.named_subject(name)

  use _ <- result.try(
    hive.send(
      db,
      model.ExecuteSignin(model.SigninData(
        namespace,
        collection,
        fields,
        subject,
      )),
      500,
    )
    |> result.replace_error(Timeout),
  )

  let response = process.receive(subject, timeout)
  case response {
    Error(_) -> Error(Timeout)
    Ok(Error(message)) -> Error(QueryError(message))
    Ok(Ok(result)) -> Ok(result)
  }
}

pub fn signup(
  timeout: Int,
  namespace: String,
  name: ProcessName,
  collection: String,
  fields: List(#(String, SurrealQL)),
) -> Result(String, DBExecutionError) {
  let subject = process.new_subject()
  let db = process.named_subject(name)

  use _ <- result.try(
    hive.send(
      db,
      model.ExecuteSignup(model.SignupData(
        namespace,
        collection,
        fields,
        subject,
      )),
      500,
    )
    |> result.replace_error(Timeout),
  )

  let response = process.receive(subject, timeout)
  case response {
    Error(_) -> Error(Timeout)
    Ok(Error(message)) -> Error(QueryError(message))
    Ok(Ok(result)) -> Ok(result)
  }
}

//---------------------------------------------------------------------------//
//                                Dispatcher                                 //
//---------------------------------------------------------------------------//

fn dispatcher_init(
  config: SuwealServiceConfig,
  subject: process.Subject(DBMessage),
) -> Result(hive.Initialised(WorkerState, DBMessage), String) {
  use wss_connection <- result.try(config.wss_connector(config.connection))

  let wss_subject = process.new_subject()

  WorkerState(subject:, state: Idling, wss_subject:, wss_connection:)
  |> hive.initialised()
  |> hive.selecting(
    process.new_selector()
    |> process.select(subject)
    |> process.select_map(wss_subject, WSSResponse),
  )
  |> Ok
}

fn log(
  config: SuwealServiceConfig,
  namespace: String,
  level: model.LogLevel,
  message: String,
  meta: value.Value,
) -> Nil {
  case config.log_callback {
    Some(callback) -> {
      callback(
        namespace,
        level,
        timestamp.system_time(),
        "SuwealService",
        message,
        meta,
      )
    }
    None -> Nil
  }
}

fn message_to_value(message: DBMessage) -> value.Value {
  value.object([
    #("message", case message {
      ExecuteQuery(namespace:, query:, ..) ->
        value.object([
          #("type", value.string("ExecuteQuery")),
          #("namespace", value.string(namespace)),
          #("query", value.string(query)),
        ])
      ExecuteSignin(data) ->
        value.object([
          #("type", value.string("ExecuteSignin")),
          #("namespace", value.string(data.namespace)),
          #("collection", value.string(data.collection)),
        ])
      ExecuteSignup(data) ->
        value.object([
          #("type", value.string("ExecuteSignup")),
          #("namespace", value.string(data.namespace)),
          #("collection", value.string(data.collection)),
        ])
      WSSResponse(surreal_wss.AuthResponse(id, ..)) ->
        value.object([
          #("type", value.string("WSSResponse")),
          #("response", value.string("AuthResponse")),
          #("id", value.int(id)),
        ])
      WSSResponse(surreal_wss.ErrorResponse(id:, error:)) ->
        value.object([
          #("type", value.string("WSSResponse")),
          #("response", value.string("ErrorResponse")),
          #("id", value.int(id)),
          #(
            "error",
            value.object([
              #("kind", value.string(error.kind)),
              #("message", value.string(error.message)),
              #("cause", value.nullable_of(error.cause, value.string)),
              #("code", value.int(error.code)),
            ]),
          ),
        ])
      WSSResponse(surreal_wss.QueryResponse(id:, ..)) ->
        value.object([
          #("type", value.string("WSSResponse")),
          #("response", value.string("QueryResponse")),
          #("id", value.int(id)),
        ])
      WSSResponse(surreal_wss.UseResponse(id:)) ->
        value.object([
          #("type", value.string("WSSResponse")),
          #("response", value.string("UseResponse")),
          #("id", value.int(id)),
        ])
    }),
  ])
}

fn handle_preparing(
  config: SuwealServiceConfig,
  namespace: String,
  query: String,
  reply_to: process.Subject(Result(Option(dynamic.Dynamic), String)),
  state: WorkerState,
  message: DBMessage,
) -> hive.Next(WorkerState) {
  case message {
    WSSResponse(surreal_wss.UseResponse(..)) -> {
      state.wss_connection
      |> surreal_wss.send(surreal_wss.Query(query, state.wss_subject))

      WorkerState(
        ..state,
        state: Processing(namespace:, query:, reply_to:),
        wss_connection: surreal_wss.with_namespace(
          state.wss_connection,
          namespace,
        ),
      )
      |> hive.keep_busy
    }
    _ -> {
      log(
        config,
        namespace,
        model.LogLevelError,
        "Unexpected message in preparing state",
        message_to_value(message),
      )

      hive.keep_busy(state)
    }
  }
}

fn handle_processing(
  config: SuwealServiceConfig,
  namespace: String,
  reply_to: process.Subject(Result(Option(dynamic.Dynamic), String)),
  state: WorkerState,
  message: DBMessage,
) -> hive.Next(WorkerState) {
  case message {
    WSSResponse(surreal_wss.ErrorResponse(error:, ..)) -> {
      log(
        config,
        namespace,
        model.LogLevelError,
        "Error response",
        message_to_value(message),
      )
      process.send(
        reply_to,
        Error("Error response from SurrealDB: " <> error.message),
      )
      hive.continue(WorkerState(..state, state: Idling))
    }
    WSSResponse(surreal_wss.QueryResponse(result:, ..)) -> {
      let first =
        list.first(result)
        |> result.replace_error("No result returned from SurrealDB")
        |> result.map(fn(response) {
          case response {
            suweal.SurrealResponse(result:, ..) -> Ok(result)
            suweal.SurrealErrorResponse(kind:, result:, type_:, time:, details:) -> {
              log(
                config,
                namespace,
                model.LogLevelError,
                "SurrealDB error",
                value.object([
                  #("kind", value.string(kind)),
                  #("result", value.string(result)),
                  #("type", value.nullable_of(type_, value.string)),
                  #("time", value.string(time)),
                  #("details", value.string(string.inspect(details))),
                ]),
              )
              Error(kind <> " " <> result)
            }
          }
        })
        |> result.flatten()

      process.send(reply_to, first)

      hive.continue(WorkerState(..state, state: Idling))
    }
    _ -> {
      log(
        config,
        namespace,
        model.LogLevelError,
        "Unexpected message in processing state",
        message_to_value(message),
      )

      hive.keep_busy(state)
    }
  }
}

fn handle_idling(
  config: SuwealServiceConfig,
  state: WorkerState,
  message: DBMessage,
) -> hive.Next(WorkerState) {
  let auth = fn(
    namespace: String,
    fields: List(#(String, SurrealQL)),
    executor: fn(String, String, List(#(String, SurrealQL))) ->
      Result(String, suweal.SurrealError),
    receiver: process.Subject(Result(String, String)),
  ) {
    let email =
      fields
      |> list.key_find("email")
      |> result.try(fn(value) {
        case value {
          surreal_ql.String(email) -> Ok(email)
          _ -> Error(Nil)
        }
      })

    let password =
      fields
      |> list.key_find("password")
      |> result.try(fn(value) {
        case value {
          surreal_ql.String(email) -> Ok(email)
          _ -> Error(Nil)
        }
      })

    case email, password {
      Ok(email), Ok(password) -> {
        let res =
          executor(
            email,
            password,
            list.filter(fields, fn(field) {
              field.0 != "email" && field.0 != "password"
            }),
          )

        case res {
          Ok(token) -> {
            process.send(receiver, Ok(token))
          }
          Error(suweal.InvalidUrl) -> {
            log(
              config,
              namespace,
              model.LogLevelError,
              "Invalid URL",
              value.object([]),
            )
            process.send(receiver, Error("Invalid URL"))
          }
          Error(suweal.HttpError(error)) -> {
            log(
              config,
              namespace,
              model.LogLevelError,
              "Http error",
              value.object([#("error", value.string(string.inspect(error)))]),
            )
            process.send(receiver, Error("Http error"))
          }
          Error(suweal.FailedToDecode(error)) -> {
            log(
              config,
              namespace,
              model.LogLevelError,
              "Failed to decode",
              value.object([#("error", value.string(string.inspect(error)))]),
            )
            process.send(receiver, Error("Failed to decode"))
          }
          Error(suweal.NoResponse) -> {
            log(
              config,
              namespace,
              model.LogLevelError,
              "No response",
              value.object([]),
            )
            process.send(receiver, Error("No response"))
          }
          Error(suweal.ErrorResponse(
            description:,
            code:,
            details:,
            information:,
          )) -> {
            log(
              config,
              namespace,
              model.LogLevelError,
              "Error response",
              value.object([
                #("description", value.string(description)),
                #("code", value.int(code)),
                #("details", value.string(details)),
                #("information", value.string(information)),
              ]),
            )
            process.send(
              receiver,
              Error("Error response from SurrealDB: " <> description),
            )
          }
        }
      }
      _, _ -> {
        log(
          config,
          namespace,
          model.LogLevelError,
          "Missing email or password",
          value.object([]),
        )
        process.send(receiver, Error("Missing email or password field"))
      }
    }
  }

  case message {
    ExecuteQuery(namespace:, query:, reply_to:) -> {
      case namespace == state.wss_connection.connection.namespace {
        True -> {
          state.wss_connection
          |> surreal_wss.send(surreal_wss.Query(query, state.wss_subject))

          WorkerState(..state, state: Processing(namespace:, query:, reply_to:))
          |> hive.keep_busy()
        }
        False -> {
          // Switch to the correct namespace before executing the query
          state.wss_connection
          |> surreal_wss.send(surreal_wss.Use(
            namespace,
            state.wss_connection.connection.database,
            state.wss_subject,
          ))

          WorkerState(..state, state: Preparing(namespace:, query:, reply_to:))
          |> hive.keep_busy()
        }
      }
    }
    ExecuteSignin(data) -> {
      auth(
        data.namespace,
        data.fields,
        fn(email, password, fields) {
          state.wss_connection.connection
          |> suweal.with_namespace(data.namespace)
          |> surreal_http.signin(data.collection, email, password, fields)
        },
        data.receiver,
      )
      hive.continue(state)
    }
    ExecuteSignup(data) -> {
      auth(
        data.namespace,
        data.fields,
        fn(email, password, fields) {
          state.wss_connection.connection
          |> suweal.with_namespace(data.namespace)
          |> surreal_http.signup(data.collection, email, password, fields)
        },
        data.receiver,
      )
      hive.continue(state)
    }
    _ -> {
      log(
        config,
        config.connection.namespace,
        model.LogLevelError,
        "Unexpected message in idling state",
        message_to_value(message),
      )

      hive.keep_busy(state)
    }
  }
}

pub fn dispatcher(
  config: SuwealServiceConfig,
) -> supervision.ChildSpecification(static_supervisor.Supervisor) {
  hive.new_with_initialiser(3000, dispatcher_init(config, _))
  |> hive.with_name(config.name)
  |> hive.with_strategy(config.strategy)
  |> hive.with_close_after(config.close_after |> duration.to_milliseconds())
  |> case config.queue {
    Some(queue) -> hive.with_queue(_, queue)
    None -> function.identity
  }
  |> hive.on_message(fn(state: WorkerState, message) {
    case state.state {
      Preparing(namespace:, query:, reply_to:) -> {
        handle_preparing(config, namespace, query, reply_to, state, message)
      }
      Processing(reply_to:, namespace:, ..) -> {
        handle_processing(config, namespace, reply_to, state, message)
      }
      Idling -> {
        handle_idling(config, state, message)
      }
    }
  })
  |> hive.supervised()
}

//---------------------------------------------------------------------------//
//                                  Worker                                   //
//---------------------------------------------------------------------------//

type WorkerQueueState {
  Preparing(
    namespace: String,
    query: String,
    reply_to: process.Subject(Result(Option(dynamic.Dynamic), String)),
  )
  Processing(
    namespace: String,
    query: String,
    reply_to: process.Subject(Result(Option(dynamic.Dynamic), String)),
  )
  Idling
}

type WorkerState {
  WorkerState(
    subject: process.Subject(DBMessage),
    state: WorkerQueueState,
    wss_subject: process.Subject(surreal_wss.SurrealWssResponse(decode.Dynamic)),
    wss_connection: surreal_wss.SurrealWssConnection,
  )
}

//-----------------------------------------------------------------------------------------------//
//                                      Database Operations                                      //
//-----------------------------------------------------------------------------------------------//

/// The type of database operation being performed, used for generating context-specific error 
/// messages in responses.
pub type OperationType {
  Create
  Read
  Update
  Delete
}

fn operation_type_verb(operation: OperationType) -> String {
  case operation {
    Create -> "create "
    Read -> "fetch "
    Update -> "update "
    Delete -> "delete "
  }
}

fn operation_type_particle(operation: OperationType) -> String {
  case operation {
    Create -> "creating "
    Read -> "fetching "
    Update -> "updating "
    Delete -> "deleting "
  }
}

pub type DbOperationErrorSet {
  DbOperationErrorSet(
    timeout: offstage.TimeoutResponse,
    internal_server_error: offstage.InternalServerErrorResponse,
    service_unavailable: offstage.ServiceUnavailableResponse,
    not_found: offstage.NotFoundResponse,
  )
}

pub fn db_operation_response(
  spec: offstage.RouteSpecBuilder,
  next: fn(
    offstage.RouteSpecBuilder,
    offstage.RouteCapability(DbOperationErrorSet, object),
  ) -> offstage.RouteSpec,
) -> offstage.RouteSpec {
  use spec, timeout <- offstage.timeout(spec)
  use spec, internal_server_error <- offstage.internal_server_error(spec)
  use spec, service_unavailable <- offstage.service_unavailable(spec)
  use spec, not_found <- offstage.not_found(spec)

  next(
    spec,
    offstage_core.capability(fn(request) {
      use timeout <- timeout.get(request)
      use internal_server_error <- internal_server_error.get(request)
      use service_unavailable <- service_unavailable.get(request)
      use not_found <- not_found.get(request)

      Ok(DbOperationErrorSet(
        timeout: timeout,
        internal_server_error: internal_server_error,
        service_unavailable: service_unavailable,
        not_found: not_found,
      ))
    }),
  )
}

/// Convert the result's error into a WispResponse with a descriptive message based on the 
/// [operation] and the given [name] (resource name).
pub fn to_response(
  result: Result(a, DBExecutionError),
  operation: OperationType,
  name: String,
  response: DbOperationErrorSet,
) -> Result(a, offstage.WispResponse) {
  result.map_error(result, fn(error) {
    case error {
      Timeout ->
        response.timeout.apply(
          "Timeout while " <> operation_type_particle(operation) <> name,
        )
      QueryError(_) ->
        response.internal_server_error.apply(
          "Failed to " <> operation_type_verb(operation) <> name,
        )
      ServiceUnavailable ->
        response.service_unavailable.apply(
          "Failed to " <> operation_type_verb(operation) <> name,
        )
      FailedToDecode(value:, errors:) -> {
        response.internal_server_error.apply(
          "Failed to decode "
          <> name
          <> " with value "
          <> value
          <> ": "
          <> string.inspect(errors),
        )
      }
      NotFound ->
        response.not_found.apply(
          "Failed to " <> operation_type_verb(operation) <> name,
        )
    }
  })
}

/// Expect a single result from a database query, returning an error response if there are no 
/// results. If there are multiple, the first result is returned and the rest are discarded.
pub fn single(
  result: Result(Option(List(a)), DBExecutionError),
) -> Result(a, DBExecutionError) {
  case result {
    Ok(Some([value, ..])) -> Ok(value)
    Ok(None) | Ok(Some([])) -> Error(NotFound)
    Error(response) -> Error(response)
  }
}

pub fn some(
  result: Result(Option(a), DBExecutionError),
) -> Result(a, DBExecutionError) {
  case result {
    Ok(Some(value)) -> Ok(value)
    Ok(None) -> Error(NotFound)
    Error(response) -> Error(response)
  }
}

/// Pick the first value of the optional list or return None
pub fn optional_single(
  result: Result(Option(List(a)), DBExecutionError),
) -> Result(Option(a), DBExecutionError) {
  case result {
    Ok(Some([value, ..])) -> Ok(Some(value))
    Ok(None) | Ok(Some([])) -> None |> Ok
    Error(response) -> Error(response)
  }
}

/// Transform a None value into an empty list
pub fn empty_if_none(
  result: Result(Option(List(a)), DBExecutionError),
) -> Result(List(a), DBExecutionError) {
  case result {
    Ok(Some(values)) -> Ok(values)
    Ok(None) -> Ok([])
    Error(response) -> Error(response)
  }
}

/// Require the result to be a non-empty list otherwise return [NotFound].
pub fn not_empty(
  result: Result(Option(List(a)), DBExecutionError),
) -> Result(List(a), DBExecutionError) {
  case result {
    Ok(Some([value, ..values])) -> Ok([value, ..values])
    Ok(None) | Ok(Some([])) -> Error(NotFound)
    Error(response) -> Error(response)
  }
}
