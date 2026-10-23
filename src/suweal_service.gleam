import gleam/dynamic
import gleam/dynamic/decode as gdecode
import gleam/erlang/process
import gleam/int
import gleam/list
import gleam/option
import gleam/result
import gleam/string
import gleam/time/duration
import hive
import hive/queue
import offstage
import offstage/dynamic/decode
import offstage/dynamic/serialize
import offstage/offstage_core
import suweal
import suweal/http/surreal_http
import suweal/node as surreal_node
import suweal/surreal_ql
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
  process.Name(hive.DispatcherMessage(DBMessage))

//-----------------------------------------------------------------------------------------------//
//                                            Execute                                            //
//-----------------------------------------------------------------------------------------------//

pub type DBExecutionError {
  Timeout
  ServiceUnavailable
  QueryError
  FailedToDecode(List(decode.DecodeError))
  NotFound
}

pub type DBRequest(a) {
  DBQueryRequest(
    timeout: Int,
    namespace: String,
    query: List(surreal_node.Node),
    parameters: List(#(String, surreal_ql.SurrealQL)),
    decoder: gdecode.Decoder(a),
  )
}

pub fn query(
  namespace: String,
  query: List(surreal_node.Node),
  serializer: serialize.Serializer(a),
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
  parameters: List(#(String, surreal_ql.SurrealQL)),
) -> DBRequest(a) {
  case request {
    DBQueryRequest(..) -> DBQueryRequest(..request, parameters: parameters)
  }
}

pub fn with_param(
  request: DBRequest(a),
  name: String,
  value: surreal_ql.SurrealQL,
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
  request: DBRequest(a),
  name: ProcessName,
) -> Result(option.Option(a), DBExecutionError) {
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
  parameters: List(#(String, surreal_ql.SurrealQL)),
  decoder: gdecode.Decoder(a),
) {
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
    Ok(Error(_)) -> Error(QueryError)
    Ok(Ok(result)) ->
      gdecode.run(
        option.unwrap(result, dynamic.nil()),
        gdecode.optional(decoder),
      )
      |> result.map_error(FailedToDecode)
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
  fields: List(#(String, surreal_ql.SurrealQL)),
) {
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
    Ok(Error(_)) -> Error(QueryError)
    Ok(Ok(result)) -> Ok(result)
  }
}

pub fn signup(
  timeout: Int,
  namespace: String,
  name: ProcessName,
  collection: String,
  fields: List(#(String, surreal_ql.SurrealQL)),
) {
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
    Ok(Error(_)) -> Error(QueryError)
    Ok(Ok(result)) -> Ok(result)
  }
}

//---------------------------------------------------------------------------//
//                                Dispatcher                                 //
//---------------------------------------------------------------------------//

pub fn dispatcher(name: ProcessName, connection: suweal.Connection) {
  hive.new_with_initialiser(3000, fn(subject) {
    use wss_connection <- result.try(
      surreal_wss.new(connection)
      |> surreal_wss.start()
      |> result.replace_error("Failed to start websocket"),
    )

    let wss_subject = process.new_subject()

    Ok(
      hive.initialised(WorkerState(
        subject:,
        state: Idling,
        wss_subject:,
        wss_connection:,
      ))
      |> hive.selecting(
        process.new_selector()
        |> process.select(subject)
        |> process.select_map(wss_subject, WSSResponse),
      ),
    )
  })
  |> hive.with_name(name)
  |> hive.with_strategy(hive.OldestInFirstOut)
  |> hive.with_close_after(duration.minutes(5) |> duration.to_milliseconds())
  |> hive.with_queue(
    queue.new()
    |> queue.with_size(100)
    |> queue.with_timeout(duration.seconds(5) |> duration.to_milliseconds())
    |> queue.with_strategy(queue.FirstInFirstOut),
  )
  |> hive.on_message(fn(state: WorkerState, message) {
    case state.state, message {
      Preparing(namespace:, query:, reply_to:),
        WSSResponse(surreal_wss.UseResponse(..))
      -> {
        surreal_wss.send(
          state.wss_connection,
          surreal_wss.Query(query, state.wss_subject),
        )

        hive.keep_busy(
          WorkerState(
            ..state,
            state: Processing(query: query, reply_to: reply_to),
            wss_connection: state.wss_connection
              |> surreal_wss.with_namespace(namespace),
          ),
        )
      }
      Preparing(..), _ -> {
        // Unexpected response
        hive.keep_busy(state)
      }
      Processing(reply_to:, ..),
        WSSResponse(surreal_wss.QueryResponse(result:, ..))
      -> {
        let first =
          list.first(result)
          |> result.replace_error("No result returned from SurrealDB")
          |> result.map(fn(response) {
            case response {
              suweal.SurrealResponse(result:, ..) -> Ok(result)
              suweal.SurrealErrorResponse(kind:, result:, ..) ->
                Error(kind <> " " <> result)
            }
          })
          |> result.flatten()

        process.send(reply_to, first)

        hive.continue(WorkerState(..state, state: Idling))
      }
      Processing(..), _ -> {
        // Unexpected response
        hive.keep_busy(state)
      }
      Idling, ExecuteQuery(namespace:, query:, reply_to:) -> {
        case namespace == state.wss_connection.connection.namespace {
          True -> {
            surreal_wss.send(
              state.wss_connection,
              surreal_wss.Query(query, state.wss_subject),
            )

            hive.keep_busy(
              WorkerState(
                ..state,
                state: Processing(query: query, reply_to: reply_to),
              ),
            )
          }
          False -> {
            surreal_wss.send(
              state.wss_connection,
              surreal_wss.Use(
                namespace,
                state.wss_connection.connection.database,
                state.wss_subject,
              ),
            )

            hive.keep_busy(
              WorkerState(
                ..state,
                state: Preparing(namespace, query: query, reply_to: reply_to),
              ),
            )
          }
        }
      }
      Idling, ExecuteSignin(data) -> {
        let email =
          list.key_find(data.fields, "email")
          |> result.map(fn(value) {
            case value {
              surreal_ql.String(email) -> Ok(email)
              _ -> Error(Nil)
            }
          })
          |> result.flatten()
        let password =
          list.key_find(data.fields, "password")
          |> result.map(fn(value) {
            case value {
              surreal_ql.String(email) -> Ok(email)
              _ -> Error(Nil)
            }
          })
          |> result.flatten()

        case email, password {
          Ok(email), Ok(password) -> {
            let res =
              surreal_http.signin(
                state.wss_connection.connection
                  |> suweal.with_namespace(data.namespace),
                data.collection,
                email,
                password,
                data.fields
                  |> list.filter(fn(field) {
                    field.0 != "email" && field.0 != "password"
                  }),
              )

            case res {
              Ok(token) -> {
                process.send(data.receiver, Ok(token))
              }
              Error(suweal.InvalidUrl) -> {
                process.send(data.receiver, Error("Invalid URL"))
              }
              Error(suweal.HttpError(_)) -> {
                process.send(data.receiver, Error("Http error"))
              }
              Error(suweal.FailedToDecode(_)) -> {
                process.send(data.receiver, Error("Failed to decode"))
              }
              Error(suweal.NoResponse) -> {
                process.send(data.receiver, Error("No response"))
              }
              Error(suweal.ErrorResponse(description:, ..)) -> {
                process.send(
                  data.receiver,
                  Error("Error response from SurrealDB: " <> description),
                )
              }
            }
          }
          _, _ -> {
            process.send(
              data.receiver,
              Error("Missing email or password field"),
            )
          }
        }

        hive.continue(state)
      }
      Idling, ExecuteSignup(data) -> {
        let email =
          list.key_find(data.fields, "email")
          |> result.map(fn(value) {
            case value {
              surreal_ql.String(email) -> Ok(email)
              _ -> Error(Nil)
            }
          })
          |> result.flatten()
        let password =
          list.key_find(data.fields, "password")
          |> result.map(fn(value) {
            case value {
              surreal_ql.String(email) -> Ok(email)
              _ -> Error(Nil)
            }
          })
          |> result.flatten()

        case email, password {
          Ok(email), Ok(password) -> {
            let res =
              surreal_http.signup(
                state.wss_connection.connection
                  |> suweal.with_namespace(data.namespace),
                data.collection,
                email,
                password,
                data.fields
                  |> list.filter(fn(field) {
                    field.0 != "email" && field.0 != "password"
                  }),
              )

            case res {
              Ok(token) -> process.send(data.receiver, Ok(token))
              Error(suweal.InvalidUrl) -> {
                process.send(data.receiver, Error("Invalid URL"))
              }
              Error(suweal.HttpError(_)) -> {
                process.send(data.receiver, Error("Http error"))
              }
              Error(suweal.FailedToDecode(_)) -> {
                process.send(data.receiver, Error("Failed to decode"))
              }
              Error(suweal.NoResponse) -> {
                process.send(data.receiver, Error("No response"))
              }
              Error(suweal.ErrorResponse(
                code:,
                details:,
                description:,
                information:,
              )) -> {
                process.send(
                  data.receiver,
                  Error(
                    "Error response from SurrealDB: "
                    <> description
                    <> " (code: "
                    <> int.to_string(code)
                    <> ", details: "
                    <> details
                    <> ", information: "
                    <> information
                    <> ")",
                  ),
                )
              }
            }

            process.send(
              data.receiver,
              Error("Missing email or password field"),
            )
          }
          _, _ -> {
            process.send(
              data.receiver,
              Error("Missing email or password field"),
            )
          }
        }

        hive.continue(state)
      }
      Idling, WSSResponse(_) -> {
        // Ignore message
        hive.keep_busy(state)
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
    reply_to: process.Subject(Result(option.Option(dynamic.Dynamic), String)),
  )
  Processing(
    query: String,
    reply_to: process.Subject(Result(option.Option(dynamic.Dynamic), String)),
  )
  Idling
}

type WorkerState {
  WorkerState(
    subject: process.Subject(DBMessage),
    state: WorkerQueueState,
    wss_subject: process.Subject(
      surreal_wss.SurrealWssResponse(gdecode.Dynamic),
    ),
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

fn operation_type_verb(operation: OperationType) {
  case operation {
    Create -> "create "
    Read -> "fetch "
    Update -> "update "
    Delete -> "delete "
  }
}

fn operation_type_particle(operation: OperationType) {
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
      QueryError ->
        response.internal_server_error.apply(
          "Failed to " <> operation_type_verb(operation) <> name,
        )
      ServiceUnavailable ->
        response.service_unavailable.apply(
          "Failed to " <> operation_type_verb(operation) <> name,
        )
      FailedToDecode(e) -> {
        response.internal_server_error.apply(
          "Failed to decode " <> name <> ": " <> string.inspect(e),
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
  result: Result(option.Option(List(a)), DBExecutionError),
) -> Result(a, DBExecutionError) {
  case result {
    Ok(option.Some([value, ..])) -> Ok(value)
    Ok(option.None) | Ok(option.Some([])) -> Error(NotFound)
    Error(response) -> Error(response)
  }
}

pub fn some(
  result: Result(option.Option(a), DBExecutionError),
) -> Result(a, DBExecutionError) {
  case result {
    Ok(option.Some(value)) -> Ok(value)
    Ok(option.None) -> Error(NotFound)
    Error(response) -> Error(response)
  }
}

/// Pick the first value of the optional list or return None
pub fn optional_single(
  result: Result(option.Option(List(a)), DBExecutionError),
) -> Result(option.Option(a), DBExecutionError) {
  case result {
    Ok(option.Some([value, ..])) -> Ok(option.Some(value))
    Ok(option.None) | Ok(option.Some([])) -> option.None |> Ok
    Error(response) -> Error(response)
  }
}

/// Transform a None value into an empty list
pub fn empty_if_none(
  result: Result(option.Option(List(a)), DBExecutionError),
) -> Result(List(a), DBExecutionError) {
  case result {
    Ok(option.Some(values)) -> Ok(values)
    Ok(option.None) -> Ok([])
    Error(response) -> Error(response)
  }
}

/// Require the result to be a non-empty list otherwise return [NotFound].
pub fn not_empty(
  result: Result(option.Option(List(a)), DBExecutionError),
) -> Result(List(a), DBExecutionError) {
  case result {
    Ok(option.Some([value, ..values])) -> Ok([value, ..values])
    Ok(option.None) | Ok(option.Some([])) -> Error(NotFound)
    Error(response) -> Error(response)
  }
}
