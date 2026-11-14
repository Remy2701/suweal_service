import gleam/dynamic.{type Dynamic}
import gleam/erlang/process.{type Subject}
import gleam/option.{type Option, None, Some}
import gleam/result
import gleam/time/duration
import gleam/time/timestamp.{type Timestamp}
import hive
import hive/queue
import pont/value
import suweal
import suweal/surreal_ql.{type SurrealQL}
import suweal/wss/surreal_wss.{type SurrealWssResponse}

/// Represents the data required to execute a query against the database.
/// - `namespace` specifies the database namespace.
/// - `query` is the SurrealQL query string.
/// - `receiver` is the process that will receive the result of the query.
pub type QueryData {
  QueryData(
    namespace: String,
    query: String,
    receiver: Subject(Result(List(Dynamic), String)),
  )
}

/// Represents the data required to sign in a user.
/// - `namespace` specifies the database namespace.
/// - `collection` is the database collection.
/// - `fields` is a list of field names and their corresponding SurrealQL values.
/// - `receiver` is the process that will receive the result of the sign-in operation.
pub type SigninData {
  SigninData(
    namespace: String,
    collection: String,
    fields: List(#(String, SurrealQL)),
    receiver: Subject(Result(String, String)),
  )
}

/// Represents the data required to sign up a user.
/// - `namespace` specifies the database namespace.
/// - `collection` is the database collection.
/// - `fields` is a list of field names and their corresponding SurrealQL values.
/// - `receiver` is the process that will receive the result of the sign-up operation.
pub type SignupData {
  SignupData(
    namespace: String,
    collection: String,
    fields: List(#(String, SurrealQL)),
    receiver: Subject(Result(String, String)),
  )
}

/// Represents the different messages that a worker process can handle.
pub type WorkerMessage {
  /// Process a queue of database queries.
  ProcessQueue(queue: List(QueryData))
  /// Process a sign-in request.
  ProcessSignin(SigninData)
  /// Process a sign-up request.
  ProcessSignup(SignupData)
  /// Handle a response from the WebSocket server.
  Response(SurrealWssResponse(Dynamic))
  /// Indicate that the worker is ready to receive messages.
  Ready
}

/// Represents the different messages that can be sent to the service dispatcher.
/// - `ExecuteQuery` executes a database query.
/// - `ExecuteSignin` executes a sign-in operation.
/// - `ExecuteSignup` executes a sign-up operation.
/// - `WSSResponse` handles a response from the WebSocket server.
pub type DBMessage {
  /// Execute a database query.
  ExecuteQuery(
    namespace: String,
    query: String,
    reply_to: Subject(Result(Option(Dynamic), String)),
  )
  /// Execute a sign-in operation.
  ExecuteSignin(SigninData)
  /// Execute a sign-up operation.
  ExecuteSignup(SignupData)
  /// Handle a response from the WebSocket server.
  WSSResponse(SurrealWssResponse(Dynamic))
}

/// Represents the name of a process that handles DB messages.
pub type ProcessName =
  process.Name(hive.DispatcherMessage(DBMessage))

// ––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––– //
//                                            Logging                                            //
// ––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––– //

/// Represents the severity level of a log message.
pub type LogLevel {
  LogLevelInfo
  LogLevelWarning
  LogLevelError
  LogLevelDebug
}

/// Represents the callback to log a message.
pub type LogCallback =
  fn(String, LogLevel, Timestamp, String, String, value.Value) -> Nil

// ––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––– //
//                                         WSS Connector                                         //
// ––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––– //

pub type WSSConnector =
  fn(suweal.Connection) -> Result(surreal_wss.SurrealWssConnection, String)

pub fn default_wss_connector(
  connection: suweal.Connection,
) -> Result(surreal_wss.SurrealWssConnection, String) {
  surreal_wss.new(connection)
  |> surreal_wss.start()
  |> result.replace_error("Failed to start websocket")
}

// ––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––– //
//                                         Configuration                                         //
// ––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––– //

/// The configuration for the Suweal service.
/// - `name` is the name of the process handling the service.
/// - `connection` is the connection to the SurrealDB instance.
/// - `log_callback` is an optional callback for logging messages.
/// - `queue` is an optional queue builder for managing query execution.
pub type SuwealServiceConfig {
  SuwealServiceConfig(
    name: ProcessName,
    connection: suweal.Connection,
    wss_connector: WSSConnector,
    log_callback: Option(LogCallback),
    queue: Option(queue.Builder),
    strategy: hive.Strategy,
    close_after: duration.Duration,
  )
}

/// Returns the default queue builder with pre-configured settings.
pub fn default_queue() -> queue.Builder {
  queue.new()
  |> queue.with_size(100)
  |> queue.with_timeout(duration.seconds(5) |> duration.to_milliseconds())
  |> queue.with_strategy(queue.FirstInFirstOut)
}

/// Returns the default configuration for the Suweal service with the given process name and connection.
pub fn default_config(
  name: ProcessName,
  connection: suweal.Connection,
) -> SuwealServiceConfig {
  SuwealServiceConfig(
    name: name,
    connection: connection,
    wss_connector: default_wss_connector,
    log_callback: None,
    queue: Some(default_queue()),
    strategy: hive.OldestInFirstOut,
    close_after: duration.minutes(5),
  )
}

/// Returns a new configuration with the specified log callback.
pub fn with_log_callback(
  config: SuwealServiceConfig,
  log_callback: LogCallback,
) -> SuwealServiceConfig {
  SuwealServiceConfig(..config, log_callback: Some(log_callback))
}

/// Returns a new configuration without a queue.
pub fn without_queue(config: SuwealServiceConfig) -> SuwealServiceConfig {
  SuwealServiceConfig(..config, queue: None)
}

/// Returns a new configuration with the specified queue builder.
pub fn with_queue(
  config: SuwealServiceConfig,
  queue: queue.Builder,
) -> SuwealServiceConfig {
  SuwealServiceConfig(..config, queue: Some(queue))
}

/// Returns a new configuration with the specified Hive strategy.
pub fn with_strategy(
  config: SuwealServiceConfig,
  strategy: hive.Strategy,
) -> SuwealServiceConfig {
  SuwealServiceConfig(..config, strategy: strategy)
}

pub fn with_close_after(
  config: SuwealServiceConfig,
  close_after: duration.Duration,
) -> SuwealServiceConfig {
  SuwealServiceConfig(..config, close_after: close_after)
}

pub fn with_wss_connector(
  config: SuwealServiceConfig,
  wss_connector: WSSConnector,
) -> SuwealServiceConfig {
  SuwealServiceConfig(..config, wss_connector: wss_connector)
}
