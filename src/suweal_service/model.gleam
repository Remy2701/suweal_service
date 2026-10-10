import gleam/dynamic
import gleam/erlang/process
import gleam/option
import suweal/surreal_ql
import suweal/wss/surreal_wss

pub type QueryData {
  QueryData(
    namespace: String,
    query: String,
    receiver: process.Subject(Result(List(dynamic.Dynamic), String)),
  )
}

pub type SigninData {
  SigninData(
    namespace: String,
    collection: String,
    fields: List(#(String, surreal_ql.SurrealQL)),
    receiver: process.Subject(Result(String, String)),
  )
}

pub type SignupData {
  SignupData(
    namespace: String,
    collection: String,
    fields: List(#(String, surreal_ql.SurrealQL)),
    receiver: process.Subject(Result(String, String)),
  )
}

pub type WorkerMessage {
  ProcessQueue(queue: List(QueryData))
  ProcessSignin(SigninData)
  ProcessSignup(SignupData)
  Response(surreal_wss.SurrealWssResponse(dynamic.Dynamic))
  Ready
}

pub type DBMessage {
  ExecuteQuery(
    namespace: String,
    query: String,
    reply_to: process.Subject(Result(option.Option(dynamic.Dynamic), String)),
  )
  ExecuteSignin(SigninData)
  ExecuteSignup(SignupData)
  WSSResponse(surreal_wss.SurrealWssResponse(dynamic.Dynamic))
}
