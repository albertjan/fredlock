namespace fredlock

open StackExchange.Redis
open System;
open FSharp.Control

/// The lock type
type Lock = {
  resource: string;
  value: string;
  expiration: int64;
}

/// Types for RedisValue conversion
/// Usage:
/// `
/// "test" |> String |> toValue
/// `
type Converter = 
  | Int64 of int64
  | Int32 of int32
  | String of string

/// RedlockState all the adjustable settings 
/// you can adjust settings like this:
/// `
/// let state = { RedlockState.Default with driftfactor = 0.02 }
/// `
/// ps. Pass this as a second parameter to the RedLock instance
type RedlockState = {
  driftFactor: float
  retries: int
  retryDelay: int
  db: int
  lockScript: string
  unlockScript: string
  extendScript: string
} with  
  static member Default = {
    driftFactor = 0.01
    retries = 3
    retryDelay = 200
    db = 0
    lockScript = @"return redis.call(""set"", KEYS[1], ARGV[1], ""NX"", ""PX"", ARGV[2])" 
    unlockScript = @"if redis.call(""get"", KEYS[1]) == ARGV[1] then return redis.call(""del"", KEYS[1]) else return 0 end" 
    extendScript =  @"if redis.call(""get"", KEYS[1]) == ARGV[1] then return redis.call(""pexpire"", KEYS[1], ARGV[2]) else return 0 end"
  } 

[<AutoOpen>]
module Convenience =
  /// Lazy random number generator
  let rand:Lazy<Random> = lazy(new Random())
  /// get a random number below i
  let random i = rand.Value.Next(i)
  /// get a random string value of 16 chars
  let randValue () =
    let arr = Array.create 16 0uy
    rand.Value.NextBytes(arr)
    arr 
      |> Array.map (fun s -> s.ToString("x2"))
      |> String.concat "" 
  /// epoch
  let epoch = new DateTime(1970, 1, 1)
  /// get milliseconds from epoch
  let toEpoch date =
    let diff:TimeSpan = (date - epoch)
    diff.TotalMilliseconds |> int64
  /// DateTime.Now in millisecs from epoch
  let nowEpoch () = DateTime.Now |> toEpoch
  /// <summary>
  /// Option coalesing operator
  /// `
  /// None |? 4 -> 4
  /// Some 4 |? 3 -> 4
  /// `
  /// </summary>
  let (|?) (x:'a option) (y:'a) = match x with | None -> y | Some z -> z
  /// make RedisValue of type
  let toValue x = 
    match x with
    | Int64 i -> RedisValue.op_Implicit (i)
    | Int32 i -> RedisValue.op_Implicit (i)
    | String s -> RedisValue.op_Implicit (s)
  /// make RedisKey of string
  let toKey x = RedisKey.op_Implicit (x |> string)

/// The main redlock type, make one of these, to do your locking.
/// API:
/// `
/// Redlock.Lock resource:string ttl:int64 -> Async<Lock option>
/// Redlock.Extend lock ttl:int64 -> Async<Lock option>
/// Redlock.Release lock -> Async<unit>
/// `
type Redlock (mp: ConnectionMultiplexer, ?state: RedlockState) =

  let state = state |? RedlockState.Default

  let mps = mp.GetEndPoints() |> Seq.map(fun ep -> ConnectionMultiplexer.Connect(ep.ToString())) |> Seq.toList

  let dbFactory () = mps |> Seq.map(fun mp -> mp.GetDatabase(state.db))

  let unlockInstance lock (db:IDatabase) = 
    db.ScriptEvaluateAsync(state.unlockScript, [|lock.resource |> toKey|], [| lock.value |> String |> toValue|]) |> Async.AwaitTask |> Async.Ignore
  
  let internalLock (resource:string) (value: string option) (ttl:int64) =
    async {
      let value, script =
        match value with
        | Some v -> v, state.extendScript
        | None -> randValue () , state.lockScript

      let dbs = dbFactory ()
      let start = nowEpoch ()

      let quorum = Math.Floor(float (dbs |> Seq.length) / 2.) + 1. |> int32
      let drift = Math.Round(state.driftFactor * float ttl) + 2. |> int64
     
      let rec attempt attempts =
        async {
          let! res = 
            dbs 
              |> Seq.map(fun db -> async {
                let! res = db.ScriptEvaluateAsync(script, [| resource |> toKey |], [| value |> String |> toValue; ttl |> Int64 |> toValue |]) |> Async.AwaitTask
                return if res.IsNull then 0 else 1
              }) 
              |> Seq.toArray 
              |> Async.Sequential
              
          let res = res |> Seq.sum
          let validityTime = ttl - (nowEpoch() - start) - drift
                   
          match (res >= quorum && validityTime > 0L, attempts = 0) with
          | (true, _) -> return Some { resource = resource; value = value; expiration = start + validityTime - drift}
          | (false, _) as x ->
            do! dbs |> Seq.map (unlockInstance { resource = resource; value = value; expiration = 0L }) |> Seq.toArray |> Async.Sequential |> Async.Ignore 
            if x |> snd then
              return None
            else
              do! state.retryDelay |> random |> Async.Sleep 
              return! attempt (attempts - 1)
        }

      return! attempt state.retries
    }
  
  /// Get a lock on a resource 
  /// `
  /// async {
  ///   let dlm = new Redlock(mp)
  ///   let! lock = dlm.Lock("resource", 1000)
  ///   match lock with 
  ///   | Some l -> 
  ///     // got the lock
  ///     printfn "Value: %s" l.Value.Value
  ///   | None ->
  ///     // failed to get lock
  ///     printfn ":("
  /// }
  /// `
  member this.Lock resource ttl = internalLock resource None ttl

  /// Extend a lock on a resource
  /// `
  /// async {
  ///   let dlm = new Redlock(mp)
  ///   let! lock = dlm.Extend(lock, 1000)
  ///   match lock with 
  ///   | Some l -> 
  ///     // got the lock
  ///     printfn "Value: %s" l.Value.Value
  ///   | None ->
  ///     // failed to get lock
  ///     printfn ":("
  /// }
  /// `
  member this.Extend rlock ttl = 
    if rlock.expiration < nowEpoch () then
      async { return None }
    else
      internalLock rlock.resource (Some rlock.value) ttl

  /// Release a lock on a resource
  /// `
  /// async {
  ///   let dlm = new Redlock(mp)
  ///   do! dlm.Release(lock)
  /// }
  /// `
  member this.Release lock = dbFactory() |> Seq.map (unlockInstance lock) |> Seq.toArray |> Async.Sequential |> Async.Ignore