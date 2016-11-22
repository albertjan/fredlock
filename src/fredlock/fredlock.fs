namespace fredlock

open StackExchange.Redis
open System;

type Lock = {
  resource: string;
  value: string option;
  expiration: int64;
}

/// Documentation for my library
///
/// ## Example
///
///     let h = Library.hello 1
///     printfn "%d" h
///
type Redlock (mp: ConnectionMultiplexer, ?db: int) =
  
  let lockScript = @"return redis.call(""set"", KEYS[1], ARGV[1], ""NX"", ""PX"", ARGV[2])" 
  let unlockScript = 
    @"if redis.call(""get"", KEYS[1]) == ARGV[1] then return redis.call(""del"", KEYS[1]) else return 0 end" 
  let extendScript = 
    @"if redis.call(""get"", KEYS[1]) == ARGV[1] then return redis.call(""pexpire"", KEYS[1], ARGV[2]) else return 0 end"

  let dbFactory () =
    match db with
    | Some i -> mp.GetDatabase(i)
    | _ -> mp.GetDatabase()

  let epoch = new DateTime(1970, 1, 1)

  let toEpoch date =
    let diff:TimeSpan = (epoch - date)
    diff.TotalMilliseconds |> int64

  let random () = 
    let arr = Array.create 16 0uy
    let rand:Random = new Random(Guid.NewGuid().GetHashCode())
    rand.NextBytes(arr)
    arr 
      |> Array.map (fun s -> s.ToString("x2"))
      |> String.concat "" 

  let internalLock (resource:string) (value:string option) (ttl:int64) =
    async {

      let db = dbFactory()

      let! value =  
        match value with 
        | Some r -> async { return RedisResult.Create(RedisValue.op_Implicit(r)) }
          //extend
        | None ->
          //lock
          let key = RedisKey.op_Implicit(resource)
          let value = RedisValue.op_Implicit(random())
          db.ScriptEvaluateAsync(lockScript, [|key|], [|value|]) |> Async.AwaitTask

      return {
        resource = resource;
        value = value |> string |> Some;
        expiration = DateTime.Now.AddMilliseconds(ttl |> float) |> toEpoch;
      }
    }

  /// Returns 42
  ///
  /// ## Parameters
  ///  - `num` - whatever
  member this.Lock resource ttl = 
    internalLock resource None ttl

  member this.Extend lock ttl = 
    async {
      // the lock has expired
      if lock.expiration < (DateTime.Now |> toEpoch) then
        return failwith (sprintf "Cannot extend lock on resource '%s' because the lock has already expired." lock.resource)
      else
        return! internalLock lock.resource lock.value ttl
    }

  member this.Release lock =
    async {
      let lock = { lock with expiration = 0L }
      let db = dbFactory()
      do! 
        match lock.value with
        | Some l -> 
          async { 
            let! res =
              db.ScriptEvaluateAsync(unlockScript, [|lock.resource |> RedisKey.op_Implicit|], [| l |>  RedisValue.op_Implicit|])
              |> Async.AwaitTask
              
            let i:string = RedisResult.op_Explicit(res)
            () //return (i |> int64)
          }
        | None ->
          async { () }
      ()
    }