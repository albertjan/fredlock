namespace fredlock

open StackExchange.Redis
open System;

type Lock = {
  resource: string;
  value: Option<string>;
  expiration: int64;
}

/// Documentation for my library
///
/// ## Example
///
///     let h = Library.hello 1
///     printfn "%d" h
///
type Redlock (servers: list<string>) =
  let epoch = new DateTime(1970, 1, 1)

  let toEpoch date =
    let diff:TimeSpan = (epoch - date)
    diff.TotalMilliseconds |> int64

  let internalLock resource value (ttl:int64) =
    async {
      return {
        resource = resource;
        value = value;
        expiration = DateTime.Now.AddMilliseconds(ttl |> float) |> toEpoch;
      }
    }

  let mp = ConnectionMultiplexer.Connect(servers |> String.concat ",")

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
      let votes = 0
      let quorum = (Math.Floor(servers.Length / 2 |> float) |> int) + 1;
      let a:IServer = mp.GetServer("a")
      let b:IDatabase = mp.GetDatabase()
      
      ()    
    }