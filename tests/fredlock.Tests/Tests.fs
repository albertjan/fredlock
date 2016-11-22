module fredlock.Tests

open fredlock
open NUnit.Framework
open StackExchange.Redis

[<Test>]
let ``should write lock on all servers`` () =
  async {
    let mp = ConnectionMultiplexer.Connect("127.0.0.1")
    let rl = Redlock(mp)
    let! res = rl.Lock "bananas" 100L
    Assert.AreEqual(res.resource, "bananas")
  } |> Async.RunSynchronously |> ignore

