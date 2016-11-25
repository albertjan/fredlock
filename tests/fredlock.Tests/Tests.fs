module fredlock.Tests

open fredlock

open FsUnit.Xunit
open Xunit
open StackExchange.Redis

/// tests assum there are three redisservers running on localhost on ports 6379, 6380, 6381
/// they'll be flushed every test run
let mp = ConnectionMultiplexer.Connect("127.0.0.1, 127.0.0.1:6380, 127.0.0.1:6381")
let mps = mp.GetEndPoints() |> Seq.map(fun ep -> ConnectionMultiplexer.Connect(ep.ToString() + ",allowAdmin=true"))
do mps |> Seq.iter (fun m -> m.GetServer(m.GetEndPoints().[0].ToString()).FlushDatabase())

[<Fact>]
let ``should write lock on all servers`` () =
  async {
    // Given
    let rl = Redlock(mp)
    
    // When
    let! res = rl.Lock "bananas" 100000L
    
    // Then
    res.Value.resource |> should equal "bananas"
    let locks = 
        mps 
        |> Seq.map (fun mp -> mp.GetDatabase()) 
        |> Seq.map (fun db -> db.StringGet(RedisKey.op_Implicit("bananas"))) 
        |> Seq.filter(fun res -> res.IsNull |> not) 
        |> Seq.map (fun res -> res |> string)
    locks |> Seq.toList |> should haveLength 3
  } |> Async.RunSynchronously |> ignore

[<Fact>]
let ``should return None when lock can't be taken`` () =
  async {
    // Given
    let rl = Redlock(mp)
    
    // When
    let! res1 = rl.Lock "other-bananas" 100000L 
    let! res2 = rl.Lock "other-bananas" 100000L 

    // Then
    res1.IsSome |> should be True
    res1.Value |> should be instanceOfType<Lock>
    res2 |> should equal None
  } |> Async.RunSynchronously |> ignore

[<Fact>]
let ``should be able to extend a lock`` () =
  async {
    // Given
    let rl = Redlock(mp)
    
    // When
    let! res1 = rl.Lock "moar-bananas" 100000L 
    let! res2 = rl.Extend res1.Value 100000L

    // Then
    res1.IsSome |> should be True
    res2.IsSome |> should be True

    res1.Value |> should be instanceOfType<Lock>
    res2.Value |> should be instanceOfType<Lock>
    res1.Value |> should not' (be sameAs res2.Value)
  } |> Async.RunSynchronously |> ignore

[<Fact>]
let ``should be able to releas a lock`` () =
  async {
    // Given
    let rl = Redlock(mp)
    
    // When
    let! res1 = rl.Lock "some-apples" 100000L 
    do! rl.Release res1.Value
    let! res2 = rl.Lock "some-apples" 100000L 

    // Then
    res1.IsSome |> should be True
    res2.IsSome |> should be True

    res1.Value |> should be instanceOfType<Lock>
    res2.Value |> should be instanceOfType<Lock>
    res1.Value |> should not' (be sameAs res2.Value)
  } |> Async.RunSynchronously |> ignore

[<Fact>]
let ``should still take a lock when one server already has it.`` () =
  async {
    // Given
    let rl = Redlock(mp)
    
    // When
    let mp = mps |> Seq.head
    let res1 = mp.GetDatabase().ScriptEvaluate(RedlockState.Default.lockScript, [|RedisKey.op_Implicit("some-rotten-apples")|], [|RedisValue.op_Implicit("gobledygook"); RedisValue.op_Implicit(10000L)|])
    let! res2 = rl.Lock "some-rotten-apples" 100000L 

    // Then
    res1.IsNull |> should be False
    res2.IsSome |> should be True

    res2.Value |> should be instanceOfType<Lock>
  } |> Async.RunSynchronously |> ignore

[<Fact>]
let ``should try more then once to get a lock`` () =
  async {
    // Given
    let rl = Redlock(mp)
    
    // When
    let! res1 = rl.Lock "pear" 100L 
    let! res2 = rl.Lock "pear" 300L 

    // Then
    res1.IsSome |> should be True
    res2.IsSome |> should be True
  } |> Async.RunSynchronously |> ignore

[<Fact>]
let ``should not be able to extend a expired lock`` () =
  async {
    // Given
    let rl = Redlock(mp)
    
    // When
    let! res1 = rl.Lock "worm" 100L
    do! Async.Sleep 100
    let! res2 = rl.Extend res1.Value 100000L

    // Then
    res1.IsSome |> should be True
    res2.IsSome |> should be False

    res1.Value |> should be instanceOfType<Lock>
  } |> Async.RunSynchronously |> ignore