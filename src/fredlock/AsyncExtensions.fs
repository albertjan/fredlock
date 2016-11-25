// The SqlDataConnection (LINQ to SQL) TypeProvider allows you to write code that uses 
// a live connection to a database. For more information, please go to 
//    http://go.microsoft.com/fwlink/?LinkId=229209
[<AutoOpen>]
module internalAsyncExtensions

  type Microsoft.FSharp.Control.Async with  
    static member Sequential (ops:Async<'T>[]) = async {
      let res = Array.zeroCreate ops.Length
      for i in 0 .. ops.Length - 1 do
        let! value = ops.[i]
        res.[i] <- value 
      return res }