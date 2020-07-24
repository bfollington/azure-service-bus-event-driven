// Learn more about F# at http://fsharp.org

open System
open System.Text
open System.Threading
open System.Threading.Tasks
open Microsoft.Azure.ServiceBus

// Utilities for Task / Async interop
module Async =
    let inline awaitPlainTask (task: Task) = 
        // rethrow exception from preceding task if it fauled
        let continuation (t : Task) : unit =
            match t.IsFaulted with
            | true -> raise t.Exception
            | arg -> ()
        task.ContinueWith continuation |> Async.AwaitTask

    let inline startAsPlainTask (work : Async<unit>) = Task.Factory.StartNew(fun () -> work |> Async.RunSynchronously)


module ServiceBus = 
    let private connectionString = ""
    let private queueName = "my-special-stream"

    let connect = QueueClient(connectionString, queueName)

    let sendMessage (client: IQueueClient) (msg: string) = 
        let busMsg = Message(Encoding.UTF8.GetBytes(msg))
        Console.WriteLine(sprintf "Sending message: %s" msg)

        client.SendAsync(busMsg) |> Async.AwaitTask

    let registerHandler (client: IQueueClient) processHandler exceptionHandler =
        let options = MessageHandlerOptions(Func<ExceptionReceivedEventArgs, Task>exceptionHandler)
        options.MaxConcurrentCalls <- 1
        options.AutoComplete <- false

        client.RegisterMessageHandler(Func<Message, CancellationToken, Task>(processHandler), options)

let processMessage (client: IQueueClient) (retryDelay: int) (handler: Message -> Result<unit, _>) (msg: Message) (cancellationToken: CancellationToken) =
    Console.WriteLine(
        sprintf
            "New Message: (%A / %A) %A"
            msg.SystemProperties.SequenceNumber
            msg.SystemProperties.DeliveryCount
            (Encoding.UTF8.GetString(msg.Body))
    )

    // My queue is set up to DeadLetter after 10 attempts
    if not cancellationToken.IsCancellationRequested
    then 
        match handler msg with
        | Ok () -> 
            printfn "Marking message as complete"
            client.CompleteAsync(msg.SystemProperties.LockToken)
        | Error e -> 
            printfn "Message failed: %A! Retrying in %ds..." e (retryDelay / 1000)
            async {
                do! client.AbandonAsync(msg.SystemProperties.LockToken) |> Async.AwaitTask
                do! Task.Delay(retryDelay) |> Async.AwaitTask
            } |> Async.startAsPlainTask
    else 
        Task.CompletedTask
    
let handleError (exceptionArgs: ExceptionReceivedEventArgs) =
    Console.WriteLine(sprintf "Message handler encountered an exception %A." exceptionArgs.Exception)
    let context = exceptionArgs.ExceptionReceivedContext
    Console.WriteLine("Exception context for troubleshooting:")
    Console.WriteLine(sprintf "- Endpoint: {%A}" context.Endpoint)
    Console.WriteLine(sprintf "- Entity Path: {%A}" context.EntityPath)
    Console.WriteLine(sprintf "- Executing Action: {%A}" context.Action)
    Task.CompletedTask

let handler (m: Message) = 
    if m.SystemProperties.DeliveryCount < 5 // Intentionally fail to simulate retry
    then Error "Code Gremlins"
    else Ok ()

[<EntryPoint>]
let main argv =
    async {
        let client = ServiceBus.connect
        let send = ServiceBus.sendMessage client

        printfn "> Press any key to send message..."

        Console.ReadKey() |> ignore

        let processor = processMessage client 5000 handler
        ServiceBus.registerHandler client processor handleError

        do! send "Hello world!"

        printfn "> Press any key to exit..."

        Console.ReadKey() |> ignore

        do! client.CloseAsync() |> Async.AwaitTask
    } |> Async.RunSynchronously

    
    0 // return an integer exit code
