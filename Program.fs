open Paxos
open Node
[<EntryPoint>]
let main argv =
    async {
        // Function to generate unique proposal values
        let generateProposalValue index = sprintf "Value%d" index

        // Create a proposer node
        let proposer = createNode (createProposer 1 3) 100 2 30

        // Create acceptor nodes
        let acceptors =
            [| for i in 1..5 -> createNode (createAcceptor i) 100 2 30 |]

        // Run 100 proposals with different values
        let! results =
            [|1..100|]
            |> Array.map (fun index ->
                let proposalValue = generateProposalValue index
                propose proposer proposalValue acceptors
            )
            |> Async.Parallel

        
        // Print the final state of the proposer and acceptors
        printfn "Final Proposer State: %A" proposer.Value
        acceptors |> Array.iter (fun node -> printfn "Acceptor %d State: %A" node.Value.AcceptorId node.Value)
        return 0 // Exit code
    } |> Async.RunSynchronously
