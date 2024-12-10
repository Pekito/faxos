module PaxosTests

open FsUnit.Xunit
open Xunit
open Paxos
let mockAcceptorHighestProposalId newId (acceptor: Acceptor)  =
    {
        acceptor with HighestProposalId = newId
    }
let mockAcceptorValue newValue (acceptor: Acceptor)  =
    {
        acceptor with AcceptedValue = Some newValue
    }

[<Fact>]
let ``Handle Prepare - Proposal Accepted`` () =
    let acceptor = createAcceptor 1
    let result = handlePrepare 5 acceptor
    match result with
    | None -> failwith "Expected to Accept"
    | Some (acceptor, value) -> 
        acceptor.AcceptorId |> should equal 1
        value |> should equal None

[<Fact>]
let ``Handle Prepare - Proposal Rejected`` () =
    let acceptor = { createAcceptor 1 with HighestProposalId = 10 }
    let result = handlePrepare 5 acceptor
    result |> should equal None

[<Fact>]
let ``Handle Accept - Proposal Accepted`` () =
    let acceptor = createAcceptor 1
    let proposalId = 5
    let value = "testValue"
    let result = handleAccept proposalId value acceptor
    match result with
    | Some updatedAcceptor ->
        updatedAcceptor.HighestProposalId |> should equal proposalId
        updatedAcceptor.AcceptedValue |> should equal (Some value)
    | None -> failwith "Expected Some updatedAcceptor"

[<Fact>]
let ``Handle Accept - Proposal Rejected`` () =
    let acceptor = { createAcceptor 1 with HighestProposalId = 10 }
    let proposalId = 5
    let value = "testValue"
    let result = handleAccept proposalId value acceptor
    result |> should equal None

[<Fact>]
let ``execute phase 1 - proposer does not reach quorum`` () =
    let proposer = createProposer 1 2


    let acceptorOne = mockAcceptorHighestProposalId  5 (createAcceptor 1)
    let acceptorTwo = mockAcceptorHighestProposalId  5 (createAcceptor 2)

    let acceptors = [acceptorOne; acceptorTwo]
    let proposalId = 1

    let result = executePhase1 proposer proposalId acceptors

    result |> should equal None
[<Fact>]
let ``execute phase 1 - proposer reaches quorum and acceptors does not have value `` () =
    let proposer = createProposer 1 2
    let acceptorOne = createAcceptor 1
    let acceptorTwo = createAcceptor 2

    let acceptors = [acceptorOne; acceptorTwo]
    let proposalId = 1

    let result = executePhase1 proposer proposalId acceptors
    match result with
    | None -> failwith "Expected a result"
    | Some (updatedAccetor, currentValue) ->
        currentValue |> should equal None
        updatedAccetor.AcceptorPromisesReceived |> should equal (Set.ofList [1;2])

[<Fact>]
let ``execute phase 1 - proposer reaches quorum and acceptors have the same value `` () =
    let proposer = createProposer 1 2
    let acceptorOne = createAcceptor 1 |> mockAcceptorHighestProposalId 1 |> mockAcceptorValue "Pedro"
    let acceptorTwo = createAcceptor 2 |> mockAcceptorHighestProposalId 1 |> mockAcceptorValue "Pedro"

    let acceptors = [acceptorOne; acceptorTwo]
    let proposalId = 2

    let result = executePhase1 proposer proposalId acceptors
    match result with
    | None -> failwith "Expected a result"
    | Some (updatedAccetor, currentValue) ->
        currentValue |> should equal (Some "Pedro")
        updatedAccetor.AcceptorPromisesReceived |> should equal (Set.ofList [1;2])

[<Fact>]
let ``execute phase 1 - proposer reaches quorum and acceptors have different values `` () =
    let proposer = createProposer 1 2
    let acceptorOne = createAcceptor 1 |> mockAcceptorHighestProposalId 1 |> mockAcceptorValue "Pedro"
    let acceptorTwo = createAcceptor 2 |> mockAcceptorHighestProposalId 2 |> mockAcceptorValue "Pires"

    let acceptors = [acceptorOne; acceptorTwo]
    let proposalId = 3

    let result = executePhase1 proposer proposalId acceptors

    let expectedResultValue = Some "Pires" // it should return the highest proposal id from the promise list
    match result with
    | None -> failwith "Expected a result"
    | Some (updatedAccetor, currentValue) ->
        currentValue |> should equal expectedResultValue
        updatedAccetor.AcceptorPromisesReceived |> should equal (Set.ofList [1;2])


[<Fact>]
let ``execute phase 1 - quorum exceeds expectations`` () =
    let proposer = createProposer 1 2

    let acceptors = [createAcceptor 1; createAcceptor 2; createAcceptor 3; createAcceptor 4; createAcceptor 5]
    let proposalId = 1

    let result = executePhase1 proposer proposalId acceptors
    result.IsSome |> should equal true