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
    | Some res ->
        res.UpdatedAcceptor.HighestProposalId |> should equal 5
        res.AcceptedValue |> should equal None

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
    | Some res ->
        res.HighestAcceptedValue |> should equal None
        res.UpdatedProposer.AcceptorPromisesReceived.Count |> should equal 2

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
    | Some res ->
         res.HighestAcceptedValue |> should equal (Some "Pedro")
         res.UpdatedProposer.AcceptorPromisesReceived.Count |> should equal 2

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
    | Some res ->
        res.HighestAcceptedValue |> should equal expectedResultValue
        res.UpdatedProposer.AcceptorPromisesReceived.Count |> should equal 2


[<Fact>]
let ``execute phase 1 - quorum exceeds expectations`` () =
    let proposer = createProposer 1 2

    let acceptors = [createAcceptor 1; createAcceptor 2; createAcceptor 3; createAcceptor 4; createAcceptor 5]
    let proposalId = 1

    let result = executePhase1 proposer proposalId acceptors
    result.IsSome |> should equal true

[<Fact>]
let ``execute phase 2 - quorum reached`` () =
    let proposer = createProposer 1 2
    let acceptorOne = createAcceptor 1
    let acceptorTwo = createAcceptor 2
    let acceptors = [acceptorOne; acceptorTwo]

    let phase1Result = {
        UpdatedProposer = proposer
        HighestAcceptedValue = Some "testValue"
    }

    let updatedAcceptors = executePhase2 phase1Result acceptors

    updatedAcceptors
    |> List.filter (fun a -> a.AcceptedValue = Some "testValue")
    |> List.length
    |> should equal 2

[<Fact>]
let ``execute phase 2 - mixed acceptors, quorum reached`` () =
    let proposer = createProposer 1 2
    let acceptorOne = createAcceptor 1
    let acceptorTwo = createAcceptor 2 |> mockAcceptorHighestProposalId 5
    let acceptorThree = createAcceptor 3
    let acceptors = [acceptorOne; acceptorTwo; acceptorThree]

    let phase1Result = {
        UpdatedProposer = proposer
        HighestAcceptedValue = Some "testValue"
    }

    let updatedAcceptors = executePhase2 phase1Result acceptors

    updatedAcceptors
    |> List.filter (fun a -> a.AcceptedValue = Some "testValue")
    |> List.length
    |> should equal 2

[<Fact>]
let ``execute phase 2 - no highest accepted value from phase 1`` () =
    let proposer = createProposer 1 2
    let acceptorOne = createAcceptor 1
    let acceptorTwo = createAcceptor 2
    let acceptors = [acceptorOne; acceptorTwo]

    let phase1Result = {
        UpdatedProposer = proposer
        HighestAcceptedValue = None
    }

    let updatedAcceptors = executePhase2 phase1Result acceptors

    updatedAcceptors
    |> List.forall (fun a -> a.AcceptedValue = None)
    |> should equal true

[<Fact>]
let ``execute phase 2 - acceptors with different highest proposals and values`` () =
    let proposer = createProposer 1 2

    // Acceptor 1: lower HighestProposalId, no AcceptedValue
    let acceptorOne = createAcceptor 1 |> mockAcceptorHighestProposalId 2

    // Acceptor 2: higher HighestProposalId, accepted a different value
    let acceptorTwo = createAcceptor 2 
                      |> mockAcceptorHighestProposalId 5 
                      |> mockAcceptorValue "OtherValue"

    // Acceptor 3: no previous AcceptedValue
    let acceptorThree = createAcceptor 3

    let acceptors = [acceptorOne; acceptorTwo; acceptorThree]

    // Phase 1 result: Proposer's CurrentProposalId is used with "ChosenValue"
    let phase1Result = {
        UpdatedProposer = { proposer with CurrentProposalId = 10 }
        HighestAcceptedValue = Some "ChosenValue"
    }
    let updatedAcceptors = executePhase2 phase1Result acceptors
    updatedAcceptors
    |> List.forall (fun x -> x.AcceptedValue = Some "ChosenValue")
    |> should equal true