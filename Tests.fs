module PaxosTests

open FsUnit.Xunit
open Xunit
open Paxos
let mockAcceptorHighestProposalId newId (acceptor: Acceptor)  =
    {
        acceptor with HighestProposalId = newId
    }
let mockAcceptorPromisedProposedId newId (acceptor: Acceptor)  =
    {
        acceptor with PromisedProposedId = newId
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
        res.UpdatedAcceptor.PromisedProposedId |> should equal 5
        res.AcceptedValue |> should equal None

[<Fact>]
let ``Handle Prepare - Proposal Rejected`` () =
    let acceptor = { createAcceptor 1 with PromisedProposedId = 10 }
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
    let acceptor = { createAcceptor 1 with PromisedProposedId = 10 }
    let proposalId = 5
    let value = "testValue"
    let result = handleAccept proposalId value acceptor
    result |> should equal None