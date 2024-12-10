module Paxos
type ProposalValue = string
type ProposerId = int
type AcceptorId = int
type ProposalId = int

type Acceptor = {
    AcceptorId: AcceptorId
    HighestProposalId: ProposalId
    AcceptedValue: ProposalValue option
}
type Proposer = {
    ProposerId: ProposerId
    CurrentProposalId: int
    AcceptorPromisesReceived: Set<Acceptor>
    QuorumSize: int // Number of Promises Required to go to phase 2
}
type HandlePrepareResult = {
    ProposalId: ProposalId;
    UpdatedAcceptor: Acceptor;
    AcceptedValue: ProposalValue option
}
type ExecutePhase1Result = {
    UpdatedProposer: Proposer;
    HighestAcceptedValue: ProposalValue option
}
let createAcceptor acceptorId =
    {
        AcceptorId = acceptorId
        HighestProposalId = 0
        AcceptedValue = None
    }
let createProposer proposerId quorumSize =
    {
        ProposerId = proposerId
        CurrentProposalId = 0
        AcceptorPromisesReceived = Set.empty
        QuorumSize = quorumSize
    }

let hasAchievedQuorum proposer =
    Set.count proposer.AcceptorPromisesReceived >= proposer.QuorumSize
let handlePrepare proposalId acceptor =
    if proposalId > acceptor.HighestProposalId then
        Some {
            UpdatedAcceptor = {acceptor with HighestProposalId = proposalId}
            ProposalId = acceptor.HighestProposalId
            AcceptedValue = acceptor.AcceptedValue
        }
    else
        None

let handleAccept proposalId value acceptor =
    if proposalId >= acceptor.HighestProposalId then
        let updatedAcceptor = 
            { acceptor with 
                HighestProposalId = proposalId
                AcceptedValue = Some value }
        Some (updatedAcceptor)
    else
        None

let executePhase1 
    proposer proposalId acceptors =
    let promises =
        acceptors 
            |> List.choose (fun x -> handlePrepare proposalId x)
    
    let updatedProposer = 
        {
            proposer with 
                CurrentProposalId = proposalId
                AcceptorPromisesReceived =
                    (promises) 
                    |> List.map (fun x -> x.UpdatedAcceptor)
                    |> Set.ofList
        }
    if(not (hasAchievedQuorum updatedProposer)) then None
    else 
        let highestAcceptedValue =
            promises
            |> List.filter (fun (x) -> Option.isSome x.UpdatedAcceptor.AcceptedValue)
            |> List.sortByDescending (fun x -> x.ProposalId)
            |> List.tryHead
            |> Option.bind (fun x -> x.AcceptedValue)
        Some {
            HighestAcceptedValue = highestAcceptedValue
            UpdatedProposer = updatedProposer
        }
    

let executePhase2 (valueFromPhase1: ExecutePhase1Result) acceptors =
    match valueFromPhase1.HighestAcceptedValue with
    | None -> acceptors
    | Some value ->
        let updatedAcceptors = 
            acceptors
            |> List.map (fun acceptor -> 
                match handleAccept valueFromPhase1.UpdatedProposer.CurrentProposalId value acceptor with
                | Some updatedAcceptor -> 
                    updatedAcceptor
                | None -> acceptor)
        updatedAcceptors