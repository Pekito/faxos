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
    AcceptorPromisesReceived: Set<AcceptorId>
    QuorumSize: int // Number of Promises Required to go to phase 2
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
        Some (acceptor, acceptor.AcceptedValue)
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
                    promises 
                    |> List.map (fun (acceptor, _) -> acceptor.AcceptorId)
                    |> Set.ofList
        }
    if(not (hasAchievedQuorum updatedProposer)) then None
    else 
        let highestAcceptedValue =
            promises
            |> List.filter (fun (acceptor, _) -> Option.isSome acceptor.AcceptedValue)
            |> List.sortByDescending (fun (acceptor, _) -> acceptor.HighestProposalId)
            |> List.tryHead
            |> Option.bind (fun (_, value) -> value)
        Some (updatedProposer, highestAcceptedValue)
    

// let executePhase2 
//     proposer proposalId value =
//     let hasAchievedQuorum = proposer.AcceptorPromisesReceived.Count >= proposer.QuorumSize
//     if hasAchievedQuorum then
//         let responses =
//             proposer.AcceptorPromisesReceived
//             |> Set.toList
//             |> List.choose (fun acceptor -> handleAccept proposalId value acceptor)
//         Some responses
//     else None 

// let propose 
//     proposer proposalId value acceptors =
//     let (updatedProposer, highestAcceptedValue) = executePhase1 proposer proposalId acceptors
//     let proposedValue = highestAcceptedValue |> Option.defaultValue value
//     match (executePhase2 updatedProposer proposalId proposedValue) with
//     | Some updatedAcceptors -> 
//         acceptors 
//         |> List.filter 
//             (fun x -> 
//                 updatedAcceptors 
//                 |> List.tryFind 
//                     (fun y -> x.AcceptorId = y.AcceptorId) <> None)
//         |> (fun x -> x @ updatedAcceptors)
//     | None -> acceptors
    
