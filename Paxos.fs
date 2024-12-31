module Paxos
type ProposalValue = string
type ProposerId = int
type AcceptorId = int
type ProposalId = int

type Acceptor = {
    AcceptorId: AcceptorId
    HighestProposalId: ProposalId
    PromisedProposedId: ProposalId
    AcceptedValue: ProposalValue option
}
type Proposer = {
    ProposerId: ProposerId
    CurrentProposalId: int
    AcceptorPromisesReceived: Set<Acceptor>
    QuorumSize: int // Number of Promises Required to go to phase 2
}
type HandlePrepareResult = {
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
        PromisedProposedId = 0
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
    if proposalId > acceptor.PromisedProposedId then
        Some {
            UpdatedAcceptor = {
                acceptor with 
                    PromisedProposedId = proposalId
                }
            AcceptedValue = acceptor.AcceptedValue
        }
    else
        None

let handleAccept proposalId value acceptor =
    if proposalId >= acceptor.PromisedProposedId then
        let updatedAcceptor = 
            { acceptor with 
                HighestProposalId = proposalId
                PromisedProposedId = proposalId
                AcceptedValue = Some value }
        Some (updatedAcceptor)
    else
        None