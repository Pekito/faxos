module Node
open System
open Paxos
type Node<'a> = {
    mutable Value: 'a
    Delay: int
    DelayMultiplier: int
    MultiplierTriggerChance: int
    mutable IsActive: bool
}

let createNode value delay delayMultiplier triggerChance = 
    {
        Value = value
        Delay = delay
        DelayMultiplier = delayMultiplier
        MultiplierTriggerChance = triggerChance
        IsActive = true
    }

let getDelayWithMultiplier (node: Node<'a>) =
    let random = Random()
    let chance = random.Next(1, 101)
    if chance <= node.MultiplierTriggerChance then
        node.Delay * node.DelayMultiplier
    else
        node.Delay


let toggleNode node =
    node.IsActive <- not node.IsActive

let handleNodeAccept proposalId value node =
    async {
        if not node.IsActive then return None
        else
            let result = handleAccept proposalId value node.Value
            if result.IsSome then node.Value <- result.Value
            let delay = getDelayWithMultiplier node
            do! Async.Sleep(delay)
            return Some node
    }

let handleNodePrepare node =
        let lambda proposalId =
            async {
                if not node.IsActive then return None
                else
                    let result = handlePrepare proposalId node.Value
                    if (result.IsSome) then node.Value <- result.Value.UpdatedAcceptor
                    let delay = getDelayWithMultiplier node
                    do! Async.Sleep(delay)
                    return result
        }
        lambda


let handleNodePhase1
    proposerNode proposalId acceptorNodes =
    async {
        let! promises =
            acceptorNodes
            |> Array.map (fun node -> handleNodePrepare node proposalId)
            |> Async.Parallel
        let promises = Array.choose id promises
    
        let updatedProposer = 
            {
                proposerNode.Value with 
                    CurrentProposalId = proposalId
                    AcceptorPromisesReceived =
                        (promises) 
                        |> Seq.map (fun x -> x.UpdatedAcceptor)
                        |> Set.ofSeq
            }
        if (not (hasAchievedQuorum updatedProposer)) then return None
        elif (proposerNode.Value.CurrentProposalId > proposalId) then return None
        else
            proposerNode.Value <- {proposerNode.Value with CurrentProposalId = proposalId}
            let highestAcceptedValue =
                promises
                |> Seq.filter (fun (x) -> Option.isSome x.UpdatedAcceptor.AcceptedValue)
                |> Seq.sortByDescending (fun x -> x.UpdatedAcceptor.HighestProposalId)
                |> Seq.tryHead
                |> Option.bind (fun x -> x.AcceptedValue)
            return Some {
                HighestAcceptedValue = highestAcceptedValue
                UpdatedProposer = updatedProposer
            } 
    }

let handleNodePhase2 phase1Result acceptors =
    async {
        match phase1Result.HighestAcceptedValue with
        | None -> return acceptors
        | Some value ->
            let updatedAcceptors = 
                acceptors
                |> Seq.map (fun acceptor ->
                    let acceptResult = 
                        handleNodeAccept 
                            phase1Result.UpdatedProposer.CurrentProposalId 
                            value 
                            acceptor 
                        |> Async.RunSynchronously
                    match acceptResult with
                    | Some updatedAcceptor -> 
                        updatedAcceptor
                    | None -> acceptor)
            return updatedAcceptors
    }

let propose proposerNode proposalValue acceptorNodes =
    let proposalId = proposerNode.Value.CurrentProposalId + 1
    proposerNode.Value <- {proposerNode.Value with CurrentProposalId = proposalId}
    async {
        let! phase1Result = handleNodePhase1 proposerNode proposalId acceptorNodes
        match phase1Result with
        | None ->
            printfn "Phase 1 Failed (Proposal %A): Quorum not achieved." proposalId
            return None
        | Some phase1Result ->
            printfn "Phase 1 Accepted (Proposal %A)" proposalId
            let valueToPropose =
                match phase1Result.HighestAcceptedValue with
                | None -> proposalValue
                | Some acceptedValue -> acceptedValue
            let phase1Result = {phase1Result with HighestAcceptedValue = Some valueToPropose}
            let! updatedAcceptors = handleNodePhase2 phase1Result acceptorNodes
            
            let isAccepted =
                updatedAcceptors
                |> Seq.filter (fun node -> node.Value.AcceptedValue = Some valueToPropose)
                |> Seq.length

            if isAccepted >= proposerNode.Value.QuorumSize then
                printfn "Phase 2 Accepted (Proposal %A): Accepted with value %A" proposalId valueToPropose
                return Some valueToPropose
            else
                printfn "Phase 2 Failed (Proposal %A): value '%s' not accepted by quorum." proposalId valueToPropose
                return None
    }