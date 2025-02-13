---- MODULE groups ----

EXTENDS Naturals, FiniteSets, util, crypto

CONSTANTS
    Users,
    \* The user that starts the group orchestrates all proposals for changes in
    \* membership.  Typically, we would want to avoid a central entity as an
    \* orchestrator, due to the lack of fault tolerance.  However, this
    \* algorithm can only succeed with participation of all parties, since we
    \* need to prove that all members are already directly connected to the
    \* newly invited user.  A predetermined leader simplifies the algorithm
    \* without a loss of fault tolerance.
    InitialMembers,
    Null,
    (*
    This function describes indirect perceptions about contact descriptions.
    We can translate "user_percetions[[ perceiver |-> userA, description |-> [
    by |-> userB, of |-> userC]]] = userD" into the english statement: "userA
    thinks that when userB describes userC they are referring to userD."  It's
    possible that userA knows the correct answer (userD = userC), they don't
    know who userB is talking about (userD = Null) or they mistake userC for
    someone else (userD /= Null /\ userD /= userC).
    A description is uniquely identified by the user describing and the user
    being described.  However, the description itself should be treated
    opaquely by users, since they must guess at who is being described, they
    cannot ever have access to the internal thoughts of others.
    *)
    UserPerceptions,
    Connections,
    InviteIds,
    \* When specifying liveness, we need requests to be droppable.  This both
    \* enables that and constrains the maximum size of the messages set, so
    \* that our state size doesn't blow up.  This needs to be large enough that
    \* problematic requests will stick around.
    MaxInFlightRequests,
    \* Proposal States
    Proposing,
    Kicking,
    \* User States
    Invited,
    Joining,
    Joined,
    Left,
    Synchronizing,
    Inviting,
    Committed,
    \* Request Type
    PleasePropose,
    Propose,
    Reject,
    Established,
    Kick,
    KickAck,
    Invite,
    SyncShare

VARIABLES
    messages,
    rng_state,
    group_perceptions,
    proposal,
    complete_proposals,
    approver_states

MemberSet == [ user : Users, id : InviteIds \union { Null } ]

ASSUME
    /\ InitialMembers \subseteq MemberSet
    /\ UserPerceptions \in [ [ perceiver : Users, description : [ by : Users, of : Users ] ] -> Users \union { Null } ]
    \* A user always correctly knows their own perceptions
    /\ \A user1, user2 \in Users : UserPerceptions[[ perceiver |-> user1, description |-> [ by |-> user1, of |-> user2 ]]] = user2
    /\ Connections \subseteq (Users \X Users)

Leader ==
    CHOOSE member \in InitialMembers : member.id = Null

PerceivedLeader(member) ==
    CHOOSE m \in group_perceptions[member] : m.id = Null

HasDirectConnection(x, y) ==
    \/ x = y
    \/ <<x, y>> \in Connections
    \/ <<y, x>> \in Connections

Init ==
    /\ messages = {}
    /\ rng_state = { invite_id \in InviteIds : (\A member \in InitialMembers : member.id /= invite_id) }
    (*
    Notably, group members learn about the changes to the group at different
    times, so we need to track their changes in perception individually.
    The empty set means the user doesn't believe themselves to be part of the
    group (they might actually know who some members are).
    On top of that, users may consider themselves in multiple groups in at the
    same time--even in this model where there is only one leader, because the
    whole point of this protocol is that invites can't be correlated.  As such,
    we need to consider what the user thinks for themselves given a their id
    within the group.
    *)
    /\ group_perceptions = [ member \in MemberSet |-> IF member \in InitialMembers THEN InitialMembers ELSE {} ]
    /\ proposal = Null
    /\ complete_proposals = {}
    /\ approver_states =
        [ x \in (InviteIds \X Users) |->
            [ state |-> IF [ id |-> x[1], user |-> x[2] ] \in InitialMembers THEN Joined ELSE Null ]
        ]

FilterThenMaybeAddMessage(message_m, f(_)) ==
    LET
        Filtered == { message \in messages : f(message) }
    IN
        IF  message_m.is_just
        THEN
            IF  \/ MaxInFlightRequests = Null
                \/ Cardinality(Filtered) < MaxInFlightRequests
            THEN
                messages' = Filtered \union { message_m.just }
            ELSE
                LET dropped == CHOOSE x \in Filtered : TRUE
                IN  messages' = (Filtered \ { dropped }) \union { message_m.just }
        ELSE
            messages' = Filtered

AddMessage(message) ==
    FilterThenMaybeAddMessage(PureMaybe(message), LAMBDA x : TRUE)

DropMessage ==
    /\ MaxInFlightRequests /= Null
    /\ \E message \in messages :
        /\ messages' = messages \ { message }
        /\ UNCHANGED <<rng_state, group_perceptions, proposal, complete_proposals, approver_states>>

DropAllMessages ==
    /\ MaxInFlightRequests /= Null
    /\ messages' = {}
    /\ UNCHANGED <<rng_state, group_perceptions, proposal, complete_proposals, approver_states>>

SendPleasePropose ==
    \* IMPORTANT: The user should still wait to receive a Propose message from
    \* the leader before acting further, since they can't send an Invite
    \* message yet anyway.  The Leader might be queuing a number of proposals,
    \* and this one could be happening after another, meaning the group size,
    \* which is important for correct invitee behaviour, cannot be known until
    \* the Leader officially starts this proposal.
    \E proposer \in MemberSet, invitee \in Users :
        /\ Cardinality(rng_state) > 0
        /\ LET invite_id == CHOOSE x \in rng_state : TRUE
           IN
            /\ group_perceptions[proposer] /= {}
            \* They must be the leader, or they must have finished joining
            \* before they start making proposals.
            /\ proposer.id /= Null =>
                approver_states[<<proposer.id, proposer.user>>].state = Joined
            /\ \A members \in group_perceptions[proposer] :
                /\ members.user /= invitee
            /\ HasDirectConnection(proposer.user, invitee)
            \* The Leader hasn't deleted their receiving queue
            /\ proposer \in group_perceptions[PerceivedLeader(proposer)]
            /\ AddMessage(
                    [ type |-> PleasePropose
                    , sender |-> proposer
                    , recipient |-> PerceivedLeader(proposer)
                    , invite_id |-> invite_id
                    , invitee_description |-> [ by |-> proposer.user, of |-> invitee ]
                    ]
                )
            /\ rng_state' = rng_state \ { invite_id }
            /\ UNCHANGED <<group_perceptions, proposal, complete_proposals, approver_states>>

LeaderReceivePleasePropose ==
    \E message \in messages :
        /\ message.type = PleasePropose
        /\ message.recipient = Leader
        /\ message.sender \in group_perceptions[Leader]
        /\ message.invite_id \notin complete_proposals
        /\ proposal = Null
        \* NOTE: As a slight optimization, the Leader can synchronously accept or reject.
        /\ proposal' =
            [ type |-> Proposing
            , invite_id |-> message.invite_id
            , invitee_description |-> message.invitee_description
            , group_size |-> Cardinality(group_perceptions[Leader])
            , awaiting_response |-> group_perceptions[Leader]
            ]
        /\ UNCHANGED <<messages, rng_state, group_perceptions, complete_proposals, approver_states>>

BroadcastProposalState(recipient) ==
    /\ proposal /= Null
    /\ \E member \in proposal.awaiting_response :
        /\ member = recipient
        \* Leader can't send the message if the recipient has deleted their
        \* queue
        /\ Leader \in group_perceptions[member]
        /\ AddMessage(
                CASE proposal.type = Proposing ->
                    [ sender |-> Leader
                    , recipient |-> member
                    , type |-> Propose
                    , invite_id |-> proposal.invite_id
                    , invitee_description |-> proposal.invitee_description
                    , group_size |-> proposal.group_size
                    ]
                  [] proposal.type = Kicking ->
                    [ sender |-> Leader
                    , recipient |-> member
                    , type |-> Kick
                    , kicked |-> proposal.kicked
                    ]
            )
        /\ UNCHANGED <<rng_state, group_perceptions, proposal, complete_proposals, approver_states>>

LeaderReceiveReject ==
    \E message \in messages :
        /\ message.type = Reject
        /\ message.recipient = Leader
        /\ message.sender \in group_perceptions[Leader]
        /\ message.invite_id \notin complete_proposals
        /\ proposal = Null
        /\ proposal.type = Proposing
        /\ message.invite_id = proposal.invite_id
        /\ complete_proposals' = complete_proposals \union { message.invite_id }
        /\ UNCHANGED <<messages, rng_state, group_perceptions, proposal, approver_states>>

LeaderReceiveEstablished ==
    /\ proposal /= Null
    /\ proposal.type = Proposing
    /\ \E message \in messages :
        /\ message.type = Established
        /\ message.recipient = Leader
        /\ message.sender \in group_perceptions[Leader]
        /\ message.invite_id = proposal.invite_id
        /\ IF  proposal.awaiting_response = { message.sender }
           THEN
               /\ proposal' = Null
               /\ complete_proposals' = complete_proposals \union { proposal.invite_id }
           ELSE
               /\ proposal' = [ proposal EXCEPT !.awaiting_response = { member \in @ : member /= message.sender } ]
               /\ UNCHANGED <<complete_proposals>>
        /\ UNCHANGED <<messages, rng_state, group_perceptions, approver_states>>

\* We don't specify exactly why, but at some point a Leader may realize that a
\* proposal is likely doomed, even if they never heard a rejection (proposals
\* with confused contacts can never resolve), and abort the proposal.
GiveUpOnProposal ==
    /\ proposal /= Null
    /\ proposal.type = Proposing
    \* If no one has established, then the leader may assume there's been
    \* confusion and will end the proposal, kicking the invitee if they did in
    \* fact join.  If some members have established, but _not_ the leader, the
    \* leader may assume the invitee is being weird and kick them.
    \* TODO: Should we always start with kicking the invitee and then only
    \* start kicking current members if they then are nonresponsive to the
    \* invitee kick?
    /\ \/ proposal.awaiting_response = group_perceptions[Leader]
       \/ Leader \in proposal.awaiting_response
    /\ complete_proposals' = complete_proposals \union { proposal.invite_id }
    /\ proposal' =
        [ type |-> Kicking
        , awaiting_response |-> { member \in group_perceptions[Leader] : member.id /= proposal.invite_id }
        , kicked |-> { proposal.invite_id }
        ]
    /\ UNCHANGED <<messages, rng_state, group_perceptions, approver_states>>

\* We don't specify exactly why, but this would likely be a human decision
\* based on trying to start a _new_ proposal, but that not being possible
\* because of a pending one.  It would then show the users that are not
\* cooperating and would offer to kick them.  It's also possible that when
\* trying to kick someone, another member goes unresponsive, meaning we need to
\* start a new round of kicking.
GiveUpOnMembers ==
    /\ proposal /= Null
    /\ \/ /\ proposal.type = Proposing
          /\ complete_proposals' = complete_proposals \union { proposal.invite_id }
       \/ /\ proposal.type = Kicking
          /\ UNCHANGED <<complete_proposals>>
    \* Leader can't be kicked.  Realistically, they should just act
    \* synchronously when giving up on a proposal.
    /\ Leader \notin proposal.awaiting_response
    /\ LET new_group == group_perceptions[Leader] \ proposal.awaiting_response
       IN
        /\ proposal' =
            [ type |-> Kicking
            , awaiting_response |-> new_group
            , kicked |-> { member.id : member \in proposal.awaiting_response }
            ]
        /\ UNCHANGED <<messages, rng_state, group_perceptions, approver_states>>

LeaderDetectLeaver ==
    \* If there is an existing proposal, expelling leavers is done via GiveUp
    \* actions.
    /\ proposal = Null
    /\ \E member \in group_perceptions[Leader] :
        /\ Leader \notin group_perceptions[member]
        /\ LET new_group == group_perceptions[Leader] \ { member }
           IN
            /\ proposal' =
                [ type |-> Kicking
                , awaiting_response |-> new_group
                , kicked |-> { member.id }
                ]
            /\ UNCHANGED <<messages, rng_state, group_perceptions, complete_proposals, approver_states>>

LeaderReceiveKickAck ==
    /\ proposal /= Null
    /\ proposal.type = Kicking
    /\ \E message \in messages :
        /\ message.type = KickAck
        /\ message.recipient = Leader
        /\ message.sender \in group_perceptions[Leader]
        /\ message.kicked = proposal.kicked
        /\ IF   proposal.awaiting_response = { message.sender }
           THEN proposal' = Null
           ELSE proposal' = [ proposal EXCEPT !.awaiting_response = { member \in @ : member /= message.sender } ]
        /\ UNCHANGED <<messages, rng_state, group_perceptions, complete_proposals, approver_states>>

ApproverReceiveProposal(recipient) ==
    \E message \in messages :
        /\ message.recipient = recipient
        /\ message.type = Propose
        \* Only Propose messages from the Leader are considered
        /\ message.sender = PerceivedLeader(message.recipient)
        /\ LET PerceivedUser == UserPerceptions[[ perceiver |-> message.recipient.user, description |-> message.invitee_description]]
               ApproverState == approver_states[<<message.invite_id, message.recipient.user>>]

           IN  CASE ApproverState.state = Null ->
                   IF
                       /\ PerceivedUser /= Null
                       /\ HasDirectConnection(message.recipient.user, PerceivedUser)
                       /\ \A members \in group_perceptions[message.recipient] :
                           /\ members.user /= PerceivedUser
                   THEN
                       /\ approver_states' =
                           [ approver_states EXCEPT ![<<message.invite_id, message.recipient.user>>] =
                               [ state |->
                                   IF   Cardinality(group_perceptions[message.recipient]) = 1
                                   THEN Inviting \* TODO: And send invite
                                   ELSE Synchronizing
                               , for_group |-> message.recipient.id
                               , user |-> PerceivedUser
                               , inviters |-> group_perceptions[message.recipient]
                               , shares_by_member |->
                                   \* Secret splitting is abstract/symbolic.
                                   \* Instead of the actual value of our share
                                   \* or secret, we ensure that that we can
                                   \* uniquely identify what (abstract) secret
                                   \* this divides, give each share an
                                   \* identifier, and supply all the
                                   \* identifiers of the shares.  That means
                                   \* that 1:1 mapping of a secret to an
                                   \* invite_id is implied, and it means that
                                   \* approvers must permanently commit to the
                                   \* choice of the secret and splits before
                                   \* sharing them.  It is critical that this
                                   \* spec treats a secret/shares as opaque,
                                   \* passing around only their value, not
                                   \* looking inside, or "generating" on in the
                                   \* wrong context, or our spec may be
                                   \* impossible to implement.
                                   [ member \in MemberSet |->
                                       IF  member = message.recipient
                                       THEN
                                           [ share_id |-> message.recipient
                                           , share_ids |-> group_perceptions[message.recipient]
                                           , secret |-> [ by |-> message.recipient.user, for |-> message.invite_id ]
                                           ]
                                       ELSE
                                           Null
                                   ]
                               ]
                           ]
                       /\ UNCHANGED <<messages, rng_state, group_perceptions, proposal, complete_proposals>>
                   ELSE
                       \* The Leader hasn't deleted their receiving queue
                       /\ message.recipient \in group_perceptions[message.sender]
                       /\ AddMessage(
                               [ sender |-> message.recipient
                               , recipient |-> message.sender
                               , type |-> Reject
                               , invite_id |-> message.invite_id
                               ]
                           )
                       /\ UNCHANGED <<rng_state, group_perceptions, proposal, complete_proposals, approver_states>>
                 [] ApproverState.state = Synchronizing ->
                   \* The approver sends a share to someone _it_ needs a share
                   \* from, this then prompts it to ack with its own share. In
                   \* reality, we notify everyone, not just a single member.
                   \* However this is a bit troublesome when modeling liveness
                   \* because it means we need to allow for many many in-flight
                   \* messages to see all the interesting scenarios.  This
                   \* still captures the "message everyone" behavior, just in a
                   \* roundabout way, where it "receives" a single proposal
                   \* message for each member it needs to notify.
                   \E to \in ApproverState.inviters \ { member \in DOMAIN ApproverState.shares_by_member : ApproverState.shares_by_member[member] /= Null } :
                       \* The intended recipient hasn't deleted their receiving queue
                       /\ message.recipient \in group_perceptions[to]
                       /\ AddMessage(
                               [ sender |-> message.recipient
                               , recipient |-> to
                               , type |-> SyncShare
                               \* Note again that shares are abstract/symbolic.
                               \* In this context, the spec implies that this
                               \* is using a pregenerated share that is
                               \* permanently associated for this invite_id.
                               , share |->
                                   [ share_id |-> to
                                   , share_ids |-> ApproverState.inviters
                                   , secret |-> [ by |-> message.recipient.user, for |-> message.invite_id ]
                                   ]
                               , invite_id |-> message.invite_id
                               \* If we have received their share, ack receipt
                               \* and send our share simultaneously.
                               , ack |-> approver_states[<<message.invite_id, message.recipient.user>>].shares_by_member[to] /= Null
                               ]
                          )
                       /\ UNCHANGED <<rng_state, group_perceptions, proposal, complete_proposals, approver_states>>
                 [] ApproverState.state = Inviting ->
                   /\ LET
                          SharesByMember ==
                              ApproverState.shares_by_member

                          MembersWhoShared ==
                              { member \in DOMAIN SharesByMember : SharesByMember[member] /= Null }

                          CollectedShares ==
                              { SharesByMember[member] : member \in MembersWhoShared }

                          OriginallyGeneratedShares ==
                              {   [ share_id |-> member
                                  , share_ids |-> ApproverState.inviters
                                  , secret |-> [ for |-> message.invite_id, by |-> message.recipient.user ]
                                  ]
                              :   member \in ApproverState.inviters
                              }

                      IN  AddMessage(
                           [ type |-> Invite
                           , sender |-> message.recipient.user
                           , recipient |-> ApproverState.user
                           , invite_id |-> message.invite_id
                           , shares |-> CollectedShares
                           , expect_shares |-> { Hash(share) : share \in OriginallyGeneratedShares }
                           , enc_group_info |->
                               Encrypt(
                                   [ by |-> message.recipient.user, for |-> message.invite_id ],
                                   {   [ member_id |-> member.id
                                       , share |-> SharesByMember[member]
                                       ]
                                   :   member \in MembersWhoShared
                                   }
                               )
                           ]
                      )
                   /\ UNCHANGED <<rng_state, group_perceptions, proposal, complete_proposals, approver_states>>
                 [] ApproverState.state = Committed ->
                   \* The Leader hasn't deleted their receiving queue
                   /\ message.recipient \in group_perceptions[message.sender]
                   /\ AddMessage(
                           [ type |-> Established
                           , sender |-> message.recipient
                           , recipient |-> message.sender
                           , invite_id |-> message.invite_id
                           ]
                      )
                   /\ UNCHANGED <<rng_state, group_perceptions, proposal, complete_proposals, approver_states>>

ApproverReceiveKick(recipient, kicked) ==
    \E message \in messages :
        /\ message.type = Kick
        /\ message.recipient = recipient
        /\ message.kicked = kicked
        \* Only Kick messages from the Leader are considered
        /\ message.sender = PerceivedLeader(message.recipient)
        /\ group_perceptions' = [ group_perceptions EXCEPT ![message.recipient] = { member \in @ : member.id \notin message.kicked } ]
        /\ approver_states' =
            FoldSet(
              LAMBDA next, prev :
                  [ prev EXCEPT ![<<next, message.recipient.user>>] = [ state |-> Committed ] ],
              approver_states,
              message.kicked
            )
        /\ FilterThenMaybeAddMessage(
               IF
                   \* The Leader hasn't deleted their queue
                   message.recipient \in group_perceptions[PerceivedLeader(message.recipient)]
               THEN
                   PureMaybe(
                       [ type |-> KickAck
                       , sender |-> message.recipient
                       , recipient |-> message.sender
                       , kicked |-> message.kicked
                       ]
                   )
               ELSE
                   Nothing,
               \* By destroying the queues, all the messages are lost
               LAMBDA m :
                   ~/\ m.type /= Invite
                    /\ m.sender.id \in message.kicked
                    /\ m.recipient = message.recipient
           )
        /\ UNCHANGED <<rng_state, proposal, complete_proposals>>

ReceiveSyncShare(recipient) ==
    \E message \in messages :
        /\ message.type = SyncShare
        /\ message.recipient = recipient
        /\ message.sender \in group_perceptions[message.recipient] \* Should be invariant
        \* We would never expect to see a valid request in a Committed state,
        \* because we know that if we ever move to committed, that the invitee
        \* received _all_ Invite messages, which are only sent from members
        \* who have recieved all shares.
        /\ approver_states[<<message.invite_id, message.recipient.user>>].state \in { Synchronizing, Inviting }
        /\ CASE approver_states[<<message.invite_id, message.recipient.user>>].state = Synchronizing ->
                /\ approver_states[<<message.invite_id, message.recipient.user>>].for_group = message.recipient.id \* Should be invariant
                /\ LET  NextApproverState ==
                            [ approver_states[<<message.invite_id, message.recipient.user>>]
                            EXCEPT !.shares_by_member = [ @ EXCEPT ![message.sender] = message.share ]
                            ]
                        MembersWhoShared == { member \in DOMAIN NextApproverState.shares_by_member : NextApproverState.shares_by_member[member] /= Null }
                        CollectedShares == { NextApproverState.shares_by_member[member] : member \in MembersWhoShared }
                        GroupInfo ==
                            {   [ member_id |-> member.id
                                , share |-> NextApproverState.shares_by_member[member]
                                ]
                            :   member \in MembersWhoShared
                            }
                        OriginallyGeneratedShares ==
                            {   [ share_id |-> member
                                , share_ids |-> NextApproverState.inviters
                                , secret |-> [ for |-> message.invite_id, by |-> message.recipient.user ]
                                ]
                            :   member \in NextApproverState.inviters
                            }

                   IN   IF  Cardinality(MembersWhoShared) = Cardinality(NextApproverState.inviters)
                        THEN
                            \* This was the final share, so we invite the new
                            \* user
                            /\ approver_states' =
                                [ approver_states EXCEPT ![<<message.invite_id, message.recipient.user>>] =
                                    [ NextApproverState EXCEPT !.state = Inviting ]
                                ]
                            /\ AddMessage(
                                   [ sender |-> message.recipient.user
                                   , recipient |-> NextApproverState.user
                                   , type |-> Invite
                                   , invite_id |-> message.invite_id
                                   , shares |-> CollectedShares
                                   , expect_shares |-> { Hash(share) : share \in OriginallyGeneratedShares }
                                   , enc_group_info |->
                                        Encrypt(
                                            [ by |-> message.recipient.user, for |-> message.invite_id ],
                                            GroupInfo
                                        )
                                   ]
                               )
                            /\ UNCHANGED <<group_perceptions>>
                        ELSE
                            /\ approver_states' =
                                [ approver_states EXCEPT ![<<message.invite_id, message.recipient.user>>] = NextApproverState ]
                            /\ IF   \/ message.ack
                                    \* Original sender has deleted their
                                    \* receiving queue
                                    \/ message.recipient \notin group_perceptions[message.sender]
                               THEN UNCHANGED <<messages>>
                               ELSE AddMessage(
                                        [ sender |-> message.recipient
                                        , recipient |-> message.sender
                                        , type |-> SyncShare
                                        \* Note again that shares are
                                        \* abstract/symbolic.  In this context,
                                        \* the spec implies that this is using
                                        \* a pregenerated share that is
                                        \* permanently associated for this
                                        \* invite_id.
                                        , share |->
                                            [ share_id |-> message.sender
                                            , share_ids |-> NextApproverState.inviters
                                            , secret |-> [ by |-> message.recipient.user, for |-> message.invite_id ]
                                            ]
                                        , invite_id |-> message.invite_id
                                        , ack |-> TRUE
                                        ]
                                    )
                            /\ UNCHANGED <<group_perceptions>>
             [] approver_states[<<message.invite_id, message.recipient.user>>].state = Inviting ->
                 /\ IF   \/ message.ack
                         \* Original sender has deleted their receiving queue
                         \/ message.recipient \notin group_perceptions[message.sender]
                    THEN UNCHANGED <<messages>>
                    ELSE AddMessage(
                             [ sender |-> message.recipient
                             , recipient |-> message.sender
                             , type |-> SyncShare
                             \* Note again that shares are abstract/symbolic.
                             \* In this context, the spec implies that this is
                             \* using a pregenerated share that is permanently
                             \* associated for this invite_id.
                             , share |->
                                 [ share_id |-> message.sender
                                 , share_ids |-> approver_states[<<message.invite_id, message.recipient.user>>].inviters
                                 , secret |-> [ by |-> message.recipient.user, for |-> message.invite_id ]
                                 ]
                             , invite_id |-> message.invite_id
                             , ack |-> TRUE
                             ]
                         )
                 /\ UNCHANGED <<group_perceptions, approver_states>>
        /\ UNCHANGED <<rng_state, proposal, complete_proposals>>

ApproverConfirmQueue(member) ==
    \E approver \in MemberSet, invite_id \in InviteIds :
        LET
            Invitee == approver_states[<<invite_id, approver.user>>].user
            InviteeMember == [ id |-> invite_id, user |-> Invitee ]
        IN
            /\ approver = member
            /\ approver_states[<<invite_id, approver.user>>].state = Inviting
            \* The approver won't confirm the queue if they've left the group
            /\ approver.id /= Null => approver_states[<<approver.id, approver.user>>].state /= Left
            \* There are two independent steps, which we handle with the same
            \* action, but not atomically.  The first is to establish a queue to
            \* receive group messages from the invitee.  The second is to notice
            \* the first step is done and record the invitee as committed to the
            \* group and inform the Leader.
            /\ IF   approver \notin group_perceptions[InviteeMember]
               THEN \* The approver can only confirm its queue if the invitee
                    \* already has, by which to use as an out-of-band channel.
                    /\ InviteeMember \in group_perceptions[approver]
                    \* The Invitee hasn't left the group (otherwise they won't
                    \* create, send out-of-band message, or secure the queue)
                    /\ approver_states[<<invite_id, Invitee>>].state /= Left
                    \* The Invitee does not believe this approver has been
                    \* kicked (Leaders can't be).
                    /\ approver.id /= Null => approver_states[<<approver.id, Invitee>>].state /= Committed
                    /\ group_perceptions' = [ group_perceptions EXCEPT ![InviteeMember] = @ \union { approver } ]
                    \* The Invitee considers themselves Joined once they've
                    \* secured all queues.  Though, this can't actually be
                    \* atomic with the final step.
                    /\ approver_states' =
                        IF  /\ approver_states[<<invite_id, Invitee>>].state = Joining
                            /\ approver_states[<<invite_id, Invitee>>].members = group_perceptions'[InviteeMember]
                        THEN
                            [ approver_states EXCEPT ![<<invite_id, Invitee>>] = [ state |-> Joined ] ]
                        ELSE
                            approver_states
                    /\ UNCHANGED <<messages>>
               ELSE /\ approver_states' = [ approver_states EXCEPT ![<<invite_id, approver.user>>] = [ state |-> Committed ] ]
                    /\ IF
                           \* Only send if the Leader hasn't deleted their
                           \* receiving queue
                           approver \in group_perceptions[PerceivedLeader(approver)]
                       THEN
                           AddMessage(
                               [ type |-> Established
                               , sender |-> approver
                               , recipient |-> PerceivedLeader(approver)
                               , invite_id |-> invite_id
                               ]
                           )
                       ELSE
                           UNCHANGED <<messages>>
                    /\ UNCHANGED <<group_perceptions>>
            /\ UNCHANGED <<rng_state, proposal, complete_proposals>>

LeaveGroup ==
    \E user \in Users, invite_id \in InviteIds :
        LET
            \* Note that the Leader cannot leave a group.  They instead kick
            \* everyone and then delete their local record of it.
            member == [ id |-> invite_id, user |-> user ]
        IN
            /\ approver_states[<<invite_id, user>>].state \in { Invited, Joining, Joined }
            \* In reality, we need a Leaving state where queues are deleted one at
            \* a time.  We gloss over that detail here.  The most important
            \* thing is that they delete the Leader's queue first, since the
            \* Leader will handle the rest of the clean up, even if the leaver
            \* doesn't.
            /\ approver_states' = [ approver_states EXCEPT ![<<invite_id, user>>] = [ state |-> Left ] ]
            /\ group_perceptions' = [ group_perceptions EXCEPT ![member] = {} ]
            \* All messages are lost when we delete the queues
            /\ FilterThenMaybeAddMessage(Nothing, LAMBDA message : ~(message.type /= Invite /\ message.recipient = member))
            /\ UNCHANGED <<rng_state, proposal, complete_proposals>>


\* Map each user
\*   Turn each expected share hash into the actual share
\*   Recover the secret from the shares
\*   Decrypt the group info
\*   For each info [ share, id ]
\*     Determine the user that generated the share
\*   Map group_info into a member set
\* Ensure all users provided the same member set
ConvertSharesToMembers(user_shares) ==
    LET
        \* The set of all shares received, ignoring who created or sent them
        AllShares == UNION { x.shares : x \in user_shares }

        UserProvidedMembersM ==
            TraverseSetMaybe(
                LAMBDA x :
                    LET
                        \* We need to find each share original split by
                        \* this member, where each member sent one share
                        OriginalSharesM ==
                            TraverseSetMaybe(
                                LAMBDA hash : FindSet(LAMBDA share : Hash(share) = hash, AllShares),
                                x.expect_shares
                            )

                        \* With the original shares, we can now combine
                        \* them to get the original secret (the decryption
                        \* key)
                        SecretM ==
                            BindMaybe(Combine, OriginalSharesM)

                        \* With the decryption key, we can now decrypt the
                        \* group info sent by the user
                        GroupInfoM ==
                            BindMaybe(LAMBDA secret : Decrypt(secret, x.enc_group_info), SecretM)

                    IN
                        \* group_info is a set of [ member_id, share ]
                        \* where the member_id is the group's identifier
                        \* for the user who originally created this share.
                        \* At this point, group info is still unique per
                        \* user, since they all know a different set of
                        \* shares by which to correlate ids.
                        BindMaybe(LAMBDA group_info :
                            TraverseSetMaybe(LAMBDA y :
                                MapMaybe(
                                    LAMBDA z : [ user |-> z.user, id |-> y.member_id ],
                                    FindSet(
                                        LAMBDA z : Hash(y.share) \in z.expect_shares,
                                        user_shares
                                    )
                                ),
                                group_info
                            ),
                            GroupInfoM
                        ),
                user_shares
            )
    IN
        BindMaybe(
            LAMBDA user_provided_members :
                IF  Cardinality(user_provided_members) = 1
                THEN
                    FindSet(LAMBDA x : TRUE, user_provided_members)
                ELSE
                    Nothing,
            UserProvidedMembersM
        )

UserReceiveInvite(sender) ==
    \E message \in messages :
        /\ message.type = Invite
        /\ message.sender = sender
        /\ LET
            ApproverState ==
                approver_states[<<message.invite_id, message.recipient>>]

            InviteeMember ==
                [ user |-> message.recipient, id |-> message.invite_id ]
           IN
            /\ ApproverState.state \in { Null, Invited, Joining }
            /\ CASE ApproverState.state \in { Null, Invited } ->
                    LET
                        BeforeNewInviteData ==
                            IF  ApproverState.state = Null
                            THEN
                                [ state |-> Invited, user_shares |-> {} ]
                            ELSE
                                ApproverState

                        NewInviteData ==
                            [ user |-> message.sender
                            , shares |-> message.shares
                            , expect_shares |-> message.expect_shares
                            , enc_group_info |-> message.enc_group_info
                            ]

                        WithNewInviteData ==
                            [ BeforeNewInviteData EXCEPT !.user_shares = @ \union { NewInviteData } ]

                        MembersM ==
                            ConvertSharesToMembers(WithNewInviteData.user_shares)

                        NextApproverState ==
                            IF  MembersM.is_just
                            THEN
                                \* Realistically, when we switch to Joining, we
                                \* would attempt to confirm all queues
                                \* immediately.  Each invite acts as a retry,
                                \* in case of a crash mid-confirm, so we just
                                \* depend on that here alone in the spec.
                                [ state |-> Joining, members |-> MembersM.just ]
                            ELSE
                                WithNewInviteData
                    IN
                        /\ approver_states' =
                            [ approver_states EXCEPT ![<<message.invite_id, message.recipient>>] = NextApproverState ]
                        /\ UNCHANGED <<messages, rng_state, group_perceptions, proposal, complete_proposals>>
                [] ApproverState.state = Joining ->
                    \E member \in ApproverState.members :
                       /\ member.user = message.sender
                       \* If the member isn't still Inviting, then they won't
                       \* ever secure the queue to complete it
                       \* TODO: it might be worth modeling this non-atomically.
                       /\ approver_states[<<message.invite_id, message.sender>>].state = Inviting
                       \* The member will only participate with queue setup if
                       \* they still believe themselves to be part of the
                       \* group.
                       /\ member.id /= Null => approver_states[<<member.id, message.sender>>].state = Joined
                       /\ group_perceptions' =
                           [ group_perceptions
                           EXCEPT ![member] = @ \union { InviteeMember }
                           ,      ![InviteeMember] = @ \union { InviteeMember }
                           ]
                       /\ UNCHANGED <<messages, rng_state, proposal, complete_proposals, approver_states>>


\* TODO: Need to be able to Kick outside of failed proposals.

(*
TODO: Byzantine Modeling
This is actually a bit easier than it at first seems.  First, we consider only
one attacker (or a group of fully coordinated attackers), since this is
obviously worse than multiple independent attackers). We should add two
constants, CompromisedConnections and CompromisedUsers.  The former represents
the Connections with a MITM, where the attacker may see all messages, block
legitimate messages, or fabricate messages from either user that look
authentic.  The latter represents a user that is either working with or
corrupted by the attacker, where the attacker may see all messages, drop
legitimate messages, or fabricate messages from the compromised user.

In order to fabricate messages, the attacker assembles messages from various
pieces of any message it can see.  It can also generate a set of random values
that it can use at any time.  Blocked messages must be moved to a special
variable so that the attacker can "remember" their composite parts, even though
they prevent their transmition.

In order to represent an attacker setting up new MITM connections, we need to
model the connection process more explicitly.

Invariants become a bit more challenging.  For example, "all parties should
agree to new members."  A corrupted member doesn't really have a choice in the
matter.  We potentially need a new variable as to whether or not a user trusts
the group, where a user that sees evidence of tampering can stop all further
participation.  In this case we need to ensure that users always trust valid
groups, but ideally they always detect when tampering has occurred (notably,
silent agents are undetectable!).
*)


Next ==
    \/ DropMessage
    \/ DropAllMessages
    \/ SendPleasePropose
    \/ LeaderReceivePleasePropose
    \/ \E member \in MemberSet : BroadcastProposalState(member)
    \/ LeaderReceiveReject
    \/ LeaderReceiveEstablished
    \/ GiveUpOnProposal
    \/ GiveUpOnMembers
    \/ LeaderDetectLeaver
    \/ LeaderReceiveKickAck
    \/ \E member \in MemberSet : ApproverReceiveProposal(member)
    \/ \E member \in MemberSet, kicked \in SUBSET InviteIds : ApproverReceiveKick(member, kicked)
    \/ \E member \in MemberSet : ReceiveSyncShare(member)
    \/ \E member \in MemberSet : ApproverConfirmQueue(member)
    \/ LeaveGroup
    \/ \E user \in Users : UserReceiveInvite(user)

AllVars ==
    <<messages, rng_state, group_perceptions, proposal, complete_proposals, approver_states>>

Fairness ==
    \* Fairness for DropAllMessages is really a proxy for fairness for dropping
    \* each individual message.  That more ideal description essentially says
    \* that no message is visible on the network forever without explicitly
    \* being resent.  However, that many disjoint fairness conditions is just
    \* too large to check.  We instead say approximate this behavior by saying
    \* that all in-flight messages must be dumped eventually (preventing loops
    \* where one message causes another that is immediately dropped).
    /\ WF_AllVars(DropAllMessages)
    /\ SF_AllVars(LeaderReceivePleasePropose)
    /\ \A member \in MemberSet :
        WF_AllVars(BroadcastProposalState(member))
    /\ SF_AllVars(LeaderReceiveReject)
    /\ SF_AllVars(LeaderReceiveEstablished)
    /\ WF_AllVars(GiveUpOnProposal)
    /\ WF_AllVars(GiveUpOnMembers)
    /\ WF_AllVars(LeaderDetectLeaver)
    /\ SF_AllVars(LeaderReceiveKickAck)
    /\ SF_AllVars(ApproverReceiveProposal(Leader))
    /\ \A kicked \in SUBSET InviteIds :
        SF_AllVars(ApproverReceiveKick(Leader, kicked))


Spec == Init /\ [][Next]_AllVars /\ Fairness

SymKeys == [ for : InviteIds, by : Users ]

InviteShares == Shares(MemberSet, SymKeys)

TypeOk ==
    approver_states \in [ InviteIds \X Users ->
        [ state : { Invited }
        , user_shares : SUBSET
            [ user : Users
            , shares : SUBSET InviteShares
            , expect_shares : SUBSET Hashed(InviteShares)
            , enc_group_info : Encrypted(SymKeys, SUBSET [ member_id : InviteIds \union { Null }, share : InviteShares ])
            ]
        ]
        \union
        [ state : { Joining }
        , members : SUBSET MemberSet
        ]
        \union
        [ state : { Inviting, Synchronizing }
        , for_group : InviteIds \union { Null }
        , user : Users
        , inviters : SUBSET MemberSet
        , shares_by_member : [ MemberSet -> (InviteShares \union { Null }) ]
        ]
        \union
        [ state : { Null, Committed, Joined, Left }
        ]
    ]

IdsAreUnique ==
    \A member \in MemberSet :
        \/ group_perceptions[member] = {}
        \/ Cardinality({ m.id : m \in group_perceptions[member] }) = Cardinality(group_perceptions[member])

IdsAndLeaderMatchAcrossAllMembers ==
    \A invite_id \in (InviteIds \union { Null }), member1, member2 \in MemberSet :
        LET matches1 == { member \in group_perceptions[member1] : member.id = invite_id }
            matches2 == { member \in group_perceptions[member2] : member.id = invite_id }
        IN  \/ matches1 = {}
            \/ matches2 = {}
            \/ matches1 = matches2

GroupPerceptionsEmptyInCertainStates ==
    \A invite_id \in InviteIds, user \in Users :
        approver_states[<<invite_id, user>>].state \in { Nothing, Invited, Left } =>
            group_perceptions[[ id |-> invite_id, user |-> user ]] = {}

CannotCommunicateWithoutAConnection ==
    \A message \in messages :
        IF   message.type \in { Invite }
        THEN HasDirectConnection(message.sender, message.recipient)
        ELSE /\ HasDirectConnection(message.sender.user, message.recipient.user)
             /\ message.sender \in group_perceptions[message.recipient]

GroupSizesMatch ==
    \A message1, message2 \in messages :
        /\ message1.type = Invite
        /\ message2.type = Invite
        /\ message1.invite_id = message2.invite_id
        => /\ Cardinality(message1.expect_shares) = Cardinality(message2.expect_shares)
           /\ Cardinality(message1.shares) = Cardinality(message2.shares)
           /\ Cardinality(message1.expect_shares) = Cardinality(message1.shares)

SyncXorReject ==
    \A message1, message2 \in messages :
        /\ message1 /= message2
        /\ message1.sender = message2.sender
        /\ message1.type = Reject
        /\ message2.type = SyncShare
        => message1.invite_id = message2.invite_id

\* Anyone that receives two invites which share an invite id, knows that
\* these two contacts know each other and that they are in a group together
\* with N people.
\* This kind of correlation should only be possible if the invite senders
\* agreed to invite that individual, meaning that they are willing to divulge
\* to the invitee that they know all members of the group.  We check this by
\* seeing if they have a state for the invite_id that brought.
NonMembersOnlyKnowMembersKnowEachOtherIfMembersAcceptedProposal ==
    \A message1, message2 \in messages :
        \/ message1 = message2
        \/  /\ message1.type = Invite
            /\ message2.type = Invite
            /\ message1.recipient = message2.recipient
            /\ message1.invite_id = message2.invite_id
            =>  /\ approver_states[<<message1.invite_id, message1.sender>>].state /= Null
                /\ approver_states[<<message2.invite_id, message2.sender>>].state /= Null

AllMembersUsers ==
    UNION { { member.user : member \in group_perceptions[x] } : x \in MemberSet }

MembersOnlyEstablishWithInvitee ==
    \A member_user \in (AllMembersUsers \ { member.user : member \in InitialMembers }) :
        \E message \in messages :
            /\ message.type = Propose
            /\ message.invitee_description.of = member_user

AllMembersAreConnectedToAllOtherMembers ==
    \A member1 \in MemberSet :
        \A member2 \in group_perceptions[member1] :
            HasDirectConnection(member1.user, member2.user)

EstablishedOnlyIfAllPerceptionsMatch ==
    \A member_user \in AllMembersUsers :
        \A message \in messages :
            /\ message.type = Propose
            /\ message.invitee_description.of = member_user
            => UserPerceptions[[ perceiver |-> message.recipient.user, description |-> message.invitee_description ]] = member_user

AllMembersEventuallyAgreeWithLeader ==
    []<>/\ proposal = Null
        /\ \A member \in group_perceptions[Leader] :
            group_perceptions[member] = group_perceptions[Leader]

NetworkActivitySettles ==
    <>[](messages = {})
====
