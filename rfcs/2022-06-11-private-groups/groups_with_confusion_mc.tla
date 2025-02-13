---- MODULE groups_with_confusion_mc ----

EXTENDS groups

CONSTANTS a, b, c, d

InviteIdsMC == 0..1

\* b has confused c and d, so when a describes either of them, b thinks that a
\* is talking about the other.  This also means that when b describes one, it
\* does so in a way that is backwards to a.
UserPerceptionsMC ==
  [ [ x \in [ perceiver : Users, description : [ by : Users, of : Users ] ] |-> x.description.of ]
  EXCEPT ![[ perceiver |-> b, description |-> [ by |-> a, of |-> c] ]] = d
  , ![[ perceiver |-> b, description |-> [ by |-> a, of |-> d] ]] = c
  , ![[ perceiver |-> a, description |-> [ by |-> b, of |-> c] ]] = d
  , ![[ perceiver |-> a, description |-> [ by |-> b, of |-> d] ]] = c
  ]

\* Everyone knows everyone
ConnectionsMC ==
  Users \X Users

InitialMembersMC ==
  { [ id |-> Null, user |-> a ] }

\* If b ever joins the group, c and d cannot, because b confuses them.
CantAddCOrDIfBJoins ==
   /\ []((c \notin group_perceptions[Leader] /\ b \in group_perceptions[Leader]) => [](c \notin group_perceptions[Leader]))
   /\ []((d \notin group_perceptions[Leader] /\ b \in group_perceptions[Leader]) => [](d \notin group_perceptions[Leader]))

====
