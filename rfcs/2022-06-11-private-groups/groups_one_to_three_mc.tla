---- MODULE groups_one_to_three_mc ----

\* In this model, we start with a newly created group (of one) and we consider
\* the possible the addition of two others (in either order), all of whom know
\* each other.

EXTENDS groups

CONSTANTS a, b, c

InviteIdsMC == 0..1

\* No one is ever confused
UserPerceptionsMC ==
  [ x \in [ perceiver : Users, description : [ by : Users, of : Users ] ] |-> x.description.of ]

\* Everyone knows everyone
ConnectionsMC ==
  Users \X Users

InitialMembersMC ==
  { [ id |-> Null, user |-> a ] }

====
