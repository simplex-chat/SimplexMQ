---- MODULE groups_mc ----

\* Our most basic model considers a group of two people who both know of two
\* others (that also know each other), and enough random numbers ar available
\* to invite either one of them.

EXTENDS groups

CONSTANTS a, b, c, d

InviteIdsMC == 0..1

\* No one is ever confused
UserPerceptionsMC ==
  [ x \in [ perceiver : Users, description : [ by : Users, of : Users ] ] |-> x.description.of ]

\* Everyone knows everyone
ConnectionsMC ==
  Users \X Users

InitialMembersMC ==
  { [ id |-> Null, user |-> a ]
  , [ id |-> 0, user |-> b ]
  }

====
