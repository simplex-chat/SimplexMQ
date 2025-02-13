---- MODULE groups_double_kick_mc ----

\* Considers the scenario where a proposal that turns into a Kick may itself
\* turn into yet another Kick, if any of members who were supposed to remain
\* then do not ack the kick.

EXTENDS groups

CONSTANTS a, b, c, d

InviteIdsMC == 0..2

\* No one is ever confused
UserPerceptionsMC ==
  [ x \in [ perceiver : Users, description : [ by : Users, of : Users ] ] |-> x.description.of ]

\* Everyone knows everyone, except c does not know d, preventing them from
\* joining the group.  They are either rejected or c becomes unresponsive and
\* is kicked (and allowing only one proposal prevents addition after kicks.
ConnectionsMC ==
  (Users \X Users) \ { <<c, d>>, <<d, c>> }

InitialMembersMC ==
  { [ id |-> Null, user |-> a ]
  , [ id |-> 0, user |-> b ]
  , [ id |-> 1, user |-> c ]
  }

====
