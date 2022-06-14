---- MODULE groups_mc ----

EXTENDS groups

CONSTANTS a, b, c, d

MaxNum == 6

NatMC == 0..MaxNum

\* No one is ever confused
UserPerceptionsMC ==
  [ x \in Users \X Users \X Users |-> x[3] ]

\* Everyone knows everyone
ConnectionsMC ==
  [ x \in Users |-> { a, b, c, d } ]

SizeConstraint == rng_state < MaxNum + 1

====
